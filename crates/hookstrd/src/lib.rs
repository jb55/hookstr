//! The ingest service half of hookstrd, as a library so the e2e tests can run
//! the real handler in-process. `main.rs` is config loading + wiring only.

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Html,
    routing::{get, post},
};
use futures_util::StreamExt;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use nostrdb::{Filter, Ndb, SubscriptionStream};
use nostrdb_net::relay::server::AuthConfig;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// How long ingest waits for nostrdb's background writer to commit a note
/// before giving up and telling the provider to retry.
const INGEST_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct Ingest {
    pub ndb: Ndb,
    pub seckey: [u8; 32],
    /// Per-provider secrets baked into the URL registered with each provider:
    /// `{provider} -> token`. The token alone authenticates a delivery and
    /// names its provider, so a leaked token can only forge events tagged
    /// with its own provider and can be revoked without disturbing the
    /// others. Values must be unique (config load enforces this) or the
    /// token -> provider lookup would be ambiguous.
    pub tokens: HashMap<String, String>,
}

/// A static info page for humans who open the service in a browser. hookstrd
/// serves the relay and this page on one port, telling a websocket upgrade on
/// `/` apart from a plain browser `GET /` itself (see [`serve`]).
const LANDING: &str = include_str!("landing.html");

/// The detailed setup/usage docs, linked from the landing page. Served at
/// `/docs.html` so the relative links between the two pages resolve the same
/// way whether hookstrd serves them or GitHub Pages does.
const DOCS: &str = include_str!("docs.html");

/// The ingest router: `GET /` -> landing page, `GET /docs.html` -> setup docs,
/// `POST /ingest/{token}/{any/path}` -> durable 204.
pub fn router(ingest: Ingest) -> Router {
    Router::new()
        .route("/", get(landing))
        .route("/docs.html", get(docs))
        .route("/ingest/{token}/{*path}", post(receive))
        .with_state(ingest)
}

/// How long to wait for a new connection's HTTP header block before giving up on
/// classifying it. In practice the whole block is in the first segment; this
/// only bounds a pathologically slow (or non-HTTP) peer.
const HEADER_PEEK_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap on the leading bytes we peek looking for the header terminator.
const HEADER_PEEK_MAX: usize = 4096;

/// Serve the authenticated nostr relay (websocket) and the webhook ingest HTTP
/// API on a **single** `listener`.
///
/// hookstrd owns the accept loop: for each connection it peeks the leading bytes
/// — without consuming them, since both handlers re-read from the start — and
/// dispatches a websocket upgrade to the NIP-42 relay and everything else to the
/// axum ingest [`router`]. One public port backs both, so a reverse proxy in
/// front needs no `Upgrade`-header routing of its own; it just forwards. The
/// relay is gated to `allowed_pubkeys` and never accepts events (the drain
/// pulls; ingest writes into nostrdb directly, not over the wire).
pub async fn serve(
    listener: TcpListener,
    ingest: Ingest,
    allowed_pubkeys: HashSet<[u8; 32]>,
) -> std::io::Result<()> {
    let ndb = ingest.ndb.clone();
    let app = router(ingest);
    let auth = Arc::new(AuthConfig {
        allowed_pubkeys,
        accept_events: false,
    });
    // Held for the loop's lifetime; dropping it (on process exit) tells every
    // live relay connection to wind down.
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    loop {
        let (stream, _peer) = listener.accept().await?;
        if is_ws_upgrade(&stream).await {
            let ndb = ndb.clone();
            let auth = Some(auth.clone());
            let shutdown_rx = shutdown_rx.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    nostrdb_net::relay::server::serve(stream, ndb, auth, shutdown_rx).await
                {
                    tracing::debug!("relay connection ended: {err}");
                }
            });
        } else {
            let app = app.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = TowerToHyperService::new(app);
                if let Err(err) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    tracing::debug!("ingest connection ended: {err}");
                }
            });
        }
    }
}

/// Peek a new connection's leading bytes (without consuming them) and decide
/// whether it's a WebSocket upgrade, by looking for an `Upgrade: websocket`
/// header. Method alone can't decide: a browser hitting `/` and a ws upgrade to
/// `/` are both `GET /`.
async fn is_ws_upgrade(stream: &TcpStream) -> bool {
    let mut buf = vec![0u8; HEADER_PEEK_MAX];
    let deadline = tokio::time::Instant::now() + HEADER_PEEK_TIMEOUT;
    loop {
        let n = match tokio::time::timeout_at(deadline, stream.peek(&mut buf)).await {
            Ok(Ok(n)) => n,
            // Timed out, closed, or errored: not a ws upgrade — let the HTTP
            // path handle (and properly error on) it.
            _ => return false,
        };
        if n == 0 {
            return false;
        }
        let head = &buf[..n];
        if let Some(end) = find_header_end(head) {
            return header_has_ws_upgrade(&head[..end]);
        }
        if n >= buf.len() {
            // Header block bigger than we'll peek; decide on what we have.
            return header_has_ws_upgrade(head);
        }
        // The block hasn't fully arrived. peek() returns immediately with
        // whatever is buffered, so yield briefly rather than hot-loop while the
        // rest is still in flight.
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Offset just past the `\r\n\r\n` ending an HTTP header block, if present.
fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

/// Whether the header block carries `Upgrade: websocket` (the mark of a
/// WebSocket handshake), matched case-insensitively.
fn header_has_ws_upgrade(head: &[u8]) -> bool {
    let head = String::from_utf8_lossy(head).to_ascii_lowercase();
    head.lines().any(|line| {
        line.split_once(':')
            .is_some_and(|(name, value)| name.trim() == "upgrade" && value.contains("websocket"))
    })
}

/// The browser landing page describing what hookstr is.
async fn landing() -> Html<&'static str> {
    Html(LANDING)
}

/// The setup & usage docs page.
async fn docs() -> Html<&'static str> {
    Html(DOCS)
}

/// Accept, persist durably, 204. 500 only when persistence itself failed,
/// so the provider retries. Never block on anything downstream of nostrdb.
#[axum::debug_handler]
async fn receive(
    State(ingest): State<Ingest>,
    Path((token, path)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    // The token alone authenticates the delivery and names its provider; the
    // rest of the URL is free-form routing data echoed into the path tag. A
    // wrong token is indistinguishable from a missing route (404), so probing
    // reveals neither which providers exist nor their paths.
    let Some(provider) = ingest
        .tokens
        .iter()
        .find(|(_, t)| **t == token)
        .map(|(provider, _)| provider.clone())
    else {
        return StatusCode::NOT_FOUND;
    };

    let rec = hookstr_core::WebhookRecord {
        path,
        provider,
        headers: headers
            .iter()
            .filter(|(name, _)| hookstr_core::keep_header(name.as_str()))
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect(),
        body: body.to_vec(),
        received_at_ms: now_ms(),
    };

    // The note and the id filter hold raw pointers (`!Send`), so extract the
    // id + wire frame and subscribe inside this block — nothing non-Send may
    // live across the awaits below.
    let (note_id, frame, sub) = {
        let Ok(note) = hookstr_core::sign_webhook_note(&rec, &ingest.seckey) else {
            tracing::error!(path = rec.path, "could not build/sign webhook note");
            return StatusCode::INTERNAL_SERVER_ERROR;
        };
        let Ok(frame) = hookstr_core::event_frame(&note) else {
            tracing::error!(path = rec.path, "could not serialize webhook note");
            return StatusCode::INTERNAL_SERVER_ERROR;
        };

        // nostrdb queues ingest to background writer threads, so a 204 at
        // queue time could lose a delivery on crash. Subscribe on the note's
        // id *before* ingesting; the subscription fires when the note is
        // committed, and that notification is what makes the 204 mean
        // "durable".
        let filter = Filter::new().ids([note.id()]).build();
        let Ok(sub) = ingest.ndb.subscribe(&[filter]) else {
            tracing::error!(
                path = rec.path,
                "could not subscribe for ingest notification"
            );
            return StatusCode::INTERNAL_SERVER_ERROR;
        };
        (*note.id(), frame, sub)
    };
    // Unsubscribes from ndb on drop, so every exit path below cleans up.
    let mut ingested = SubscriptionStream::new(ingest.ndb.clone(), sub).notes_per_await(1);

    if let Err(err) = ingest.ndb.process_client_event(&frame) {
        tracing::error!(path = rec.path, "could not queue webhook for ingest: {err}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    match tokio::time::timeout(INGEST_DEADLINE, ingested.next()).await {
        Ok(Some(_)) => {
            tracing::info!(
                path = rec.path,
                id = hex::encode(note_id),
                bytes = rec.body.len(),
                "stored webhook"
            );
            StatusCode::NO_CONTENT
        }
        _ => {
            tracing::error!(
                path = rec.path,
                "webhook was not durable within {INGEST_DEADLINE:?}"
            );
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before 1970")
        .as_millis() as u64
}
