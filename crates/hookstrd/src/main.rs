//! hookstrd — always-online half of hookstr. Two listeners, one nostrdb:
//! an axum ingest endpoint providers POST to, and an embedded
//! nostrdb_relay the drain client syncs from. See ../../SPIKE.md.

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use futures_util::StreamExt;
use nostrdb::{Config, Filter, Ndb, SubscriptionStream};
use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod config;

use config::HookstrdConfig;

/// How long ingest waits for nostrdb's background writer to commit a note
/// before giving up and telling the provider to retry.
const INGEST_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct Ingest {
    ndb: Ndb,
    seckey: [u8; 32],
    /// Long random secret baked into the URL registered with providers.
    token: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cfg = HookstrdConfig::load()?;

    std::fs::create_dir_all(&cfg.db_path)?;
    let ndb = Ndb::new(&cfg.db_path, &Config::new().set_mapsize(cfg.mapsize))?;

    // The relay side: EVENT/REQ(+live)/CLOSE + NIP-77 responder from
    // notedeck/crates/nostrdb_relay, NIP-42-gated to the drain client's
    // pubkey. Read-only: the drain pulls, nothing pushes into this db over
    // the wire.
    let _relay = nostrdb_relay::spawn_with_auth(
        ndb.clone(),
        cfg.relay_addr,
        nostrdb_relay::AuthConfig {
            allowed_pubkeys: HashSet::from([cfg.drain_pubkey()?]),
            accept_events: false,
        },
    )?;

    let ingest = Ingest {
        ndb,
        seckey: cfg.seckey()?,
        token: cfg.ingest_token.clone(),
    };
    let app = Router::new()
        .route("/ingest/{token}/{provider}/{type}", post(receive))
        .with_state(ingest);
    let listener = tokio::net::TcpListener::bind(&cfg.ingest_addr).await?;
    tracing::info!("ingest listening on http://{}", cfg.ingest_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

/// Accept, persist durably, 204. 500 only when persistence itself failed,
/// so the provider retries. Never block on anything downstream of nostrdb.
#[axum::debug_handler]
async fn receive(
    State(ingest): State<Ingest>,
    Path((token, provider, hook_type)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    if token != ingest.token {
        return StatusCode::NOT_FOUND;
    }

    let rec = hookstr_core::WebhookRecord {
        path: format!("{provider}/{hook_type}"),
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
            tracing::error!(path = rec.path, "could not subscribe for ingest notification");
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
