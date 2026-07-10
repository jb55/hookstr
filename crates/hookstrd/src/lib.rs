//! The ingest service half of hookstrd, as a library so the e2e tests can run
//! the real handler in-process. `main.rs` is config loading + wiring only.

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use futures_util::StreamExt;
use nostrdb::{Filter, Ndb, SubscriptionStream};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How long ingest waits for nostrdb's background writer to commit a note
/// before giving up and telling the provider to retry.
const INGEST_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct Ingest {
    pub ndb: Ndb,
    pub seckey: [u8; 32],
    /// Per-provider secrets baked into the URL registered with each provider:
    /// `{provider} -> token`. A leaked token only works for its own provider
    /// and can be revoked without disturbing the others.
    pub tokens: HashMap<String, String>,
}

/// The ingest router: `POST /ingest/{token}/{provider}/{type}` -> durable 204.
pub fn router(ingest: Ingest) -> Router {
    Router::new()
        .route("/ingest/{token}/{provider}/{type}", post(receive))
        .with_state(ingest)
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
    // A provider is authenticated by the secret registered for it. An unknown
    // provider or a wrong token is indistinguishable from a missing route
    // (404), so probing reveals neither which providers exist nor their paths.
    if ingest.tokens.get(&provider) != Some(&token) {
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
