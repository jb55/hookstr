//! Webhook <-> nostr event schema shared by hookstrd (encode) and
//! hookstr_cli (decode).
//!
//! One inbound webhook delivery == one immutable, regular nostr event signed
//! by the server's key. See SPIKE.md § "Event schema" for the full rationale.
//!
//! Layout:
//!
//! - `kind`: [`KIND_WEBHOOK`] (3003, regular/immutable — verify against the
//!   nips registry before this leaves spike stage)
//! - `content`: the raw HTTP body, verbatim, when it is valid UTF-8 (the
//!   normal case — provider webhook bodies are JSON). Otherwise base64, with
//!   an `["encoding","base64"]` tag. Byte-exactness is load-bearing: the
//!   consumer HMACs the raw body against `x-signature`.
//! - tags:
//!   - `["t", <provider>]` — provider discriminator ("acme"), named by the
//!     ingest token that authenticated the delivery.
//!     Single-letter so nostrdb / relays index it (`Filter::tags(.., 't')`).
//!   - `["path", <path>]` — everything after the token in the ingest URL
//!     ("acme/events"); the replay-target lookup key.
//!   - `["header", <name>, <value>]` — one per preserved request header,
//!     name lowercased, hop-by-hop and transport headers scrubbed.
//!   - `["received_at", <unix-ms>]` — receipt time, ms precision
//!     (`created_at` carries the same instant at seconds precision and is the
//!     negentropy ordering key).
//!   - `["encoding", "base64"]` — only when content is base64.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use nostrdb::{NdbStr, NdbStrVariant, Note, NoteBuildOptions, NoteBuilder};

/// Regular (non-replaceable, non-ephemeral: 1000 <= n < 10000) kind for a
/// stored webhook delivery. 3003 is unassigned in the NIPs registry at time
/// of writing;
pub const KIND_WEBHOOK: u32 = 3003;

pub const TAG_PATH: &str = "path";
pub const TAG_HEADER: &str = "header";
pub const TAG_RECEIVED_AT: &str = "received_at";
pub const TAG_ENCODING: &str = "encoding";
pub const ENCODING_BASE64: &str = "base64";

/// Headers that must not be recorded or replayed: hop-by-hop (RFC 9110 §7.6.1),
/// transport framing (reqwest recomputes these), and reverse-proxy breadcrumbs
/// Caddy prepends. Everything else is preserved verbatim — `x-signature`,
/// delivery-id and content-type headers are the ones consumers
/// actually reads, but we keep the lot rather than curate.
pub const SCRUBBED_HEADERS: &[&str] = &[
    "host",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "accept-encoding",
    "expect",
    "x-forwarded-for",
    "x-forwarded-proto",
    "x-forwarded-host",
    "x-real-ip",
];

/// Whether a request header should be preserved on the event (and therefore
/// replayed). `name` must already be lowercase (axum's `HeaderMap` keys are).
pub fn keep_header(name: &str) -> bool {
    !SCRUBBED_HEADERS.contains(&name)
}

/// A header value at replay time. nostrdb stores any 64-char-hex tag string
/// (e.g. an HMAC-SHA256 `x-signature`) with the packed-id flag, so reading it
/// back as a `str` yields nothing — the value comes out as a 32-byte `Id`.
/// Re-hex those so a hex signature survives the store/replay round-trip
/// byte-for-byte; anything genuinely textual passes through untouched.
fn header_value(value: NdbStr<'_>) -> String {
    match value.variant() {
        NdbStrVariant::Str(text) => text.to_owned(),
        NdbStrVariant::Id(id) => hex::encode(id),
    }
}

/// A webhook delivery, as captured at ingest or reconstructed at replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookRecord {
    /// Provider discriminator, e.g. "acme". Named by the ingest token that
    /// authenticated the delivery.
    pub provider: String,
    /// Everything after the token in the ingest URL, e.g. "acme/events".
    /// Replay routing key.
    pub path: String,
    /// Preserved headers, lowercased names, request order. Already scrubbed.
    pub headers: Vec<(String, String)>,
    /// The raw HTTP body, byte-exact.
    pub body: Vec<u8>,
    /// Receipt time in unix milliseconds.
    pub received_at_ms: u64,
}

#[derive(Debug)]
pub enum SchemaError {
    WrongKind(u32),
    MissingTag(&'static str),
    BadBase64,
    BuildFailed,
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::WrongKind(k) => write!(f, "not a webhook event (kind {k})"),
            SchemaError::MissingTag(t) => write!(f, "webhook event missing '{t}' tag"),
            SchemaError::BadBase64 => write!(f, "base64 content did not decode"),
            SchemaError::BuildFailed => write!(f, "note build/sign failed"),
        }
    }
}

impl std::error::Error for SchemaError {}

/// Build and sign the nostr event for one webhook delivery.
///
/// The body is stored verbatim in `content` when it is valid UTF-8 (JSON
/// string escaping round-trips UTF-8 text exactly, and `Note::content()`
/// hands back the identical bytes on the other side); a non-UTF-8 body falls
/// back to base64 plus an `["encoding","base64"]` tag rather than being
/// rejected — never lose a webhook.
///
/// `created_at` is set explicitly to `received_at_ms / 1000` instead of
/// letting [`NoteBuildOptions`] stamp "now", so the negentropy ordering key
/// and the `received_at` tag can never disagree.
pub fn sign_webhook_note(
    rec: &WebhookRecord,
    seckey: &[u8; 32],
) -> Result<Note<'static>, SchemaError> {
    let (content, base64_encoded) = match std::str::from_utf8(&rec.body) {
        Ok(text) => (text.to_owned(), false),
        Err(_) => (BASE64.encode(&rec.body), true),
    };

    let mut b = NoteBuilder::new()
        .kind(KIND_WEBHOOK)
        .content(&content)
        .created_at(rec.received_at_ms / 1000)
        .options(NoteBuildOptions::default().created_at(false))
        .start_tag()
        .tag_str("t")
        .tag_str(&rec.provider)
        .start_tag()
        .tag_str(TAG_PATH)
        .tag_str(&rec.path)
        .start_tag()
        .tag_str(TAG_RECEIVED_AT)
        .tag_str(&rec.received_at_ms.to_string());

    for (name, value) in &rec.headers {
        b = b
            .start_tag()
            .tag_str(TAG_HEADER)
            .tag_str(name)
            .tag_str(value);
    }

    if base64_encoded {
        b = b.start_tag().tag_str(TAG_ENCODING).tag_str(ENCODING_BASE64);
    }

    b.sign(seckey).build().ok_or(SchemaError::BuildFailed)
}

/// Reconstruct the delivery from a stored note. Inverse of
/// [`sign_webhook_note`]; `parse(sign(rec)) == rec` (header order included —
/// tags preserve insertion order).
pub fn parse_webhook_note(note: &Note<'_>) -> Result<WebhookRecord, SchemaError> {
    if note.kind() != KIND_WEBHOOK {
        return Err(SchemaError::WrongKind(note.kind()));
    }

    let mut provider = None;
    let mut path = None;
    let mut received_at_ms = None;
    let mut headers = Vec::new();
    let mut base64_encoded = false;

    for tag in note.tags() {
        match tag.get_str(0) {
            Some("t") => provider = tag.get_str(1).map(str::to_owned),
            Some(TAG_PATH) => path = tag.get_str(1).map(str::to_owned),
            Some(TAG_RECEIVED_AT) => {
                received_at_ms = tag.get_str(1).and_then(|s| s.parse().ok());
            }
            Some(TAG_HEADER) => {
                if let (Some(name), Some(value)) = (tag.get_str(1), tag.get(2).map(header_value)) {
                    headers.push((name.to_owned(), value));
                }
            }
            Some(TAG_ENCODING) => {
                base64_encoded = tag.get_str(1) == Some(ENCODING_BASE64);
            }
            _ => {}
        }
    }

    let body = if base64_encoded {
        BASE64
            .decode(note.content())
            .map_err(|_| SchemaError::BadBase64)?
    } else {
        note.content().as_bytes().to_vec()
    };

    Ok(WebhookRecord {
        provider: provider.ok_or(SchemaError::MissingTag("t"))?,
        path: path.ok_or(SchemaError::MissingTag(TAG_PATH))?,
        headers,
        body,
        received_at_ms: received_at_ms.ok_or(SchemaError::MissingTag(TAG_RECEIVED_AT))?,
    })
}

/// The `["EVENT", {...}]` client frame for a note — what
/// `Ndb::process_client_event` ingests and what gets published to a relay.
pub fn event_frame(note: &Note<'_>) -> Result<String, nostrdb::Error> {
    Ok(format!(r#"["EVENT",{}]"#, note.json()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECKEY: [u8; 32] = [1; 32];

    fn delivery(body: Vec<u8>) -> WebhookRecord {
        WebhookRecord {
            provider: "acme".to_owned(),
            path: "acme/events".to_owned(),
            headers: vec![
                ("content-type".to_owned(), "application/json".to_owned()),
                ("x-signature".to_owned(), "t=123,v=abcdef".to_owned()),
                ("x-webhook-id".to_owned(), "wh_42".to_owned()),
            ],
            body,
            received_at_ms: 1_751_900_000_123,
        }
    }

    #[test]
    fn roundtrips_utf8_body() {
        let rec = delivery(br#"{"event":"ping","amount":100}"#.to_vec());
        let note = sign_webhook_note(&rec, &SECKEY).unwrap();

        assert_eq!(note.kind(), KIND_WEBHOOK);
        // created_at (negentropy ordering key) and the received_at tag must
        // carry the same instant.
        assert_eq!(note.created_at(), rec.received_at_ms / 1000);
        // UTF-8 body stays verbatim in content — greppable, no encoding tag.
        assert_eq!(note.content().as_bytes(), rec.body.as_slice());

        assert_eq!(parse_webhook_note(&note).unwrap(), rec);
    }

    #[test]
    fn roundtrips_non_utf8_body_via_base64() {
        let rec = delivery(vec![0xff, 0xfe, 0x00, 0x01]);
        let note = sign_webhook_note(&rec, &SECKEY).unwrap();

        assert_ne!(note.content().as_bytes(), rec.body.as_slice());
        assert_eq!(parse_webhook_note(&note).unwrap(), rec);
    }

    #[test]
    fn roundtrips_64_hex_signature_header() {
        // An HMAC-SHA256 signature is 64 lowercase hex chars, which nostrdb
        // stores with the packed-id flag — get_str would return None and the
        // header would silently vanish on replay, failing the consumer's
        // signature check. Regression guard for that byte-exact round-trip.
        let sig = "0161f28c2f5d54f98abe6cb82d8dea190fa3ee38059f79115e81bf679805706d";
        let rec = WebhookRecord {
            provider: "acme".to_owned(),
            path: "acme/events".to_owned(),
            headers: vec![
                ("content-type".to_owned(), "application/json".to_owned()),
                ("x-signature".to_owned(), sig.to_owned()),
            ],
            body: b"{}".to_vec(),
            received_at_ms: 1_751_900_000_123,
        };

        let note = sign_webhook_note(&rec, &SECKEY).unwrap();
        let parsed = parse_webhook_note(&note).unwrap();

        // The hex value survives verbatim rather than vanishing as a packed id.
        assert!(
            parsed
                .headers
                .contains(&("x-signature".to_owned(), sig.to_owned()))
        );
        assert_eq!(parsed, rec);
    }

    #[test]
    fn keeps_header_order_and_scrubs_transport() {
        assert!(keep_header("x-signature"));
        assert!(keep_header("x-webhook-id"));
        assert!(keep_header("content-type"));
        assert!(!keep_header("content-length"));
        assert!(!keep_header("x-forwarded-for"));
        assert!(!keep_header("connection"));

        let rec = delivery(b"{}".to_vec());
        let note = sign_webhook_note(&rec, &SECKEY).unwrap();
        assert_eq!(parse_webhook_note(&note).unwrap().headers, rec.headers);
    }
}
