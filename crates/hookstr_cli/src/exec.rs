//! `drain --exec`: deliver due webhooks to a command in batches instead of
//! POSTing them one by one.
//!
//! Each batch is one run of the command under `sh -c`, with the batch on
//! stdin as NDJSON [`envelope`]s, oldest first, and `HOOKSTR_PHASE` in the
//! environment. Exit 0 acks the whole batch; a non-zero exit or a timeout
//! fails the whole batch, which retries through the usual replay backoff.
//! There is no partial ack: consumers are idempotent, as at-least-once
//! delivery already requires.
//!
//! `peek --json` prints the same envelope lines, so
//! `hookstr peek --all --json | ./consumer` feeds a consumer exactly what an
//! exec drain would.

use anyhow::Context;
use base64::Engine;
use hookstr_core::WebhookRecord;
use serde_json::{Map, Value, json};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

/// How a drain runs its `--exec` command.
pub struct Exec {
    /// Shell command line, run with `sh -c`.
    pub cmd: String,
    /// Most envelopes in one batch.
    pub batch_max: usize,
    /// In follow mode, how long to gather arrivals after the first one
    /// before delivering them as one batch.
    pub batch_window: Duration,
    /// How long one run may take before it is killed and the batch fails.
    pub timeout: Duration,
}

/// Which part of a drain a batch comes from, passed as `HOOKSTR_PHASE`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    /// The catch-up replay of what was stored before the drain started.
    Backfill,
    /// New webhooks arriving while following.
    Live,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Backfill => "backfill",
            Phase::Live => "live",
        }
    }
}

/// One delivery as a JSON object, the unit of `drain --exec` stdin and
/// `peek --json` stdout. See the README's "Envelope format" for the contract.
///
/// `body` is the parsed JSON value when the body parses as JSON
/// (`body_encoding: "json"`), else the text when it is UTF-8 (`"utf8"`), else
/// standard base64 of the bytes (`"base64"`). Repeated header names are
/// joined with ", ", as HTTP allows.
pub fn envelope(id: &[u8; 32], rec: &WebhookRecord) -> Value {
    let mut headers = Map::new();
    for (name, value) in &rec.headers {
        match headers.get_mut(name) {
            Some(Value::String(joined)) => {
                joined.push_str(", ");
                joined.push_str(value);
            }
            _ => {
                headers.insert(name.clone(), Value::String(value.clone()));
            }
        }
    }
    let (body, body_encoding) = match serde_json::from_slice::<Value>(&rec.body) {
        Ok(value) => (value, "json"),
        Err(_) => match std::str::from_utf8(&rec.body) {
            Ok(text) => (Value::String(text.to_owned()), "utf8"),
            Err(_) => (
                Value::String(base64::engine::general_purpose::STANDARD.encode(&rec.body)),
                "base64",
            ),
        },
    };
    json!({
        "id": hex::encode(id),
        "path": rec.path,
        "received_at_ms": rec.received_at_ms,
        "provider": rec.provider,
        "headers": headers,
        "body": body,
        "body_encoding": body_encoding,
    })
}

/// [`envelope`] as one NDJSON line, newline included.
pub fn envelope_line(id: &[u8; 32], rec: &WebhookRecord) -> String {
    // Serializing a Value can't fail, and compact output never contains a
    // raw newline (one inside a string is escaped), so this is one line.
    let mut line = serde_json::to_string(&envelope(id, rec)).expect("Value serializes");
    line.push('\n');
    line
}

/// Run the command once with `input` on stdin. Ok means exit 0, the batch's
/// ack. The command's stdout and stderr are the drain's.
pub async fn run_batch(exec: &Exec, phase: Phase, input: &[u8]) -> anyhow::Result<()> {
    let mut child = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(&exec.cmd)
        .env("HOOKSTR_PHASE", phase.as_str())
        .stdin(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("spawning `{}`", exec.cmd))?;
    let mut stdin = child.stdin.take().expect("stdin is piped");
    let finished = tokio::time::timeout(exec.timeout, async {
        // A command that exits without reading all of stdin closes the pipe
        // on us. Its exit status still decides the batch.
        if let Err(err) = stdin.write_all(input).await
            && err.kind() != std::io::ErrorKind::BrokenPipe
        {
            return Err(anyhow::Error::from(err).context("writing the batch to stdin"));
        }
        drop(stdin);
        Ok(child.wait().await?)
    })
    .await;
    match finished {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => anyhow::bail!("`{}` {status}", exec.cmd),
        Ok(Err(err)) => Err(err),
        Err(_) => {
            child.kill().await.ok();
            anyhow::bail!("`{}` timed out after {:?}, killed", exec.cmd, exec.timeout)
        }
    }
}

/// Parse a duration flag: `500ms`, `2s`, `5m`, `1h`, or bare seconds.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("`{s}`: expected a number with an optional ms/s/m/h unit"))?;
    match unit {
        "ms" => Ok(Duration::from_millis(n)),
        "" | "s" => Ok(Duration::from_secs(n)),
        "m" => Ok(Duration::from_secs(n * 60)),
        "h" => Ok(Duration::from_secs(n * 3600)),
        _ => Err(format!("`{s}`: unknown unit `{unit}`, use ms, s, m or h")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(headers: &[(&str, &str)], body: &[u8]) -> WebhookRecord {
        WebhookRecord {
            provider: "acme".to_owned(),
            path: "acme/events".to_owned(),
            headers: headers
                .iter()
                .map(|(n, v)| ((*n).to_owned(), (*v).to_owned()))
                .collect(),
            body: body.to_vec(),
            received_at_ms: 1_700_000_000_123,
        }
    }

    #[test]
    fn envelope_carries_each_body_kind() {
        let id = [0xab; 32];
        let env = envelope(&id, &rec(&[], br#"{"b":1,"a":[true]}"#));
        assert_eq!(env["body_encoding"], "json");
        assert_eq!(env["body"], json!({"b": 1, "a": [true]}));
        // Key order survives (serde_json's preserve_order), body and envelope.
        let line = envelope_line(&id, &rec(&[], br#"{"b":1,"a":[true]}"#));
        assert!(line.starts_with(&format!(
            r#"{{"id":"{}","path":"acme/events""#,
            "ab".repeat(32)
        )));
        assert!(line.contains(r#""body":{"b":1,"a":[true]}"#), "{line}");

        let env = envelope(&id, &rec(&[], b"plain text, not json"));
        assert_eq!(env["body_encoding"], "utf8");
        assert_eq!(env["body"], "plain text, not json");

        let env = envelope(&id, &rec(&[], &[0xff, 0x00, 0xfe]));
        assert_eq!(env["body_encoding"], "base64");
        assert_eq!(env["body"], "/wD+");

        // A JSON string body is still "json", distinct from raw text.
        let env = envelope(&id, &rec(&[], br#""quoted""#));
        assert_eq!(env["body_encoding"], "json");
        assert_eq!(env["body"], "quoted");
    }

    #[test]
    fn envelope_joins_repeated_headers_and_is_one_line() {
        let id = [1; 32];
        let r = rec(
            &[
                ("x-a", "1"),
                ("content-type", "application/json"),
                ("x-a", "2"),
            ],
            b"{\n  \"pretty\": \"line\\nbreak\"\n}\n",
        );
        let env = envelope(&id, &r);
        assert_eq!(
            env["headers"],
            json!({"x-a": "1, 2", "content-type": "application/json"})
        );
        assert_eq!(env["received_at_ms"], 1_700_000_000_123u64);
        assert_eq!(env["provider"], "acme");
        let line = envelope_line(&id, &r);
        assert_eq!(line.matches('\n').count(), 1, "one line: {line:?}");
        assert!(line.ends_with('\n'));
    }

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("2s"), Ok(Duration::from_secs(2)));
        assert_eq!(parse_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_duration("90"), Ok(Duration::from_secs(90)));
        assert_eq!(parse_duration("5m"), Ok(Duration::from_secs(300)));
        assert!(parse_duration("2x").is_err());
        assert!(parse_duration("s").is_err());
    }
}
