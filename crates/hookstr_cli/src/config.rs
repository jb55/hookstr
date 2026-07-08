//! drain config. See ../../../config/hookstr_cli.example.toml.

use anyhow::Context;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct DrainConfig {
    /// wss://hooks.example.com
    pub relay_url: String,
    /// Local mirror nostrdb (negentropy needs the full local set).
    pub db_path: String,
    /// Replay bookkeeping (event id -> status/attempts/next_retry).
    pub redb_path: String,
    /// File containing the drain nsec (its pubkey is hookstrd's allowlist).
    pub nsec_path: String,
    /// Default replay routing. The event's `path` tag mirrors the ingest
    /// URL's `{provider}/{type}` suffix, so deliveries are self-describing:
    /// they replay to `{target_base}/{provider}/{type}` with no per-path
    /// config as long as the consumer's route mirrors the ingest path too.
    pub target_base: Option<String>,
    /// Per-path overrides for consumers whose route does not mirror the
    /// ingest path, e.g. "acme/events" = "http://localhost:3000/hooks".
    #[serde(default)]
    pub targets: HashMap<String, String>,
}

impl DrainConfig {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading config {path}"))?;
        toml::from_str(&text).with_context(|| format!("parsing {path}"))
    }

    /// Where a delivery with this `path` tag replays to: an explicit
    /// override if configured, otherwise under `target_base`.
    pub fn target_for(&self, path: &str) -> Option<String> {
        if let Some(target) = self.targets.get(path) {
            return Some(target.clone());
        }
        self.target_base
            .as_ref()
            .map(|base| format!("{}/{path}", base.trim_end_matches('/')))
    }

    pub fn seckey(&self) -> anyhow::Result<[u8; 32]> {
        let nsec = std::fs::read_to_string(&self.nsec_path)
            .with_context(|| format!("reading nsec from {}", self.nsec_path))?;
        let (seckey, _pubkey) = nostr_relay_sync::parse_nsec(nsec.trim())
            .map_err(|e| anyhow::anyhow!("{}: {e}", self.nsec_path))?;
        Ok(seckey)
    }
}
