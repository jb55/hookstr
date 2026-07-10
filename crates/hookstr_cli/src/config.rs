//! drain config. See ../../../config/hookstr_cli.example.toml.

use anyhow::Context;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct DrainConfig {
    /// wss://hooks.example.com
    pub relay_url: String,
    /// Providers this drain pulls, matched on each event's indexed `t` tag
    /// (the provider name from hookstrd's `[ingest_tokens]`). Set from the
    /// drain's positional args, not the config file; empty pulls every
    /// provider.
    #[serde(skip)]
    pub providers: Vec<String>,
    /// Local mirror nostrdb (negentropy needs the full local set).
    pub db_path: String,
    /// Replay bookkeeping (event id -> status/attempts/next_retry).
    pub redb_path: String,
    /// File containing the drain nsec (its pubkey is hookstrd's allowlist).
    pub nsec_path: String,
    /// Default replay routing. The event's `path` tag mirrors everything
    /// after the token in the ingest URL, so deliveries are self-describing:
    /// they replay to `{target_base}/{path}` with no per-path config as long
    /// as the consumer's route mirrors the ingest path too.
    pub target_base: Option<String>,
    /// Per-path overrides for consumers whose route does not mirror the
    /// ingest path, e.g. "acme/events" = "http://localhost:3000/hooks".
    #[serde(default)]
    pub targets: HashMap<String, String>,
}

impl DrainConfig {
    pub fn load(path: Option<&str>) -> anyhow::Result<Self> {
        let (path, hint) = match path {
            Some(path) => (std::path::PathBuf::from(path), ""),
            None => (default_path()?, " (run `hookstr init` to create one)"),
        };
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading config {}{hint}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
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

/// `$XDG_CONFIG_HOME/hookstr/config.toml`, with the usual `~/.config`
/// fallback when XDG_CONFIG_HOME is unset.
pub fn default_path() -> anyhow::Result<std::path::PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        return Ok(std::path::PathBuf::from(xdg).join("hookstr/config.toml"));
    }
    let home = std::env::var_os("HOME")
        .ok_or_else(|| anyhow::anyhow!("neither XDG_CONFIG_HOME nor HOME is set; pass --config"))?;
    Ok(std::path::PathBuf::from(home).join(".config/hookstr/config.toml"))
}

/// `$XDG_DATA_HOME/hookstr`, with the usual `~/.local/share` fallback: where
/// `init` points the local mirror db and replay state.
pub fn default_data_dir() -> anyhow::Result<std::path::PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_DATA_HOME") {
        return Ok(std::path::PathBuf::from(xdg).join("hookstr"));
    }
    let home = std::env::var_os("HOME")
        .ok_or_else(|| anyhow::anyhow!("neither XDG_DATA_HOME nor HOME is set"))?;
    Ok(std::path::PathBuf::from(home).join(".local/share/hookstr"))
}
