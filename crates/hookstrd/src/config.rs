//! hookstrd config. See ../../../config/hookstrd.example.toml.

use anyhow::Context;
use clap::Parser;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Parser)]
#[command(about = "store-and-forward webhook daemon: HTTP ingest + localhost nostr relay")]
struct Args {
    /// Path to the TOML config file.
    #[arg(long, default_value = "hookstrd.toml")]
    config: String,
}

#[derive(Deserialize)]
pub struct HookstrdConfig {
    /// nostrdb directory. Store-everything: no pruning, size the mapsize
    /// generously and forget about it.
    pub db_path: String,
    pub mapsize: usize,
    /// The old combined ingest + relay port. Only read so a config that still
    /// sets it fails with a pointer to `ingest_addr` (see
    /// [`HookstrdConfig::ingest_addr`]).
    #[serde(default)]
    listen_addr: Option<SocketAddr>,
    /// Plaintext listener for the webhook ingest HTTP API. The reverse proxy
    /// terminates TLS and forwards provider POSTs here.
    #[serde(default)]
    ingest_addr: Option<String>,
    /// The nostr relay (ws) the drain syncs from. It has no auth and accepts
    /// writes, so keep it off the public internet: the default binds
    /// localhost, and the drain reaches it over a tunnel (or bind a private
    /// interface, e.g. a WireGuard address).
    #[serde(default = "default_relay_addr")]
    pub relay_addr: SocketAddr,
    /// Per-provider ingest secrets. The `{token}` in the URL alone
    /// authenticates a delivery and names its provider, so each provider gets
    /// its own secret you can revoke without disturbing the others. Values
    /// must be unique or the token -> provider lookup would be ambiguous.
    pub ingest_tokens: HashMap<String, String>,
    /// File containing the server nsec (not the key itself — keep secrets
    /// out of config that might get committed).
    pub nsec_path: String,
}

fn default_relay_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8081))
}

impl HookstrdConfig {
    pub fn load() -> anyhow::Result<Self> {
        let args = Args::parse();
        let text = std::fs::read_to_string(&args.config)
            .with_context(|| format!("reading config {}", args.config))?;
        let cfg: Self =
            toml::from_str(&text).with_context(|| format!("parsing {}", args.config))?;
        let unique: std::collections::HashSet<_> = cfg.ingest_tokens.values().collect();
        anyhow::ensure!(
            unique.len() == cfg.ingest_tokens.len(),
            "ingest_tokens values must be unique: a token names its provider"
        );
        // Fail fast on a bad address rather than after opening the db.
        cfg.ingest_addr()?;
        Ok(cfg)
    }

    /// The ingest listen address. Ingest and the relay always bind
    /// separately now: the relay has no auth, so it can't share the public
    /// port the way the old combined `listen_addr` mode had it.
    pub fn ingest_addr(&self) -> anyhow::Result<&str> {
        match (self.listen_addr, &self.ingest_addr) {
            (Some(_), _) => anyhow::bail!(
                "listen_addr (one combined ingest + relay port) is gone: the relay has no auth, so it can't share the public port. Rename it to ingest_addr; the relay binds relay_addr (default 127.0.0.1:8081)"
            ),
            (None, Some(addr)) => Ok(addr),
            (None, None) => anyhow::bail!("no ingest listener: set ingest_addr"),
        }
    }

    /// The server keypair as (seckey, hex pubkey). The key signs every
    /// webhook event; the pubkey is what the drain pins as `server_pubkey`.
    pub fn keys(&self) -> anyhow::Result<([u8; 32], String)> {
        let nsec = std::fs::read_to_string(&self.nsec_path)
            .with_context(|| format!("reading nsec from {}", self.nsec_path))?;
        let (seckey, pubkey) = nostrdb_net::relay::sync::parse_nsec(nsec.trim())
            .map_err(|e| anyhow::anyhow!("{}: {e}", self.nsec_path))?;
        Ok((seckey, pubkey.hex()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse a config with the given address lines (the rest is boilerplate).
    fn cfg(addr_lines: &str) -> HookstrdConfig {
        let text = format!(
            "db_path = \"x\"\nmapsize = 1\nnsec_path = \"n\"\n\
             {addr_lines}\n[ingest_tokens]\nacme = \"t\"\n"
        );
        toml::from_str(&text).expect("parse")
    }

    #[test]
    fn ingest_addr_alone_puts_the_relay_on_localhost() {
        let cfg = cfg("ingest_addr = \"127.0.0.1:8080\"");
        assert_eq!(cfg.ingest_addr().unwrap(), "127.0.0.1:8080");
        assert_eq!(cfg.relay_addr, default_relay_addr());
    }

    #[test]
    fn relay_addr_overrides_the_default() {
        let cfg = cfg("ingest_addr = \"127.0.0.1:8080\"\nrelay_addr = \"10.0.0.1:8081\"");
        assert_eq!(cfg.relay_addr, "10.0.0.1:8081".parse().unwrap());
    }

    #[test]
    fn combined_listen_addr_is_error() {
        let err = cfg("listen_addr = \"127.0.0.1:8080\"")
            .ingest_addr()
            .unwrap_err();
        assert!(format!("{err}").contains("ingest_addr"), "{err}");
    }

    #[test]
    fn no_address_is_error() {
        assert!(cfg("").ingest_addr().is_err());
    }
}
