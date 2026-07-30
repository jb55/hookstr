//! hookstrd config. See ../../../config/hookstrd.example.toml.

use anyhow::Context;
use clap::Parser;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Parser)]
#[command(about = "store-and-forward webhook daemon: HTTP ingest + authenticated nostr relay")]
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
    /// Combined mode: one plaintext listener serving both the webhook ingest
    /// HTTP API and the nostr relay (ws), demuxed by the `Upgrade` header (see
    /// `hookstrd::serve`). Set this *or* `ingest_addr` + `relay_addr`, not both.
    #[serde(default)]
    pub listen_addr: Option<SocketAddr>,
    /// Separate mode: the axum webhook-ingest listener, plaintext (the reverse
    /// proxy terminates TLS). Pairs with `relay_addr`.
    #[serde(default)]
    pub ingest_addr: Option<String>,
    /// Separate mode: the nostr relay (ws) listener. Splitting it from
    /// `ingest_addr` lets it bind a private interface — e.g. a WireGuard
    /// address — and stay off the public internet the way ingest can't.
    #[serde(default)]
    pub relay_addr: Option<SocketAddr>,
    /// Per-provider ingest secrets. The `{token}` in the URL alone
    /// authenticates a delivery and names its provider, so each provider gets
    /// its own secret you can revoke without disturbing the others. Values
    /// must be unique or the token -> provider lookup would be ambiguous.
    pub ingest_tokens: HashMap<String, String>,
    /// File containing the server nsec (not the key itself — keep secrets
    /// out of config that might get committed).
    pub nsec_path: String,
    /// NIP-42 allowlist: the drain client's pubkey (hex).
    pub drain_pubkey: String,
}

/// How the ingest HTTP API and relay ws are bound. Combined is the default;
/// separate is the advanced opt-in for keeping the relay off the public net.
pub enum Listen {
    /// One port, ingest + relay demuxed by the `Upgrade` header.
    Combined(SocketAddr),
    /// Two listeners; the relay can bind a private interface.
    Separate { ingest_addr: String, relay_addr: SocketAddr },
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
        // Fail fast on a bad addressing combo rather than after opening the db.
        cfg.listen()?;
        Ok(cfg)
    }

    /// Resolve the listener configuration, erroring unless exactly one of the
    /// two forms is fully specified. Combined (`listen_addr`) is the default;
    /// separate (`ingest_addr` + `relay_addr`) is the advanced alternative.
    pub fn listen(&self) -> anyhow::Result<Listen> {
        match (self.listen_addr, &self.ingest_addr, self.relay_addr) {
            (Some(addr), None, None) => Ok(Listen::Combined(addr)),
            (None, Some(ingest_addr), Some(relay_addr)) => Ok(Listen::Separate {
                ingest_addr: ingest_addr.clone(),
                relay_addr,
            }),
            (Some(_), _, _) => anyhow::bail!(
                "listen_addr sets one combined port; drop ingest_addr/relay_addr to use it"
            ),
            (None, None, None) => anyhow::bail!(
                "no listener: set listen_addr (one combined port), or ingest_addr + relay_addr to split them"
            ),
            (None, _, _) => anyhow::bail!(
                "separate binds need both ingest_addr and relay_addr (or use a single listen_addr)"
            ),
        }
    }

    pub fn seckey(&self) -> anyhow::Result<[u8; 32]> {
        let nsec = std::fs::read_to_string(&self.nsec_path)
            .with_context(|| format!("reading nsec from {}", self.nsec_path))?;
        let (hrp, data) = bech32::decode(nsec.trim())
            .map_err(|_| anyhow::anyhow!("{} does not hold bech32", self.nsec_path))?;
        anyhow::ensure!(
            hrp.as_str() == "nsec",
            "expected an nsec in {}, got '{}'",
            self.nsec_path,
            hrp.as_str()
        );
        data.try_into()
            .map_err(|_| anyhow::anyhow!("nsec did not decode to 32 bytes"))
    }

    pub fn drain_pubkey(&self) -> anyhow::Result<[u8; 32]> {
        hex::decode(&self.drain_pubkey)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| anyhow::anyhow!("drain_pubkey must be a 64-char hex pubkey"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse a config with the given address lines (the rest is boilerplate).
    fn cfg(addr_lines: &str) -> HookstrdConfig {
        let text = format!(
            "db_path = \"x\"\nmapsize = 1\nnsec_path = \"n\"\ndrain_pubkey = \"ab\"\n\
             {addr_lines}\n[ingest_tokens]\nacme = \"t\"\n"
        );
        toml::from_str(&text).expect("parse")
    }

    #[test]
    fn listen_addr_is_combined() {
        assert!(matches!(
            cfg("listen_addr = \"127.0.0.1:8080\"").listen().unwrap(),
            Listen::Combined(_)
        ));
    }

    #[test]
    fn ingest_and_relay_addr_is_separate() {
        assert!(matches!(
            cfg("ingest_addr = \"127.0.0.1:8080\"\nrelay_addr = \"10.0.0.1:8081\"")
                .listen()
                .unwrap(),
            Listen::Separate { .. }
        ));
    }

    #[test]
    fn combined_and_separate_together_is_error() {
        assert!(
            cfg("listen_addr = \"127.0.0.1:8080\"\nrelay_addr = \"10.0.0.1:8081\"")
                .listen()
                .is_err()
        );
    }

    #[test]
    fn no_address_is_error() {
        assert!(cfg("").listen().is_err());
    }

    #[test]
    fn half_a_separate_pair_is_error() {
        assert!(cfg("ingest_addr = \"127.0.0.1:8080\"").listen().is_err());
    }
}
