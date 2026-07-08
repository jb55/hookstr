//! hookstrd config. See ../../../config/hookstrd.example.toml.

use anyhow::Context;
use clap::Parser;
use serde::Deserialize;
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
    /// axum ingest listener, plaintext localhost (the reverse proxy
    /// terminates TLS).
    pub ingest_addr: String,
    /// nostrdb_relay ws listener, plaintext localhost.
    pub relay_addr: SocketAddr,
    /// Secret path segment in the URL registered with providers.
    pub ingest_token: String,
    /// File containing the server nsec (not the key itself — keep secrets
    /// out of config that might get committed).
    pub nsec_path: String,
    /// NIP-42 allowlist: the drain client's pubkey (hex).
    pub drain_pubkey: String,
}

impl HookstrdConfig {
    pub fn load() -> anyhow::Result<Self> {
        let args = Args::parse();
        let text = std::fs::read_to_string(&args.config)
            .with_context(|| format!("reading config {}", args.config))?;
        toml::from_str(&text).with_context(|| format!("parsing {}", args.config))
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
