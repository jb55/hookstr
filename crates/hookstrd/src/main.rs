//! hookstrd — always-online half of hookstr. One nostrdb behind an
//! authenticated nostr relay (ws) the drain client syncs from and an HTTP
//! ingest endpoint providers POST to. By default both share one port,
//! demuxed by `hookstrd::serve`; separate `ingest_addr`/`relay_addr` binds
//! are an advanced option (e.g. relay on a private interface). See ../../SPIKE.md.

use config::Listen;
use hookstrd::Ingest;
use nostrdb::{Config, Ndb};
use std::collections::HashSet;

mod config;

use config::HookstrdConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cfg = HookstrdConfig::load()?;

    std::fs::create_dir_all(&cfg.db_path)?;
    let ndb = Ndb::new(&cfg.db_path, &Config::new().set_mapsize(cfg.mapsize))?;

    let ingest = Ingest {
        ndb: ndb.clone(),
        seckey: cfg.seckey()?,
        tokens: cfg.ingest_tokens.clone(),
    };
    // NIP-42 allowlist: only the drain client's pubkey may read the relay.
    // Read-only — the drain pulls, nothing pushes into this db over the wire.
    let allowed = HashSet::from([cfg.drain_pubkey()?]);

    match cfg.listen()? {
        // Default: one port, ingest + relay demuxed by the Upgrade header.
        Listen::Combined(addr) => {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            tracing::info!("hookstrd listening on {addr} (webhook ingest + relay ws)");
            hookstrd::serve(listener, ingest, allowed).await?;
        }
        // Advanced: relay on its own listener (bind a private interface here to
        // keep it off the public net); ingest served separately.
        Listen::Separate {
            ingest_addr,
            relay_addr,
        } => {
            let _relay = nostrdb_net::relay::server::spawn_with_auth(
                ndb,
                relay_addr,
                nostrdb_net::relay::server::AuthConfig {
                    allowed_pubkeys: allowed,
                    accept_events: false,
                },
            )?;
            tracing::info!("relay listening on ws://{relay_addr}");
            let listener = tokio::net::TcpListener::bind(&ingest_addr).await?;
            tracing::info!("ingest listening on http://{ingest_addr}");
            axum::serve(listener, hookstrd::router(ingest)).await?;
        }
    }
    Ok(())
}
