//! hookstrd — always-online half of hookstr. One nostrdb behind two
//! listeners: the public HTTP ingest endpoint providers POST to, and a
//! localhost-only nostr relay (ws) the drain syncs from over a tunnel.

use hookstrd::Ingest;
use nostrdb::{Config, Ndb};

mod config;

use config::HookstrdConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cfg = HookstrdConfig::load()?;
    let ingest_addr = cfg.ingest_addr()?;

    std::fs::create_dir_all(&cfg.db_path)?;
    let ndb = Ndb::new(&cfg.db_path, &Config::new().set_mapsize(cfg.mapsize))?;

    let seckey = cfg.seckey()?;
    let ingest = Ingest {
        ndb: ndb.clone(),
        seckey,
        tokens: cfg.ingest_tokens.clone(),
    };

    // Held for the process lifetime; dropping it stops the relay.
    let _relay = hookstrd::spawn_relay(ndb, cfg.relay_addr)?;

    let listener = tokio::net::TcpListener::bind(ingest_addr).await?;
    tracing::info!(
        "hookstrd ingest on {ingest_addr}, relay on ws://{}",
        cfg.relay_addr
    );
    hookstrd::serve(listener, ingest).await?;
    Ok(())
}
