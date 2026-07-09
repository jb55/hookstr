//! hookstrd — always-online half of hookstr. Two listeners, one nostrdb:
//! an axum ingest endpoint providers POST to, and an embedded
//! nostrdb_relay the drain client syncs from. See ../../SPIKE.md.

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

    // The relay side: EVENT/REQ(+live)/CLOSE + NIP-77 responder from
    // notedeck/crates/nostrdb_relay, NIP-42-gated to the drain client's
    // pubkey. Read-only: the drain pulls, nothing pushes into this db over
    // the wire.
    let _relay = nostrdb_relay::spawn_with_auth(
        ndb.clone(),
        cfg.relay_addr,
        nostrdb_relay::AuthConfig {
            allowed_pubkeys: HashSet::from([cfg.drain_pubkey()?]),
            accept_events: false,
        },
    )?;

    let app = hookstrd::router(Ingest {
        ndb,
        seckey: cfg.seckey()?,
        token: cfg.ingest_token.clone(),
    });
    let listener = tokio::net::TcpListener::bind(&cfg.ingest_addr).await?;
    tracing::info!("ingest listening on http://{}", cfg.ingest_addr);
    axum::serve(listener, app).await?;
    Ok(())
}
