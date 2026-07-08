//! hookstr_cli — dev-machine half of hookstr. Pull-only:
//!
//! connect -> NIP-42 AUTH -> sync down whatever we're missing -> replay each
//! stored delivery against its configured target, oldest first.
//!
//! Milestone 1 syncs with a dumb `until`-paginated REQ; negentropy reconcile
//! plus a live-tail subscription replace it in milestone 2 (see ../../SPIKE.md).
//!
//! Replay is at-least-once by design: the consumer dedupes on the provider's
//! delivery-id header, so crash-mid-replay is always safe.

use clap::{Parser, Subcommand};
use hookstr_core::WebhookRecord;
use nostrdb::{Config, Filter, Ndb, Transaction};
use serde_json::json;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

mod config;
mod replay_state;

use config::DrainConfig;
use replay_state::ReplayState;

/// Drain page size; matches nostrdb_relay's per-REQ stored-replay cap.
const PAGE: usize = 500;
/// Cap on the local replay query. Far above any realistic backlog; if it is
/// ever hit, the next run picks up the rest.
const MAX_LOCAL_QUERY: i32 = 1_000_000;

#[derive(Parser)]
#[command(about = "hookstr drain: sync stored webhooks and replay them locally")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Sync webhooks down from the relay and replay them against the
    /// configured targets.
    Drain {
        /// Path to the TOML config file.
        #[arg(long, default_value = "hookstr_cli.toml")]
        config: String,
    },
    /// Generate a keypair (for either side; the drain's pubkey goes in
    /// hookstrd's allowlist).
    Keygen,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    match Args::parse().command {
        Command::Drain { config } => drain(DrainConfig::load(&config)?).await,
        Command::Keygen => keygen(),
    }
}

fn keygen() -> anyhow::Result<()> {
    let keypair = enostr::FullKeypair::generate();
    let nsec = bech32::encode::<bech32::Bech32>(
        bech32::Hrp::parse_unchecked("nsec"),
        &keypair.secret_key.to_secret_bytes(),
    )?;
    println!("nsec:   {nsec}");
    println!("pubkey: {}", keypair.pubkey.hex());
    Ok(())
}

async fn drain(cfg: DrainConfig) -> anyhow::Result<()> {
    let seckey = cfg.seckey()?;
    let state = ReplayState::open(&cfg.redb_path)?;
    std::fs::create_dir_all(&cfg.db_path)?;
    let ndb = Ndb::new(&cfg.db_path, &Config::new())?;

    let mut relay = relay_sync::Relay::connect(&cfg.relay_url)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    relay
        .authenticate(&seckey)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Milestone-1 dumb drain: page backwards with `until` past the relay's
    // stored-replay cap. Pages overlap on the boundary timestamp so nothing
    // is skipped — ndb dedupes re-ingests, and `seen` detects the page that
    // brings nothing new (the loop's only exit).
    let mut seen: HashSet<[u8; 32]> = HashSet::new();
    let mut until = now_s() + 600;
    loop {
        let filter = json!({
            "kinds": [hookstr_core::KIND_WEBHOOK],
            "until": until,
            "limit": PAGE,
        });
        let received = relay
            .sync_into(&ndb, &filter.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let fresh = received.iter().filter(|id| seen.insert(**id)).count();
        if fresh == 0 {
            break;
        }
        relay_sync::await_ingest(&ndb, &received).await;
        until = oldest_created_at(&ndb, &received).unwrap_or(0);
        tracing::info!("synced {fresh} new webhook event(s)");
    }

    // Everything stored locally, oldest first. Collected owned so the
    // non-Send Transaction is gone before the replay awaits below.
    let mut deliveries = {
        let txn = Transaction::new(&ndb)?;
        let filter = Filter::new()
            .kinds([hookstr_core::KIND_WEBHOOK as u64])
            .build();
        ndb.query(&txn, &[filter], MAX_LOCAL_QUERY)?
            .into_iter()
            .filter_map(|result| {
                let rec = hookstr_core::parse_webhook_note(&result.note).ok()?;
                Some((*result.note.id(), rec))
            })
            .collect::<Vec<_>>()
    };
    deliveries.sort_by_key(|(_, rec)| rec.received_at_ms);

    let http = reqwest::Client::new();
    let (mut replayed, mut failed, mut unmapped) = (0usize, 0usize, 0usize);
    for (id, rec) in &deliveries {
        if !state.is_due(id, now_s())? {
            continue;
        }
        let Some(target) = cfg.target_for(&rec.path) else {
            // Unroutable provider path: leave unreplayed so a later config
            // change picks it up.
            unmapped += 1;
            continue;
        };
        match replay(&http, &target, rec).await {
            Ok(()) => {
                state.mark_replayed(id)?;
                replayed += 1;
                tracing::info!(path = rec.path, id = hex::encode(id), "replayed");
            }
            Err(err) => {
                state.mark_failed(id, now_s())?;
                failed += 1;
                tracing::warn!(path = rec.path, id = hex::encode(id), "replay failed: {err:#}");
            }
        }
    }
    tracing::info!(
        "drain complete: {replayed} replayed, {failed} failed, {unmapped} with no target ({} stored total)",
        deliveries.len()
    );
    Ok(())
}

/// POST the delivery to its target: byte-exact body, preserved headers. Any
/// 2xx from the consumer counts as replayed.
async fn replay(http: &reqwest::Client, target: &str, rec: &WebhookRecord) -> anyhow::Result<()> {
    let mut req = http.post(target);
    for (name, value) in &rec.headers {
        req = req.header(name, value);
    }
    let resp = req.body(rec.body.clone()).send().await?;
    anyhow::ensure!(resp.status().is_success(), "target answered {}", resp.status());
    Ok(())
}

/// Smallest created_at among `ids`, read back from the local db (`sync_into`
/// returns only ids). The non-Send Transaction stays scoped here.
fn oldest_created_at(ndb: &Ndb, ids: &[[u8; 32]]) -> Option<u64> {
    let txn = Transaction::new(ndb).ok()?;
    ids.iter()
        .filter_map(|id| ndb.get_note_by_id(&txn, id).ok().map(|note| note.created_at()))
        .min()
}

fn now_s() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before 1970")
        .as_secs()
}
