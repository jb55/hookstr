//! hookstr_cli — dev-machine half of hookstr. Pull-only:
//!
//! connect -> NIP-42 AUTH -> sync down whatever we're missing -> replay each
//! stored delivery against its configured target, oldest first. With
//! `--follow`, stay connected and replay new webhooks in realtime as they
//! arrive, reconnecting (with backoff) when the connection drops.
//!
//! Trust boundary: the relay connection only feeds nostrdb's ingester, which
//! verifies before committing. Everything we act on — including the realtime
//! notifications in follow mode — is read back out of the local nostrdb, so
//! replay never trusts a wire frame.
//!
//! Milestone 1 syncs with a dumb `until`-paginated REQ; negentropy reconcile
//! replaces the catch-up in milestone 2 (see ../../SPIKE.md).
//!
//! Replay is at-least-once by design: the consumer dedupes on the provider's
//! delivery-id header, so crash-mid-replay is always safe.

use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use hookstr_core::WebhookRecord;
use nostrdb::{Config, Filter, Ndb, SubscriptionStream, Transaction};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod config;
mod replay_state;

use config::DrainConfig;
use replay_state::ReplayState;

/// Drain page size; matches nostrdb_relay's per-REQ stored-replay cap.
const PAGE: usize = 500;
/// Cap on the local replay query. Far above any realistic backlog; if it is
/// ever hit, the next run picks up the rest.
const MAX_LOCAL_QUERY: i32 = 1_000_000;
/// How long `--follow` waits before redialing a dropped connection.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// This drain's webhook set: our dedicated kind, narrowed to the configured
/// providers via the indexed `t` tag. Empty `providers` means every provider
/// (kind-only). Use `.json()` for the wire form on REQ / reconcile.
fn webhook_filter(providers: &[String]) -> Filter {
    let mut b = Filter::new().kinds([hookstr_core::KIND_WEBHOOK as u64]);
    if !providers.is_empty() {
        b = b.tags(providers.iter().map(String::as_str), 't');
    }
    b.build()
}

#[derive(Parser)]
#[command(about = "hookstr drain: sync stored webhooks and replay them locally")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Sync webhooks down from the relay, replay whatever is missing, then
    /// keep following: new webhooks replay in realtime as they arrive, and a
    /// dropped connection redials. Pass --once for a single catch-up pass
    /// (cron-style) instead.
    Drain {
        /// Path to the TOML config file
        /// (default: $XDG_CONFIG_HOME/hookstr/config.toml).
        #[arg(long)]
        config: Option<String>,
        /// Exit after catch-up + replay instead of following.
        #[arg(long)]
        once: bool,
        /// Replay target base URL for this run — deliveries POST to
        /// {target}/{path} — overriding the config's target_base (explicit
        /// [targets] path overrides still win).
        #[arg(long)]
        target: Option<String>,
        /// Providers to drain, matched on each event's indexed `t` tag (the
        /// provider names from hookstrd's [ingest_tokens]). Empty = all;
        /// run one drain per consumer, each scoped to what it handles.
        providers: Vec<String>,
    },
    /// First-run setup: generate the drain keypair and write it plus a
    /// starter config under ~/.config/hookstr/, with the local mirror db
    /// under ~/.local/share/hookstr/. Prints the pubkey that goes in
    /// hookstrd's drain_pubkey allowlist. Refuses to overwrite an existing
    /// config; an existing keypair is kept.
    Init {
        /// hookstrd's relay endpoint, e.g. wss://hooks.example.com
        /// (placeholder written if omitted).
        relay_url: Option<String>,
        /// Where to write the TOML config file
        /// (default: $XDG_CONFIG_HOME/hookstr/config.toml).
        #[arg(long)]
        config: Option<String>,
    },
    /// Generate a keypair (for either side; the drain's pubkey goes in
    /// hookstrd's allowlist).
    Keygen,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Default to info so drain progress (reconcile/replay/follow) is visible
    // without RUST_LOG; RUST_LOG still overrides when set.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    match Args::parse().command {
        Command::Drain {
            config,
            once,
            target,
            providers,
        } => {
            let mut cfg = DrainConfig::load(config.as_deref())?;
            cfg.providers = providers;
            if target.is_some() {
                cfg.target_base = target;
            }
            drain(cfg, !once).await
        }
        Command::Init { relay_url, config } => init(relay_url, config),
        Command::Keygen => keygen(),
    }
}

/// Fresh keypair as (nsec, hex pubkey).
fn generate_keypair() -> anyhow::Result<(String, String)> {
    let secp = secp256k1::Secp256k1::new();
    let (seckey, pubkey) = secp.generate_keypair(&mut secp256k1::rand::rngs::OsRng);
    let nsec = bech32::encode::<bech32::Bech32>(
        bech32::Hrp::parse_unchecked("nsec"),
        &seckey.secret_bytes(),
    )?;
    Ok((nsec, hex::encode(pubkey.x_only_public_key().0.serialize())))
}

fn keygen() -> anyhow::Result<()> {
    let (nsec, pubkey) = generate_keypair()?;
    println!("nsec:   {nsec}");
    println!("pubkey: {pubkey}");
    Ok(())
}

fn init(relay_url: Option<String>, config: Option<String>) -> anyhow::Result<()> {
    let cfg_path = match config {
        Some(path) => std::path::PathBuf::from(path),
        None => config::default_path()?,
    };
    anyhow::ensure!(
        !cfg_path.exists(),
        "{} already exists; delete it first to re-init",
        cfg_path.display()
    );
    let cfg_dir = cfg_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} has no parent directory", cfg_path.display()))?;
    std::fs::create_dir_all(cfg_dir)?;

    // Keep an existing keypair: it may already be on hookstrd's allowlist.
    let nsec_path = cfg_dir.join("drain.nsec");
    let pubkey = if nsec_path.exists() {
        let nsec = std::fs::read_to_string(&nsec_path)?;
        let (_seckey, pubkey) = nostr_relay_sync::parse_nsec(nsec.trim())
            .map_err(|e| anyhow::anyhow!("{}: {e}", nsec_path.display()))?;
        println!("keeping existing keypair {}", nsec_path.display());
        hex::encode(pubkey)
    } else {
        let (nsec, pubkey) = generate_keypair()?;
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&nsec_path)?
            .write_all(format!("{nsec}\n").as_bytes())?;
        println!("wrote keypair {}", nsec_path.display());
        pubkey
    };

    let data_dir = config::default_data_dir()?;
    std::fs::create_dir_all(&data_dir)?;

    let relay_url = relay_url.unwrap_or_else(|| "wss://hooks.example.com".to_owned());
    std::fs::write(
        &cfg_path,
        format!(
            r#"# hookstr drain config. See config/hookstr.example.toml in the repo for
# every option (per-path replay targets, etc).
relay_url = "{relay_url}"
db_path = "{db}"
redb_path = "{redb}"
nsec_path = "{nsec}"

# Deliveries are self-describing: a delivery ingested at
# /ingest/<token>/acme/events replays to {{target_base}}/acme/events.
target_base = "http://localhost:3000/webhooks"
"#,
            db = data_dir.join("ndb").display(),
            redb = data_dir.join("replays.redb").display(),
            nsec = nsec_path.display(),
        ),
    )?;
    println!("wrote config {}", cfg_path.display());
    println!();
    println!("pubkey: {pubkey}");
    println!("        ^ add this to hookstrd's drain_pubkey allowlist");
    println!("edit the config's relay_url / target_base, then run `hookstr drain`");
    Ok(())
}

async fn drain(cfg: DrainConfig, follow: bool) -> anyhow::Result<()> {
    let seckey = cfg.seckey()?;
    let state = ReplayState::open(&cfg.redb_path)?;
    std::fs::create_dir_all(&cfg.db_path)?;
    let ndb = Ndb::new(&cfg.db_path, &Config::new())?;
    let http = reqwest::Client::new();

    loop {
        match connect_and_drain(&cfg, &seckey, &state, &ndb, &http, follow).await {
            // One-shot ran to completion. (Follow mode only returns Err.)
            Ok(()) => return Ok(()),
            // An auth refusal is permanent (wrong key / not on the
            // allowlist) — reconnecting would just spin.
            Err(err) if follow && !format!("{err:#}").contains("refused auth") => {
                tracing::warn!("connection lost: {err:#}; reconnecting in {RECONNECT_DELAY:?}");
                tokio::time::sleep(RECONNECT_DELAY).await;
            }
            Err(err) => return Err(err),
        }
    }
}

/// One connection's worth of work: catch up, replay the backlog, and (in
/// follow mode) hold the standing subscription until the connection dies.
async fn connect_and_drain(
    cfg: &DrainConfig,
    seckey: &[u8; 32],
    state: &ReplayState,
    ndb: &Ndb,
    http: &reqwest::Client,
    follow: bool,
) -> anyhow::Result<()> {
    let mut relay = nostr_relay_sync::Relay::connect(&cfg.relay_url)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    relay
        .authenticate(seckey)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    catch_up(&cfg.providers, &mut relay, ndb).await?;

    // Follow plumbing goes up *before* the backlog replay so no arrival can
    // fall between the two. Notifications come off the local nostrdb
    // subscription — it fires only for verified, committed notes; the
    // standing REQ merely feeds the ingester. Its stored phase re-sends
    // recent events, which both plugs the gap since the last catch-up page
    // and is harmlessly deduped by ndb otherwise.
    let mut incoming = if follow {
        let filter = webhook_filter(&cfg.providers);
        let wire = filter.json().map_err(|e| anyhow::anyhow!("{e}"))?;
        let sub = ndb.subscribe(&[filter])?;
        let stream = SubscriptionStream::new(ndb.clone(), sub).notes_per_await(1);

        relay
            .stream_into(ndb, &wire)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Some(stream)
    } else {
        None
    };

    replay_backlog(cfg, state, ndb, http).await?;

    let Some(incoming) = incoming.as_mut() else {
        return Ok(());
    };
    tracing::info!("following: replaying new webhooks as they arrive");
    loop {
        tokio::select! {
            pumped = relay.pump_one(ndb) => {
                pumped.map_err(|e| anyhow::anyhow!("{e}"))?;
            }
            keys = incoming.next() => {
                let Some(keys) = keys else {
                    anyhow::bail!("local subscription ended");
                };
                // Resolve keys to ids in a scoped (non-Send) txn, then replay.
                let ids: Vec<[u8; 32]> = {
                    let txn = Transaction::new(ndb)?;
                    keys.iter()
                        .filter_map(|key| ndb.get_note_by_key(&txn, *key).ok().map(|note| *note.id()))
                        .collect()
                };
                for id in ids {
                    replay_event(cfg, state, ndb, http, &id).await?;
                }
            }
        }
    }
}

/// Pull whatever the relay has that we don't: negentropy reconcile —
/// O(difference) on the wire — then fetch the missing events by id.
/// Pull-only: `Diff::have` is ignored (hookstrd's db is the source of truth;
/// nothing pushes into it from here).
async fn catch_up(
    providers: &[String],
    relay: &mut nostr_relay_sync::Relay,
    ndb: &Ndb,
) -> anyhow::Result<()> {
    let filter = webhook_filter(providers);
    let wire = filter.json().map_err(|e| anyhow::anyhow!("{e}"))?;
    let storage =
        nostr_relay_sync::local_set(ndb, &filter).map_err(|e| anyhow::anyhow!("{e}"))?;

    let diff = relay
        .reconcile(&wire, storage)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Fetch missing events by id, chunked under the relay's per-REQ
    // stored-replay cap.
    for chunk in diff.need.chunks(PAGE) {
        let ids: Vec<String> = chunk.iter().map(hex::encode).collect();
        let received = relay
            .sync_into(ndb, &json!({ "ids": ids }).to_string())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        nostr_relay_sync::await_ingest(ndb, &received).await;
    }
    if !diff.need.is_empty() {
        tracing::info!("reconciled {} missing webhook event(s)", diff.need.len());
    }
    Ok(())
}

/// Replay everything stored locally that is due, oldest first.
async fn replay_backlog(
    cfg: &DrainConfig,
    state: &ReplayState,
    ndb: &Ndb,
    http: &reqwest::Client,
) -> anyhow::Result<()> {
    // Collected owned so the non-Send Transaction is gone before the awaits.
    let mut backlog = {
        let txn = Transaction::new(ndb)?;
        let filter = webhook_filter(&cfg.providers);
        ndb.query(&txn, &[filter], MAX_LOCAL_QUERY)?
            .into_iter()
            .map(|result| (*result.note.id(), result.note.created_at()))
            .collect::<Vec<_>>()
    };
    backlog.sort_by_key(|(_, created_at)| *created_at);

    let (mut replayed, mut failed, mut unrouted) = (0usize, 0usize, 0usize);
    for (id, _) in &backlog {
        match replay_event(cfg, state, ndb, http, id).await? {
            Outcome::Replayed => replayed += 1,
            Outcome::Failed => failed += 1,
            Outcome::NoTarget => unrouted += 1,
            Outcome::NotDue => {}
        }
    }
    tracing::info!(
        "drain complete: {replayed} replayed, {failed} failed, {unrouted} with no target ({} stored total)",
        backlog.len()
    );
    Ok(())
}

enum Outcome {
    Replayed,
    Failed,
    NoTarget,
    /// Already replayed, in backoff, or not (yet) in the local db.
    NotDue,
}

/// Replay one stored delivery if it's due: load it from the local db (the
/// only trusted source), route it, POST it, record the outcome.
async fn replay_event(
    cfg: &DrainConfig,
    state: &ReplayState,
    ndb: &Ndb,
    http: &reqwest::Client,
    id: &[u8; 32],
) -> anyhow::Result<Outcome> {
    if !state.is_due(id, now_s())? {
        return Ok(Outcome::NotDue);
    }
    let rec = {
        let txn = Transaction::new(ndb)?;
        ndb.get_note_by_id(&txn, id)
            .ok()
            .and_then(|note| hookstr_core::parse_webhook_note(&note).ok())
    };
    let Some(rec) = rec else {
        return Ok(Outcome::NotDue);
    };
    let Some(target) = cfg.target_for(&rec.path) else {
        // Unroutable provider path: leave unreplayed so a later config
        // change picks it up.
        return Ok(Outcome::NoTarget);
    };

    match replay(http, &target, &rec).await {
        Ok(()) => {
            state.mark_replayed(id)?;
            tracing::info!(path = rec.path, id = hex::encode(id), "replayed");
            Ok(Outcome::Replayed)
        }
        Err(err) => {
            state.mark_failed(id, now_s())?;
            tracing::warn!(path = rec.path, id = hex::encode(id), "replay failed: {err:#}");
            Ok(Outcome::Failed)
        }
    }
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

fn now_s() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before 1970")
        .as_secs()
}
