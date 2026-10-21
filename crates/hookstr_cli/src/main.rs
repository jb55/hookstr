//! hookstr_cli — dev-machine half of hookstr. Pull-only:
//!
//! negentropy-sync whatever we're missing from hookstrd's relay -> replay each
//! stored delivery against its configured target, oldest first. With
//! `--follow`, keep a live subscription open and replay new webhooks in
//! realtime as they arrive; the sync [`Session`] reconnects and re-runs the
//! backfill on its own when the connection drops.
//!
//! Trust boundary: the relay connection only feeds nostrdb's ingester, which
//! verifies before committing. Everything we act on — including the realtime
//! notifications in follow mode — is read back out of the local nostrdb, so
//! replay never trusts a wire frame. The relay has no auth and accepts writes,
//! so every filter is pinned to `server_pubkey`: only events hookstrd itself
//! signed are ever synced or acted on.
//!
//! `peek` runs the same sync but prints deliveries instead of replaying them,
//! and never writes replay state.
//!
//! Replay is at-least-once by design: the consumer dedupes on the provider's
//! delivery-id header, so crash-mid-replay is always safe.

use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use hookstr_core::WebhookRecord;
use nostrdb::{Config, Filter, Ndb, SubscriptionStream, Transaction};
use nostrdb_net::relay::sync::Session;
use std::time::{SystemTime, UNIX_EPOCH};

mod config;
mod replay_state;

use config::DrainConfig;
use replay_state::ReplayState;

/// Cap on the local replay query. Far above any realistic backlog; if it is
/// ever hit, the next run picks up the rest.
const MAX_LOCAL_QUERY: i32 = 1_000_000;
/// The sync session's subscription id for this drain's webhook set.
const SUBSCRIPTION_ID: &str = "hookstr";

/// This drain's webhook set: our dedicated kind, signed by hookstrd's key,
/// narrowed to the configured providers via the indexed `t` tag. Empty
/// `providers` means every provider.
fn webhook_filter(server_pubkey: &[u8; 32], providers: &[String]) -> Filter {
    let mut b = Filter::new()
        .kinds([hookstr_core::KIND_WEBHOOK as u64])
        .authors([server_pubkey]);
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
        /// Replay target base URL — deliveries POST to {target}/{path}. This
        /// is where the drain delivers, which is specific to the consumer it
        /// feeds, so it lives here rather than in the config. Explicit
        /// [targets] path overrides in the config still win; leave unset to
        /// route purely via those.
        #[arg(long)]
        target: Option<String>,
        /// Providers to drain, matched on each event's indexed `t` tag (the
        /// provider names from hookstrd's [ingest_tokens]). Empty = all;
        /// run one drain per consumer, each scoped to what it handles.
        providers: Vec<String>,
    },
    /// First-run setup: write a starter config under ~/.config/hookstr/,
    /// with the local mirror db under ~/.local/share/hookstr/. Refuses to
    /// overwrite an existing config.
    Init {
        /// hookstrd's relay endpoint as the drain reaches it, usually through
        /// a tunnel (default: ws://127.0.0.1:8081).
        relay_url: Option<String>,
        /// hookstrd's hex pubkey, which it logs at startup (placeholder
        /// written if omitted; the drain refuses to start until it's set).
        #[arg(long)]
        server_pubkey: Option<String>,
        /// Where to write the TOML config file
        /// (default: $XDG_CONFIG_HOME/hookstr/config.toml).
        #[arg(long)]
        config: Option<String>,
    },
    /// Sync webhooks down from the relay like `drain`, but print them to
    /// stdout instead of replaying them: path, event id, headers and body.
    /// Never POSTs and never touches replay state, so a later drain still
    /// delivers everything shown. Prints the undelivered backlog and exits;
    /// pass --follow to keep printing new webhooks as they arrive.
    Peek {
        /// Path to the TOML config file
        /// (default: $XDG_CONFIG_HOME/hookstr/config.toml).
        #[arg(long)]
        config: Option<String>,
        /// Stay connected and print new webhooks as they arrive.
        #[arg(long)]
        follow: bool,
        /// Show every stored webhook, including ones already replayed or
        /// waiting out a retry backoff, not just the ones a drain would send
        /// next.
        #[arg(long)]
        all: bool,
        /// Providers to show, matched on each event's indexed `t` tag.
        /// Empty = all.
        providers: Vec<String>,
    },
    /// Generate a keypair, for hookstrd's nsec_path. Its pubkey is the
    /// drain's server_pubkey.
    Keygen,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Default to info so drain progress (reconcile/replay/follow) is visible
    // without RUST_LOG; RUST_LOG still overrides when set. Logs go to stderr
    // so stdout carries only command output (peeked deliveries, keygen) and
    // stays pipeable.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
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
            cfg.target_base = target;
            // Nowhere to deliver = nothing to do. A bare --target or any
            // [targets] entry is enough; only the empty-both case is an error.
            anyhow::ensure!(
                cfg.target_base.is_some() || !cfg.targets.is_empty(),
                "no replay target: pass --target <url>, or add a [targets] entry to the config"
            );
            drain(cfg, !once).await
        }
        Command::Peek {
            config,
            follow,
            all,
            providers,
        } => {
            let mut cfg = DrainConfig::load(config.as_deref())?;
            cfg.providers = providers;
            peek(cfg, follow, all).await
        }
        Command::Init {
            relay_url,
            server_pubkey,
            config,
        } => init(relay_url, server_pubkey, config),
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

fn init(
    relay_url: Option<String>,
    server_pubkey: Option<String>,
    config: Option<String>,
) -> anyhow::Result<()> {
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

    let data_dir = config::default_data_dir()?;
    std::fs::create_dir_all(&data_dir)?;

    let relay_url = relay_url.unwrap_or_else(|| "ws://127.0.0.1:8081".to_owned());
    let server_pubkey = server_pubkey.unwrap_or_else(|| "CHANGE-ME".to_owned());
    std::fs::write(
        &cfg_path,
        format!(
            r#"# hookstr drain config. See config/hookstr.example.toml in the repo for
# every option (per-path replay targets, etc).
#
# hookstrd's relay has no auth, so it listens on localhost only; reach it
# over a tunnel, e.g. `ssh -NL 8081:127.0.0.1:8081 hooks.example.com`.
relay_url = "{relay_url}"
# hookstrd's pubkey (it logs it at startup). Only events it signed are
# synced or replayed.
server_pubkey = "{server_pubkey}"
db_path = "{db}"
redb_path = "{redb}"

# Where deliveries replay is a per-run flag, not config, since it's specific
# to whichever consumer this drain feeds:
#   hookstr drain --target http://localhost:3000/webhooks
# Deliveries are self-describing, so a delivery ingested at
# /ingest/<token>/acme/events then replays to {{target}}/acme/events.
#
# Optional: pin specific paths to other consumers (these win over --target).
# (A drain with neither --target nor a [targets] entry refuses to start.)
#[targets]
#"acme/events" = "http://localhost:4000/hooks"
"#,
            db = data_dir.join("ndb").display(),
            redb = data_dir.join("replays.redb").display(),
        ),
    )?;
    println!("wrote config {}", cfg_path.display());
    if server_pubkey == "CHANGE-ME" {
        println!("set its server_pubkey to the pubkey hookstrd logs at startup");
    }
    println!("then: hookstr drain --target http://localhost:3000/webhooks");
    Ok(())
}

/// What a run does with each stored delivery it finds.
enum Mode<'a> {
    /// `drain`: POST it to its target and record the outcome.
    Replay {
        state: &'a ReplayState,
        http: &'a reqwest::Client,
    },
    /// `peek`: print it. With a replay state, only deliveries a drain would
    /// send next are printed; without one (`--all`), every one is.
    Peek { state: Option<&'a ReplayState> },
}

async fn drain(cfg: DrainConfig, follow: bool) -> anyhow::Result<()> {
    let state = ReplayState::open(&cfg.redb_path)?;
    let http = reqwest::Client::new();
    run(
        &cfg,
        &Mode::Replay {
            state: &state,
            http: &http,
        },
        follow,
    )
    .await
}

async fn peek(cfg: DrainConfig, follow: bool, all: bool) -> anyhow::Result<()> {
    // --all never consults replay state, so it doesn't even open it. That
    // also makes it the one peek that works beside a running drain: redb
    // locks the file for whichever process opened it first.
    let state = if all {
        None
    } else {
        Some(ReplayState::open(&cfg.redb_path).map_err(|err| {
            anyhow::anyhow!(
                "opening replay state {}: {err:#} (if a drain is running, `peek --all` works without it)",
                cfg.redb_path
            )
        })?)
    };
    run(
        &cfg,
        &Mode::Peek {
            state: state.as_ref(),
        },
        follow,
    )
    .await
}

/// Sync the webhook set into the local mirror, handle the backlog, and in
/// follow mode keep handling new arrivals until the process is stopped.
///
/// The sync [`Session`] owns the relay connection: a NIP-77 negentropy
/// backfill of the webhook set (re-run on every reconnect, so gaps while
/// disconnected are filled) plus, when following, a live REQ. It ingests into
/// `ndb`; we only ever read back out of it.
async fn run(cfg: &DrainConfig, mode: &Mode<'_>, follow: bool) -> anyhow::Result<()> {
    let server_pubkey = cfg.server_pubkey()?;
    std::fs::create_dir_all(&cfg.db_path)?;
    let ndb = Ndb::new(&cfg.db_path, &Config::new())?;

    // The local subscription goes up *before* the session starts syncing, so
    // no arrival can fall between the backlog pass and the follow loop. It
    // fires only for verified, committed notes.
    let mut incoming = if follow {
        let sub = ndb.subscribe(&[webhook_filter(&server_pubkey, &cfg.providers)])?;
        Some(SubscriptionStream::new(ndb.clone(), sub).notes_per_await(1))
    } else {
        None
    };

    let session = Session::new(ndb.clone());
    let live = if follow {
        vec![webhook_filter(&server_pubkey, &cfg.providers)]
    } else {
        Vec::new()
    };
    session.set_subscription(
        SUBSCRIPTION_ID,
        cfg.relay_url.clone(),
        live,
        vec![webhook_filter(&server_pubkey, &cfg.providers)],
    );
    session.wait_for_sync().await;
    let status = session.backfill_status().borrow().clone();
    if status.failed {
        // One-shot: nothing to fall back on. Following: the session keeps
        // reconnecting and re-runs the backfill, so carry on with what's local.
        anyhow::ensure!(
            follow,
            "could not sync from {} (is the tunnel up?)",
            cfg.relay_url
        );
        tracing::warn!(
            "initial sync from {} failed; following anyway, it retries on reconnect",
            cfg.relay_url
        );
    } else if status.fetched > 0 {
        tracing::info!("synced {} missing webhook event(s)", status.fetched);
    }

    handle_backlog(cfg, &ndb, &server_pubkey, mode).await?;

    let Some(incoming) = incoming.as_mut() else {
        return Ok(());
    };
    tracing::info!("following: handling new webhooks as they arrive");
    while let Some(keys) = incoming.next().await {
        // Resolve keys to ids in a scoped (non-Send) txn, then handle.
        let ids: Vec<[u8; 32]> = {
            let txn = Transaction::new(&ndb)?;
            keys.iter()
                .filter_map(|key| ndb.get_note_by_key(&txn, *key).ok().map(|note| *note.id()))
                .collect()
        };
        for id in ids {
            handle_event(cfg, &ndb, mode, &id).await?;
        }
    }
    anyhow::bail!("local subscription ended")
}

/// Handle everything stored locally that is due, oldest first.
async fn handle_backlog(
    cfg: &DrainConfig,
    ndb: &Ndb,
    server_pubkey: &[u8; 32],
    mode: &Mode<'_>,
) -> anyhow::Result<()> {
    // Collected owned so the non-Send Transaction is gone before the awaits.
    let mut backlog = {
        let txn = Transaction::new(ndb)?;
        let filter = webhook_filter(server_pubkey, &cfg.providers);
        ndb.query(&txn, &[filter], MAX_LOCAL_QUERY)?
            .into_iter()
            .map(|result| (*result.note.id(), result.note.created_at()))
            .collect::<Vec<_>>()
    };
    backlog.sort_by_key(|(_, created_at)| *created_at);

    let (mut replayed, mut failed, mut unrouted, mut printed) = (0usize, 0usize, 0usize, 0usize);
    for (id, _) in &backlog {
        match handle_event(cfg, ndb, mode, id).await? {
            Outcome::Replayed => replayed += 1,
            Outcome::Failed => failed += 1,
            Outcome::NoTarget => unrouted += 1,
            Outcome::Printed => printed += 1,
            Outcome::NotDue => {}
        }
    }
    if let Mode::Peek { .. } = mode {
        tracing::info!(
            "peek complete: {printed} printed ({} stored total)",
            backlog.len()
        );
        return Ok(());
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
    /// Peek: printed.
    Printed,
    /// Already replayed, in backoff, or not (yet) in the local db.
    NotDue,
}

/// Handle one stored delivery if it's due: load it from the local db (the
/// only trusted source), then print it (peek) or route, POST and record it
/// (drain).
async fn handle_event(
    cfg: &DrainConfig,
    ndb: &Ndb,
    mode: &Mode<'_>,
    id: &[u8; 32],
) -> anyhow::Result<Outcome> {
    let due_state = match mode {
        Mode::Replay { state, .. } => Some(*state),
        Mode::Peek { state } => *state,
    };
    if let Some(state) = due_state {
        if !state.is_due(id, now_s())? {
            return Ok(Outcome::NotDue);
        }
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
    let Mode::Replay { state, http } = mode else {
        print_delivery(id, cfg.target_for(&rec.path).as_deref(), &rec)?;
        return Ok(Outcome::Printed);
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
            tracing::warn!(
                path = rec.path,
                id = hex::encode(id),
                "replay failed: {err:#}"
            );
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
    anyhow::ensure!(
        resp.status().is_success(),
        "target answered {}",
        resp.status()
    );
    Ok(())
}

/// Peek output: the delivery as its replay POST would carry it, under a
/// separator naming its path and event id. The POST line appears only when
/// the config's [targets] routes the path. A non-UTF-8 body prints as hex.
fn print_delivery(id: &[u8; 32], target: Option<&str>, rec: &WebhookRecord) -> anyhow::Result<()> {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    writeln!(
        out,
        "--- {} id={} received_at_ms={}",
        rec.path,
        hex::encode(id),
        rec.received_at_ms
    )?;
    if let Some(target) = target {
        writeln!(out, "POST {target}")?;
    }
    for (name, value) in &rec.headers {
        writeln!(out, "{name}: {value}")?;
    }
    writeln!(out)?;
    match std::str::from_utf8(&rec.body) {
        Ok(text) => writeln!(out, "{text}")?,
        Err(_) => writeln!(
            out,
            "<{}-byte non-UTF-8 body, hex> {}",
            rec.body.len(),
            hex::encode(&rec.body)
        )?,
    }
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

fn now_s() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before 1970")
        .as_secs()
}
