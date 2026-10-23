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
//! replay never trusts a wire frame. The relay has no auth; who can reach its
//! address is the access control.
//!
//! `peek` runs the same sync but prints deliveries instead of replaying them,
//! and never writes replay state.
//!
//! `drain --exec` delivers to a command instead of a URL, in batches; see
//! [`exec`].
//!
//! Replay is at-least-once by design: the consumer dedupes on the provider's
//! delivery-id header, so crash-mid-replay is always safe.

use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use hookstr_core::WebhookRecord;
use nostrdb::{Config, Filter, Ndb, SubscriptionStream, Transaction};
use nostrdb_net::relay::sync::Session;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod config;
mod exec;
mod replay_state;

use config::DrainConfig;
use exec::{Exec, Phase};
use replay_state::{Due, ReplayState};

/// Cap on the local replay query. Far above any realistic backlog; if it is
/// ever hit, the next run picks up the rest.
const MAX_LOCAL_QUERY: i32 = 1_000_000;
/// The sync session's subscription id for this drain's webhook set.
const SUBSCRIPTION_ID: &str = "hookstr";

/// This drain's webhook set: our dedicated kind, narrowed to the configured
/// providers via the indexed `t` tag. Empty `providers` means every provider.
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
        /// Replay target base URL — deliveries POST to {target}/{path}. This
        /// is where the drain delivers, which is specific to the consumer it
        /// feeds, so it lives here rather than in the config. Explicit
        /// [targets] path overrides in the config still win; leave unset to
        /// route purely via those.
        #[arg(long, conflicts_with = "exec")]
        target: Option<String>,
        #[command(flatten)]
        exec: ExecArgs,
        /// Providers to drain, matched on each event's indexed `t` tag (the
        /// provider names from hookstrd's [ingest_tokens]). At least one is
        /// required: an empty list would match every provider, so a bare
        /// `drain` would deliver (and ack) webhooks meant for other
        /// consumers. Run one drain per consumer, scoped to what it handles.
        #[arg(required = true, num_args = 1..)]
        providers: Vec<String>,
    },
    /// First-run setup: write a starter config under ~/.config/hookstr/,
    /// with the local mirror db under ~/.local/share/hookstr/. Refuses to
    /// overwrite an existing config.
    Init {
        /// hookstrd's relay endpoint as the drain reaches it, usually through
        /// a tunnel (default: ws://127.0.0.1:8081).
        relay_url: Option<String>,
        /// Where to write the TOML config file
        /// (default: $XDG_CONFIG_HOME/hookstr/config.toml).
        #[arg(long)]
        config: Option<String>,
    },
    /// Sync webhooks down from the relay like `drain`, but print them to
    /// stdout instead of replaying them: path, event id, headers and body.
    /// Never POSTs and never writes replay state, so a later drain still
    /// delivers everything shown. Prints the undelivered backlog and exits;
    /// pass --follow to keep printing new webhooks as they arrive. With
    /// --exec, runs a `drain --exec` consumer instead of printing, acks kept
    /// in memory for this run only.
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
        /// Print one JSON envelope per line instead of the readable form:
        /// exactly what `drain --exec` feeds its command, so `peek --all
        /// --json | ./consumer` tries a consumer out.
        #[arg(long, conflicts_with = "exec")]
        json: bool,
        #[command(flatten)]
        exec: ExecArgs,
        /// Providers to show, matched on each event's indexed `t` tag.
        /// Empty = all.
        providers: Vec<String>,
    },
    /// Generate a keypair, for hookstrd's nsec_path.
    Keygen,
}

/// `--exec` and its tuning flags, shared by `drain` and `peek`.
#[derive(clap::Args)]
struct ExecArgs {
    /// Deliver to this command instead of POSTing (drain) or printing
    /// (peek). It runs under `sh -c` once per batch, with the batch on stdin
    /// as NDJSON envelopes (one per line, oldest first; the same lines `peek
    /// --json` prints) and HOOKSTR_PHASE=backfill|live in its environment.
    /// Exit 0 acks the whole batch; a non-zero exit or a timeout retries the
    /// whole batch after the replay backoff. Batches run one at a time, in
    /// order. The config's [targets] aren't used. Under peek, acks are kept
    /// in memory and forgotten on exit, so a later drain still delivers
    /// everything.
    #[arg(long)]
    exec: Option<String>,
    // The tuning flags are Options, checked and defaulted in `into_exec`
    // rather than with clap's `requires` (which a default value or the
    // --target conflict can skip): a stray --batch-max must not quietly
    // start a URL drain.
    /// Most envelopes in one --exec batch [default: 500].
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    batch_max: Option<u32>,
    /// While following, gather webhooks for this long after one arrives
    /// and deliver them as one --exec batch (one provider action often
    /// fires several hooks at once). E.g. 500ms, 2s, 1m [default: 2s].
    #[arg(long, value_parser = exec::parse_duration)]
    batch_window: Option<Duration>,
    /// Kill an --exec run that takes longer than this and retry its
    /// batch. E.g. 30s, 5m [default: 60s].
    #[arg(long, value_parser = exec::parse_duration)]
    exec_timeout: Option<Duration>,
    /// Base of the retry backoff after a failed delivery, in seconds; it
    /// doubles per failed attempt up to 2^7 times. For tests.
    #[arg(long, default_value_t = replay_state::BACKOFF_BASE_SECS, hide = true)]
    retry_base_secs: u64,
}

impl ExecArgs {
    /// The [`Exec`] to run, or None without --exec. The tuning flags without
    /// --exec are an error.
    fn into_exec(self) -> anyhow::Result<Option<Exec>> {
        let Some(cmd) = self.exec else {
            anyhow::ensure!(
                self.batch_max.is_none()
                    && self.batch_window.is_none()
                    && self.exec_timeout.is_none(),
                "--batch-max, --batch-window and --exec-timeout only apply with --exec"
            );
            return Ok(None);
        };
        Ok(Some(Exec {
            cmd,
            batch_max: self.batch_max.unwrap_or(500) as usize,
            batch_window: self.batch_window.unwrap_or(Duration::from_secs(2)),
            timeout: self.exec_timeout.unwrap_or(Duration::from_secs(60)),
        }))
    }
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
            exec,
            providers,
        } => {
            let retry_base_secs = exec.retry_base_secs;
            let exec = exec.into_exec()?;
            let mut cfg = DrainConfig::load(config.as_deref())?;
            cfg.providers = providers;
            let state = ReplayState::open_with_backoff(&cfg.redb_path, retry_base_secs)?;
            if let Some(exec) = exec {
                let state = ExecState {
                    acks: &state,
                    delivered: None,
                };
                return drain_exec(&cfg, &state, &exec, !once).await;
            }
            cfg.target_base = target;
            // Nowhere to deliver = nothing to do. A bare --target or any
            // [targets] entry is enough; only the empty-both case is an error.
            anyhow::ensure!(
                cfg.target_base.is_some() || !cfg.targets.is_empty(),
                "no replay target: pass --target <url> or --exec <cmd>, or add a [targets] entry to the config"
            );
            drain(cfg, state, !once).await
        }
        Command::Peek {
            config,
            follow,
            all,
            json,
            exec,
            providers,
        } => {
            let retry_base_secs = exec.retry_base_secs;
            let exec = exec.into_exec()?;
            let mut cfg = DrainConfig::load(config.as_deref())?;
            cfg.providers = providers;
            if let Some(exec) = exec {
                return peek_exec(&cfg, &exec, follow, all, retry_base_secs).await;
            }
            peek(cfg, follow, all, json).await
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

    let data_dir = config::default_data_dir()?;
    std::fs::create_dir_all(&data_dir)?;

    let relay_url = relay_url.unwrap_or_else(|| "ws://127.0.0.1:8081".to_owned());
    std::fs::write(
        &cfg_path,
        format!(
            r#"# hookstr drain config. See config/hookstr.example.toml in the repo for
# every option (per-path replay targets, etc).
#
# hookstrd's relay has no auth, so it listens on localhost (or a private
# interface) only; reach it over a tunnel or VPN, e.g.
# `ssh -NL 8081:127.0.0.1:8081 hooks.example.com`.
relay_url = "{relay_url}"
db_path = "{db}"
redb_path = "{redb}"

# Where deliveries replay is a per-run flag, not config, since it's specific
# to whichever consumer this drain feeds:
#   hookstr drain --target http://localhost:3000/webhooks <provider>
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
    println!("then: hookstr drain --target http://localhost:3000/webhooks <provider>");
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
    /// send next are printed; without one (`--all`), every one is. `json`
    /// prints [`exec::envelope`] lines instead of the readable form.
    Peek {
        state: Option<&'a ReplayState>,
        json: bool,
    },
}

async fn drain(cfg: DrainConfig, state: ReplayState, follow: bool) -> anyhow::Result<()> {
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

async fn peek(cfg: DrainConfig, follow: bool, all: bool, json: bool) -> anyhow::Result<()> {
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
            json,
        },
        follow,
    )
    .await
}

/// `peek --exec`: run an exec consumer exactly as `drain --exec` would, but
/// with its acks and failures in an in-memory replay state that dies with
/// the process. Without --all, the real replay state is read (never written)
/// so webhooks a drain already delivered are skipped, as plain peek skips
/// them.
async fn peek_exec(
    cfg: &DrainConfig,
    exec: &Exec,
    follow: bool,
    all: bool,
    retry_base_secs: u64,
) -> anyhow::Result<()> {
    let delivered = if all {
        None
    } else {
        Some(ReplayState::open(&cfg.redb_path).map_err(|err| {
            anyhow::anyhow!(
                "opening replay state {}: {err:#} (if a drain is running, `peek --all --exec` works without it)",
                cfg.redb_path
            )
        })?)
    };
    let acks = ReplayState::in_memory(retry_base_secs)?;
    let state = ExecState {
        acks: &acks,
        delivered: delivered.as_ref(),
    };
    drain_exec(cfg, &state, exec, follow).await
}

/// Where an exec run reads what's due and records its acks.
struct ExecState<'a> {
    /// Takes every ack and failure: the real replay state for a drain, an
    /// in-memory one for peek.
    acks: &'a ReplayState,
    /// Peek only: the real replay state, read and never written. A webhook
    /// it doesn't call due now (replayed, or backing off) isn't delivered.
    delivered: Option<&'a ReplayState>,
}

impl ExecState<'_> {
    fn due(&self, id: &[u8; 32], now: u64) -> anyhow::Result<Due> {
        if let Some(delivered) = self.delivered {
            match delivered.due(id, now)? {
                Due::Now => {}
                not_now => return Ok(not_now),
            }
        }
        self.acks.due(id, now)
    }
}

/// The local mirror after the initial sync, plus, when following, the
/// stream of new arrivals. Holds the sync session, which keeps syncing for
/// as long as this lives.
struct Synced {
    ndb: Ndb,
    incoming: Option<SubscriptionStream>,
    _session: Session,
}

/// Sync the webhook set into the local mirror, handle the backlog, and in
/// follow mode keep handling new arrivals until the process is stopped.
async fn run(cfg: &DrainConfig, mode: &Mode<'_>, follow: bool) -> anyhow::Result<()> {
    // Bind the session by name: `..` would drop it here and stop syncing.
    let Synced {
        ndb,
        mut incoming,
        _session,
    } = sync(cfg, follow).await?;

    handle_backlog(cfg, &ndb, mode).await?;

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

/// Open the local mirror and run the initial sync.
///
/// The sync [`Session`] owns the relay connection: a NIP-77 negentropy
/// backfill of the webhook set (re-run on every reconnect, so gaps while
/// disconnected are filled) plus, when following, a live REQ. It ingests into
/// `ndb`; we only ever read back out of it.
async fn sync(cfg: &DrainConfig, follow: bool) -> anyhow::Result<Synced> {
    std::fs::create_dir_all(&cfg.db_path)?;
    let ndb = Ndb::new(&cfg.db_path, &Config::new())?;

    // The local subscription goes up *before* the session starts syncing, so
    // no arrival can fall between the backlog pass and the follow loop. It
    // fires only for verified, committed notes.
    let incoming = if follow {
        let sub = ndb.subscribe(&[webhook_filter(&cfg.providers)])?;
        Some(SubscriptionStream::new(ndb.clone(), sub).notes_per_await(1))
    } else {
        None
    };

    let session = Session::new(ndb.clone());
    let live = if follow {
        vec![webhook_filter(&cfg.providers)]
    } else {
        Vec::new()
    };
    session.set_subscription(
        SUBSCRIPTION_ID,
        cfg.relay_url.clone(),
        live,
        vec![webhook_filter(&cfg.providers)],
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
    Ok(Synced {
        ndb,
        incoming,
        _session: session,
    })
}

/// `drain --exec`: deliver everything due in batches, then, when following,
/// deliver again whenever webhooks arrive or a failed batch's backoff ends.
///
/// Every pass re-reads what's due from the local mirror and replay state,
/// so arrivals, retries and the backlog all go through the one path, oldest
/// first. A failed batch ends its pass: later batches wait for the next one,
/// which keeps them behind it.
async fn drain_exec(
    cfg: &DrainConfig,
    state: &ExecState<'_>,
    exec: &Exec,
    follow: bool,
) -> anyhow::Result<()> {
    let Synced {
        ndb,
        mut incoming,
        _session,
    } = sync(cfg, follow).await?;
    // Every webhook already stored and not yet delivered when the run
    // starts: a batch holding any of them is a backfill batch. Filled by the
    // first pass, drained as they're acked.
    let mut backlog = None;
    loop {
        let next_retry = exec_pass(cfg, &ndb, state, exec, &mut backlog).await?;
        let Some(incoming) = incoming.as_mut() else {
            return Ok(());
        };
        let retry = async {
            match next_retry {
                Some(at) => {
                    tokio::time::sleep(Duration::from_secs(at.saturating_sub(now_s()))).await
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            keys = incoming.next() => {
                anyhow::ensure!(keys.is_some(), "local subscription ended");
                // Gather whatever else lands within the window into this
                // batch. The window is fixed from the first arrival, so a
                // steady trickle can't hold a batch back forever.
                let deadline = tokio::time::Instant::now() + exec.batch_window;
                while let Ok(Some(_)) = tokio::time::timeout_at(deadline, incoming.next()).await {}
            }
            () = retry => {}
        }
    }
}

/// Deliver everything due, oldest first, in batches of at most
/// `exec.batch_max`, stopping at the first batch that fails. Returns the
/// earliest backoff deadline among failed deliveries, if any.
///
/// `backlog` is None before the first pass, which fills it with every
/// undelivered webhook it finds, due now or backing off. A batch is
/// [`Phase::Backfill`] while it holds any of them and [`Phase::Live`]
/// otherwise, so a retried backlog batch stays backfill and an arrival
/// delivered while it backs off goes out live.
async fn exec_pass(
    cfg: &DrainConfig,
    ndb: &Ndb,
    state: &ExecState<'_>,
    exec: &Exec,
    backlog: &mut Option<std::collections::HashSet<[u8; 32]>>,
) -> anyhow::Result<Option<u64>> {
    let now = now_s();
    let mut next_retry = None::<u64>;
    let mut due = Vec::new();
    let mut pending = std::collections::HashSet::new();
    {
        let txn = Transaction::new(ndb)?;
        for result in ndb.query(&txn, &[webhook_filter(&cfg.providers)], MAX_LOCAL_QUERY)? {
            let id = *result.note.id();
            match state.due(&id, now)? {
                Due::Now => {
                    if let Ok(rec) = hookstr_core::parse_webhook_note(&result.note) {
                        pending.insert(id);
                        due.push((id, rec));
                    }
                }
                Due::Later(at) => {
                    pending.insert(id);
                    next_retry = Some(next_retry.map_or(at, |n| n.min(at)));
                }
                Due::Never => {}
            }
        }
    }
    let backlog = backlog.get_or_insert(pending);
    // received_at_ms is hookstrd's receipt clock, finer than created_at.
    due.sort_by(|(a_id, a), (b_id, b)| (a.received_at_ms, a_id).cmp(&(b.received_at_ms, b_id)));

    for batch in due.chunks(exec.batch_max) {
        let input: String = batch
            .iter()
            .map(|(id, rec)| exec::envelope_line(id, rec))
            .collect();
        let ids: Vec<[u8; 32]> = batch.iter().map(|(id, _)| *id).collect();
        let phase = if ids.iter().any(|id| backlog.contains(id)) {
            Phase::Backfill
        } else {
            Phase::Live
        };
        match exec::run_batch(exec, phase, input.as_bytes()).await {
            Ok(()) => {
                state.acks.mark_replayed_all(&ids)?;
                tracing::info!(phase = phase.as_str(), "delivered a batch of {}", ids.len());
                if phase == Phase::Backfill {
                    for id in &ids {
                        backlog.remove(id);
                    }
                    if backlog.is_empty() {
                        tracing::info!("caught up: every batch from here on is live");
                    }
                }
            }
            Err(err) => {
                if let Some(at) = state.acks.mark_failed_all(&ids, now_s())? {
                    next_retry = Some(next_retry.map_or(at, |n| n.min(at)));
                }
                tracing::warn!(
                    phase = phase.as_str(),
                    "batch of {} failed, retrying in {}s: {err:#}",
                    ids.len(),
                    next_retry.unwrap_or(0).saturating_sub(now_s())
                );
                return Ok(next_retry);
            }
        }
    }
    Ok(next_retry)
}

/// Handle everything stored locally that is due, oldest first.
async fn handle_backlog(cfg: &DrainConfig, ndb: &Ndb, mode: &Mode<'_>) -> anyhow::Result<()> {
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
        Mode::Peek { state, .. } => *state,
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
        if let Mode::Peek { json: true, .. } = mode {
            use std::io::Write;
            let mut out = std::io::stdout().lock();
            out.write_all(exec::envelope_line(id, &rec).as_bytes())?;
            out.flush()?;
        } else {
            print_delivery(id, cfg.target_for(&rec.path).as_deref(), &rec)?;
        }
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
