//! End-to-end: the real hookstrd ingest router + relay run in-process, and the
//! real `hookstr` binary drains against them — provider POST -> durable 204 ->
//! negentropy drain -> byte-exact replay to a local target, plus redb dedupe,
//! realtime follow, and peek.

use std::collections::HashMap;
use std::time::Duration;

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use nostrdb::{Config, Ndb};
use tokio::sync::mpsc;

const SERVER_SECRET: [u8; 32] = [0x42; 32];
const TOKEN: &str = "test-ingest-token";

/// One replayed delivery as the target saw it: `{provider}/{type}` route,
/// headers, and the raw body.
type Delivery = (String, HashMap<String, String>, Vec<u8>);

/// The whole deployment, minus the CLI: the real hookstrd ingest server and
/// relay over one nostrdb, a capture target for replays, and the config file
/// the drain binary will run with.
struct World {
    tmp: tempfile::TempDir,
    /// hookstrd's ingest HTTP port.
    port: u16,
    /// Keeps the relay up for the test's lifetime.
    _relay: nostrdb_net::relay::server::RelayHandle,
    deliveries: mpsc::UnboundedReceiver<Delivery>,
    cfg_path: std::path::PathBuf,
    /// The capture target's base URL, passed to the drain via `--target`.
    target_url: String,
}

impl World {
    async fn start() -> World {
        let tmp = tempfile::tempdir().expect("tempdir");

        let db_path = tmp.path().join("server-db");
        std::fs::create_dir_all(&db_path).expect("db dir");
        let ndb = Ndb::new(db_path.to_str().unwrap(), &Config::new()).expect("ndb");

        // The real hookstrd: ingest (http) on one listener, the relay (ws) on
        // its own, both over the one nostrdb, as `hookstrd` runs in production.
        let ingest = hookstrd::Ingest {
            ndb: ndb.clone(),
            seckey: SERVER_SECRET,
            tokens: HashMap::from([("acme".to_owned(), TOKEN.to_owned())]),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { hookstrd::serve(listener, ingest).await.unwrap() });
        let relay =
            hookstrd::spawn_relay(ndb.clone(), "127.0.0.1:0".parse().unwrap()).expect("relay");

        // The replay target: capture `{provider}/{type}` + headers + body and
        // answer 200, mirroring the ingest path shape under /hook.
        let (tx, deliveries) = mpsc::unbounded_channel();
        let target = Router::new()
            .route("/hook/{provider}/{type}", post(capture))
            .with_state(tx);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind target");
        let target_port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, target).await.unwrap() });

        let target_url = format!("http://127.0.0.1:{target_port}/hook");
        let cfg_path = tmp.path().join("hookstr.toml");
        std::fs::write(
            &cfg_path,
            format!(
                r#"relay_url = "{}"
db_path = "{}"
redb_path = "{}"
"#,
                relay.url(),
                tmp.path().join("cli-db").display(),
                tmp.path().join("replay.redb").display(),
            ),
        )
        .expect("config");

        World {
            tmp,
            port,
            _relay: relay,
            deliveries,
            cfg_path,
            target_url,
        }
    }

    fn ingest_url(&self, token: &str, path: &str) -> String {
        format!("http://127.0.0.1:{}/ingest/{token}/{path}", self.port)
    }

    /// POST a webhook the way a provider would and return the status.
    async fn ingest(&self, path: &str, headers: &[(&str, &str)], body: &[u8]) -> StatusCode {
        let mut req = reqwest::Client::new()
            .post(self.ingest_url(TOKEN, path))
            .body(body.to_vec());
        for (name, value) in headers {
            req = req.header(*name, *value);
        }
        let status = req.send().await.expect("ingest request").status();
        StatusCode::from_u16(status.as_u16()).unwrap()
    }

    fn drain_cmd(&self) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_hookstr"));
        cmd.args(["drain", "--config", self.cfg_path.to_str().unwrap()])
            .current_dir(self.tmp.path());
        cmd
    }

    /// Run `drain --once` to completion and hand back the process output.
    async fn drain_once(&self) -> std::process::Output {
        tokio::time::timeout(
            Duration::from_secs(60),
            self.drain_cmd()
                .args(["--target", &self.target_url, "--once"])
                .output(),
        )
        .await
        .expect("drain finishes within timeout")
        .expect("drain spawns")
    }

    /// Run `peek` (one-shot) with extra args and hand back its stdout.
    async fn peek(&self, args: &[&str]) -> String {
        let output = tokio::time::timeout(
            Duration::from_secs(60),
            tokio::process::Command::new(env!("CARGO_BIN_EXE_hookstr"))
                .args(["peek", "--config", self.cfg_path.to_str().unwrap()])
                .args(args)
                .current_dir(self.tmp.path())
                .output(),
        )
        .await
        .expect("peek finishes within timeout")
        .expect("peek spawns");
        assert!(
            output.status.success(),
            "peek failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    async fn next_delivery(&mut self) -> Delivery {
        tokio::time::timeout(Duration::from_secs(30), self.deliveries.recv())
            .await
            .expect("delivery within timeout")
            .expect("target channel open")
    }
}

async fn capture(
    State(tx): State<mpsc::UnboundedSender<Delivery>>,
    Path((provider, hook_type)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let headers = headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    let _ = tx.send((format!("{provider}/{hook_type}"), headers, body.to_vec()));
    StatusCode::OK
}

#[tokio::test]
async fn drain_once_replays_ingested_webhooks_byte_exact() {
    let mut world = World::start().await;

    // A wrong ingest token is indistinguishable from a missing route.
    let status = reqwest::Client::new()
        .post(world.ingest_url("wrong-token", "acme/invoice"))
        .body("nope")
        .send()
        .await
        .expect("request")
        .status();
    assert_eq!(status.as_u16(), 404, "wrong token is a 404");

    // One JSON delivery and one binary (invalid UTF-8) delivery, so the
    // base64 storage fallback is exercised end-to-end too.
    let json_body = br#"{"event":"invoice.paid","amount_cents":1234}"#;
    let blob_body: &[u8] = &[0xff, 0x00, 0xfe, 0x42, b'!'];
    assert_eq!(
        world
            .ingest(
                "acme/invoice",
                &[
                    ("content-type", "application/json"),
                    ("x-webhook-id", "wh_1"),
                    ("x-signature", "sig_abc123"),
                ],
                json_body,
            )
            .await,
        StatusCode::NO_CONTENT,
        "ingest answers 204 once durable"
    );
    // The token alone authenticates: the path after it is free-form and need
    // not start with the provider name the token is registered under.
    assert_eq!(
        world
            .ingest("v1/blob", &[("x-webhook-id", "wh_2")], blob_body)
            .await,
        StatusCode::NO_CONTENT
    );

    let output = world.drain_once().await;
    assert!(
        output.status.success(),
        "drain --once failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut by_path = HashMap::new();
    for _ in 0..2 {
        let (path, headers, body) = world.next_delivery().await;
        by_path.insert(path, (headers, body));
    }

    let (headers, body) = &by_path["acme/invoice"];
    assert_eq!(body, json_body, "JSON body replays byte-exact");
    assert_eq!(
        headers.get("x-webhook-id").map(String::as_str),
        Some("wh_1")
    );
    assert_eq!(
        headers.get("x-signature").map(String::as_str),
        Some("sig_abc123"),
        "signature header rides along for the consumer to verify"
    );
    assert_eq!(
        headers.get("content-type").map(String::as_str),
        Some("application/json")
    );

    let (headers, body) = &by_path["v1/blob"];
    assert_eq!(body, blob_body, "binary body survives the base64 roundtrip");
    assert_eq!(
        headers.get("x-webhook-id").map(String::as_str),
        Some("wh_2")
    );

    // Replay is recorded in redb: a second pass finds nothing due.
    let output = world.drain_once().await;
    assert!(
        output.status.success(),
        "second drain failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        world.deliveries.try_recv().is_err(),
        "an already-replayed webhook must not replay again"
    );
}

#[tokio::test]
async fn follow_replays_new_webhooks_in_realtime() {
    let mut world = World::start().await;

    // Backlog before the drain starts; its replay doubles as the readiness
    // signal that the follower is connected and past catch-up.
    assert_eq!(
        world
            .ingest("acme/invoice", &[("x-webhook-id", "wh_1")], b"backlog")
            .await,
        StatusCode::NO_CONTENT
    );

    let mut child = world
        .drain_cmd()
        .args(["--target", &world.target_url])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn follower");

    let (path, _, body) = world.next_delivery().await;
    assert_eq!(
        (path.as_str(), body.as_slice()),
        ("acme/invoice", &b"backlog"[..])
    );

    // Arrives while the standing subscription is up: no reconnect, no new
    // drain run — the follower must replay it as it lands.
    assert_eq!(
        world
            .ingest("acme/invoice", &[("x-webhook-id", "wh_2")], b"realtime")
            .await,
        StatusCode::NO_CONTENT
    );
    let (path, headers, body) = world.next_delivery().await;
    assert_eq!(
        (path.as_str(), body.as_slice()),
        ("acme/invoice", &b"realtime"[..])
    );
    assert_eq!(
        headers.get("x-webhook-id").map(String::as_str),
        Some("wh_2")
    );

    child.kill().await.expect("stop follower");
}

#[tokio::test]
async fn targets_override_wins_over_target_flag() {
    let mut world = World::start().await;

    // Pin acme/events to the capture server via a [targets] override, then
    // point --target at a black hole: only the override can route this
    // delivery, so a capture proves [targets] beats --target.
    let cfg = std::fs::read_to_string(&world.cfg_path).unwrap();
    std::fs::write(
        &world.cfg_path,
        format!(
            "{cfg}\n[targets]\n\"acme/events\" = \"{}/acme/events\"\n",
            world.target_url
        ),
    )
    .unwrap();

    assert_eq!(
        world
            .ingest("acme/events", &[("x-webhook-id", "wh_t")], b"pinned")
            .await,
        StatusCode::NO_CONTENT
    );

    let output = tokio::time::timeout(
        Duration::from_secs(60),
        world
            .drain_cmd()
            .args(["--once", "--target", "http://127.0.0.1:1/nowhere"])
            .output(),
    )
    .await
    .expect("drain finishes within timeout")
    .expect("drain spawns");
    assert!(
        output.status.success(),
        "drain failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let (path, _, body) = world.next_delivery().await;
    assert_eq!(
        (path.as_str(), body.as_slice()),
        ("acme/events", &b"pinned"[..])
    );
}

#[tokio::test]
async fn drain_without_any_target_errors() {
    let world = World::start().await;

    // No --target and no [targets] in the config: there is nowhere to deliver,
    // so the drain must refuse to start (before it even connects) rather than
    // sync a backlog it can't route anywhere.
    let output = tokio::time::timeout(
        Duration::from_secs(60),
        world.drain_cmd().arg("--once").output(),
    )
    .await
    .expect("drain exits within timeout")
    .expect("drain spawns");

    assert!(!output.status.success(), "drain with no target must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no replay target"),
        "expected a 'no replay target' error, got: {stderr}"
    );
}

#[tokio::test]
async fn init_writes_config_once() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let init = |relay: &'static str| {
        let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_hookstr"));
        cmd.args(["init", relay])
            .env("XDG_CONFIG_HOME", tmp.path().join("cfg"))
            .env("XDG_DATA_HOME", tmp.path().join("data"));
        async move { cmd.output().await.expect("init runs") }
    };

    let out = init("ws://127.0.0.1:9999").await;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let cfg_path = tmp.path().join("cfg/hookstr/config.toml");
    let cfg = std::fs::read_to_string(&cfg_path).expect("config written");
    assert!(
        cfg.contains(r#"relay_url = "ws://127.0.0.1:9999""#),
        "{cfg}"
    );

    // A second init must not clobber the config.
    let out = init("ws://127.0.0.1:1111").await;
    assert!(!out.status.success(), "re-init must refuse");
    assert!(std::fs::read_to_string(&cfg_path).unwrap().contains("9999"));
}

#[tokio::test]
async fn peek_prints_without_replaying_and_all_shows_replayed() {
    let mut world = World::start().await;
    assert_eq!(
        world
            .ingest(
                "acme/invoice",
                &[("x-webhook-id", "wh_peek")],
                br#"{"event":"peek"}"#,
            )
            .await,
        StatusCode::NO_CONTENT
    );

    // Undelivered: peek shows it, and nothing reaches the target.
    let stdout = world.peek(&[]).await;
    assert!(
        stdout.contains("--- acme/invoice"),
        "path printed: {stdout}"
    );
    assert!(
        stdout.contains("x-webhook-id: wh_peek"),
        "headers printed: {stdout}"
    );
    assert!(
        stdout.contains(r#"{"event":"peek"}"#),
        "body printed: {stdout}"
    );
    assert!(
        !stdout.contains("POST "),
        "no [targets] route, so no POST line: {stdout}"
    );
    assert!(
        world.deliveries.try_recv().is_err(),
        "peek must not POST anything"
    );

    // Peek left replay state alone, so a real drain still delivers it.
    let output = world.drain_once().await;
    assert!(
        output.status.success(),
        "drain after peek failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (path, _headers, body) = world.next_delivery().await;
    assert_eq!(
        (path.as_str(), body.as_slice()),
        ("acme/invoice", &br#"{"event":"peek"}"#[..])
    );

    // Replayed now: plain peek has nothing to show, --all still shows it.
    let stdout = world.peek(&[]).await;
    assert!(
        !stdout.contains("acme/invoice"),
        "a replayed webhook is not due: {stdout}"
    );
    let stdout = world.peek(&["--all"]).await;
    assert!(
        stdout.contains("--- acme/invoice"),
        "--all shows replayed webhooks: {stdout}"
    );
}

/// A `drain --exec` consumer that records every run as
/// `attempts/<n>-<phase>.ndjson` (its stdin), then exits 0. Touch `fail-once`
/// in its dir to make the next run exit 1, or `hang-once` to make it write
/// its pid to `hung.pid` and sleep past any test timeout.
struct Recorder {
    dir: std::path::PathBuf,
    /// The `--exec` command line.
    cmd: String,
}

impl Recorder {
    fn new(world: &World) -> Recorder {
        let dir = world.tmp.path().join("recorder");
        std::fs::create_dir_all(dir.join("attempts")).expect("recorder dir");
        let script = dir.join("record.sh");
        std::fs::write(
            &script,
            format!(
                r#"dir='{}'
n=$(printf %03d "$(ls "$dir/attempts" | wc -l)")
cat > "$dir/stdin.$$"
mv "$dir/stdin.$$" "$dir/attempts/$n-$HOOKSTR_PHASE.ndjson"
if [ -e "$dir/hang-once" ]; then rm "$dir/hang-once"; echo $$ > "$dir/hung.pid"; exec sleep 60; fi
if [ -e "$dir/fail-once" ]; then rm "$dir/fail-once"; exit 1; fi
exit 0
"#,
                dir.display()
            ),
        )
        .expect("recorder script");
        let cmd = format!("exec sh '{}'", script.display());
        Recorder { dir, cmd }
    }

    fn arm(&self, flag: &str) {
        std::fs::write(self.dir.join(flag), "").expect("arm recorder");
    }

    /// Every run so far, in order: (phase, stdin lines parsed as JSON).
    fn attempts(&self) -> Vec<(String, Vec<serde_json::Value>)> {
        let mut names: Vec<_> = std::fs::read_dir(self.dir.join("attempts"))
            .expect("attempts dir")
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
            .into_iter()
            .map(|name| {
                let phase = name
                    .trim_end_matches(".ndjson")
                    .split_once('-')
                    .unwrap()
                    .1
                    .to_owned();
                let text = std::fs::read_to_string(self.dir.join("attempts").join(&name)).unwrap();
                let lines = text
                    .lines()
                    .map(|line| serde_json::from_str(line).expect("envelope line is JSON"))
                    .collect();
                (phase, lines)
            })
            .collect()
    }

    /// Wait until at least `n` runs are recorded and return them all.
    async fn wait_for(&self, n: usize) -> Vec<(String, Vec<serde_json::Value>)> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let attempts = self.attempts();
            if attempts.len() >= n {
                return attempts;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "wanted {n} exec runs, got {}: {attempts:?}",
                attempts.len()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// The bodies a batch carried, as text, in order.
fn batch_bodies(lines: &[serde_json::Value]) -> Vec<String> {
    lines.iter().map(|env| env["body"].to_string()).collect()
}

impl World {
    /// Ingest `bodies` as acme/invoice deliveries, a few ms apart so their
    /// receipt order is unambiguous.
    async fn ingest_all(&self, bodies: &[&str]) {
        for (i, body) in bodies.iter().enumerate() {
            let id = format!("wh_{i}_{body}");
            assert_eq!(
                self.ingest("acme/invoice", &[("x-webhook-id", &id)], body.as_bytes())
                    .await,
                StatusCode::NO_CONTENT
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Start a following `drain --exec` with extra args.
    fn spawn_exec_follower(&self, recorder: &Recorder, args: &[&str]) -> tokio::process::Child {
        self.drain_cmd()
            .args(["--exec", &recorder.cmd])
            .args(args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn exec follower")
    }
}

#[tokio::test]
async fn exec_once_delivers_batches_oldest_first_and_acks_on_exit_zero() {
    let world = World::start().await;
    let recorder = Recorder::new(&world);
    world
        .ingest_all(&[r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#])
        .await;

    let output = tokio::time::timeout(
        Duration::from_secs(60),
        world
            .drain_cmd()
            .args(["--once", "--exec", &recorder.cmd, "--batch-max", "2"])
            .output(),
    )
    .await
    .expect("drain finishes")
    .expect("drain spawns");
    assert!(
        output.status.success(),
        "drain --exec failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // --batch-max 2 splits three deliveries 2 + 1, oldest first.
    let attempts = recorder.attempts();
    assert_eq!(attempts.len(), 2, "{attempts:?}");
    assert_eq!(attempts[0].0, "backfill");
    assert_eq!(attempts[1].0, "backfill");
    assert_eq!(batch_bodies(&attempts[0].1), [r#"{"n":1}"#, r#"{"n":2}"#]);
    assert_eq!(batch_bodies(&attempts[1].1), [r#"{"n":3}"#]);

    let env = &attempts[0].1[0];
    assert_eq!(env["path"], "acme/invoice");
    assert_eq!(env["provider"], "acme");
    assert_eq!(env["headers"]["x-webhook-id"], r#"wh_0_{"n":1}"#);
    assert_eq!(env["body_encoding"], "json");
    assert_eq!(env["id"].as_str().unwrap().len(), 64, "hex event id");
    assert!(env["received_at_ms"].as_u64().unwrap() > 1_600_000_000_000);

    // peek --json prints the very same envelope lines exec got on stdin.
    let mut fed: Vec<String> = attempts
        .iter()
        .flat_map(|(_, lines)| lines.iter().map(|env| env.to_string()))
        .collect();
    let mut peeked: Vec<String> = world
        .peek(&["--all", "--json"])
        .await
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .unwrap()
                .to_string()
        })
        .collect();
    fed.sort();
    peeked.sort();
    assert_eq!(peeked, fed);

    // Exit 0 acked every batch: a second pass runs nothing.
    let output = world
        .drain_cmd()
        .args(["--once", "--exec", &recorder.cmd])
        .output()
        .await
        .expect("drain spawns");
    assert!(output.status.success());
    assert_eq!(recorder.attempts().len(), 2, "acked batches must not rerun");
    assert_eq!(
        world.peek(&["--json"]).await,
        "",
        "nothing left undelivered"
    );
}

#[tokio::test]
async fn exec_nonzero_exit_retries_the_same_batch() {
    let world = World::start().await;
    let recorder = Recorder::new(&world);
    world.ingest_all(&["first", "second"]).await;
    recorder.arm("fail-once");

    // A 1s backoff base makes the first retry come 2s after the failure.
    let mut child = world.spawn_exec_follower(&recorder, &["--retry-base-secs", "1"]);
    let attempts = recorder.wait_for(2).await;
    child.kill().await.expect("stop follower");

    assert_eq!(attempts[0].1, attempts[1].1, "the retry is the same batch");
    assert_eq!(batch_bodies(&attempts[1].1), [r#""first""#, r#""second""#]);
    // The catch-up isn't done until a pass gets through, so the retry is
    // still backfill.
    assert_eq!(attempts[1].0, "backfill");
    assert_eq!(attempts.len(), 2, "acked on the retry: {attempts:?}");
}

#[tokio::test]
async fn exec_timeout_kills_the_command_and_retries() {
    let world = World::start().await;
    let recorder = Recorder::new(&world);
    world.ingest_all(&["slow"]).await;
    recorder.arm("hang-once");

    let mut child = world.spawn_exec_follower(
        &recorder,
        &["--exec-timeout", "1s", "--retry-base-secs", "1"],
    );
    let attempts = recorder.wait_for(2).await;
    child.kill().await.expect("stop follower");
    assert_eq!(attempts[0].1, attempts[1].1, "the retry is the same batch");

    // The hung run was killed at the timeout, not left sleeping.
    let pid = std::fs::read_to_string(recorder.dir.join("hung.pid")).unwrap();
    let alive = std::process::Command::new("kill")
        .args(["-0", pid.trim()])
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap()
        .success();
    assert!(!alive, "timed-out command {pid} still running");
}

#[tokio::test]
async fn exec_phase_flips_to_live_and_the_window_coalesces() {
    let world = World::start().await;
    let recorder = Recorder::new(&world);
    world.ingest_all(&["backlog"]).await;

    let mut child = world.spawn_exec_follower(&recorder, &["--batch-window", "3s"]);
    let attempts = recorder.wait_for(1).await;
    assert_eq!(attempts[0].0, "backfill");
    assert_eq!(batch_bodies(&attempts[0].1), [r#""backlog""#]);

    // Two arrivals inside one window go out as one live batch.
    world.ingest_all(&["live-a", "live-b"]).await;
    let attempts = recorder.wait_for(2).await;
    child.kill().await.expect("stop follower");
    assert_eq!(attempts[1].0, "live");
    assert_eq!(batch_bodies(&attempts[1].1), [r#""live-a""#, r#""live-b""#]);
}

#[tokio::test]
async fn peek_json_envelopes_round_trip_every_body_kind() {
    use base64::Engine;
    let world = World::start().await;
    let json_body = b"{\n  \"z\": 1,\n  \"a\": {\"nested\": [1, 2.5, null]}\n}\n";
    let text_body = b"plain=text&not=json";
    let blob_body: &[u8] = &[0xff, 0x00, 0xfe, b'\n', 0x42];
    for (path, body) in [
        ("acme/json", &json_body[..]),
        ("acme/text", &text_body[..]),
        ("acme/blob", blob_body),
    ] {
        assert_eq!(
            world
                .ingest(path, &[("x-a", "1"), ("x-a", "2")], body)
                .await,
            StatusCode::NO_CONTENT
        );
    }

    let stdout = world.peek(&["--all", "--json"]).await;
    let envs: HashMap<String, serde_json::Value> = stdout
        .lines()
        .map(|line| {
            let env: serde_json::Value = serde_json::from_str(line).expect("one envelope per line");
            (env["path"].as_str().unwrap().to_owned(), env)
        })
        .collect();
    assert_eq!(envs.len(), 3, "{stdout}");

    let env = &envs["acme/json"];
    assert_eq!(env["body_encoding"], "json");
    assert_eq!(
        env["body"],
        serde_json::from_slice::<serde_json::Value>(json_body).unwrap()
    );
    assert_eq!(env["headers"]["x-a"], "1, 2", "repeated headers joined");

    let env = &envs["acme/text"];
    assert_eq!(env["body_encoding"], "utf8");
    assert_eq!(env["body"].as_str().unwrap().as_bytes(), text_body);

    let env = &envs["acme/blob"];
    assert_eq!(env["body_encoding"], "base64");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(env["body"].as_str().unwrap())
        .unwrap();
    assert_eq!(decoded, blob_body);
}

#[tokio::test]
async fn exec_tuning_flags_need_exec() {
    let world = World::start().await;
    let output = world
        .drain_cmd()
        .args(["--once", "--target", &world.target_url, "--batch-max", "5"])
        .output()
        .await
        .expect("drain spawns");
    assert!(
        !output.status.success(),
        "a stray --batch-max must not start a URL drain"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("only apply with --exec"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
