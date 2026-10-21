//! End-to-end: the real hookstrd ingest router + relay run in-process, and the
//! real `hookstr` binary drains against them — provider POST -> durable 204 ->
//! negentropy drain -> byte-exact replay to a local target, plus redb dedupe,
//! realtime follow, and forged events in the open relay never replaying.

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
const INTRUDER_SECRET: [u8; 32] = [0x99; 32];
const TOKEN: &str = "test-ingest-token";

/// One replayed delivery as the target saw it: `{provider}/{type}` route,
/// headers, and the raw body.
type Delivery = (String, HashMap<String, String>, Vec<u8>);

fn pubkey(secret: &[u8; 32]) -> [u8; 32] {
    secp256k1::SecretKey::from_slice(secret)
        .expect("seckey")
        .x_only_public_key(&secp256k1::Secp256k1::new())
        .0
        .serialize()
}

/// The whole deployment, minus the CLI: the real hookstrd ingest server and
/// relay over one nostrdb, a capture target for replays, and the config file
/// the drain binary will run with.
struct World {
    tmp: tempfile::TempDir,
    /// hookstrd's ingest HTTP port.
    port: u16,
    /// hookstrd's nostrdb, for injecting events straight into the relay's
    /// store the way an outside client's `EVENT` would land.
    ndb: Ndb,
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
server_pubkey = "{}"
db_path = "{}"
redb_path = "{}"
"#,
                relay.url(),
                hex::encode(pubkey(&SERVER_SECRET)),
                tmp.path().join("cli-db").display(),
                tmp.path().join("replay.redb").display(),
            ),
        )
        .expect("config");

        World {
            tmp,
            port,
            ndb,
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
        cmd.args(["init", relay, "--server-pubkey", "ab".repeat(32).as_str()])
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
    assert!(
        cfg.contains(&format!(r#"server_pubkey = "{}""#, "ab".repeat(32))),
        "{cfg}"
    );

    // A second init must not clobber the config.
    let out = init("ws://127.0.0.1:1111").await;
    assert!(!out.status.success(), "re-init must refuse");
    assert!(std::fs::read_to_string(&cfg_path).unwrap().contains("9999"));
}

#[tokio::test]
async fn forged_events_in_the_open_relay_never_replay() {
    let mut world = World::start().await;

    // The relay has no auth and ingests any EVENT, so an outsider can plant a
    // well-formed webhook event signed with their own key. Land one straight
    // in hookstrd's store, exactly where such an EVENT ends up.
    let forged = hookstr_core::WebhookRecord {
        provider: "acme".to_owned(),
        path: "acme/invoice".to_owned(),
        headers: vec![("x-webhook-id".to_owned(), "wh_forged".to_owned())],
        body: b"forged".to_vec(),
        received_at_ms: 1,
    };
    let frame = {
        let note = hookstr_core::sign_webhook_note(&forged, &INTRUDER_SECRET).expect("sign");
        hookstr_core::event_frame(&note).expect("frame")
    };
    world.ndb.process_client_event(&frame).expect("inject");

    assert_eq!(
        world
            .ingest("acme/invoice", &[("x-webhook-id", "wh_real")], b"real")
            .await,
        StatusCode::NO_CONTENT
    );

    let output = world.drain_once().await;
    assert!(
        output.status.success(),
        "drain failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (_, headers, body) = world.next_delivery().await;
    assert_eq!(body, b"real", "only hookstrd-signed events replay");
    assert_eq!(
        headers.get("x-webhook-id").map(String::as_str),
        Some("wh_real")
    );
    // Give a stray forged replay time to show up before calling it absent.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        world.deliveries.try_recv().is_err(),
        "the forged event must never replay"
    );

    let stdout = world.peek(&["--all"]).await;
    assert!(
        !stdout.contains("wh_forged"),
        "peek never shows forged events either: {stdout}"
    );
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
