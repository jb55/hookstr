//! End-to-end: the real hookstrd ingest router + NIP-42 relay run in-process,
//! and the real `hookstr` binary drains against them — provider POST ->
//! durable 204 -> authed negentropy drain -> byte-exact replay to a local
//! target, plus redb dedupe, realtime follow, and allowlist refusal.

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
const DRAIN_SECRET: [u8; 32] = [0x43; 32];
const INTRUDER_SECRET: [u8; 32] = [0x99; 32];
const TOKEN: &str = "test-ingest-token";

/// One replayed delivery as the target saw it: `{provider}/{type}` route,
/// headers, and the raw body.
type Delivery = (String, HashMap<String, String>, Vec<u8>);

fn nsec(secret: &[u8; 32]) -> String {
    bech32::encode::<bech32::Bech32>(bech32::Hrp::parse_unchecked("nsec"), secret).expect("bech32")
}

fn pubkey(secret: &[u8; 32]) -> [u8; 32] {
    secp256k1::SecretKey::from_slice(secret)
        .expect("seckey")
        .x_only_public_key(&secp256k1::Secp256k1::new())
        .0
        .serialize()
}

/// The whole deployment, minus the CLI: hookstrd's ingest router + auth-gated
/// relay over one nostrdb, a capture target for replays, and the config file
/// the drain binary will run with.
struct World {
    tmp: tempfile::TempDir,
    ingest_port: u16,
    deliveries: mpsc::UnboundedReceiver<Delivery>,
    cfg_path: std::path::PathBuf,
    /// The capture target's base URL, as written into the config's
    /// target_base.
    target_url: String,
    _relay: nostrdb_relay::RelayHandle,
}

impl World {
    /// `drain_secret` is what goes in the CLI's nsec file; the relay allowlist
    /// is always the legitimate drain key, so passing another key exercises
    /// the refusal path.
    async fn start(drain_secret: &[u8; 32]) -> World {
        let tmp = tempfile::tempdir().expect("tempdir");

        let db_path = tmp.path().join("server-db");
        std::fs::create_dir_all(&db_path).expect("db dir");
        let ndb = Ndb::new(db_path.to_str().unwrap(), &Config::new()).expect("ndb");

        let relay = nostrdb_relay::spawn_with_auth(
            ndb.clone(),
            "127.0.0.1:0".parse().unwrap(),
            nostrdb_relay::AuthConfig {
                allowed_pubkeys: [pubkey(&DRAIN_SECRET)].into(),
                accept_events: false,
            },
        )
        .expect("relay");

        let app = hookstrd::router(hookstrd::Ingest {
            ndb,
            seckey: SERVER_SECRET,
            tokens: HashMap::from([("acme".to_owned(), TOKEN.to_owned())]),
        });
        let ingest = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ingest");
        let ingest_port = ingest.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(ingest, app).await.unwrap() });

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

        let nsec_path = tmp.path().join("drain.nsec");
        std::fs::write(&nsec_path, format!("{}\n", nsec(drain_secret))).expect("nsec");

        let target_url = format!("http://127.0.0.1:{target_port}/hook");
        let cfg_path = tmp.path().join("hookstr.toml");
        std::fs::write(
            &cfg_path,
            format!(
                r#"relay_url = "{}"
db_path = "{}"
redb_path = "{}"
nsec_path = "{}"
target_base = "{target_url}"
"#,
                relay.url(),
                tmp.path().join("cli-db").display(),
                tmp.path().join("replay.redb").display(),
                nsec_path.display(),
            ),
        )
        .expect("config");

        World {
            tmp,
            ingest_port,
            deliveries,
            cfg_path,
            target_url,
            _relay: relay,
        }
    }

    fn ingest_url(&self, token: &str, path: &str) -> String {
        format!("http://127.0.0.1:{}/ingest/{token}/{path}", self.ingest_port)
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
            self.drain_cmd().arg("--once").output(),
        )
        .await
        .expect("drain finishes within timeout")
        .expect("drain spawns")
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
    let mut world = World::start(&DRAIN_SECRET).await;

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
    assert_eq!(headers.get("x-webhook-id").map(String::as_str), Some("wh_1"));
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
    assert_eq!(headers.get("x-webhook-id").map(String::as_str), Some("wh_2"));

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
    let mut world = World::start(&DRAIN_SECRET).await;

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
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn follower");

    let (path, _, body) = world.next_delivery().await;
    assert_eq!((path.as_str(), body.as_slice()), ("acme/invoice", &b"backlog"[..]));

    // Arrives while the standing subscription is up: no reconnect, no new
    // drain run — the follower must replay it as it lands.
    assert_eq!(
        world
            .ingest("acme/invoice", &[("x-webhook-id", "wh_2")], b"realtime")
            .await,
        StatusCode::NO_CONTENT
    );
    let (path, headers, body) = world.next_delivery().await;
    assert_eq!((path.as_str(), body.as_slice()), ("acme/invoice", &b"realtime"[..]));
    assert_eq!(headers.get("x-webhook-id").map(String::as_str), Some("wh_2"));

    child.kill().await.expect("stop follower");
}

#[tokio::test]
async fn drain_target_flag_overrides_config_target_base() {
    let mut world = World::start(&DRAIN_SECRET).await;

    // Break the config's target_base; only the --target flag can route this
    // delivery to the capture server.
    let cfg = std::fs::read_to_string(&world.cfg_path).unwrap();
    std::fs::write(
        &world.cfg_path,
        cfg.replace(&world.target_url, "http://127.0.0.1:1/nowhere"),
    )
    .unwrap();

    assert_eq!(
        world
            .ingest("acme/events", &[("x-webhook-id", "wh_t")], b"flagged")
            .await,
        StatusCode::NO_CONTENT
    );

    let target = world.target_url.clone();
    let output = tokio::time::timeout(
        Duration::from_secs(60),
        world.drain_cmd().args(["--once", "--target", &target]).output(),
    )
    .await
    .expect("drain finishes within timeout")
    .expect("drain spawns");
    assert!(
        output.status.success(),
        "drain --target failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let (path, _, body) = world.next_delivery().await;
    assert_eq!((path.as_str(), body.as_slice()), ("acme/events", &b"flagged"[..]));
}

#[tokio::test]
async fn init_writes_keypair_and_config_once() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let init = |relay: &'static str| {
        let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_hookstr"));
        cmd.args(["init", relay])
            .env("XDG_CONFIG_HOME", tmp.path().join("cfg"))
            .env("XDG_DATA_HOME", tmp.path().join("data"));
        async move { cmd.output().await.expect("init runs") }
    };

    let out = init("wss://first.example").await;
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    let pubkey = stdout
        .lines()
        .find_map(|l| l.strip_prefix("pubkey: "))
        .expect("init prints the allowlist pubkey")
        .to_owned();

    let cfg_path = tmp.path().join("cfg/hookstr/config.toml");
    let cfg = std::fs::read_to_string(&cfg_path).expect("config written");
    assert!(cfg.contains(r#"relay_url = "wss://first.example""#), "{cfg}");
    let nsec_path = tmp.path().join("cfg/hookstr/drain.nsec");
    let nsec = std::fs::read_to_string(&nsec_path).expect("nsec written");
    let (_, pk) = nostr_relay_sync::parse_nsec(nsec.trim()).expect("valid nsec");
    assert_eq!(hex::encode(pk), pubkey, "printed pubkey matches the keypair");

    // A second init must not clobber the config...
    let out = init("wss://second.example").await;
    assert!(!out.status.success(), "re-init must refuse");
    assert!(
        std::fs::read_to_string(&cfg_path)
            .unwrap()
            .contains("first.example")
    );

    // ...and after deleting it, the existing keypair is kept (it may already
    // be on hookstrd's allowlist).
    std::fs::remove_file(&cfg_path).unwrap();
    let out = init("wss://second.example").await;
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(
        std::fs::read_to_string(&nsec_path).unwrap(),
        nsec,
        "re-init keeps the existing keypair"
    );
}

#[tokio::test]
async fn drain_with_a_key_off_the_allowlist_is_refused() {
    let world = World::start(&INTRUDER_SECRET).await;

    let output = world.drain_once().await;
    assert!(
        !output.status.success(),
        "an unallowlisted key must not drain"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("refused auth"),
        "failure names the auth refusal: {stderr}"
    );
}
