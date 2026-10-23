# hookstr

A store-and-forward webhook service backed by [nostr](https://github.com/nostr-protocol/nostr).

Webhook providers demand an always-online HTTPS endpoint and typically
auto-disable it after repeated delivery failures. A consumer behind a
tunnel — sometimes offline, tunnel URL rotating — loses that race
constantly.

hookstr splits delivery from consumption:

- **`hookstrd`** runs permanently on always-online infra behind a
  TLS-terminating reverse proxy. It accepts any webhook with a fast 2xx,
  persists it as a signed nostr event in [nostrdb], and serves those
  events back over an embedded nostr relay that listens on localhost
  only. The drain reaches it through a tunnel.
- **`hookstr drain`** runs wherever the consumer lives. It
  negentropy-syncs ([NIP-77]) whatever it missed while offline, replays
  each delivery as a real HTTP request — byte-exact body, preserved
  headers — against a configured local target, then keeps following:
  new webhooks replay in realtime while it's connected, reconnecting
  and re-syncing on its own when the connection drops.

Providers see one endpoint that never goes down. The consumer sees every
webhook, eventually, in order, with valid signatures.

```
 provider A ──POST──▶ reverse proxy (hooks.example.com, TLS)
 provider B ──POST──▶   └── /ingest/{token}/*  ──▶ hookstrd axum ──▶ nostrdb
                                                                       │
                              127.0.0.1:8081  hookstrd relay ◀─────────┘
                                                       ▲
                          tunnel (ssh -L / Tailscale) + NIP-77 sync + live REQ
                                                       │
 dev machine:  hookstr cli ◀───────────────────────────┘
                    │ replay (HTTP, raw body + preserved headers)
                    ▼
               http://localhost:PORT/<consumer webhook route>
```

## Design

- **Byte-exact body preservation.** Providers commonly sign the raw HTTP
  body (an `x-signature`-style HMAC) and consumers validate against the
  exact bytes. Bodies and signature headers survive verbatim end-to-end.
- **At-least-once replay.** Consumers dedupe on a delivery-id header, so
  conservative replay (crash mid-replay, re-drain, retry) is safe — which
  makes header preservation load-bearing, not cosmetic.
- **Fast 2xx.** Ingest accepts, waits for durable persistence, responds
  204. 5xx only when persistence itself failed, so the provider retries.
  Nothing downstream of the store is ever on the ingest path.
- **Store everything.** No deletion, ever. Negentropy reconciles over the
  full set, which makes sync trivial — and webhook bodies are a few KB,
  so even a million deliveries is single-digit GB.
- **Pull-only, private relay.** The relay has no auth and will store any
  event a client sends it, so who can reach its address is the access
  control: it binds to localhost or a private interface (e.g. WireGuard),
  and the drain reaches it through a tunnel or the VPN. On the client,
  replay only acts on notes read back out of the local nostrdb after its
  ingester has verified and committed them — never on wire frames.

Each inbound delivery is one immutable kind `3003` nostr event, signed by
the server's key: the raw body as content (base64 + an `encoding` tag if
it isn't UTF-8), the ingest path in a `path` tag for replay routing, and
one `header` tag per preserved header. Schema lives in
[`hookstr-core`](crates/hookstr-core/src/lib.rs).

## Setup

### Server (the always-online box)

Generate a keypair for the server:

```
$ hookstr keygen
nsec:   nsec1...
pubkey: 8f2a...
```

Write the nsec to a file, then configure `hookstrd.toml` (see
[config/hookstrd.example.toml](config/hookstrd.example.toml)):

```toml
db_path = "/var/lib/hookstr/ndb"
mapsize = 34359738368            # 32 GiB; store-everything, so be generous
ingest_addr = "127.0.0.1:8080"  # webhook ingest, behind the proxy
relay_addr = "127.0.0.1:8081"   # relay ws, localhost only (the default)
nsec_path = "/var/lib/hookstr/server.nsec"

# Per-provider ingest secrets; each token authenticates and names its
# provider, and can be rotated/revoked without disturbing the others.
[ingest_tokens]
acme = "long-random-secret"
stripe = "another-long-random-secret"
```

Run ingest behind any TLS-terminating proxy. Only `ingest_addr` goes
through it; never proxy `relay_addr`, since the relay has no auth. Caddy
sketch:

```caddyfile
hooks.example.com {
        reverse_proxy localhost:8080
}
```

Instead of localhost plus a tunnel, `relay_addr` can bind a private
interface, e.g. a WireGuard address, that only the drain machine can reach.
The old combined `listen_addr` mode (ingest and relay on one port) is gone,
since an unauthenticated relay can't share the public port. A config that
still sets it fails at startup and says to rename it to `ingest_addr`.

```
$ hookstrd --config hookstrd.toml
INFO hookstrd ingest on 127.0.0.1:8080, relay on ws://127.0.0.1:8081
```

Register `https://hooks.example.com/ingest/<token>/<any/path>` with each
provider — e.g. `/ingest/s3cret/acme/events`. The token alone
authenticates the delivery (and tags it with its provider); the rest of
the path is free-form and becomes the replay routing key. The token is
also the spam gate for a necessarily public endpoint; payload
authenticity is still enforced by the consumer's own signature check on
replay.

### Client (the dev machine)

Open a tunnel to hookstrd's relay. Any tunnel works; with ssh:

```
$ ssh -NL 8081:127.0.0.1:8081 hooks.example.com
```

`hookstr init` writes a starter `~/.config/hookstr/config.toml`, with the
local mirror db under `~/.local/share/hookstr/`. Give it the relay URL as
the tunnel exposes it (default `ws://127.0.0.1:8081`):

```
$ hookstr init ws://127.0.0.1:8081
wrote config /home/you/.config/hookstr/config.toml
then: hookstr drain --target http://localhost:3000/webhooks <provider>
```

You point the drain at your local consumer with `--target` (below), not
the config — where a drain delivers is specific to the consumer it feeds,
not to the durable sync relationship the config describes. Deliveries are
self-describing (the event's `path` tag mirrors everything after the token
in the ingest URL), so a delivery ingested at `/ingest/<token>/acme/events`
replays to `{target}/acme/events`. A `[targets]` table in the config holds
per-path overrides for consumers whose routes don't mirror the ingest path
(these win over `--target`); see
[config/hookstr.example.toml](config/hookstr.example.toml) for every
option. A drain with neither `--target` nor a `[targets]` entry has nowhere
to deliver and refuses to start.

Then drain, naming the providers this consumer handles (the provider
names from hookstrd's `[ingest_tokens]`):

```
$ hookstr drain --target http://localhost:3000/webhooks acme
```

This negentropy-syncs the missed backlog, replays it oldest-first, and
then follows — each new webhook replays in under a second while the
connection is up, and a dropped connection reconnects and re-syncs on
its own. Pass `--once` to exit after the
catch-up replay instead (cron-style). The positional providers are
required: one or more (`hookstr drain --target … acme stripe`). A drain
with none refuses to start, since it would deliver, and mark delivered,
webhooks meant for every other consumer. So one hookstrd serves several
consumers, each running its own scoped drain with its own `--target`:

```
$ hookstr drain --target http://localhost:3003 stripe
```

To look at webhooks without delivering them, use `hookstr peek`. It
syncs like a drain, prints each undelivered webhook to stdout (path,
event id, headers, body) and exits. It never POSTs and never writes
replay state, so a later drain still delivers everything it showed.
`--follow` keeps printing new webhooks as they arrive, `--all` includes
ones already replayed, and positional providers narrow it as with drain:

```
$ hookstr peek acme
--- acme/invoice id=5f3c… received_at_ms=1761040000000
content-type: application/json
x-webhook-id: wh_1

{"event":"invoice.paid","amount_cents":1234}
```

A `POST <url>` line follows the separator when the config's `[targets]`
routes that path. A plain `peek` reads the drain's replay state to know
what's undelivered, and redb lets only one process have that open, so
beside a running `drain` use `peek --all`.

`peek --json` prints one JSON envelope per line instead (the format is
under [Envelope format](#envelope-format)), the same lines `drain --exec`
feeds its command.

To develop a `drain --exec` consumer, run it under `peek --exec` (below).
It is the same delivery loop, with the same batches, `HOOKSTR_PHASE`,
`--batch-window`, timeouts and retries, but the acks live in memory and
are forgotten when peek exits. Nothing is written, so you can rerun it as
often as you like, and a real drain later still delivers everything:

```
$ hookstr peek --all --exec ./notify.py gitlab            # replay the whole store as backfill
$ hookstr peek --follow --exec ./notify.py gitlab         # then live batches as webhooks arrive
```

Without `--all`, `peek --exec` reads the drain's replay state (read-only)
and skips what a drain already delivered, as plain `peek` does. With
`--all` it feeds everything stored and can run beside a drain.

### Delivering to a command: `drain --exec`

A consumer doesn't have to be an HTTP server. With `--exec <cmd>` in
place of `--target`, the drain runs a command per batch of webhooks and
uses its exit code as the ack, with the same sync, backfill, ordering,
retries and replay state as a URL drain:

```
$ hookstr drain --config ~/.config/hookstr/notify.toml --exec ./notify.py gitlab
```

- **Invocation.** `<cmd>` runs under `sh -c`, once per batch, from the
  drain's working directory. The batch is on its **stdin**, closed after
  the last line. The command's stdout and stderr are the drain's.
- **stdin** is [NDJSON envelopes](#envelope-format), one per line,
  oldest first by receipt time. A batch is never empty.
- **`HOOKSTR_PHASE`** is `backfill` while the drain catches up on what
  was stored before it started, and `live` from then on, for webhooks
  arriving while it follows. A backfill batch can hold hundreds of old
  webhooks, so it's the place to summarize instead of alerting per event.
  A batch is `backfill` when it holds any webhook that was already
  stored and undelivered when the drain started (due then or backing
  off), and `live` otherwise. So a backfill batch that fails retries as
  `backfill`, and a webhook that arrives while it backs off goes out as
  `live`. Once every backlog webhook is acked, every batch is `live`. It
  is the only variable hookstr sets. Headers stay in the envelope and never
  go in the environment, since stored headers can hold secrets (e.g.
  `x-gitlab-token`) and the environment leaks into child processes and
  `ps`.
- **Ack.** Exit 0 acks the whole batch: every webhook in it is marked
  replayed and never sent again. A non-zero exit, a crash, or running
  past `--exec-timeout` (the command is killed) fails the whole batch.
  Every webhook in it backs off (30s, doubling per failed attempt, up to
  about an hour) and then goes out again in a later batch. There is no
  partial ack. A consumer must be idempotent (dedupe on `id`, or on the
  provider's delivery-id header), which at-least-once delivery requires
  anyway.
- **Ordering.** Batches run one at a time, never concurrently. A failed
  batch stops the pass, so nothing behind it goes out until it retries.
  But once its backoff is running, newer webhooks that arrive are
  delivered while it waits. So a retried batch can come after webhooks
  received later; sort on `received_at_ms` if order matters.
- **Batching.** `--batch-max N` (default 500) caps a batch. While
  following, `--batch-window <dur>` (default `2s`) gathers whatever
  arrives within that long of the first arrival into one batch, since
  one provider action often fires several hooks at once. The window is
  fixed from the first arrival, so a steady trickle can't hold a batch
  back. `--exec-timeout <dur>` (default `60s`) bounds each run.
  Durations take `ms`, `s`, `m` or `h`, or bare seconds. These three
  flags are only valid with `--exec`.
- `--exec` and `--target` are mutually exclusive, and an exec drain
  ignores the config's `[targets]`. `--once` and positional providers work
  as for any drain. `--once` makes one catch-up pass and stops at the
  first failed batch. Like a URL drain, it exits 0 even then, and the log
  says what failed.

Replay state is keyed by event id alone, not per consumer, and the drain
holds the redb file for the life of the run. So give an exec drain its
own config file with its own `redb_path` and `db_path`. Sharing them with
a URL drain would let whichever consumer acks a webhook first mark it
delivered for the other, and the second drain on one redb file fails to
start anyway.

For a worked consumer, see
[`contrib/gitlab-notify`](contrib/gitlab-notify/README.md): desktop
notifications for GitLab pipelines, reviews, conflicts and comments, with a
backfill summary and per-MR comment digests.

#### Envelope format

Each line is one compact JSON object followed by `\n`. A line never
contains a raw newline (one inside a string is escaped). Keys come in
this order, and a consumer should ignore keys it doesn't know:

| key | type | meaning |
|---|---|---|
| `id` | string | The webhook's nostr event id, 64 lowercase hex chars. Stable across retries and across `peek`: the dedupe key. |
| `path` | string | Everything after the token in the ingest URL, e.g. `gitlab/events`. |
| `received_at_ms` | integer | When hookstrd received it, unix milliseconds. |
| `provider` | string | The provider name the ingest token is registered under in hookstrd's `[ingest_tokens]`, e.g. `gitlab`. What positional providers match. |
| `headers` | object | The preserved request headers, names lowercased, in request order. Values are strings. A repeated header name has its values joined with `", "`. hookstrd has already dropped hop-by-hop, framing (`host`, `content-length`, …) and reverse-proxy (`x-forwarded-*`, `x-real-ip`) headers. |
| `body` | any | The request body, carried as `body_encoding` says. |
| `body_encoding` | string | `"json"`: the body parsed as JSON and `body` is that value (object, array, string, number, bool or null). `"utf8"`: not JSON but valid UTF-8, and `body` is the text as a string. `"base64"`: neither, and `body` is standard base64 (with padding) of the raw bytes. |

A `"json"` body is the parsed value, re-serialized. Its key order is
kept, but whitespace is not, and a number too big for a 64-bit integer
or a double loses precision. A consumer that has to verify an HMAC over
the exact raw bytes should take a URL drain, which replays the body
byte-exact. A JSON string body (`"hi"`) is `body_encoding: "json"`,
which is how it is told apart from the bare text `hi` (`"utf8"`).

An example line, wrapped here for reading:

```json
{"id":"5f3c…","path":"gitlab/events","received_at_ms":1761040000000,
 "provider":"gitlab","headers":{"content-type":"application/json",
 "x-gitlab-event":"Push Hook","x-gitlab-token":"…"},
 "body":{"object_kind":"push","ref":"refs/heads/main"},"body_encoding":"json"}
```

A minimal consumer:

```python
#!/usr/bin/env python3
import json, os, sys
events = [json.loads(line) for line in sys.stdin]
if os.environ["HOOKSTR_PHASE"] == "backfill":
    print(f"{len(events)} webhooks while you were away")
else:
    for e in events:
        print(e["path"], e["headers"].get("x-gitlab-event"))
# exiting 0 acks the batch; raise or sys.exit(1) to have it retried
```

Replay state (attempt counts, backoff) is client-local in redb, keyed by
event id — a replay target being down (consumer not running) just means
backoff and retry; the events are already durable.

## Failure modes

| failure | outcome |
|---|---|
| ingest box down | provider retries per its schedule; this is the *only* failure surface providers see |
| nostrdb write failure | ingest 500s → provider retries |
| client crashes mid-replay | at-least-once + consumer dedupe → safe |
| duplicate provider deliveries | distinct events; consumer dedupes on its delivery id |
| replay target down | redb retry state + backoff; events are already durable |
| someone reaches the relay | it has no auth: they can read every stored webhook, and push events the drain would replay. Keep `relay_addr` on localhost or a private interface |
| ingest URL leaks | rotate the path token, re-register with providers |

## Crates

- [`hookstrd`](crates/hookstrd) — the daemon: axum ingest + the
  embedded `nostrdb_net` relay (EVENT/REQ/CLOSE, live subscriptions,
  NIP-77 responder).
- [`hookstr_cli`](crates/hookstr_cli) — drain client, built on
  `nostrdb_net`'s sync `Session` (negentropy backfill, live REQ,
  reconnects).
- [`hookstr-core`](crates/hookstr-core) — shared event schema:
  `WebhookRecord`, `sign_webhook_note`, `parse_webhook_note`.

`cargo test --workspace` runs end-to-end tests with no external services:
[`crates/hookstr_cli/tests/e2e.rs`](crates/hookstr_cli/tests/e2e.rs)
spins up the real hookstrd router + relay in-process and drives the real
drain binary — byte-exact replay (including a binary/base64 body), redb
dedupe across runs, realtime follow, peek, and `drain --exec` batching,
retries, timeouts and phases.

Both crates pin [nostrdb-rs]'s `sync-progress` branch by git rev.

Full design doc, including the reasoning behind each constraint and open
questions: [SPIKE.md](SPIKE.md).

[nostrdb]: https://github.com/damus-io/nostrdb
[nostrdb-rs]: https://github.com/damus-io/nostrdb-rs
[NIP-77]: https://github.com/nostr-protocol/nips/blob/master/77.md
