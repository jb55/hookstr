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
- **Pull-only, trusting one key.** The relay has no auth and will store
  any event a client sends it, so it binds to localhost and the drain
  reaches it through a tunnel. The drain holds a copy of hookstrd's key
  (`server_nsec_path`): it syncs and replays only events signed by it, so
  anything else pushed into the relay is never acted on. On the client,
  replay only acts on notes read back out of the local nostrdb after its
  ingester has verified and committed them — never on wire frames.

Each inbound delivery is one immutable kind `3003` nostr event, signed by
the server's key: the raw body as content (base64 + an `encoding` tag if
it isn't UTF-8), the ingest path in a `path` tag for replay routing, and
one `header` tag per preserved header. Schema lives in
[`hookstr-core`](crates/hookstr-core/src/lib.rs).

## Setup

### Server (the always-online box)

Generate a keypair for the server. The drain gets a copy of the same
nsec file:

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
the tunnel exposes it (default `ws://127.0.0.1:8081`) and a copy of
hookstrd's nsec file:

```
$ scp hooks.example.com:/var/lib/hookstr/server.nsec ~/.config/hookstr/
$ hookstr init ws://127.0.0.1:8081 --server-nsec-path ~/.config/hookstr/server.nsec
wrote config /home/you/.config/hookstr/config.toml
then: hookstr drain --target http://localhost:3000/webhooks
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

Then drain:

```
$ hookstr drain --target http://localhost:3000/webhooks
```

This negentropy-syncs the missed backlog, replays it oldest-first, and
then follows — each new webhook replays in under a second while the
connection is up, and a dropped connection reconnects and re-syncs on
its own. Pass `--once` to exit after the
catch-up replay instead (cron-style). Positional args scope the drain
to specific providers (`hookstr drain --target … acme stripe`; empty =
all) — so one hookstrd serves several consumers, each running its own
scoped drain with its own `--target`:

```
$ hookstr drain --target http://localhost:3003 acme
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
| someone reaches the relay | it has no auth: they can read every stored webhook. Keep `relay_addr` on localhost and behind a tunnel |
| events pushed into the relay | stored, but never synced or replayed: the drain only acts on events signed by the key in `server_nsec_path` |
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
dedupe across runs, realtime follow, peek, and forged events never
replaying.

Both crates pin [nostrdb-rs]'s `sync-progress` branch by git rev.

Full design doc, including the reasoning behind each constraint and open
questions: [SPIKE.md](SPIKE.md).

[nostrdb]: https://github.com/damus-io/nostrdb
[nostrdb-rs]: https://github.com/damus-io/nostrdb-rs
[NIP-77]: https://github.com/nostr-protocol/nips/blob/master/77.md
