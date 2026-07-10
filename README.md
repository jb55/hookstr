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
  events back over an authenticated ([NIP-42]) embedded nostr relay.
- **`hookstr drain`** runs wherever the consumer lives. It
  negentropy-syncs ([NIP-77]) whatever it missed while offline, replays
  each delivery as a real HTTP request — byte-exact body, preserved
  headers — against a configured local target, then keeps following:
  new webhooks replay in realtime while it's connected, redialing with
  backoff when the connection drops.

Providers see one endpoint that never goes down. The consumer sees every
webhook, eventually, in order, with valid signatures.

```
 provider A ──POST──▶ reverse proxy (hooks.example.com, TLS)
 provider B ──POST──▶   ├── /ingest/{token}/*  ──▶ hookstrd axum ──▶ nostrdb
                        └── /  (ws upgrade)    ──▶ hookstrd relay ◀── nostrdb
                                                       ▲
                                 NIP-42 AUTH + NIP-77 sync + live REQ
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
- **Pull-only.** The relay is read-only (NIP-42-gated, `accept_events =
  false`); hookstrd's DB is the source of truth and nothing pushes into
  it over the wire. On the client, replay only acts on notes read back
  out of the local nostrdb after its ingester has verified and committed
  them — never on wire frames.

Each inbound delivery is one immutable kind `3003` nostr event, signed by
the server's key: the raw body as content (base64 + an `encoding` tag if
it isn't UTF-8), the ingest path in a `path` tag for replay routing, and
one `header` tag per preserved header. Schema lives in
[`hookstr-core`](crates/hookstr-core/src/lib.rs).

## Setup

### Server (the always-online box)

Generate a keypair for the server, and get the drain client's pubkey
(generated below) for the allowlist:

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
ingest_addr = "127.0.0.1:8080"
relay_addr = "127.0.0.1:8081"
nsec_path = "/var/lib/hookstr/server.nsec"
drain_pubkey = "<drain client pubkey, hex>"

# Per-provider ingest secrets; each token authenticates and names its
# provider, and can be rotated/revoked without disturbing the others.
[ingest_tokens]
acme = "long-random-secret"
stripe = "another-long-random-secret"
```

Run it behind any TLS-terminating proxy; Caddy sketch:

```caddyfile
hooks.example.com {
        handle /ingest/* {
                reverse_proxy localhost:8080
        }
        handle {
                reverse_proxy localhost:8081   # ws upgrade passes through
        }
}
```

```
$ hookstrd --config hookstrd.toml
```

Register `https://hooks.example.com/ingest/<token>/<any/path>` with each
provider — e.g. `/ingest/s3cret/acme/events`. The token alone
authenticates the delivery (and tags it with its provider); the rest of
the path is free-form and becomes the replay routing key. The token is
also the spam gate for a necessarily public endpoint; payload
authenticity is still enforced by the consumer's own signature check on
replay.

### Client (the dev machine)

Generate the drain keypair (`hookstr keygen` again — its pubkey is
what goes in `drain_pubkey` above), write the nsec to a file, and
configure `~/.config/hookstr/config.toml` (or pass `--config`; see
[config/hookstr.example.toml](config/hookstr.example.toml)):

```toml
relay_url = "wss://hooks.example.com"
db_path = "/var/lib/hookstr-cli/ndb"
redb_path = "/var/lib/hookstr-cli/replays.redb"
nsec_path = "/var/lib/hookstr-cli/drain.nsec"

# Deliveries are self-describing: the event's path tag mirrors everything
# after the token in the ingest URL, so a delivery ingested at
# /ingest/<token>/acme/events replays to {target_base}/acme/events...
target_base = "http://localhost:3000/webhooks"

# ...with optional per-path overrides for consumers whose routes don't
# mirror the ingest path.
#[targets]
#"acme/events" = "http://localhost:4000/hooks"
```

Then drain:

```
$ hookstr drain
```

This connects, AUTHs, negentropy-syncs the missed backlog, replays it
oldest-first, and then follows — each new webhook replays in under a
second while the connection is up. Pass `--once` to exit after the
catch-up replay instead (cron-style). Positional args scope the drain
to specific providers (`hookstr drain acme stripe`; empty = all) — run
one drain per consumer, each scoped to what it handles.

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
| unauthed ws client | NIP-42: no REQ/NEG until AUTH from an allowlisted pubkey |
| ingest URL leaks | rotate the path token, re-register with providers |

## Crates

- [`hookstrd`](crates/hookstrd) — the daemon: axum ingest +
  embedded [nostrdb-relay] (EVENT/REQ/CLOSE, live subscriptions, NIP-42,
  NIP-77 responder).
- [`hookstr_cli`](crates/hookstr_cli) — drain client, built on
  [nostr-relay-sync] (NIP-42 client, negentropy reconcile, live streaming).
- [`hookstr-core`](crates/hookstr-core) — shared event schema:
  `WebhookRecord`, `sign_webhook_note`, `parse_webhook_note`.

`cargo test --workspace` runs end-to-end tests with no external services:
[`crates/hookstr_cli/tests/e2e.rs`](crates/hookstr_cli/tests/e2e.rs)
spins up the real hookstrd router + relay in-process and drives the real
drain binary — byte-exact replay (including a binary/base64 body), redb
dedupe across runs, realtime follow, and allowlist refusal.

Full design doc, including the reasoning behind each constraint and open
questions: [SPIKE.md](SPIKE.md).

[nostrdb]: https://github.com/damus-io/nostrdb
[nostrdb-relay]: https://github.com/jb55/nostrdb-relay
[nostr-relay-sync]: https://github.com/jb55/nostr-relay-sync
[NIP-42]: https://github.com/nostr-protocol/nips/blob/master/42.md
[NIP-77]: https://github.com/nostr-protocol/nips/blob/master/77.md
