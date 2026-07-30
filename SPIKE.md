# hookstr — store-and-forward webhook service, backed by nostr

## What this is

Webhook providers demand an always-online HTTPS endpoint and typically
auto-disable it after repeated delivery failures. A consumer behind a
tunnel — sometimes offline, tunnel URL rotating — loses that race
constantly.

hookstr splits delivery from consumption. `hookstrd` runs permanently on
always-online infra behind a TLS-terminating reverse proxy and does two
things: accept any webhook with a fast 2xx and persist it as a signed nostr
event in nostrdb, and serve those events back over an **authenticated nostr
relay** (NIP-42). `hookstr_cli` runs wherever the consumer lives,
negentropy-syncs (NIP-77) whatever it missed while offline, live-tails new
deliveries in realtime while connected, and replays each one as a real HTTP
request — byte-exact body, preserved headers — against a configured local
target per provider.

Providers see one endpoint that never goes down. The consumer sees every
webhook, eventually, in order, with valid signatures.

```
 provider A ──POST──▶ reverse proxy (hooks.example.com, TLS)
 provider B ──POST──▶   │  forwards everything to one port
                        ▼
                     hookstrd (one listener, demuxes by Upgrade header)
                        ├── /ingest/{token}/*  ──▶ axum ingest ──▶ nostrdb
                        └── /  (ws upgrade)    ──▶ relay        ◀── nostrdb
                                                       ▲
                                 NIP-42 AUTH + NIP-77 sync + live REQ
                                                       │
 dev machine:  hookstr_cli ◀───────────────────────────┘
                    │ replay (HTTP, raw body + preserved headers)
                    ▼
               http://localhost:PORT/<consumer webhook route>
```

## Design constraints

1. **Byte-exact body preservation.** Providers commonly sign the raw HTTP
   body (an `x-signature`-style HMAC header) and consumers validate against
   the exact bytes. The relay must preserve body bytes and all signature
   headers verbatim end-to-end.
2. **At-least-once replay must be safe.** Consumers commonly dedupe on a
   delivery-id header or payload field, so conservative replay (crash
   mid-replay, re-drain, retry) is fine — but that makes header
   preservation *dedupe-load-bearing*, not cosmetic.
3. **Fast 2xx.** Ingest accepts, persists durably, responds 204. 5xx only
   when persistence itself failed, so the provider retries. Never block on
   anything downstream of the store.
4. **Ordering is best-effort.** Providers already deliver out of order
   under retry, so consumers can't assume ordering anyway. Replay in
   `created_at` ascending because it's cheap and less surprising.

See the appendix for how these constraints were verified against the first
real consumer.

## Event schema

One inbound delivery == one immutable, regular nostr event, signed by the
server's keypair. Implemented and documented in
`crates/hookstr-core/src/lib.rs` (`WebhookRecord`, `sign_webhook_note`,
`parse_webhook_note`, `event_frame`).

- **kind 3003** — regular range (1000 ≤ n < 10000): non-replaceable,
  non-ephemeral, exactly the semantics of an immutable delivery log.
  Unassigned in the NIPs registry at time of writing (re-verify before
  building).
- **content** — the raw body, verbatim UTF-8 string. JSON string escaping
  round-trips UTF-8 text exactly and nostrdb's `Note::content()` returns the
  identical bytes, so HMACs survive. Provider bodies are JSON in practice,
  so this is the 100% case and keeps events greppable (`nak`/nostrdb queries
  show real payloads). A non-UTF-8 body falls back to base64 +
  `["encoding","base64"]` tag rather than being rejected — never lose a
  webhook.
- **tags** — `["t", provider]` (single-letter → indexed, filterable);
  `["path", "<provider>/<type>"]` (the replay-routing key);
  `["header", name, value]` per preserved header (lowercased, request
  order); `["received_at", unix-ms]`. `created_at` carries the same instant
  at seconds precision and is the negentropy ordering key — set explicitly
  so the two can never disagree.
- **headers** — preserve everything except hop-by-hop (RFC 9110 §7.6.1),
  transport framing (`content-length`, `accept-encoding` — the replay
  client recomputes), and reverse-proxy breadcrumbs (`x-forwarded-*`,
  `x-real-ip`). Keep the lot rather than curate: signature and delivery-id
  headers are the known-load-bearing ones, but every provider names its own.

## hookstrd

One listener behind the reverse proxy, one nostrdb. hookstrd owns the accept
loop (`hookstrd::serve`): it peeks each connection (without consuming it) and
hands a websocket upgrade to the relay and everything else to the axum ingest
router. So the proxy just forwards to a single port — no `Upgrade`-header
routing of its own.

**Ingest (axum).** `POST /ingest/{token}/{provider}/{type}`. The `{token}`
path segment is a long random secret baked into the URL registered with
each provider — the spam gate for a necessarily public endpoint. Payload
*authenticity* is enforced downstream by the consumer's own signature
check, so edge verification of provider signatures is optional
(deliberately deferred; it would spread provider secrets onto the ingest
box). Handler: read raw bytes, build `WebhookRecord`, `sign_webhook_note`,
ingest via `Ndb::process_client_event` with the `event_frame`, **wait for
durability, then 204**. nostrdb ingest is queued to background writer
threads, so responding 204 at queue time could lose a delivery on crash.
Solved with nostrdb's own subscriptions: subscribe on an ids filter for the
note before ingesting, and nostrdb notifies the subscription when ingest
completes — await that, then 204. (No polling; nostr_relay_sync's `await_ingest`
is the poll-based fallback if ever needed.) Persistence failure → 500 so
the provider retries. Never block on anything downstream of nostrdb.

**Relay (ws).** The server side already exists:
[`nostrdb_relay`](https://github.com/jb55/nostrdb-relay) (~450 lines,
extracted from notedeck 2026-07-08 so hookstr builds on any machine) is a
complete embeddable minimal relay — `spawn(ndb, addr) -> RelayHandle` speaking
EVENT / REQ (stored replay **plus live phase** via `SubscriptionStream`) /
CLOSE, and a NIP-77 responder (NEG-OPEN/NEG-MSG/NEG-CLOSE). hookstrd embeds
it next to the axum listener. No relay reimplementation.

Two things it needs that it doesn't have:

- **NIP-42 AUTH — core requirement, not polish.** The relay must challenge
  on connect and reject REQ/NEG-OPEN until the client AUTHs with a pubkey
  on the allowlist. This is what makes it safe to expose the ws side
  publicly and lets the client hold a standing subscription for realtime
  webhooks. nostrdb_relay has no auth on either side today; it's our crate,
  so implement it upstream there (challenge issue, `AUTH` frame validation,
  per-connection authed flag gating REQ/NEG). Interim fallback while that
  lands: a secret ws path in the reverse proxy.
- **Stored-replay cap.** nostrdb_relay caps REQ stored replay at 500 notes.
  Fine once negentropy is in (reconcile pulls exactly the missing set); the
  milestone-1 dumb drain must paginate with `until` instead.

## hookstr_cli

Builds on [`nostr_relay_sync`](https://github.com/jb55/nostr-relay-sync)
(extracted 2026-07-08 from notedeck's `relay_sync`, which is also used for
headway-issue sync between headway_cli and the notedeck GUI; the standalone
copy drops the `enostr` dep — pubkeys are `[u8; 32]`, `parse_nsec` derives
via secp256k1):

- `Relay::connect`, `Relay::authenticate(seckey)` — NIP-42 client half.
- `Relay::reconcile(filter_json, NegentropyStorageVector) -> Diff{need,
  have}` — NIP-77 initiator; `local_set(ndb, filter)` builds the local side.
- `sync_into(ndb, filter_json)` — one-shot REQ→ingest until EOSE.
- `stream_into(ndb, filter_json)` — the same without the trailing CLOSE:
  the subscription stays open and `pump_one(ndb)` feeds each subsequent
  frame to the ingester.
- `reconcile_sync` — bidirectional (chunks of 300); **do not use**: the
  drain is pull-only. hookstrd's DB is the source of truth and nothing
  should push into it from the consumer side.
- `open_ndb`, `await_ingest`, nsec helpers (`login`/`stored_nsec`/`parse_nsec`).

Default drain behavior (`hookstr_cli drain`): connect → AUTH → negentropy
reconcile with `{"kinds":[3003]}` → fetch `Diff::need` into the local
nostrdb → replay the due backlog oldest-first → **follow**: hold a standing
REQ and replay each new webhook as it arrives, redialing (with backoff) when
the connection drops. `--once` exits after the backlog replay instead
(cron-style).

**Trust boundary.** The relay connection only feeds nostrdb's ingester,
which verifies before committing. Realtime notifications come off a *local*
`ndb.subscribe(kinds=[3003])` stream — it fires only for verified, committed
notes — never off ids claimed in wire frames. (Same pattern as hookstrd's
durable-then-204, from the other side.)

wss TLS note: nostr_relay_sync pins `tokio-tungstenite = "0.24"` featureless;
hookstr declares the same version with `rustls-tls-native-roots`, and feature
unification gives its `connect_async` wss support.

**Replay.** Reconstruct with `parse_webhook_note`, then POST with the
preserved headers and byte-exact body. Deliveries are self-describing: the
`path` tag mirrors the ingest URL's `{provider}/{type}` suffix, so a single
`--target` base routes everything to `{target}/{provider}/{type}` when the
consumer's route mirrors the ingest path (it's a per-run flag, not config —
where a drain delivers is specific to the consumer it feeds, not to the
durable sync relationship the config describes); a `[targets]` map in the
config holds per-path overrides for consumers that don't. Success/attempt
state is
**client-local** in redb (32-byte event id → attempts/next_retry) — never
modeled as nostr events, which would pollute the sync set. Exponential
backoff on failure (target down = consumer not running, normal).
At-least-once + consumer-side dedupe makes all crash-mid-replay windows
safe.

## Retention: store everything

**Decision: no deletion, ever.** Negentropy reconciles over the full set,
so keeping everything is not a compromise — it's what makes sync trivial
(no "server pruned events the client never saw" edge, no rotation
choreography, `since` bounds are optimization only). nostrdb-rs at the
pinned rev exposes no deletion anyway (verified: no delete fn in `ndb.rs`;
`NDB_NOTE_FLAG_DELETED` is an unexposed bindings constant; `Ndb::compact`
filters by author, useless here). Disk math says who cares: webhook bodies
are a few KB; even 1M deliveries ≈ single-digit GB. Set the LMDB mapsize
generously at open and forget about it.

## Reverse proxy

Any TLS-terminating proxy works. hookstrd serves ingest and the relay on one
port and sorts the two out itself, so the proxy just forwards everything —
no header matching to route websocket upgrades. Caddy sketch:

```caddyfile
hooks.example.com {
        reverse_proxy localhost:8080
}
```

hookstrd (`hookstrd::serve`) peeks each connection and dispatches a websocket
upgrade to the relay and everything else — provider `POST /ingest/*` and a
browser `GET /` (the static landing page) — to the axum ingest router. Method
alone can't tell them apart (a ws upgrade and a browser hit are both `GET /`),
so it keys on the `Upgrade: websocket` header.

This one-port combine is the default. Advanced deployments can instead set
`ingest_addr` + `relay_addr` (in place of `listen_addr`) to bind the two on
separate listeners — putting the relay on a private interface (e.g. WireGuard),
off the public net, with only ingest behind the proxy. Config load requires
exactly one of the two forms; `HookstrdConfig::listen` resolves it.

TLS is the proxy's problem. hookstrd listens plaintext on localhost only.

## Failure modes

| failure | outcome |
|---|---|
| ingest box down | provider retries per its schedule; this is now the *only* failure surface providers see, and it's rare |
| nostrdb write failure | ingest 500s → provider retries |
| client crashes mid-replay | at-least-once + consumer dedupe → safe |
| duplicate provider deliveries | distinct nostr events (different receipt instants); consumer dedupes on its delivery id |
| replay target down | redb retry state + backoff; events are already durable |
| unauthed ws client | NIP-42: no REQ/NEG until AUTH from allowlisted pubkey |
| ingest URL leaks | rotate the path token, re-register with providers |

## Milestones

1. **Ingest + store + authed relay + dumb drain.** — **DONE (2026-07-08).**
   axum ingest with durable-then-204; embed nostrdb_relay; NIP-42 in
   nostrdb_relay (server, `spawn_with_auth`) and relay_sync
   (`Relay::authenticate`) with pubkey allowlist; drain via paginated REQ
   (`until`-pagination past the 500 cap) + replay + redb state. This alone
   solves the provider-disables-my-endpoint problem end to end. Verified by
   an end-to-end smoke test: curl → 204 → authed drain → byte-exact replay
   (headers preserved), redb dedupe across runs, allowlist refusal for a
   wrong key.
2. **Negentropy + live-tail.** — **DONE (2026-07-08).** `Relay::reconcile`
   pull-only catch-up is the default (no paginated fallback — the relay is
   assumed to speak NIP-77); follow mode via `stream_into` + `pump_one`,
   with new-event notifications taken from the local nostrdb subscription.
   Smoke-tested: a delivery made while following replays in under a second.

   Both milestones are also covered by cargo e2e tests (2026-07-09):
   `crates/hookstr_cli/tests/e2e.rs` runs the real hookstrd router + relay
   in-process and drives the real drain binary (byte-exact replay incl. a
   binary/base64 body, redb dedupe, realtime follow, allowlist refusal) —
   hookstrd was split into lib + thin main for this. The client protocol
   itself (auth, reconcile diff, sync, stream/pump trust boundary) is
   covered in nostr-relay-sync's own `tests/e2e.rs` against a spawned
   nostrdb_relay. `cargo test --workspace` needs no running services.
3. **Polish.** Optional edge signature verification per provider; metrics /
   `nak`-style inspection recipes; systemd unit + deploy notes.

## Open questions

- kind 3003 — re-verify unassigned in the NIPs registry before building.
- ~~NIP-42 shape in nostrdb_relay~~ — decided in milestone 1: REQ, NEG-OPEN
  *and* EVENT are gated behind AUTH; `AuthConfig.accept_events = false`
  additionally makes the relay read-only (hookstrd uses this — the drain is
  pull-only). Challenge is per-connection, no expiry beyond the connection;
  AUTH events must be within 10 min of now. The `relay` tag is not checked
  (behind a proxy there's nothing local to compare against).
- durable-then-204: the subscription notification is assumed == durable; the
  smoke test proves ordering functionally (drain sees the note after 204),
  but confirm against nostrdb internals that notification fires
  post-LMDB-commit, not merely post-process.
- rustls flavor: aws-lc-rs vs ring — whatever notedeck converges on.
- nostrdb max note size vs pathological webhook bodies — find the limit,
  decide truncate-vs-reject (and whether providers that big exist).
- ~~relay_sync still depends on notedeck's `enostr` internally~~ — resolved
  by the 2026-07-08 extraction: `nostr_relay_sync` has no enostr dep. The
  in-tree notedeck `relay_sync` (and its NIP-42 additions) still exists;
  whether notedeck switches to the standalone crates is a notedeck decision.
  Repo/crate naming may still change (`nostrdb_relay_sync`?) before any
  crates.io publish.
- LMDB mapsize default for a store-everything DB.

## Appendix: first deployment (commerce / Modern Treasury)

The consumer that motivated this: a NestJS API at `localhost:3003`
receiving Modern Treasury sandbox webhooks during local dev via an ngrok
free tunnel that dies whenever the laptop sleeps — after which MT disables
the endpoint. `hookstrd` will run behind Caddy on a static home IP.

The generic design constraints above, verified against this consumer
(`~/dev/hydra/commerce`):

1. **Raw-body HMAC**: the MT handler reads `x-signature` and calls the MT
   SDK's `webhooks.validateSignature(rawBody, signature)` against the exact
   raw bytes
   (`apps/api/src/webhooks/providers/modern-treasury/modern-treasury.handler.ts:79-88`).
2. **Dedupe on a delivery-id header**: `x-webhook-id`, not anything in the
   payload (`modern-treasury.handler.ts:95`; unique-index insert in
   `webhooks.service.ts:44-58` skips duplicates).
3. **Ordering non-critical**: the handler re-fetches entity state from the
   MT API rather than trusting event payloads.

Target routing for `hookstr_cli` — this consumer's webhook routes mirror
the ingest path (`/{provider}/{type}`), so one base covers every provider,
current and future, with zero per-provider config:

```
$ hookstr drain --target http://localhost:3003/api/v1/webhook
```
