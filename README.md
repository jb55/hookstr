# hookstr

A store-and-forward webhook service backed by nostr: `hookstrd` runs on
always-online home infra, gives webhook providers an endpoint that never
goes down, and persists every delivery as a signed nostr event in nostrdb;
`hookstr_cli drain` negentropy-syncs missed deliveries to a dev machine over
an authenticated (NIP-42) relay connection, replays them byte-exact against
a local HTTP target, then keeps following — new webhooks replay in realtime
while it runs (`--once` for a single catch-up pass).

Design doc: [SPIKE.md](SPIKE.md). Shared event schema:
[crates/hookstr-core](crates/hookstr-core/src/lib.rs).
