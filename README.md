# hookstr

A store-and-forward webhook service backed by nostr: `hookstrd` runs on
always-online home infra, gives webhook providers an endpoint that never
goes down, and persists every delivery as a signed nostr event in nostrdb;
`hookstr_cli` negentropy-syncs missed deliveries to a dev machine (plus
realtime live-tail over an authenticated relay connection) and replays them
byte-exact against a local HTTP target.

Design doc: [SPIKE.md](SPIKE.md). Shared event schema:
[crates/hookstr-core](crates/hookstr-core/src/lib.rs).
