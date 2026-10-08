# logos-zcash-wallet-core-module

`zcash_wallet_core_module`: the Zcash wallet engine for Logos. A light client built on
librustzcash's pre-release NU7 cohort (`zcash_client_backend` 0.25.0-pre.1,
`zcash_client_sqlite` 0.23.0-pre.1), running as a Rust module inside its own `logos_host`.

It answers only `zcash_wallet_backend`. Apps and the CLI reach it through the backend,
which holds the roles; this module holds the keys.

## What it does

- **Keys at rest.** A 24-word phrase and a random database key, each sealed with age's
  scrypt recipient under the wallet password. Changing the password rewrites those two
  small files; the database is not re-keyed.
- **Encrypted storage.** The wallet database and the block cache are SQLCipher
  (`rusqlite` `bundled-sqlcipher`: CommonCrypto on Apple, vendored OpenSSL elsewhere),
  opened through `WalletDb::from_connection`.
- **Sync over Tor.** Every call goes through a `socks5h://` proxy; SOCKS credentials pick
  the circuit. Compact blocks download in 1,000-block chunks from the tip down, one circuit
  per chunk, so the request order says nothing about where the wallet's notes are. Tree
  states are fetched only at chunk boundaries; a scan that starts mid-chunk advances the
  boundary state through cached blocks (`sync::frontier`). Each chunk must continue the tree
  state below it and lead to the one above. Scan calls are capped at about 2,000 shielded
  outputs, so a stop request is honoured within a second.
- **Restore without a lookup.** Restores start from a bundled checkpoint
  (`checkpoints/*.json`, every 10,000 blocks; mainnet entries agree across two operators),
  so no server learns the birthday. `examples/checkpoints.rs` rebuilds them.
- **Sends.** A proposal spends from one shielded pool at a time, Ironwood first; using
  several pools needs explicit consent (ZIP 315). Approval decrypts the phrase, proves
  with Sapling parameters checked against `zcash_proofs`' hashes, signs with an explicit,
  NU7-aware expiry, and broadcasts each transaction on a fresh circuit.
- **Shielding.** One transparent address per transaction, never several (ZIP 315),
  offered above 0.001 ZEC.

## API

Every structured value is a JSON string: `{ "ok": true, ... }` or `{ "ok": false, "error" }`.

| Method | Returns |
|---|---|
| `start_job(kind, params)` | `{ jobId, receipt }` for `create_wallet`, `restore_wallet`, `open_wallet`, `close_wallet`, `change_password`, `propose`, `propose_shielding`, `sign_and_send` |
| `job_status`, `job_result`, `ack_job`, `cancel_job` | Keyed by job id and receipt |
| `list_wallets(network)`, `wallet_status()`, `sync_status()` | Reads |
| `balances(account)` | Per pool: spendable, pending change, pending spendability, total; transparent funds per address |
| `addresses(account)`, `new_address(account)` | The shielded-only Unified Address and the current transparent address |
| `history(account, page)` | Newest first, with kind, pools, memos and the amount that crossed pools |
| `reveal_seed(password)`, `export_viewing_key(account, password)` | Once, after checking the password |

Events: `wallet_state_changed`, `sync_progress`, `balance_changed`, `job_finished`.

Job parameters carry the route table the backend got from `zcash_node_module`:
`{ "routes": { "proxy": "socks5h://127.0.0.1:9050", "servers": ["https://..."] } }`.

## Development

```bash
nix build github:logos-co/logos-module-builder#rust-sdk-src -o logos-rust-sdk-src
cd rust-lib && cargo test --no-default-features
```

Live tests need a Tor SOCKS port:

```bash
ZCASH_TEST_TOR=socks5h://127.0.0.1:9050 cargo test --no-default-features -- --ignored --nocapture
```

`ZCASH_PARAMS_DIR` points at a directory holding `sapling-spend.params` and
`sapling-output.params` for sends during development; packaged builds ship them beside
the plugin.

Callers other than the backend are refused. For tests, a `callers.json` in the instance's
persistence directory can admit more: `{ "modules": [...], "allowHost": true }`.

## Regtest

`tools/regtest/chain.sh` runs a local chain: zebrad 7.0.0-rc.0 or later (regtest disables
proof of work, so its `generate` RPC mines on demand) and two lightwalletd on loopback.
`prepare` mines Orchard coinbase before NU6.3, then transparent coinbase, then Ironwood
coinbase, and matures it all. The wallet joins with proxy `"direct"`, which only a regtest
wallet accepts, and only for `http://127.0.0.1:` servers. A module instance runs regtest when
its persistence directory holds `regtest.json`.

logos-zebra-nix builds both processes: `regtest-zebrad` runs zebrad's command line on
libzebrad_c, and `lightwalletd` is v0.5.4. `nix build .#regtest-chain` packages the script
with `regtest.json` (in `share/regtest/`) for harnesses outside this repository, such as
the app's doctest.

```bash
cd rust-lib
nix build github:logos-co/logos-zebra-nix#regtest-zebrad -o zebrad
nix build github:logos-co/logos-zebra-nix#lightwalletd -o lightwalletd
cargo run --no-default-features --example regtest_keys -- ../tools/regtest/regtest.json > keys.json
jq -r .phrase keys.json > phrase.txt
ZEBRAD=$PWD/zebrad/bin/zebrad LIGHTWALLETD=$PWD/lightwalletd/bin/lightwalletd CHAIN_DIR=/tmp/zchain \
  ../tools/regtest/chain.sh prepare "$(jq -r .orchardUa keys.json)" "$(jq -r .transparent keys.json)"
REGTEST_HEIGHTS=../tools/regtest/regtest.json REGTEST_PHRASE=phrase.txt \
REGTEST_RPC=http://127.0.0.1:28232 REGTEST_SERVERS=http://127.0.0.1:29061,http://127.0.0.1:29063 \
ZCASH_PARAMS_DIR=... cargo test --release --no-default-features --test regtest -- --ignored --nocapture
```

The test restores the wallet, pays from Ironwood, shields the transparent coinbase, and runs
the ZIP 318 migration of the Orchard coinbase to the end. Mining to a transparent address
(`chain.sh start <taddr>`) is fastest while the migration waits on its schedule.

`examples/find_payment.rs` trial-decrypts a block range, the mempool or one transaction
with a viewing key, outside any wallet: it tells "not on chain" from "on chain but missed".
