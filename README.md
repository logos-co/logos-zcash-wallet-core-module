# logos-zcash-wallet-core-module

`zcash_wallet_core_module`: the Zcash wallet engine for Logos. A light client built on
librustzcash's NU7 release (`zcash_client_backend` 0.25.0, `zcash_client_sqlite` 0.23.0),
running as a Rust module inside its own `logos_host`.

It answers only `zcash_wallet_backend`. Apps and the CLI reach it through the backend,
which holds the roles; this module holds the keys.

## What it does

- **Keys at rest.** A 24-word phrase and a random database key, each sealed with age's
  scrypt recipient under the wallet password. Changing the password rewrites those two
  small files; the database is not re-keyed.
- **Encrypted storage.** The wallet database and the block cache are SQLCipher
  (`rusqlite` `bundled-sqlcipher`: CommonCrypto on Apple, vendored OpenSSL elsewhere),
  opened through `WalletDb::from_connection`. Each open first applies the
  `zcash_client_sqlite` migrations the database lacks, so a wallet made by an older build
  is brought up to date; a migration that needs the seed unseals it with the password
  given to open.
- **Sync over Tor.** Every call goes through Tor unless the routes say otherwise: the Tor that comes with Logos
  (`tor_module`), reached over Logos IPC, where each gRPC call is one HTTP/2 request it makes
  and TLS is its job; or the user's own, through a `socks5h://` proxy. Either way each client
  takes its own circuit (an isolation key, or SOCKS credentials). Two exceptions, both
  reached directly:
  - servers on the user's own network, which Tor cannot reach: private, loopback, CGNAT and
    link-local addresses, or `.local`, `.lan`, `.home.arpa` and `.internal` names that resolve
    only to such addresses;
  - an https server the user chose to reach without Tor (the routes' `direct` list). An onion
    service is always reached through Tor.

  Compact blocks download in 1,000-block chunks from the tip down, one circuit
  per chunk, so the request order says nothing about where the wallet's notes are. A chunk
  comes 50 blocks a call: one from late 2022's spam is up to ~100 MB, more than one request
  over Tor can carry. A call that times out or breaks off is made again on a fresh circuit,
  in halves if it ran out of time. Tree states are fetched only at chunk
  boundaries; a scan that starts mid-chunk advances the boundary state through cached blocks
  (`sync::frontier`). Each chunk must continue the tree state below it and lead to the one
  above. Scan calls are capped at about 2,000 shielded outputs, so a stop request is honoured
  within a second. The tip is polled every 30 s, and each of a poll's calls gives up after
  60 s. Tips, a new wallet's tree state and subtree roots come from one sync server while it
  answers; after a failure the next one is asked, at once until each has had its turn.
- **Cross-check.** Another operator's servers take turns, every 2 minutes, comparing the block
  at the lower of two tips with the chain the wallet reads: the local node's, or with sync
  split from lookups, the sync server's. Two mismatches in a row set `lastError`; `crossCheck`
  in `sync_status` holds the last result.
- **Use Tor for what identifies you.** Each call has a class: `sync` (compact blocks, tree
  states, subtree roots), `tip` (tips, server info, the cross-check's read of the chain the
  wallet syncs from), `details` (a transaction by txid), `taddr` (transactions by transparent
  address), `broadcast` (sends, and asking another operator whether it has one) and `mempool`.
  The core's own `check` class reads the cross-checking operator and is never direct by class.
  The classes in the routes'
  `directFor` (`sync` and `tip`, never more) dial directly, onion services excepted; the rest
  take the proxy. With another operator in the routes, sync and tips stay with the first
  server's operator, `details` and `taddr` lookups go to the others, and sends try the others
  first, so the operator that sees this IP download blocks never sees what the wallet looks
  up. The others also check its chain, and no fallback dials them directly: with one sync
  server, tips have none. Each lookup then also waits up to 60 s more, on top of the usual 0-20 s, so its timing
  is harder to match to a direct download. The local node keeps all of this as before: it
  serves every read itself.
- **Restore without a lookup.** Restores start from a bundled checkpoint
  (`checkpoints/*.json`, every 10,000 blocks; mainnet entries agree across two operators),
  so no server learns the birthday. `examples/checkpoints.rs` rebuilds them.
- **Sends.** A proposal spends from one shielded pool at a time, Ironwood first; using
  several pools needs explicit consent (ZIP 315). Approval decrypts the phrase, proves
  with Sapling parameters checked against `zcash_proofs`' hashes, signs with an explicit,
  NU7-aware expiry, and broadcasts each transaction on a fresh circuit. Within two minutes
  another broadcast server must know it, else it is sent there too; with only one, the local
  node is asked when it syncs the wallet, and the payment goes back to that server, never
  through the node.
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
| `history(account, page)` | Newest first, with kind, pools, memos and the amount that crossed pools. Each row also says what the transaction was for: payments to others (`to`) and to the wallet's own addresses (`toSelf`), or for a shielding, the amount shielded and the transparent addresses it spent (`from`) |
| `reveal_seed(password)`, `export_viewing_key(account, password)` | Once, after checking the password |

Events: `wallet_state_changed`, `sync_progress`, `balance_changed`, `job_finished`.

Job parameters carry the route table the backend got from `zcash_node_module`:
`{ "routes": { "proxy": "tor", "servers": ["https://..."], "direct": ["https://..."] } }`.
The proxy is `tor` for the built-in Tor, or the user's own as `socks5h://host:port`.
`direct`, optional, lists the servers to reach without Tor. When nothing goes through Tor, the
proxy may be `"direct"`, which means none.

With Tor for what identifies you, the routes also carry `directFor: ["sync", "tip"]`, where
lookups go (`details`, `taddr`; the sync servers when absent) and each server's operator
(`operators`, url to name). Any other class in `directFor` is refused. A core older than this
ignores the four fields and keeps every call on the proxy.

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

`tools/regtest/chain.sh` runs a local chain: zebrad 7.0.0 or later (regtest disables
proof of work, so its `generate` RPC mines on demand) and two lightwalletd on loopback.
`prepare` mines Orchard coinbase before NU6.3, then transparent coinbase, then Ironwood
coinbase, and matures it all. NU7 activates at 200, so the wallet works past it, as on
testnet and soon mainnet: sends expire 120 blocks out, and the migration's delays scale by
three. The wallet reaches its `http://127.0.0.1:` servers directly, as
it does any server on the user's own network. A module instance runs regtest when
its persistence directory holds `regtest.json`.

logos-zebra-nix builds both processes: `regtest-zebrad` runs zebrad's command line on
libzebrad_c, and `lightwalletd` is v0.5.5. `nix build .#regtest-chain` packages the script
with `regtest.json` (in `share/regtest/`) for harnesses outside this repository, such as
the app's doctest. The script also runs under Git Bash on Windows, against zebra-nix's
Windows builds of both processes; `packages.x86_64-windows.regtest-chain` packages it for the
doctest's Windows leg. There it stops each process with `taskkill`.

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

The test restores the wallet and pays from Ironwood. It then pays itself 100 zatoshis at a
transparent address, which it must report as uneconomic: worth less than spending them would
cost. It shields the transparent coinbase and runs the ZIP 318 migration of the Orchard
coinbase to the end. Mining to a transparent address
(`chain.sh start <taddr>`) is fastest while the migration waits on its schedule.

`tests/tor_use.rs` checks where each class goes on the same chain. It puts two operators,
`https://lwd-a.test` and `https://lwd-b.test`, behind counting fronts (the `test-dial` feature,
off in every build but this test's, dials them in plain HTTP/2), with stand-ins for tor_module
and the local node. With Tor for what identifies you, no lookup or send goes direct, and the
second operator sees nothing direct and checks the first's chain over Tor; with Tor for nothing,
tor_module is never called; with the local node, every read stays on it and the cross-check
goes over Tor.

```bash
REGTEST_HEIGHTS=../tools/regtest/regtest.json REGTEST_PHRASE=phrase.txt \
REGTEST_RPC=http://127.0.0.1:28232 REGTEST_SERVERS=http://127.0.0.1:29061,http://127.0.0.1:29063 \
ZCASH_PARAMS_DIR=... cargo test --release --no-default-features --features test-dial --test tor_use -- --ignored --nocapture
```

`examples/find_payment.rs` trial-decrypts a block range, the mempool or one transaction
with a viewing key, outside any wallet: it tells "not on chain" from "on chain but missed".
