#!/bin/bash
# A local regtest chain for the wallet tests: zebrad (7.0.0-rc.0 or later, PoW off on regtest)
# plus two lightwalletd on loopback, standing in for two operators.
#   chain.sh start <miner address>   (re)start zebrad mining to that address, and both lightwalletd
#   chain.sh prepare <ua> <taddr>    mine the test chain: Orchard coinbase, transparent, Ironwood
#   chain.sh mine <n> | rpc <method> [json] | lwd | stop
# Env: ZEBRAD, LIGHTWALLETD (binaries), CHAIN_DIR (state, default ./chain). Upgrade heights
# come from regtest.json beside this script, the file the wallet core reads.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
C="${CHAIN_DIR:-$PWD/chain}"; mkdir -p "$C"
ZEBRAD="${ZEBRAD:?set ZEBRAD to a zebrad binary}"; LWD="${LIGHTWALLETD:?set LIGHTWALLETD to a lightwalletd binary}"
RPC_PORT="${RPC_PORT:-28232}"; P2P_PORT="${P2P_PORT:-28233}"; RPC="127.0.0.1:$RPC_PORT"
LWD_PORTS=(29061 29063)
NU6_3=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["nu6_3"])' "$HERE/regtest.json")
# Own session, so stopping whatever started the chain does not take it down. perl execs the
# daemon, so `$!` is the daemon's own PID.
DETACH=(perl -MPOSIX -e 'POSIX::setsid(); exec @ARGV' --)
rpc() { curl -s --max-time 300 -H 'content-type: application/json' --data-binary "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":${2:-[]}}" "http://$RPC/"; }
height() { rpc getblockcount | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"])'; }
mine_to() { while [ "$(height)" -lt "$1" ]; do n=$(( $1 - $(height) )); [ $n -gt 20 ] && n=20; rpc generate "[$n]" > /dev/null; done; }
# Kills by PID and waits for the exit, so a restart never talks to the old process.
stop_pid() {
  [ -f "$C/$1.pid" ] || return 0
  local pid; pid=$(cat "$C/$1.pid")
  kill "$pid" 2>/dev/null
  while kill -0 "$pid" 2>/dev/null; do sleep 0.2; done
  rm -f "$C/$1.pid"
}
case "${1:-}" in
start)
  # zebrad backs up its non-finalized blocks every 5 s; a faster restart loses the newest.
  [ -f "$C/zebrad.pid" ] && sleep 6
  stop_pid zebrad
  cat > "$C/zebrad.toml" <<TOML
[mining]
miner_address = "$2"
[network]
network = "Regtest"
listen_addr = "127.0.0.1:$P2P_PORT"
cache_dir = false
[network.testnet_parameters.activation_heights]
NU5 = 1
NU6 = 1
"NU6.1" = 1
"NU6.2" = 1
"NU6.3" = $NU6_3
[rpc]
listen_addr = "$RPC"
enable_cookie_auth = false
[state]
cache_dir = "$C/state"
[tracing]
use_color = false
TOML
  "${DETACH[@]}" "$ZEBRAD" -c "$C/zebrad.toml" start < /dev/null >> "$C/zebrad.log" 2>&1 &
  echo $! > "$C/zebrad.pid"
  until rpc getblockcount | grep -q result; do
    kill -0 "$(cat "$C/zebrad.pid")" 2>/dev/null || { echo "zebrad exited; see $C/zebrad.log"; exit 1; }
    sleep 1
  done
  "$0" lwd
  echo "height $(height)";;
lwd)
  for i in 0 1; do
    if [ ! -f "$C/lwd$i.pid" ] || ! kill -0 "$(cat "$C/lwd$i.pid")" 2>/dev/null; then
      mkdir -p "$C/lwd$i"
      "${DETACH[@]}" "$LWD" --no-tls-very-insecure --grpc-bind-addr "127.0.0.1:${LWD_PORTS[$i]}" \
        --http-bind-addr "127.0.0.1:$(( ${LWD_PORTS[$i]} + 10 ))" --rpchost 127.0.0.1 --rpcport "$RPC_PORT" \
        --rpcuser x --rpcpassword x --data-dir "$C/lwd$i" --log-file "$C/lwd$i.log" < /dev/null >> "$C/lwd$i.out" 2>&1 &
      echo $! > "$C/lwd$i.pid"
    fi
  done
  echo "lightwalletd: http://127.0.0.1:${LWD_PORTS[0]},http://127.0.0.1:${LWD_PORTS[1]}";;
prepare)
  # 20 blocks of Orchard coinbase (to migrate), transparent coinbase up to NU6.3 (to shield),
  # 20 blocks of Ironwood coinbase to the same UA, then transparent blocks until all of it has
  # matured. A shielded coinbase costs a proof per block, so only 40 blocks carry one.
  "$0" start "$2" > /dev/null; mine_to 20
  "$0" start "$3" > /dev/null; mine_to "$NU6_3"
  "$0" start "$2" > /dev/null; mine_to $(( NU6_3 + 20 ))
  "$0" start "$3" > /dev/null; mine_to $(( NU6_3 + 130 ))
  echo "height $(height)";;
mine) mine_to $(( $(height) + $2 )); echo "height $(height)";;
rpc) rpc "$2" "${3:-[]}";;
stop) stop_pid lwd0; stop_pid lwd1; stop_pid zebrad;;
*) sed -n '2,9p' "$0"; exit 2;;
esac
