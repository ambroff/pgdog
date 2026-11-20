#!/bin/bash
# Deterministic reproducers for M2 findings, without Jepsen. Uses the Incus
# nodes: PgDog on n1, shards on n3/n4/n5 (Postgres already installed by an
# earlier Jepsen run). psql sessions run on n3 and connect to PgDog on n1.
#
#   bash repro/sharded.sh setup             # schema on every shard, start PgDog (pool size 1)
#   bash repro/sharded.sh reload-wedge      # F6: RELOAD while a txn holds a shard's only connection
#   bash repro/sharded.sh checkout-deadlock # L1: two txns, different home shards, then a broadcast read
#   bash repro/sharded.sh teardown
#
# Run setup again between scenarios to get a fresh PgDog.
set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
SSH=(ssh -o BatchMode=yes -i "$HOME/.ssh/id_jepsen")
SHARDS=(n3 n4 n5)
PGDOG=n1
CLIENT=n3   # has psql

on() { local n=$1; shift; "${SSH[@]}" "root@$n" "$@"; }
# psql against PgDog (simple protocol), from the client node.
pg() { echo "env PGPASSWORD=jepsen psql -X -v ON_ERROR_STOP=1 -h $PGDOG -p 6432 -U jepsen -d jepsen"; }
admin() { on $CLIENT "PGPASSWORD=jepsen psql -X -h $PGDOG -p 6432 -U admin -d admin -c '$1'"; }
ts() { date -u +%H:%M:%S.%3N; }

setup() {
    for i in "${!SHARDS[@]}"; do
        n=${SHARDS[$i]}
        on "$n" "cd /tmp && sudo -u postgres psql -q -c \"DROP DATABASE IF EXISTS jepsen WITH (FORCE)\" \
            -c \"DROP ROLE IF EXISTS jepsen\" -c \"CREATE ROLE jepsen LOGIN PASSWORD 'jepsen'\" \
            -c \"CREATE DATABASE jepsen OWNER jepsen\" && \
            sudo -u postgres psql -q -d jepsen -c \"SET ROLE jepsen; CREATE TABLE append (k bigint PRIMARY KEY, sk bigint NOT NULL, v text NOT NULL); \
            INSERT INTO append VALUES ($((i * 1000000 + 1)), $((i * 1000000 + 1)), 'x')\""
    done
    on $PGDOG "pkill -x pgdog; mkdir -p /opt/pgdog-repro"
    scp -q -o BatchMode=yes -i "$HOME/.ssh/id_jepsen" "$REPO_ROOT/target/release/pgdog" "root@$PGDOG:/opt/pgdog-repro/pgdog"
    {
        cat <<'EOF'
[general]
host = "0.0.0.0"
port = 6432
default_pool_size = 1
min_pool_size = 0
pooler_mode = "transaction"
EOF
        for i in 0 1 2; do
            printf '\n[[databases]]\nname = "jepsen"\nhost = "%s"\nport = 5432\ndatabase_name = "jepsen"\nshard = %d\n' "${SHARDS[$i]}" "$i"
        done
        printf '\n[[sharded_tables]]\ndatabase = "jepsen"\nname = "append"\ncolumn = "k"\ndata_type = "bigint"\n'
        for i in 0 1 2; do
            printf '[[sharded_tables.mapping]]\nshard = %d\nstart = %d\nend = %d\n' "$i" $((i * 1000000)) $(((i + 1) * 1000000))
        done
        printf '\n[admin]\nname = "admin"\nuser = "admin"\npassword = "jepsen"\n'
    } | on $PGDOG "cat > /opt/pgdog-repro/pgdog.toml"
    printf '[[users]]\nname = "jepsen"\ndatabase = "jepsen"\npassword = "jepsen"\n' |
        on $PGDOG "cat > /opt/pgdog-repro/users.toml"
    on $PGDOG "cd /opt/pgdog-repro && chmod +x pgdog && (RUST_LOG=info nohup ./pgdog --config pgdog.toml --users users.toml > pgdog.log 2>&1 &) && sleep 2 && ss -ltn | grep -q 6432"
    echo "$(ts) PgDog up on $PGDOG (pool size 1 per shard)"
}

teardown() {
    on $PGDOG "pkill -x pgdog; rm -rf /opt/pgdog-repro"
}

log_tail() { on $PGDOG "grep -v 'connected to database\|new connection requested\|new server connection' /opt/pgdog-repro/pgdog.log | tail -${1:-15}"; }

# F6. Pool size 1. The deadlock needs a client that is already in a txn and
# makes its first checkout after the RELOAD: PgDog then moves it to the new
# cluster mid-txn (get_conn: Offline -> safe_reload), it gets a connection
# from the new pool, and its next statement waits for the new cluster to be
# ready (no timeout) while holding that connection. Readiness needs the shard
# 2 schema, whose loader needs that same connection.
#   T-5  A: BEGIN; read shard 2           A holds the only shard-2 connection
#   T-4  B: BEGIN                          buffered, no connection
#   T 0  RELOAD                            loader L queues for shard 2
#   T+2  B: read shard 2                   B moves to the new cluster, queues behind L
#   T+5  L's checkout times out; L re-queues at the tail (behind B)
#   T+7  A: COMMIT                         A's connection goes to B; B's read runs
#   T+8  B: read shard 2 again             hangs: waits for readiness, holding the connection
# Expect: B's last read never returns (40 s timeout here); the log repeats
# "error loading schema for shard 2: all replicas down"; a new client C's
# BEGIN on shard 0 also hangs; RECONNECT releases everything.
reload_wedge() {
    on $CLIENT "$(pg) -c 'SELECT v FROM append WHERE k = 2000001' >/dev/null"
    echo "$(ts) T-5 A: BEGIN; read shard 2 (holds the only shard-2 connection until T+7)"
    on $CLIENT "( echo 'BEGIN;'; echo 'SELECT v FROM append WHERE k = 2000001;'; sleep 12; \
                  echo '\\echo A COMMIT'; echo 'COMMIT;' ) | timeout 60 $(pg) 2>&1 | sed 's/^/A| /'" &
    a=$!
    sleep 1
    echo "$(ts) T-4 B: BEGIN; (read shard 2 at T+2 and again at T+8)"
    on $CLIENT "( echo 'BEGIN;'; sleep 6; echo '\\echo B first read'; echo 'SELECT v FROM append WHERE k = 2000001;'; \
                  sleep 6; echo '\\echo B second read'; echo 'SELECT v FROM append WHERE k = 2000001;'; echo 'COMMIT;' ) | \
                (time timeout 40 $(pg)) 2>&1 | sed 's/^/B| /'; echo \"B| exit=\${PIPESTATUS[0]}\"" &
    b=$!
    sleep 4
    echo "$(ts) T 0 admin: RELOAD"; admin RELOAD
    sleep 12
    echo "$(ts) T+12 C: BEGIN; read shard 0 (unrelated shard), 15 s timeout"
    on $CLIENT "( echo 'BEGIN;'; echo 'SELECT v FROM append WHERE k = 1;'; echo 'COMMIT;' ) | \
                (time timeout 15 $(pg)) 2>&1 | sed 's/^/C| /'"
    echo "$(ts) SHOW POOLS:"; admin 'SHOW POOLS' | cut -c1-200
    wait $a $b
    echo "$(ts) PgDog log:"; log_tail 12
}

# L1. Pool size 1 per shard. A holds shard 0, B holds shard 1; each then
# sends a read by sk (broadcast), which needs every shard. Expect: both wait
# for checkout_timeout (5 s), then FATAL checkout timeout.
checkout_deadlock() {
    for who in "A 1" "B 1000001"; do
        set -- $who
        on $CLIENT "( echo 'BEGIN;'; echo 'SELECT v FROM append WHERE k = $2;'; sleep 1; \
                      echo '\\echo $1 broadcast read sent'; echo 'SELECT v FROM append WHERE sk = $2;'; echo 'COMMIT;' ) | \
                    (time timeout 30 $(pg)) 2>&1; echo \"$1 exit=\$?\"" &
    done
    wait
    echo "$(ts) PgDog log:"; log_tail 8
}

case "${1:-}" in
    setup) setup ;;
    teardown) teardown ;;
    reload-wedge) reload_wedge ;;
    checkout-deadlock) checkout_deadlock ;;
    *) echo "usage: $0 setup|reload-wedge|checkout-deadlock|teardown" >&2; exit 2 ;;
esac
