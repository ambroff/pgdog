#!/bin/bash
# Deterministic reproducers for M3 (cross-shard 2PC), without Jepsen. Uses
# the Incus nodes: PgDog on n1, shards on n3 (0), n4 (1), n5 (2); psql
# sessions run on n5 and connect to PgDog on n1. Postgres must already be
# installed (any earlier Jepsen run does that).
#
#   bash repro/two_pc.sh setup                 schema on every shard (incl. the gate trigger)
#   bash repro/two_pc.sh <scenario> [options]  each scenario (re)starts PgDog itself
#   bash repro/two_pc.sh teardown
#
# Options (where a scenario restarts PgDog):
#   --no-node-id   restart PgDog without NODE_ID (H1)
#   --wipe         restart PgDog with an empty WAL dir (H2)
#   --2pc off      two_phase_commit = false (default on)
#   --auto on      two_phase_commit_auto = true
#
# How the phase windows are held open: every test txn inserts a row into
# jepsen_gate on the shard to stall. jepsen_gate has a DEFERRABLE INITIALLY
# DEFERRED constraint trigger, which Postgres runs inside PREPARE TRANSACTION
# (before the gid exists). With mode 'block' it waits for a shared advisory
# lock that this script holds exclusively ("closing the gate"); with mode
# 'fail' it raises. Phase 2 has no user code, so it is held open by an
# iptables rule on the shard that drops packets of PgDog's one connection
# (matched by its source port), after its PREPARE has gone through.
set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
SSH=(ssh -o BatchMode=yes -i "$HOME/.ssh/id_jepsen")
SHARDS=(n3 n4 n5)
PGDOG=n1
CLIENT=n5
DIR=/opt/pgdog-repro
GATE_LOCK=4242

NODE_ID=pgdog-1
WIPE=0
TWO_PC=true
AUTO=""

on() { local n=$1; shift; "${SSH[@]}" "root@$n" "$@"; }
ts() { date -u +%H:%M:%S.%3N; }
say() { echo "$(ts) $*"; }
# psql against PgDog (simple protocol), from the client node.
pg() { echo "env PGPASSWORD=jepsen PGAPPNAME=repro psql -X -h $PGDOG -p 6432 -U jepsen -d jepsen"; }
# SQL straight on a shard, as postgres. $1 = shard index.
sq() { on "${SHARDS[$1]}" "cd /tmp && sudo -u postgres psql -X -d jepsen -At -c \"$2\""; }
admin() { on $CLIENT "PGPASSWORD=jepsen psql -X -h $PGDOG -p 6432 -U admin -d admin -c '$1'"; }

# Shard s's key j (range mapping, 1e6 per shard).
key() { echo $(($1 * 1000000 + $2)); }

parse_opts() {
    while (($#)); do
        case "$1" in
            --no-node-id) NODE_ID="" ;;
            --wipe) WIPE=1 ;;
            --2pc) TWO_PC=$([[ $2 == on ]] && echo true || echo false); shift ;;
            --auto) AUTO=$([[ $2 == on ]] && echo true || echo false); shift ;;
            *) echo "unknown option $1" >&2; exit 2 ;;
        esac
        shift
    done
}

setup() {
    for i in "${!SHARDS[@]}"; do
        n=${SHARDS[$i]}
        # Orphaned prepared txns from earlier runs would block DROP DATABASE.
        on "$n" "cd /tmp && for g in \$(sudo -u postgres psql -At -c \"SELECT gid FROM pg_prepared_xacts\"); do \
                   sudo -u postgres psql -q -d jepsen -c \"ROLLBACK PREPARED '\$g'\"; done 2>/dev/null; \
                 sudo -u postgres psql -q -c \"DROP DATABASE IF EXISTS jepsen WITH (FORCE)\" \
            -c \"DROP ROLE IF EXISTS jepsen\" -c \"CREATE ROLE jepsen LOGIN PASSWORD 'jepsen'\" \
            -c \"CREATE DATABASE jepsen OWNER jepsen\"" || exit 1
        on "$n" "cd /tmp && sudo -u postgres psql -q -v ON_ERROR_STOP=1 -d jepsen" <<EOF || exit 1
SET ROLE jepsen;
CREATE TABLE append (k bigint PRIMARY KEY, sk bigint NOT NULL, v text NOT NULL);
CREATE TABLE acct (k bigint PRIMARY KEY, bal bigint NOT NULL CHECK (bal >= 0));
INSERT INTO acct VALUES ($(key $i 1), 5);
CREATE TABLE jepsen_gate (k bigint PRIMARY KEY, mode text NOT NULL);
CREATE FUNCTION jepsen_gate_fn() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN
  IF NEW.mode = 'block' THEN
    PERFORM pg_advisory_lock_shared($GATE_LOCK);
    PERFORM pg_advisory_unlock_shared($GATE_LOCK);
  ELSIF NEW.mode = 'fail' THEN
    RAISE EXCEPTION 'jepsen gate: deferred check failed on shard $i';
  END IF;
  RETURN NULL;
END \$\$;
-- Jepsen runs leave lock_timeout set server-wide; the gate must wait forever.
ALTER FUNCTION jepsen_gate_fn() SET lock_timeout = 0;
CREATE CONSTRAINT TRIGGER jepsen_gate_t AFTER INSERT ON jepsen_gate
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION jepsen_gate_fn();
EOF
    done
    on $PGDOG "pkill -9 -x pgdog; umount $DIR/wal 2>/dev/null; rm -rf $DIR; mkdir -p $DIR/wal"
    scp -q -o BatchMode=yes -i "$HOME/.ssh/id_jepsen" "$REPO_ROOT/target/release/pgdog" "root@$PGDOG:$DIR/pgdog"
    on $PGDOG "chmod +x $DIR/pgdog"
    printf '[[users]]\nname = "jepsen"\ndatabase = "jepsen"\npassword = "jepsen"\n' | on $PGDOG "cat > $DIR/users.toml"
    say "schema ready on ${SHARDS[*]}"
}

write_config() {
    {
        cat <<EOF
[general]
host = "0.0.0.0"
port = 6432
default_pool_size = 4
pooler_mode = "transaction"
two_phase_commit = $TWO_PC
two_phase_commit_wal_dir = "$DIR/wal"
EOF
        [[ -n $AUTO ]] && echo "two_phase_commit_auto = $AUTO"
        for i in 0 1 2; do
            printf '\n[[databases]]\nname = "jepsen"\nhost = "%s"\nport = 5432\ndatabase_name = "jepsen"\nshard = %d\n' "${SHARDS[$i]}" "$i"
        done
        for t in append acct jepsen_gate; do
            printf '\n[[sharded_tables]]\ndatabase = "jepsen"\nname = "%s"\ncolumn = "k"\ndata_type = "bigint"\n' "$t"
            for i in 0 1 2; do
                printf '[[sharded_tables.mapping]]\nshard = %d\nstart = %d\nend = %d\n' "$i" $((i * 1000000)) $(((i + 1) * 1000000))
            done
        done
        printf '\n[admin]\nname = "admin"\nuser = "admin"\npassword = "jepsen"\n'
    } | on $PGDOG "cat > $DIR/pgdog.toml"
}

# Starts PgDog with the current NODE_ID / WIPE settings. The log is appended
# to, with a marker, so a scenario's whole history stays in one file.
start_pgdog() {
    write_config
    local env="RUST_LOG=info"
    [[ -n $NODE_ID ]] && env="$env NODE_ID=$NODE_ID"
    on $PGDOG "pkill -9 -x pgdog; sleep 0.3"
    ((WIPE)) && on $PGDOG "rm -rf $DIR/wal/*"
    on $PGDOG "cd $DIR && echo '=== start NODE_ID=${NODE_ID:-unset} wipe=$WIPE 2pc=$TWO_PC auto=${AUTO:-default}' >> pgdog.log && \
               ($env nohup ./pgdog --config pgdog.toml --users users.toml >> pgdog.log 2>&1 &) && \
               for i in \$(seq 50); do ss -ltn | grep -q ':6432 ' && exit 0; sleep 0.2; done; exit 1" ||
        { say "PgDog did not start"; log_tail 20; exit 1; }
    say "PgDog up (NODE_ID=${NODE_ID:-unset}, wipe=$WIPE, two_phase_commit=$TWO_PC, auto=${AUTO:-default})"
}

kill_pgdog() { on $PGDOG "pkill -9 -x pgdog; sleep 0.5"; say "PgDog SIGKILLed"; }

log_tail() { on $PGDOG "grep -v 'connected to database\|new connection requested\|new server connection\|client disconnected' $DIR/pgdog.log | tail -${1:-15}"; }
log_2pc() { on $PGDOG "grep -E '2pc|=== start|ERROR|WARN' $DIR/pgdog.log | grep -v 'checkpointer started' | tail -${1:-20}"; }

teardown() {
    on $PGDOG "pkill -9 -x pgdog; umount $DIR/wal 2>/dev/null; rm -rf $DIR"
    for n in "${SHARDS[@]}"; do on "$n" "iptables -S INPUT | grep -q repro && iptables-save | grep -v repro | iptables-restore"; done
}

# --- gate --------------------------------------------------------------------

# Holds the gate lock on shard $1 in a background session (application_name gate).
gate_close() {
    on "${SHARDS[$1]}" "cd /tmp && (sudo -u postgres env PGAPPNAME=gate psql -X -d jepsen -c 'SELECT pg_advisory_lock($GATE_LOCK)' -c 'SELECT pg_sleep(3600)' >/dev/null 2>&1 &)"
    for _ in $(seq 50); do
        [[ $(sq "$1" "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = $GATE_LOCK AND granted") == 1 ]] && { say "gate closed on shard $1"; return; }
        sleep 0.1
    done
    say "gate on shard $1 did not close"; exit 1
}
gate_open() {
    # The PREPARE we stalled must still be there (not timed out or killed).
    [[ ${2:-} == check ]] && { await_gated "$1" >/dev/null || { say "gated PREPARE on shard $1 is gone before the gate opened"; exit 1; }; }
    sq "$1" "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'gate'" >/dev/null
    say "gate opened on shard $1"
}

# Waits until shard $1 has a PREPARE TRANSACTION stuck on the gate; prints its pid and client port.
await_gated() {
    for _ in $(seq 100); do
        local r
        r=$(sq "$1" "SELECT pid, client_port, query FROM pg_stat_activity WHERE query LIKE 'PREPARE TRANSACTION%' AND wait_event_type = 'Lock' LIMIT 1")
        [[ -n $r ]] && { echo "$r"; return; }
        sleep 0.1
    done
    say "no gated PREPARE on shard $1" >&2; return 1
}

prepared() { sq "$1" "SELECT gid FROM pg_prepared_xacts WHERE left(gid, 12) = '__pgdog_2pc_'" | paste -sd' '; }

await_prepared() {
    for _ in $(seq 100); do
        [[ -n $(prepared "$1") ]] && return
        sleep 0.1
    done
    say "nothing prepared on shard $1"; return 1
}
await_not_prepared() {
    for _ in $(seq 100); do
        [[ -z $(prepared "$1") ]] && return
        sleep 0.1
    done
    say "shard $1 still has prepared txns"; return 1
}

# iptables on shard $1: drop (or reset, with $3 = reset) packets from PgDog's connection with source port $2.
cut_conn() {
    local n=${SHARDS[$1]} ip
    ip=$(on "$n" "getent ahostsv4 $PGDOG | awk 'NR==1{print \$1}'")
    if [[ ${3:-drop} == reset ]]; then
        on "$n" "iptables -I INPUT -s $ip -p tcp --sport $2 -m comment --comment repro -j REJECT --reject-with tcp-reset; \
                 iptables -D INPUT -s $ip -p tcp --sport $2 -m comment --comment repro -j DROP 2>/dev/null"
    else
        on "$n" "iptables -I INPUT -s $ip -p tcp --sport $2 -m comment --comment repro -j DROP"
    fi
    say "shard $1: ${3:-drop} packets from $PGDOG:$2"
}
# After PgDog is killed, its kernel keeps retransmitting the COMMIT PREPARED
# that the rule dropped; once the rule goes, the shard's old backend would
# run it, which a real crash (or a real network loss) would not do. So end
# that backend (the one serving PgDog's connection from port $2) first.
drop_backend() {
    sq "$1" "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE client_port = $2" >/dev/null
    sleep 0.5
    say "shard $1: terminated the backend of PgDog's connection from port $2"
}
heal_conns() {
    for n in "${SHARDS[@]}"; do on "$n" "iptables-save | grep -q repro && iptables-save | grep -v repro | iptables-restore"; done
    say "iptables rules removed"
}

# --- the test txn ------------------------------------------------------------

TOKEN=0
# Runs, in the background on the client node, a cross-shard txn that appends
# token $TOKEN to one key on shard 0 and one on shard 1, plus a gate row with
# mode $1 on shard 1. Output lines are prefixed with T|.
txn_bg() {
    TOKEN=$(($(date +%s%N) / 1000000 % 1000000))
    local k0 k1 g
    k0=$(key 0 $TOKEN); k1=$(key 1 $TOKEN); g=$(key 1 $TOKEN)
    on $CLIENT "( echo 'BEGIN;'; \
                  echo \"INSERT INTO append VALUES ($k0, $k0, '$TOKEN');\"; \
                  echo \"INSERT INTO append VALUES ($k1, $k1, '$TOKEN');\"; \
                  echo \"INSERT INTO jepsen_gate VALUES ($g, '$1');\"; \
                  echo '\\echo T COMMIT sent'; echo 'COMMIT;' ) | timeout ${2:-120} $(pg) 2>&1 | sed 's/^/T| /'; \
                echo \"T| psql exit=\${PIPESTATUS[0]}\"" &
    TXN_PID=$!
    say "txn $TOKEN started: keys $k0 (shard 0), $k1 (shard 1), gate mode $1 on shard 1"
}

# Final state of the test txn: is its token on shard 0 / shard 1, and what is still prepared.
report() {
    local k0 k1
    k0=$(key 0 $TOKEN); k1=$(key 1 $TOKEN)
    local r0 r1
    r0=$(sq 0 "SELECT count(*) FROM append WHERE k = $k0")
    r1=$(sq 1 "SELECT count(*) FROM append WHERE k = $k1")
    say "RESULT txn $TOKEN: shard 0 row=$r0, shard 1 row=$r1 => $( ((r0 == r1)) && { ((r0)) && echo committed || echo absent; } || echo PARTIAL)"
    for s in 0 1 2; do say "RESULT shard $s prepared: [$(prepared $s)]"; done
    say "PgDog SHOW TRANSACTIONS:"; admin 'SHOW TRANSACTIONS' 2>&1 | sed 's/^/    /'
}

# --- scenarios ---------------------------------------------------------------

# Every scenario starts clean: no PgDog, no gate sessions, no iptables
# rules, and no prepared txns left over from an earlier scenario.
fresh() {
    on $PGDOG "pkill -9 -x pgdog" || true
    heal_conns >/dev/null
    for s in 0 1 2; do
        sq $s "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'gate'" >/dev/null
        for g in $(prepared $s); do sq $s "ROLLBACK PREPARED '$g'" >/dev/null; done
    done
}

# W2: PREPARE landed on shard 0 but not shard 1 (stalled by the gate), then
# PgDog is killed. The stalled PREPARE is then aborted (its backend
# terminated) and PgDog restarts with the given options.
#   expect, NODE_ID kept + WAL kept: shard 0 rolled back, no orphans, absent
#   expect (H1), --no-node-id:       shard 0's prepared txn orphaned forever
#   expect (H2), --wipe:             same
phase1() {
    fresh
    start_pgdog
    gate_close 1
    txn_bg block
    local g; g=$(await_gated 1) || exit 1
    say "shard 1 PREPARE stalled: $g"
    await_prepared 0 && say "shard 0 prepared: $(prepared 0)"
    kill_pgdog
    sq 1 "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE 'PREPARE TRANSACTION%'" >/dev/null
    gate_open 1
    wait $TXN_PID
    parse_opts "$@"
    start_pgdog
    sleep 5
    log_2pc 12
    report
}

# Late PREPARE (R8): as phase1, but the stalled PREPARE on shard 1 is left
# running. PgDog restarts with NODE_ID and WAL intact and rolls the txn back
# (shard 1's ROLLBACK PREPARED finds no gid yet: 42704, ignored). Then the
# gate opens and shard 1's PREPARE completes, with nobody left to resolve it.
#   expect: orphan on shard 1, holding its row locks
late_prepare() {
    fresh
    start_pgdog
    gate_close 1
    txn_bg block
    local g; g=$(await_gated 1) || exit 1
    say "shard 1 PREPARE stalled: $g"
    await_prepared 0 && say "shard 0 prepared: $(prepared 0)"
    kill_pgdog
    parse_opts "$@"
    start_pgdog
    for _ in $(seq 50); do
        [[ -z $(prepared 0) ]] && break
        sleep 0.2
    done
    say "recovery done with shard 0 (prepared there: [$(prepared 0)])"
    gate_open 1 check
    sleep 2
    wait $TXN_PID
    sleep 3
    log_2pc 12
    report
    say "a writer on the orphan's row (lock_timeout 2 s):"
    on $CLIENT "$(pg) -c \"SET lock_timeout = '2s'\" -c \"INSERT INTO append VALUES ($(key 1 $TOKEN), 0, 'x')\"" 2>&1 | sed 's/^/    /'
}

# W3: the phase-2 record is durable, COMMIT PREPARED reached shard 0 but not
# shard 1 (its connection's packets are dropped), then PgDog is killed and
# restarts with the given options.
#   expect, NODE_ID kept + WAL kept: recovery commits shard 1 -> committed
#   expect (H1/H2):                  shard 0 committed, shard 1 orphaned -> PARTIAL
phase2() {
    fresh
    start_pgdog
    gate_close 1
    txn_bg block
    local g port; g=$(await_gated 1) || exit 1
    port=$(echo "$g" | cut -d'|' -f2)
    say "shard 1 PREPARE stalled: $g"
    cut_conn 1 "$port"
    gate_open 1 check
    await_prepared 1 && say "shard 1 prepared: $(prepared 1)"
    await_not_prepared 0 && say "shard 0 committed (no longer prepared); shard 1 still: $(prepared 1)"
    sleep 1
    kill_pgdog
    drop_backend 1 "$port"
    heal_conns
    wait $TXN_PID
    parse_opts "$@"
    start_pgdog
    sleep 5
    log_2pc 12
    report
}

# H4: as phase2, but instead of killing PgDog, shard 1 resets the stalled
# connection, so COMMIT PREPARED fails there after shard 0 committed.
#   expect: the client gets an error on COMMIT (FATAL 58000), yet the txn
#   ends up committed on both shards (the 2PC monitor finishes it).
h4() {
    fresh
    parse_opts "$@"
    start_pgdog
    gate_close 1
    txn_bg block
    local g port; g=$(await_gated 1) || exit 1
    port=$(echo "$g" | cut -d'|' -f2)
    cut_conn 1 "$port"
    gate_open 1 check
    await_prepared 1
    await_not_prepared 0 && say "shard 0 committed; shard 1 still prepared: $(prepared 1)"
    cut_conn 1 "$port" reset
    wait $TXN_PID
    heal_conns
    sleep 3
    log_2pc 10
    report
}

# W3 with the WAL on a full filesystem (WAL write errors are swallowed):
# NODE_ID kept, WAL "kept", but nothing reached it.
#   expect: client acked normally while the disk was full; after the
#   phase-2 kill, recovery knows nothing -> PARTIAL + orphan on shard 1
enospc() {
    fresh
    on $PGDOG "pkill -9 -x pgdog; umount $DIR/wal 2>/dev/null; rm -rf $DIR/wal; mkdir -p $DIR/wal && mount -t tmpfs -o size=512k tmpfs $DIR/wal"
    start_pgdog
    on $PGDOG "dd if=/dev/zero of=$DIR/wal/ballast bs=4k 2>/dev/null; df -h $DIR/wal | tail -1"
    say "WAL filesystem full"
    # The segment's first page is already allocated, so the first ~60 txns
    # still fit; run enough to need a new page.
    say "150 cross-shard txns while the WAL disk is full:"
    local base=$(( ($(date +%s) % 1000) * 1000 ))
    on $CLIENT "for i in \$(seq 150); do echo 'BEGIN;'; echo \"INSERT INTO append VALUES (\$(($base + i)), 0, 'enospc$base');\"; \
                echo \"INSERT INTO append VALUES (\$((1000000 + $base + i)), 0, 'enospc$base');\"; echo 'COMMIT;'; done | $(pg) -q 2>&1 | sort | uniq -c" | sed 's/^/    /'
    say "committed: shard 0 $(sq 0 "SELECT count(*) FROM append WHERE v = 'enospc$base'"), shard 1 $(sq 1 "SELECT count(*) FROM append WHERE v = 'enospc$base'") (of 150)"
    on $PGDOG "ls -la $DIR/wal; grep -c 'wal writer error' $DIR/pgdog.log; grep -m2 'wal writer error' $DIR/pgdog.log" | sed 's/^/    /'
    gate_close 1
    txn_bg block
    local g port; g=$(await_gated 1) || exit 1
    port=$(echo "$g" | cut -d'|' -f2)
    cut_conn 1 "$port"
    gate_open 1 check
    await_prepared 1
    await_not_prepared 0 && say "shard 0 committed; shard 1 still prepared: $(prepared 1)"
    sleep 1
    kill_pgdog
    drop_backend 1 "$port"
    heal_conns
    wait $TXN_PID
    on $PGDOG "rm -f $DIR/wal/ballast; ls -la $DIR/wal"
    parse_opts "$@"
    start_pgdog
    sleep 5
    log_2pc 15
    report
    on $PGDOG "pkill -9 -x pgdog; umount $DIR/wal"
}

# Phase-1 failure (deferred check raises on shard 1 at PREPARE / COMMIT).
#   expect, 2PC on:  client gets the error, both shards roll back
#   expect, --2pc off: shard 1's COMMIT fails, shard 0 commits -> PARTIAL
deferred_fail() {
    fresh
    parse_opts "$@"
    start_pgdog
    txn_bg fail
    wait $TXN_PID
    sleep 1
    report
}

# C12 with an error PgDog generates itself (a syntax error), then COMMIT.
#   Postgres: the txn is aborted; COMMIT answers ROLLBACK; nothing commits.
c12_syntax() {
    fresh
    parse_opts "$@"
    start_pgdog
    TOKEN=$(($(date +%s%N) / 1000000 % 1000000))
    local k0 k1; k0=$(key 0 $TOKEN); k1=$(key 1 $TOKEN)
    say "through PgDog: BEGIN; insert shard 0; insert shard 1; SELEC 1; update shard 0; COMMIT"
    on $CLIENT "$(pg) -c 'BEGIN' -c \"INSERT INTO append VALUES ($k0, 0, 'a')\" -c \"INSERT INTO append VALUES ($k1, 0, 'a')\" \
                -c 'SELEC 1' -c \"UPDATE append SET v = 'after-error' WHERE k = $k0\" -c 'COMMIT'" 2>&1 | sed 's/^/    /'
    report
    say "same statements straight to Postgres (shard 0 only):"
    on $CLIENT "env PGPASSWORD=jepsen psql -X -h ${SHARDS[0]} -U jepsen -d jepsen -c 'BEGIN' -c \"INSERT INTO append VALUES ($((k0 + 1)), 0, 'a')\" -c 'SELEC 1' \
                -c \"UPDATE append SET v = 'after-error' WHERE k = $((k0 + 1))\" -c 'COMMIT'" 2>&1 | sed 's/^/    /'
    say "row on shard 0 after direct run: $(sq 0 "SELECT count(*) FROM append WHERE k = $((k0 + 1))")"
}

# COMMIT AND CHAIN: the chained txn must stay one atomic txn.
#   Postgres: the second pair of writes is rolled back by the final ROLLBACK.
chain() {
    fresh
    parse_opts "$@"
    start_pgdog
    TOKEN=$(($(date +%s%N) / 1000000 % 1000000))
    local k0 k1; k0=$(key 0 $TOKEN); k1=$(key 1 $TOKEN)
    on $CLIENT "$(pg) -c 'BEGIN' -c \"INSERT INTO append VALUES ($k0, 0, 'first')\" -c \"INSERT INTO append VALUES ($k1, 0, 'first')\" \
                -c 'COMMIT AND CHAIN' -c \"INSERT INTO append VALUES ($((k0 + 1)), 0, 'chained')\" -c \"INSERT INTO append VALUES ($((k1 + 1)), 0, 'chained')\" \
                -c 'ROLLBACK'" 2>&1 | sed 's/^/    /'
    say "chained rows (should be 0 and 0): shard 0=$(sq 0 "SELECT count(*) FROM append WHERE k = $((k0 + 1))"), shard 1=$(sq 1 "SELECT count(*) FROM append WHERE k = $((k1 + 1))")"
}

# Autocommit cross-shard UPDATE where one shard's part violates a CHECK
# (shard 0's account has 5, the debit is 10).
#   expect, auto off: shard 1's credit commits, shard 0's debit fails -> money created
#   expect, --auto on: both roll back
autocommit() {
    fresh
    parse_opts "$@"
    start_pgdog
    local a b; a=$(key 0 1); b=$(key 1 1)
    say "before: $(sq 0 "SELECT bal FROM acct WHERE k = $a") + $(sq 1 "SELECT bal FROM acct WHERE k = $b")"
    on $CLIENT "$(pg) -c \"UPDATE acct SET bal = bal + CASE WHEN k = $a THEN -10 ELSE 10 END WHERE k IN ($a, $b)\"" 2>&1 | sed 's/^/    /'
    say "after:  $(sq 0 "SELECT bal FROM acct WHERE k = $a") + $(sq 1 "SELECT bal FROM acct WHERE k = $b")"
    sq 0 "UPDATE acct SET bal = 5 WHERE k = $a" >/dev/null; sq 1 "UPDATE acct SET bal = 5 WHERE k = $b" >/dev/null
}

# A read-only (by content) cross-shard BEGIN block: does it go through 2PC?
readonly_2pc() {
    fresh
    parse_opts "$@"
    start_pgdog
    for s in 0 1; do sq $s "ALTER SYSTEM SET log_statement = 'all'" >/dev/null; sq $s "SELECT pg_reload_conf()" >/dev/null; done
    on $CLIENT "$(pg) -c 'BEGIN' -c \"SELECT v FROM append WHERE k = $(key 0 1)\" -c \"SELECT v FROM append WHERE k = $(key 1 1)\" -c 'COMMIT'" >/dev/null 2>&1
    sleep 0.5
    for s in 0 1; do
        say "shard $s, last statements:"
        on "${SHARDS[$s]}" "grep 'statement:' /var/log/postgresql/postgresql-17-main.log | tail -4" | sed 's/^/    /'
        sq $s "ALTER SYSTEM RESET log_statement" >/dev/null; sq $s "SELECT pg_reload_conf()" >/dev/null
    done
}

# JDBC executeBatch, autocommit on, inserts on shard 0 and shard 1, the last
# one a duplicate key on shard 1. Postgres runs a batch (one Sync) as one
# implicit txn: nothing commits.
#   expect (F16): PgDog commits shard 0's insert (shard 1's first insert rolls back with the duplicate in its implicit txn), even
#   with two_phase_commit and two_phase_commit_auto on
batch() {
    fresh
    parse_opts "$@"
    start_pgdog
    TOKEN=$(($(date +%s%N) / 1000000 % 1000000))
    local k0 k1; k0=$(key 0 $TOKEN); k1=$(key 1 $TOKEN)
    sq 1 "INSERT INTO append VALUES ($k1, 0, 'existing')" >/dev/null
    (cd "$SCRIPT_DIR/.." && lein run -m clojure.main repro/batch.clj "$k0" "$k1" 2>&1 | grep -v WARN)
    say "RESULT batch rows: shard 0 k=$k0: $(sq 0 "SELECT count(*) FROM append WHERE k = $k0"), shard 1 k=$((k1 + 1)): $(sq 1 "SELECT count(*) FROM append WHERE k = $((k1 + 1))") (Postgres: 0 and 0)"
    say "same batch straight to shard 0 (both keys local there):"
    sq 0 "INSERT INTO append VALUES ($((k0 + 10)), 0, 'existing')" >/dev/null
    (cd "$SCRIPT_DIR/.." && sed 's/:host "n1" :port 6432/:host "n3" :port 5432/' repro/batch.clj > /tmp/batch-direct.clj && \
        lein run -m clojure.main /tmp/batch-direct.clj "$((k0 + 5))" "$((k0 + 10))" 2>&1 | grep -v WARN)
    say "RESULT direct rows: k=$((k0 + 5)): $(sq 0 "SELECT count(*) FROM append WHERE k = $((k0 + 5))"), k=$((k0 + 11)): $(sq 0 "SELECT count(*) FROM append WHERE k = $((k0 + 11))")"
}

# Recovery needs every shard: an in-doubt txn on shards 0 and 1 stays
# prepared (holding locks) while shard 2, which it never touched, is down.
stuck_shard() {
    fresh
    start_pgdog
    gate_close 1
    txn_bg block
    await_gated 1 >/dev/null || exit 1
    await_prepared 0 && say "shard 0 prepared: $(prepared 0)"
    kill_pgdog
    sq 1 "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE 'PREPARE TRANSACTION%'" >/dev/null
    gate_open 1
    wait $TXN_PID
    on "${SHARDS[2]}" "systemctl kill -s STOP postgresql@17-main"; say "shard 2 (not in the txn) paused"
    start_pgdog
    sleep 15
    say "after 15 s: shard 0 prepared: [$(prepared 0)]"
    on "${SHARDS[2]}" "systemctl kill -s CONT postgresql@17-main"; say "shard 2 resumed"
    sleep 10
    say "after resume + 10 s: shard 0 prepared: [$(prepared 0)]"
    log_2pc 8
}

# Cross-shard deadlock: A locks shard 0's row then wants shard 1's; B the
# other way round. Each Postgres sees only one half of the cycle, so its
# deadlock detector never fires; with lock_timeout = 0 (the Postgres
# default) both wait forever. Not specific to 2PC, but 2PC (and
# two_phase_commit_auto) holds every shard's locks until all have prepared.
#   expect: both sessions still waiting after 30 s
xdeadlock() {
    fresh
    parse_opts "$@"
    start_pgdog
    local a b; a=$(key 0 1); b=$(key 1 1)
    local s1="SET lock_timeout = 0" one="UPDATE acct SET bal = bal + 1 WHERE k ="
    on $CLIENT "( timeout 45 $(pg) -c \"$s1\" -c 'BEGIN' -c \"$one $a\" -c 'SELECT pg_sleep(1)' -c \"$one $b\" -c 'COMMIT' 2>&1 | sed 's/^/A| /' ) & \
                ( timeout 45 $(pg) -c \"$s1\" -c 'BEGIN' -c \"$one $b\" -c 'SELECT pg_sleep(1)' -c \"$one $a\" -c 'COMMIT' 2>&1 | sed 's/^/B| /' ) & \
                sleep 30; echo \"after 30 s, psql sessions still running: \$(pgrep -fc 'psql -X -h $PGDOG')\"" | sed 's/^/    /' &
    local w=$!
    sleep 25
    for s in 0 1; do say "shard $s waiting on a lock: $(sq $s "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'"), granted row locks held by: $(sq $s "SELECT count(DISTINCT pid) FROM pg_locks WHERE locktype = 'transactionid' AND granted AND mode = 'ExclusiveLock'")"; done
    wait $w
    say "ending both sessions"
    on $CLIENT "pkill -f 'psql -X -h $PGDOG'" || true
    sleep 1
    sq 0 "UPDATE acct SET bal = 5 WHERE k = $a" >/dev/null; sq 1 "UPDATE acct SET bal = 5 WHERE k = $b" >/dev/null
}

cmd=${1:-}; shift || true
case "$cmd" in
    setup) setup ;;
    teardown) teardown ;;
    phase1) phase1 "$@" ;;
    late-prepare) late_prepare "$@" ;;
    phase2) phase2 "$@" ;;
    h4) h4 "$@" ;;
    enospc) enospc "$@" ;;
    deferred-fail) deferred_fail "$@" ;;
    c12-syntax) c12_syntax "$@" ;;
    chain) chain "$@" ;;
    autocommit) autocommit "$@" ;;
    readonly-2pc) readonly_2pc "$@" ;;
    batch) batch "$@" ;;
    stuck-shard) stuck_shard "$@" ;;
    xdeadlock) xdeadlock "$@" ;;
    *) echo "usage: $0 setup|teardown|phase1|late-prepare|phase2|h4|enospc|deferred-fail|c12-syntax|chain|autocommit|readonly-2pc|batch|stuck-shard|xdeadlock [options]" >&2; exit 2 ;;
esac
