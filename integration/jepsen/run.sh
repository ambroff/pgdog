#!/bin/bash
# Smoke test for the PgDog Jepsen suite: builds PgDog, makes sure the Incus
# nodes exist, then runs the append workload directly against Postgres
# (control) and through PgDog, with no faults. Extra args go to `lein run test`.
#
#   bash integration/jepsen/run.sh
#   bash integration/jepsen/run.sh --time-limit 300 --nemesis kill-pgdog,partition
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)

incus_cmd() {
    if incus info >/dev/null 2>&1; then "$@"; else sg incus-admin -c "$(printf '%q ' "$@")"; fi
}

(cd "$REPO_ROOT" && cargo build --release -p pgdog)
incus_cmd bash "$SCRIPT_DIR/nodes.sh" up

cd "$SCRIPT_DIR"
COMMON=(--nodes n1,n2,n3,n4,n5 --concurrency 10 --time-limit 60 --pgdog-bin "$REPO_ROOT/target/release/pgdog")

lein run test "${COMMON[@]}" --control "$@"
lein run test "${COMMON[@]}" "$@"
