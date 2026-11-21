#!/bin/bash
# Runs a matrix of Jepsen tests and prints one summary line per run.
#
#   bash integration/jepsen/matrix.sh m1.matrix [extra lein args...]
#
# Each non-blank, non-# line of the matrix file is a set of `lein run test`
# arguments. The common arguments (nodes, binary) and any extra arguments given
# here are added to every line. Every run happens even if earlier ones fail.
# Summaries go to stdout and to store/matrix-<timestamp>.tsv.
set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)

matrix=${1:?usage: matrix.sh MATRIX_FILE [extra lein args...]}
matrix=$(realpath "$matrix")
shift

cd "$SCRIPT_DIR"
mkdir -p store
summary="store/matrix-$(date +%Y%m%dT%H%M%S).tsv"
printf 'exit\tvalid\tok\tfail\tinfo\tanomalies\twrong\tfirst-wrong-s\tbackends\tpools\tplacement\txacts/txn\tatomic\tpartial\tfail-vis\tok-lost\torphans\tbank\tstore\targs\n' | tee "$summary"

# Pulls a count out of results.edn. The first match is in the top-level
# :stats map, which comes before any workload checker output.
field() { grep -m1 -oP ":$1 \K\d+" "$2"; }
# Any token after a key (nil, true, 1.23, ...), first match or "-".
token() { grep -m1 -oP ":$1 \K[^,}\s]+" "$2" || echo -; }
# Elle's anomaly types, e.g. G2-item+G-single-item, or "-". Sharded runs
# have one Elle result per shard; this is the union.
anomalies() { grep -oP ':anomaly-types [(\[]\K[^)\]]*' "$1" | tr ' ' '\n' | sed 's/://g' | grep . | sort -u | paste -sd+ || true; }
# :valid? of a named sub-checker, e.g. :backends {... :valid? true}.
subvalid() { grep -A40 -m1 -P "^\s*:$1 \{" "$2" | grep -m1 -oP ':valid\? \K[^,}\s]+' || echo -; }

while IFS= read -r line; do
    [[ -z "${line// }" || "$line" == \#* ]] && continue
    read -ra args <<<"$line"
    before=$(readlink -f store/latest 2>/dev/null)
    lein run test --nodes n1,n2,n3,n4,n5 --pgdog-bin "$REPO_ROOT/target/release/pgdog" \
        "${args[@]}" "$@" </dev/null >/dev/null 2>&1
    status=$?
    run=$(readlink -f store/latest)
    results="$run/results.edn"
    if [[ "$run" != "$before" && -f "$results" ]]; then
        valid=$(tail -1 "$results" | grep -oP ':valid\? \K\S+(?=\})')
        ok=$(field ok-count "$results")
        fail=$(field fail-count "$results")
        info=$(field info-count "$results")
        anom=$(anomalies "$results"); anom=${anom:--}
        wrong=$(token wrong-count "$results")
        first=$(token first-wrong-s "$results")
        backends=$(subvalid backends "$results")
        pools=$(subvalid pools "$results")
        placement=$(subvalid placement "$results")
        xpt=$(token xacts-per-txn "$results")
        atomic=$(subvalid atomicity "$results")
        partial=$(token partial-count "$results")
        failvis=$(token fail-visible-count "$results")
        oklost=$(token ok-lost-count "$results")
        orphans=$(token orphan-count "$results")
        bank=$(subvalid bank "$results")
    else
        valid=missing ok=- fail=- info=- anom=- wrong=- first=- backends=- pools=- placement=- xpt=-
        atomic=- partial=- failvis=- oklost=- orphans=- bank=-
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$status" "$valid" "$ok" "$fail" "$info" \
        "$anom" "$wrong" "$first" "$backends" "$pools" "$placement" "$xpt" "$atomic" "$partial" "$failvis" "$oklost" "$orphans" "$bank" \
        "${run#"$SCRIPT_DIR"/}" "$line" | tee -a "$summary"
done <"$matrix"
