#!/bin/bash
# Lifecycle for the Jepsen node VMs (Incus).
#
#   nodes.sh up [--containers]   create n1..nN, wait for cloud-init, snapshot "clean"
#   nodes.sh reset               restore every node to the "clean" snapshot
#   nodes.sh down                delete every node
#   nodes.sh status              list nodes
#
# Environment:
#   NODES       number of nodes (default 5)
#   SUBNET      first three octets of the incusbr0 subnet (default 10.77.0);
#               node i gets $SUBNET.$((10 + i)). The host's /etc/hosts must map
#               these to n1..nN (see PLAN.org).
#   SSH_KEY     private key Jepsen uses (default ~/.ssh/id_jepsen)
#   IMAGE       default images:debian/13/cloud
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
NODES=${NODES:-5}
SUBNET=${SUBNET:-10.77.0}
SSH_KEY=${SSH_KEY:-$HOME/.ssh/id_jepsen}
IMAGE=${IMAGE:-images:debian/13/cloud}
SNAPSHOT=clean

nodes() { for i in $(seq 1 "$NODES"); do echo "n$i"; done; }
ip_of() { echo "$SUBNET.$((10 + ${1#n}))"; }

preflight() {
    incus info >/dev/null 2>&1 || {
        echo "incus is not reachable; are you in the incus-admin group? (try: sg incus-admin -c '$0 $*')" >&2
        exit 1
    }
    [[ -f "$SSH_KEY.pub" ]] || {
        echo "missing $SSH_KEY.pub; create it with: ssh-keygen -t rsa -b 4096 -N '' -f $SSH_KEY" >&2
        exit 1
    }
    local bridge
    bridge=$(incus network get incusbr0 ipv4.address)
    [[ "$bridge" == "$SUBNET.1/24" ]] || {
        echo "incusbr0 is $bridge, expected $SUBNET.1/24 (incus network set incusbr0 ipv4.address=$SUBNET.1/24)" >&2
        exit 1
    }
    for n in $(nodes); do
        getent hosts "$n" | grep -q "^$(ip_of "$n")\b" || {
            echo "host /etc/hosts does not map $n to $(ip_of "$n")" >&2
            exit 1
        }
    done
    # Docker's FORWARD policy drops bridge traffic unless DOCKER-USER lets it
    # through. We can't read iptables without root, so probe connectivity
    # after boot instead (see wait_ssh).
}

hosts_block() {
    echo "127.0.0.1 localhost"
    for n in $(nodes); do echo "$(ip_of "$n") $n"; done
}

wait_ssh() {
    local n=$1
    for _ in $(seq 1 60); do
        if ssh -i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=3 \
            -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            "root@$n" true 2>/dev/null; then
            return 0
        fi
        sleep 2
    done
    echo "$n: no SSH from the host. If Docker is running, check the DOCKER-USER rules for incusbr0." >&2
    return 1
}

up() {
    local vm_flag=(--vm)
    local limits=(-c limits.cpu=2 -c limits.memory=4GiB -d root,size=20GiB)
    if [[ "${1:-}" == "--containers" ]]; then
        vm_flag=()
        limits=(-c limits.cpu=2 -c limits.memory=4GiB)
    fi

    local user_data
    user_data=$(sed "s|@SSH_PUBKEY@|$(cat "$SSH_KEY.pub")|" "$SCRIPT_DIR/cloud-init.yaml")

    for n in $(nodes); do
        if incus info "$n" >/dev/null 2>&1; then
            echo "$n already exists, skipping create (use 'nodes.sh down' to rebuild)"
            continue
        fi
        echo "creating $n ($(ip_of "$n"))"
        incus init "$IMAGE" "$n" "${vm_flag[@]}" "${limits[@]}" \
            -c cloud-init.user-data="$user_data"
        incus config device override "$n" eth0 ipv4.address="$(ip_of "$n")"
        incus start "$n"
    done

    for n in $(nodes); do
        echo "waiting for $n"
        # The VM agent takes a few seconds to come up before exec works.
        until incus exec "$n" -- true 2>/dev/null; do sleep 2; done
        incus exec "$n" -- cloud-init status --wait >/dev/null || {
            echo "$n: cloud-init failed; see: incus exec $n -- cloud-init status --long" >&2
            exit 1
        }
        hosts_block | incus exec "$n" -- tee /etc/hosts >/dev/null
        wait_ssh "$n"
        ssh -i "$SSH_KEY" -o BatchMode=yes "root@$n" test -e /etc/cloud/cloud-init.disabled || {
            echo "$n: cloud-init did not finish provisioning (no /etc/cloud/cloud-init.disabled)" >&2
            exit 1
        }
    done

    # Jepsen's SSH client wants known_hosts entries (or --strict-host-key-checking false).
    for n in $(nodes); do ssh-keygen -R "$n" >/dev/null 2>&1 || true; done
    for n in $(nodes); do ssh-keyscan -t rsa,ed25519 "$n" 2>/dev/null; done >>"$HOME/.ssh/known_hosts"

    for n in $(nodes); do
        if ! incus snapshot show "$n" "$SNAPSHOT" >/dev/null 2>&1; then
            echo "snapshotting $n"
            incus snapshot create "$n" "$SNAPSHOT"
        fi
    done
    echo "nodes ready: $(nodes | paste -sd, -)"
}

reset() {
    for n in $(nodes); do
        echo "restoring $n"
        incus stop --force "$n" 2>/dev/null || true
        incus snapshot restore "$n" "$SNAPSHOT"
        incus start "$n"
    done
    for n in $(nodes); do wait_ssh "$n"; done
}

down() {
    for n in $(nodes); do
        incus delete --force "$n" 2>/dev/null && echo "deleted $n" || true
        ssh-keygen -R "$n" >/dev/null 2>&1 || true
    done
}

cmd=${1:-}
shift || true
case "$cmd" in
    up) preflight; up "$@" ;;
    reset) preflight; reset ;;
    down) down ;;
    status) incus list -c ns4tS "^n[0-9]+$" ;;
    *) sed -n '2,15p' "$0"; exit 1 ;;
esac
