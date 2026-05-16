#!/bin/sh
# install.sh – runs inside the DaemonSet init container.
#
# What it does:
#   1. Copies all CNI plugin binaries to the host node's /opt/cni/bin/
#   2. Writes the CNI network config to the host node's /etc/cni/net.d/
#
# The host paths are bind-mounted into the container at:
#   /host/opt/cni/bin    (host's /opt/cni/bin)
#   /host/etc/cni/net.d  (host's /etc/cni/net.d)

set -e

CNI_BIN_SRC=${CNI_BIN_SRC:-/opt/cni/bin}
CNI_BIN_DIR=${CNI_BIN_DIR:-/host/opt/cni/bin}
CNI_CONF_DIR=${CNI_CONF_DIR:-/host/etc/cni/net.d}

echo "==> Installing purenet CNI"

mkdir -p "$CNI_BIN_DIR" "$CNI_CONF_DIR"

# ── 1. Install binaries ──────────────────────────────────────────────────────
# Copy the purenet shim into the host's CNI bin directory.
# No external IPAM binaries needed — IP allocation is handled in-process.
for bin in purenet; do
    echo "    installing $bin"
    install -m 0755 "$CNI_BIN_SRC/$bin" "$CNI_BIN_DIR/$bin"
done

# ── 2. Write CNI network config ──────────────────────────────────────────────
# Only write if no config already exists for this network so a re-scheduled
# pod (e.g. after a node reboot) does not stomp on manual customisations.
CONF_FILE="$CNI_CONF_DIR/10-purenet.conf"

if [ ! -f "$CONF_FILE" ]; then
    echo "    writing $CONF_FILE"
    cp /etc/cni/net.d/10-purenet.conf "$CONF_FILE"
else
    echo "    $CONF_FILE already exists, skipping"
fi

echo "==> purenet CNI installed successfully"
