#!/bin/sh
# BEACONaggregator entrypoint
#
# The src/ modules import each other with bare names (e.g. `from helpers import …`),
# so we must run from inside src/ where Python can resolve those imports.
#
# All I/O goes to /data, which is bind-mounted from the host.
# The pipeline creates timestamped subdirs under /data automatically.
#
# Environment variables:
#   BEACONLIST_SOURCE  – optional path or URL to a BEACONlist JSON file.
#                        Defaults to the canonical GitHub URL in beacons.py.
#   BEACONS_DIR        – optional path to an already-downloaded beacons directory.
#                        If set, the download step is skipped.

set -e

echo "=== BEACONaggregator pipeline starting ==="

cd /app/src

# Symlink /app/src/data → /data so the pipeline's relative Path("data/…") calls
# resolve to the bind-mounted volume without any code changes.
if [ ! -e data ]; then
    ln -s /data data
fi

# Build the argument list dynamically based on env vars.
ARGS=""

if [ -n "$BEACONLIST_SOURCE" ]; then
    echo "Using beaconlist: $BEACONLIST_SOURCE"
    ARGS="$ARGS --beaconlist $BEACONLIST_SOURCE"
else
    echo "Using default beaconlist (GitHub)"
fi

if [ -n "$BEACONS_DIR" ]; then
    echo "Reusing beacons dir: $BEACONS_DIR"
    ARGS="$ARGS --beacons-dir $BEACONS_DIR"
fi

echo "Output directory: /data"

python main.py $ARGS

echo "=== BEACONaggregator pipeline complete ==="
