#!/bin/sh
# BEACONaggregator entrypoint
#
# The src/ modules import each other with bare names (e.g. `from helpers import …`),
# so we must run from inside src/ where Python can resolve those imports.
#
# All I/O goes to /data, which is bind-mounted from the host.
# The pipeline creates timestamped subdirs under /data automatically,
# so no further path wiring is needed here.

set -e

echo "=== BEACONaggregator pipeline starting ==="
echo "Output directory: /data"

cd /app/src

# Redirect all pipeline output to /data.
# main_pipeline() constructs paths as Path("data/…") relative to cwd,
# so we symlink /app/src/data → /data to keep the code's assumptions intact.
if [ ! -e data ]; then
    ln -s /data data
fi

python main.py

echo "=== BEACONaggregator pipeline complete ==="
