# BEACONaggregator – Dockerfile
#
# Build:  docker build -t beaconaggregator .
# Run:    docker run --rm -v ./data:/data beaconaggregator

FROM python:3.11-slim

# ── System deps ──────────────────────────────────────────────────────────────
# pyarrow ships its own Arrow C++ runtime (no system lib needed).
# duckdb is a pure Python wheel on 3.11-slim. Nothing extra required.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ──────────────────────────────────────────────────────────────
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application code ─────────────────────────────────────────────────────────
COPY src/ ./src/

# ── Data volume ──────────────────────────────────────────────────────────────
# All pipeline output lands under /data (bind-mounted at runtime).
VOLUME ["/data"]

# ── Entrypoint ───────────────────────────────────────────────────────────────
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
