# BEACONaggregator
An aggregator using the output of https://github.com/fid-philosophie/BEACONlist

## how to

### activate venv
#### Windows
```
python -m venv venv
.\venv\Scripts\activate
```

#### Linux/macOS
```
python3 -m venv venv
source venv/bin/activate
```

### install packages
```
pip install -r requirements.txt
```

### run main.py from src/:
```
cd src
python main.py
```

#### options
```
python main.py --beaconlist <path or URL>   # use a local file or custom URL instead of the default GitHub source
python main.py --beacons-dir <path>         # skip download and reuse an existing beacons directory
python main.py --pick                       # interactively pick a beaconlist from data/beaconlist/
python main.py --reuse-beacons              # interactively pick an existing beacons directory
```

---

## Docker

### build
```
docker build -t beaconaggregator .
```

### run (default: fetches BEACONlist from GitHub, downloads all BEACON files)
```
docker run --rm -v ./data:/data beaconaggregator
```

### run with a local BEACONlist
Mount your `data/` directory and point `BEACONLIST_SOURCE` at the file inside it:
```
docker run --rm \
  -v ./data:/data \
  -e BEACONLIST_SOURCE=/data/beaconlist/BEACONlist.json \
  beaconaggregator
```

### resume a previous run (skip re-downloading ~270 MB of BEACON files)
If you already have a downloaded beacons directory from a previous run, set `BEACONS_DIR` to skip the download step:
```
docker run --rm \
  -v ./data:/data \
  -e BEACONS_DIR=/data/beacons/beacons_20250101-1200 \
  beaconaggregator
```

Both env vars can be combined.

### using docker compose
```
docker compose run --rm beaconaggregator
```

To override env vars without editing `docker-compose.yml`:
```
BEACONLIST_SOURCE=/data/beaconlist/BEACONlist.json docker compose run --rm beaconaggregator
```

---

## output

### the merged parquet should pop up in `/data/merged/`

### you can have a closer look at/into the data via the samples in `/data/split_aggregations/`

### `/data/beaconlist_matches/` will contain the beaconlist with an additional field `matched_parquets` that gives a hint on what BEACON types were matched/used for which BEACON