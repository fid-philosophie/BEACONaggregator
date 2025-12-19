# BEACONaggregator
An aggregator using the output of https://github.com/fid-philosophie/BEACONlist_PRIVATE

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

### run pipeline from repo root:
```
python src/main.py pipeline
```

## output

### the merged parquet should pop up in `/data/merged/` (timestamped)

### you can have a closer look at/into the data via the samples in `/data/split_aggregations/samples/`

### `/data/beaconlist/` will contain the beaconlist with an additional field `matched_parquets` that gives a hint on what BEACON types were matched/used for which BEACON

## data layout (current)

- `data/beacons/<run_id>/` downloaded beacon files + `beacon_downloads_metadata.json`
- `data/aggregations/<run_id>/` parquet aggregations for the run
- `data/split_aggregations/` split parquets (overwritten each run)
- `data/split_aggregations/samples/` JSON samples for inspection
- `data/merged/` merged parquet outputs (timestamped) + `beacons_merged_latest.parquet`
- `data/beaconlist/` JSON with `matched_parquets`

## CLI

### run full pipeline
```
python src/main.py pipeline
```
This creates a new `run_id` (timestamp) and writes outputs into the folders described above.

### subcommands
Most subcommands expect an existing `run_id` to locate inputs under `data/beacons/<run_id>/` and `data/aggregations/<run_id>/`.
```
python src/main.py download --run-id 20250101-120000
python src/main.py aggregate --run-id 20250101-120000
python src/main.py add-metadata --run-id 20250101-120000
python src/main.py split --run-id 20250101-120000
python src/main.py resolve data/split_aggregations/target-null_col1-2.parquet
python src/main.py sample data/split_aggregations/target-null_col1-2_resolved.parquet --out-dir data/split_aggregations/samples
python src/main.py match data/split_aggregations/target-null_col1-2_resolved.parquet --out-dir data/beaconlist
python src/main.py merge data/split_aggregations/*_resolved.parquet --out-parquet data/merged/beacons_merged_custom.parquet
```

### schema flexibility
The output schema is still evolving. You can pass a custom column list when aggregating:
```
python src/main.py pipeline --columns source_file,TARGET,NAME,FEED,TIMESTAMP,col1,col2,col3,col4,col5
```
