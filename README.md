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

### run main.py from root:
```
python src/main.py 
```

## output

### the merged parquet should pop up in `/data/merged/`

### you can have a closer look at/into the data via the samples in `/data/split_aggregations/`

### `/data/beaconlist/` will contain the beaconlist with an additional field `matched_parquets` that gives a hint on what BEACON types were matched/used for which BEACON 