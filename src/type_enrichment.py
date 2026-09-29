from pathlib import Path
import duckdb
from datetime import datetime
import time
import requests

import gzip
import json


def download_from_lobid(jsonl_file):    
    url = "https://lobid.org/gnd/search"
    headers = {
        "Accept": "application/x-jsonlines",
        "Accept-Encoding": "gzip",
    }
    params = {"q": "*", "format": "jsonl"}
    print("Start downloading lobid dump")
    print("This will take a while...")
    written = 0
    last_update = 0
    

    with requests.get(url, headers=headers, params=params, stream=True) as r:
        r.raise_for_status()
        r.raw.decode_content = False    
     

        with open(jsonl_file.with_suffix(jsonl_file.suffix + ".gz"), "wb") as f:
            while True:
                chunk = r.raw.read(4 * 1024 * 1024)  
                written += len(chunk )
                if not chunk:
                    break
                f.write(chunk)
                now = time.time()
                if now - last_update > 0.2:
                    print(
                        f"\rDownloading... {written / 1024 / 1024:.1f} MB",
                        end="",
                        flush=True,
                    )
                    last_update = now
        print("Unziping")
        with gzip.open(jsonl_file.with_suffix(jsonl_file.suffix + ".gz"), "rt", encoding="utf-8") as fin, open(jsonl_file, "w", encoding="utf-8") as fout:
            for line in fin:
                obj = json.loads(line)
                # keep only needed fields:
                new_json = {
                    "id": obj.get("id"),
                        "gndIdentifier": obj.get("gndIdentifier"),
                        "type": obj.get("type"),
                        "oldAuthorityNumber": obj.get("oldAuthorityNumber"),
                }       
                fout.write(json.dumps(new_json, ensure_ascii=False) + "\n")
        print("\nDeleting gzip file")
        jsonl_file.with_suffix(jsonl_file.suffix + ".gz").unlink(missing_ok=True)
        print("done\n")
        
        
    

def main():
    data_dir = Path("data")
    merged_dir = data_dir / "merged"
    lobid_dump = data_dir / "lobid" 
    Path.mkdir(lobid_dump, exist_ok=True)
    lobid_dump = lobid_dump / "lobid-gnd.jsonl"
    
    # only download if file doesn't exists or file is older than one week:
    if not (lobid_dump.exists() and lobid_dump.is_file()) or lobid_dump.is_file() and time.time() - lobid_dump.stat().st_mtime  > 7 * 24 * 60 * 60:
        download_from_lobid(lobid_dump)
    else:
        print(f"{lobid_dump} allready exists. Skipping download...")    

    timestamp = datetime.now().strftime("%Y%m%d")  


    # Nimmt einfach das erste parquet im merged dir, das muss dann noch entsprechend angepasst werden..
    #parquet_file = next(merged_dir.iterdir())
    parquet_file = sorted(merged_dir.glob("*.parquet"))[-1] # die letzte (alphabetisch sortiert die neueste) .parquet Datei wählen
    
    # hier muss dann auch noch der pfad der ausgabe angepasst werden 
    out_file = data_dir / f"beacon_with_types_{timestamp}.parquet"

    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    

    print("Loading parquet file")
    con.execute("""
    CREATE TABLE records AS
    SELECT *
    FROM read_parquet(?)
    """, [str(parquet_file)])


    # get all distinct authority ids
    con.execute("""
    CREATE TEMP TABLE distinct_gnds AS
    SELECT DISTINCT authority_id AS gnd
    FROM records
    WHERE authority_id IS NOT NULL
    """)

    print("Found", con.execute("SELECT COUNT(*) FROM distinct_gnds").fetchall()[0][0], "different autority ids in", con.execute("SELECT COUNT(*) FROM records").fetchall()[0][0], "records.")

   
    con.execute("""
    CREATE TEMP TABLE gnd_types AS
    SELECT
        gnd,
        ANY_VALUE(gnd_type) AS gnd_type,
        list_sort(ANY_VALUE(gnd_type_list)) AS gnd_type_list
    FROM (

        -- aktuelle GND
        SELECT
            regexp_replace(
                regexp_replace(id, '^https?://d-nb\\.info/gnd/', ''),
                '^\\([a-zA-Z]{2}-[0-9]{3}[a-zA-Z]?\\)',
                ''
            ) AS gnd,

            type AS gnd_type_list,

            CASE
                WHEN list_contains(type,'Person') THEN 'Person'
                WHEN list_contains(type,'CorporateBody') THEN 'CorporateBody'
                WHEN list_contains(type,'Work') THEN 'Work'
                WHEN list_contains(type,'SubjectHeading') THEN 'SubjectHeading'
                WHEN list_contains(type,'PlaceOrGeographicName') THEN 'PlaceOrGeographicName'
                WHEN list_contains(type,'Family') THEN 'Family'
                ELSE 'Other'
            END AS gnd_type

        FROM read_json_auto(?)

        UNION ALL

        -- alte GND
        SELECT
            regexp_replace(
                regexp_replace(old, '^https?://d-nb\\.info/gnd/', ''),
                '^\\([a-zA-Z]{2}-[0-9]{3}[a-zA-Z]?\\)',
                ''
            ) AS gnd,

            type AS gnd_type_list,

            CASE
                WHEN list_contains(type,'Person') THEN 'Person'
                WHEN list_contains(type,'CorporateBody') THEN 'CorporateBody'
                WHEN list_contains(type,'Work') THEN 'Work'
                WHEN list_contains(type,'SubjectHeading') THEN 'SubjectHeading'
                WHEN list_contains(type,'PlaceOrGeographicName') THEN 'PlaceOrGeographicName'
                WHEN list_contains(type,'Family') THEN 'Family'
                ELSE 'Other'
            END AS gnd_type

        FROM read_json_auto(?),
        UNNEST(oldAuthorityNumber) AS t(old)

    ) x
    JOIN distinct_gnds USING (gnd) GROUP BY gnd      
    """, [str(lobid_dump), str(lobid_dump)])
    
    print("Found", con.execute("SELECT COUNT(*) FROM gnd_types").fetchall()[0][0], "different gnds in lobid dump")
    ################## join types to all (beacon_uri, authority_id) tuples:
    
    print("Joining types back to records")
    con.execute("""
    CREATE TABLE final AS
    SELECT
        r.*,
        t.gnd_type,
        COALESCE(t.gnd_type_list, []) AS gnd_type_list
    FROM records r
    LEFT JOIN gnd_types t
    ON regexp_replace(
                regexp_replace(r.authority_id, '^https?://d-nb\\.info/gnd/', ''),
                '^\\([a-zA-Z]{2}-[0-9]{3}[a-zA-Z]?\\)',
                ''
            )  = regexp_replace(
                regexp_replace(t.gnd, '^https?://d-nb\\.info/gnd/', ''),
                '^\\([a-zA-Z]{2}-[0-9]{3}[a-zA-Z]?\\)',
                ''
            )
    """)


    #### count matched and unmatched authority_ids:
    print("\n",con.execute("""
    SELECT
    COUNT(*) AS total,
    COUNT(gnd_type) AS matched,
    COUNT(*) - COUNT(gnd_type) AS unmatched
    FROM final
    """).df(), "\n")


    ########### file output:
    print(f"Writing parque file {out_file}")

    con.execute("""
    COPY final TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
    """, [str(out_file)])

    print("Done.")
    print()
    
    print(" example with match per beacon_uri:")
    print(
        con.execute("""
        SELECT DISTINCT ON (beacon_uri)
            authority_id, gnd_type, gnd_type_list, beacon_uri
        FROM read_parquet(?) 
        WHERE gnd_type IS NOT NULL
        LIMIT 20
        """, [str(out_file)]).df()
    )
    print()
    
    print(" example without match per beacon_uri:")
    print(
        con.execute("""
        SELECT DISTINCT ON (beacon_uri)
            authority_id, gnd_type, gnd_type_list, beacon_uri
        FROM read_parquet(?) 
        WHERE gnd_type IS NULL
        LIMIT 20
        """, [str(out_file)]).df()
    )
    print()
    print("Finished!")

    


  
if __name__ == "__main__":
    main()