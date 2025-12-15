import requests

RAW_URL = (
    "https://raw.githubusercontent.com/"
    "fid-philosophie/BEACONlist/main/latest/BEACONlist.json"
)

def LoadBeaconList():
    resp = requests.get(RAW_URL, timeout=30)
    resp.raise_for_status()  # raises if e.g. 404 / 403
    data = resp.json()       # assuming it's valid JSON
    return data
