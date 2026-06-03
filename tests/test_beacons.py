# tests/test_beacons.py

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import beacons


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_load_beacon_list_uses_default_url(monkeypatch):
    expected = {"source": "default-url"}
    called = {}

    def fake_get(url, timeout):
        called["url"] = url
        called["timeout"] = timeout
        return DummyResponse(expected)

    monkeypatch.setattr(beacons.requests, "get", fake_get)

    result = beacons.load_beacon_list()

    assert result == expected
    assert called["url"] == beacons.RAW_URL
    assert called["timeout"] == 30


def test_load_beacon_list_uses_custom_url(monkeypatch):
    expected = {"source": "custom-url"}
    custom_url = "https://example.org/custom.json"
    called = {}

    def fake_get(url, timeout):
        called["url"] = url
        called["timeout"] = timeout
        return DummyResponse(expected)

    monkeypatch.setattr(beacons.requests, "get", fake_get)

    result = beacons.load_beacon_list(custom_url)

    assert result == expected
    assert called["url"] == custom_url
    assert called["timeout"] == 30


def test_load_beacon_list_reads_local_file(tmp_path):
    expected = {"source": "local-file", "items": [1, 2, 3]}
    json_file = tmp_path / "beaconlist.json"
    json_file.write_text(json.dumps(expected), encoding="utf-8")

    result = beacons.load_beacon_list(str(json_file))

    assert result == expected


def test_load_beacon_list_raises_for_missing_local_file(tmp_path):
    missing_file = tmp_path / "does-not-exist.json"

    with pytest.raises(FileNotFoundError):
        beacons.load_beacon_list(str(missing_file))