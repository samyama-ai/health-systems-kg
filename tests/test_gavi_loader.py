"""Tests for GAVI supply chain loader."""

import csv, json, os, tempfile
import pytest

SAMPLE_COUNTRIES = [
    {"iso_code": "IND", "name": "India", "who_region": "SEAR", "income_level": "Lower middle income"},
    {"iso_code": "NGA", "name": "Nigeria", "who_region": "AFR", "income_level": "Lower middle income"},
]

SAMPLE_GAVI = [
    {"country_code": "IND", "vaccine_name": "Pentavalent", "doses_shipped": 120000000, "doses_used": 115000000, "wastage_pct": 4.2, "year": 2023},
    {"country_code": "NGA", "vaccine_name": "Pentavalent", "doses_shipped": 30000000, "doses_used": 24000000, "wastage_pct": 20.0, "year": 2023},
    {"country_code": "NGA", "vaccine_name": "PCV13", "doses_shipped": 25000000, "doses_used": 22000000, "wastage_pct": 12.0, "year": 2023},
]

def _setup(tmpdir):
    os.makedirs(os.path.join(tmpdir, "who_spar"), exist_ok=True)
    os.makedirs(os.path.join(tmpdir, "gavi"), exist_ok=True)
    with open(os.path.join(tmpdir, "who_spar", "countries.json"), "w") as f:
        json.dump(SAMPLE_COUNTRIES, f)
    with open(os.path.join(tmpdir, "gavi", "supply.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SAMPLE_GAVI[0].keys()); w.writeheader(); w.writerows(SAMPLE_GAVI)

@pytest.fixture(scope="module")
def gavi_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        _setup(tmpdir)
        try:
            from samyama import SamyamaClient
            from etl.helpers import Registry
            from etl.spar_loader import load_spar
            from etl.gavi_loader import load_gavi
            client = SamyamaClient.embedded()
            registry = Registry()
            load_spar(client, tmpdir, registry)
            stats = load_gavi(client, tmpdir, registry)
            yield client, stats, registry
        except ImportError:
            pytest.skip("samyama not available")

def _q(client, cypher):
    try:
        r = client.query_readonly(cypher, "default")
        return [dict(zip(r.columns, row)) for row in r.records]
    except Exception:
        r = client.query(cypher, "default")
        return [dict(zip(r.columns, row)) for row in r.records]

class TestSupplyChainNodes:
    def test_created(self, gavi_data):
        client, _, _ = gavi_data
        rows = _q(client, "MATCH (s:SupplyChain) RETURN count(*) AS c")
        assert rows[0]["c"] >= 3

    def test_linked(self, gavi_data):
        client, _, _ = gavi_data
        rows = _q(client, """
            MATCH (s:SupplyChain)-[:SUPPLIES]->(c:Country {name: 'Nigeria'})
            RETURN s.vaccine_name, s.wastage_pct ORDER BY s.vaccine_name
        """)
        assert len(rows) >= 2

    def test_stats(self, gavi_data):
        _, stats, _ = gavi_data
        assert stats["source"] == "gavi"
        assert stats["supply_chain_nodes"] >= 3
