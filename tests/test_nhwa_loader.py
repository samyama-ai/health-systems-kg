"""Tests for WHO NHWA (health workforce) loader."""

import csv, json, os, tempfile
import pytest

SAMPLE_COUNTRIES = [
    {"iso_code": "IND", "name": "India", "who_region": "SEAR", "income_level": "Lower middle income"},
    {"iso_code": "NGA", "name": "Nigeria", "who_region": "AFR", "income_level": "Lower middle income"},
]

SAMPLE_NHWA = [
    {"country_code": "IND", "profession": "physicians", "count": 1300000, "density_per_10k": 9.1, "year": 2022},
    {"country_code": "IND", "profession": "nurses_midwives", "count": 3200000, "density_per_10k": 22.4, "year": 2022},
    {"country_code": "NGA", "profession": "physicians", "count": 75000, "density_per_10k": 3.3, "year": 2022},
]

def _setup(tmpdir):
    os.makedirs(os.path.join(tmpdir, "who_spar"), exist_ok=True)
    os.makedirs(os.path.join(tmpdir, "who_nhwa"), exist_ok=True)
    with open(os.path.join(tmpdir, "who_spar", "countries.json"), "w") as f:
        json.dump(SAMPLE_COUNTRIES, f)
    with open(os.path.join(tmpdir, "who_nhwa", "nhwa.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SAMPLE_NHWA[0].keys()); w.writeheader(); w.writerows(SAMPLE_NHWA)

@pytest.fixture(scope="module")
def nhwa_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        _setup(tmpdir)
        try:
            from samyama import SamyamaClient
            from etl.helpers import Registry
            from etl.spar_loader import load_spar
            from etl.nhwa_loader import load_nhwa
            client = SamyamaClient.embedded()
            registry = Registry()
            load_spar(client, tmpdir, registry)
            stats = load_nhwa(client, tmpdir, registry)
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

class TestWorkforceNodes:
    def test_created(self, nhwa_data):
        client, _, _ = nhwa_data
        rows = _q(client, "MATCH (h:HealthWorkforce) RETURN count(*) AS c")
        assert rows[0]["c"] >= 3

    def test_linked(self, nhwa_data):
        client, _, _ = nhwa_data
        rows = _q(client, """
            MATCH (h:HealthWorkforce)-[:SERVES]->(c:Country {name: 'India'})
            RETURN h.profession, h.density_per_10k ORDER BY h.profession
        """)
        assert len(rows) >= 2

    def test_stats(self, nhwa_data):
        _, stats, _ = nhwa_data
        assert stats["source"] == "who_nhwa"
        assert stats["workforce_nodes"] >= 3
