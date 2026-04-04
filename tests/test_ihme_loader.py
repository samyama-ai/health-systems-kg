"""Tests for IHME health expenditure loader."""

import csv, json, os, tempfile
import pytest

SAMPLE_COUNTRIES = [
    {"iso_code": "IND", "name": "India", "who_region": "SEAR", "income_level": "Lower middle income"},
    {"iso_code": "NGA", "name": "Nigeria", "who_region": "AFR", "income_level": "Lower middle income"},
]

SAMPLE_IHME = [
    {"country_code": "IND", "indicator": "che_pc_usd", "indicator_name": "Current health expenditure per capita (US$)", "year": 2021, "value": 63.0},
    {"country_code": "IND", "indicator": "che_gdp_pct", "indicator_name": "Current health expenditure (% of GDP)", "year": 2021, "value": 3.3},
    {"country_code": "NGA", "indicator": "che_pc_usd", "indicator_name": "Current health expenditure per capita (US$)", "year": 2021, "value": 18.0},
]

def _setup(tmpdir):
    os.makedirs(os.path.join(tmpdir, "who_spar"), exist_ok=True)
    os.makedirs(os.path.join(tmpdir, "ihme"), exist_ok=True)
    with open(os.path.join(tmpdir, "who_spar", "countries.json"), "w") as f:
        json.dump(SAMPLE_COUNTRIES, f)
    with open(os.path.join(tmpdir, "ihme", "expenditure.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SAMPLE_IHME[0].keys()); w.writeheader(); w.writerows(SAMPLE_IHME)

@pytest.fixture(scope="module")
def ihme_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        _setup(tmpdir)
        try:
            from samyama import SamyamaClient
            from etl.helpers import Registry
            from etl.spar_loader import load_spar
            from etl.ihme_loader import load_ihme
            client = SamyamaClient.embedded()
            registry = Registry()
            load_spar(client, tmpdir, registry)
            stats = load_ihme(client, tmpdir, registry)
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

class TestExpenditureNodes:
    def test_created(self, ihme_data):
        client, _, _ = ihme_data
        rows = _q(client, "MATCH (f:FundingFlow) WHERE f.donor = 'IHME' RETURN count(*) AS c")
        assert rows[0]["c"] >= 3

    def test_linked(self, ihme_data):
        client, _, _ = ihme_data
        rows = _q(client, """
            MATCH (f:FundingFlow)-[:FUNDED_BY]->(c:Country {name: 'India'})
            WHERE f.donor = 'IHME'
            RETURN f.indicator_name, f.value ORDER BY f.indicator
        """)
        assert len(rows) >= 2

    def test_stats(self, ihme_data):
        _, stats, _ = ihme_data
        assert stats["source"] == "ihme"
        assert stats["funding_nodes"] >= 3
