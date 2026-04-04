"""Tests for Global Fund funding loader."""

import csv, json, os, tempfile
import pytest

SAMPLE_COUNTRIES = [
    {"iso_code": "IND", "name": "India", "who_region": "SEAR", "income_level": "Lower middle income"},
    {"iso_code": "NGA", "name": "Nigeria", "who_region": "AFR", "income_level": "Lower middle income"},
]

SAMPLE_GLOBALFUND = [
    {"country_code": "IND", "donor": "Global Fund", "disease_component": "HIV/AIDS", "amount_usd": 250000000, "year": 2023},
    {"country_code": "IND", "donor": "Global Fund", "disease_component": "Tuberculosis", "amount_usd": 180000000, "year": 2023},
    {"country_code": "NGA", "donor": "Global Fund", "disease_component": "Malaria", "amount_usd": 400000000, "year": 2023},
]

def _setup(tmpdir):
    os.makedirs(os.path.join(tmpdir, "who_spar"), exist_ok=True)
    os.makedirs(os.path.join(tmpdir, "globalfund"), exist_ok=True)
    with open(os.path.join(tmpdir, "who_spar", "countries.json"), "w") as f:
        json.dump(SAMPLE_COUNTRIES, f)
    with open(os.path.join(tmpdir, "globalfund", "disbursements.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SAMPLE_GLOBALFUND[0].keys()); w.writeheader(); w.writerows(SAMPLE_GLOBALFUND)

@pytest.fixture(scope="module")
def globalfund_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        _setup(tmpdir)
        try:
            from samyama import SamyamaClient
            from etl.helpers import Registry
            from etl.spar_loader import load_spar
            from etl.globalfund_loader import load_globalfund
            client = SamyamaClient.embedded()
            registry = Registry()
            load_spar(client, tmpdir, registry)
            stats = load_globalfund(client, tmpdir, registry)
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

class TestFundingNodes:
    def test_created(self, globalfund_data):
        client, _, _ = globalfund_data
        rows = _q(client, "MATCH (f:FundingFlow) RETURN count(*) AS c")
        assert rows[0]["c"] >= 3

    def test_linked(self, globalfund_data):
        client, _, _ = globalfund_data
        rows = _q(client, """
            MATCH (f:FundingFlow)-[:FUNDED_BY]->(c:Country {name: 'India'})
            RETURN f.disease_component, f.amount_usd ORDER BY f.amount_usd DESC
        """)
        assert len(rows) >= 2

    def test_stats(self, globalfund_data):
        _, stats, _ = globalfund_data
        assert stats["source"] == "globalfund"
        assert stats["funding_nodes"] >= 3
