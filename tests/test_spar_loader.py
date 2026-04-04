"""Tests for WHO SPAR (IHR capacity) loader."""

import csv
import json
import os
import tempfile

import pytest

SAMPLE_COUNTRIES = [
    {"iso_code": "IND", "name": "India", "who_region": "SEAR", "income_level": "Lower middle income"},
    {"iso_code": "NGA", "name": "Nigeria", "who_region": "AFR", "income_level": "Lower middle income"},
    {"iso_code": "BRA", "name": "Brazil", "who_region": "AMR", "income_level": "Upper middle income"},
]

SAMPLE_SPAR = [
    {"country_code": "IND", "capacity_code": "C1", "capacity_name": "Legislation and financing", "year": 2023, "score": 80},
    {"country_code": "IND", "capacity_code": "C2", "capacity_name": "IHR coordination", "year": 2023, "score": 60},
    {"country_code": "NGA", "capacity_code": "C1", "capacity_name": "Legislation and financing", "year": 2023, "score": 40},
    {"country_code": "BRA", "capacity_code": "C1", "capacity_name": "Legislation and financing", "year": 2023, "score": 100},
]


def _write_json(tmpdir, subdir, filename, data):
    path = os.path.join(tmpdir, subdir)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, filename), "w") as f:
        json.dump(data, f)


def _write_csv(tmpdir, subdir, filename, rows):
    path = os.path.join(tmpdir, subdir)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, filename), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture(scope="module")
def spar_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_json(tmpdir, "who_spar", "countries.json", SAMPLE_COUNTRIES)
        _write_csv(tmpdir, "who_spar", "spar.csv", SAMPLE_SPAR)
        try:
            from samyama import SamyamaClient
            from etl.helpers import Registry
            from etl.spar_loader import load_spar
            client = SamyamaClient.embedded()
            registry = Registry()
            stats = load_spar(client, tmpdir, registry)
            yield client, stats, registry
        except ImportError:
            pytest.skip("samyama package not available")


def _q(client, cypher):
    try:
        r = client.query_readonly(cypher, "default")
        return [dict(zip(r.columns, row)) for row in r.records]
    except Exception:
        r = client.query(cypher, "default")
        return [dict(zip(r.columns, row)) for row in r.records]


class TestCountryNodes:
    def test_created(self, spar_data):
        client, _, _ = spar_data
        rows = _q(client, "MATCH (c:Country) RETURN count(*) AS c")
        assert rows[0]["c"] == 3

    def test_iso_code(self, spar_data):
        client, _, _ = spar_data
        rows = _q(client, "MATCH (c:Country {name: 'India'}) RETURN c.iso_code")
        assert rows[0]["c.iso_code"] == "IND"


class TestEmergencyResponseNodes:
    def test_created(self, spar_data):
        client, _, _ = spar_data
        rows = _q(client, "MATCH (e:EmergencyResponse) RETURN count(*) AS c")
        assert rows[0]["c"] >= 4

    def test_linked_to_country(self, spar_data):
        client, _, _ = spar_data
        rows = _q(client, """
            MATCH (e:EmergencyResponse)-[:CAPACITY_FOR]->(c:Country {name: 'India'})
            RETURN e.capacity_name, e.score ORDER BY e.capacity_code
        """)
        assert len(rows) >= 2
        assert rows[0]["e.score"] == 80

    def test_brazil_perfect_score(self, spar_data):
        client, _, _ = spar_data
        rows = _q(client, """
            MATCH (e:EmergencyResponse)-[:CAPACITY_FOR]->(c:Country {name: 'Brazil'})
            RETURN e.score
        """)
        assert rows[0]["e.score"] == 100


class TestStats:
    def test_stats(self, spar_data):
        _, stats, _ = spar_data
        assert stats["source"] == "who_spar"
        assert stats["country_nodes"] == 3
        assert stats["emergency_response_nodes"] >= 4
        assert stats["capacity_for_edges"] >= 4
