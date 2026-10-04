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


# ── Value distribution, not row counts (samyama-graph#1609, #1815) ───────────
#
# SAMPLE_COUNTRIES above gives every country a who_region and an income_level.
# The real `data/who_spar/countries.json` gives 233 countries and 0 non-empty
# values for either, because WHO's GHO country dimension does not carry them.
# The fixture therefore had data the source does not, and the published
# snapshot shipped both columns present on all 233 Country nodes and non-empty
# on none. These tests use the source's actual shape.

EMPTY_COLUMN_COUNTRIES = [
    {"iso_code": "IND", "name": "India", "who_region": "", "income_level": ""},
    {"iso_code": "NGA", "name": "Nigeria", "who_region": "  ", "income_level": ""},
]

# The capacity name WHO actually publishes for C1 — the only one of the 15
# whose name contains a comma, which is what shifted the columns in #1609.
C1_NAME = "C1 - Policy, legal and normative instruments"

COMMA_SPAR = [
    {"country_code": "IND", "capacity_code": "C01", "capacity_name": C1_NAME, "year": 2021, "score": 47},
    {"country_code": "NGA", "capacity_code": "C01", "capacity_name": C1_NAME, "year": 2023, "score": 40},
    {"country_code": "IND", "capacity_code": "C03", "capacity_name": "C3 - Financing", "year": 2021, "score": 60},
]


@pytest.fixture(scope="module")
def source_shaped_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_json(tmpdir, "who_spar", "countries.json", EMPTY_COLUMN_COUNTRIES)
        _write_csv(tmpdir, "who_spar", "spar.csv", COMMA_SPAR)
        try:
            from samyama import SamyamaClient
            from etl.helpers import Registry
            from etl.spar_loader import load_spar
            client = SamyamaClient.embedded()
            stats = load_spar(client, tmpdir, Registry())
            yield client, stats
        except ImportError:
            pytest.skip("samyama package not available")


class TestNoEmptyColumns:
    @pytest.mark.parametrize("key", ["who_region", "income_level"])
    def test_a_column_with_no_values_is_not_written(self, source_shaped_data, key):
        client, _ = source_shaped_data
        rows = _q(client, f"MATCH (c:Country) WHERE exists(c.{key}) RETURN count(*) AS c")
        assert rows[0]["c"] == 0, (
            f"{key} has no value in the source, so no Country node may carry "
            f"the key: exists(c.{key}) must answer false, not "
            "true-with-nothing (samyama-graph#1815)"
        )

    def test_the_columns_that_do_have_values_are_still_written(self, source_shaped_data):
        client, _ = source_shaped_data
        rows = _q(client, "MATCH (c:Country) WHERE c.iso_code <> '' RETURN count(*) AS c")
        assert rows[0]["c"] == 2

    def test_a_region_is_written_when_the_source_has_one(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_json(tmpdir, "who_spar", "countries.json",
                        [{"iso_code": "IND", "name": "India",
                          "who_region": "SEAR", "income_level": ""}])
            _write_csv(tmpdir, "who_spar", "spar.csv", COMMA_SPAR[:1])
            try:
                from samyama import SamyamaClient
                from etl.helpers import Registry
                from etl.spar_loader import load_spar
            except ImportError:
                pytest.skip("samyama package not available")
            client = SamyamaClient.embedded()
            load_spar(client, tmpdir, Registry())
            rows = _q(client, "MATCH (c:Country) RETURN c.who_region")
            assert rows[0]["c.who_region"] == "SEAR"
            rows = _q(client, "MATCH (c:Country) WHERE exists(c.income_level) RETURN count(*) AS c")
            assert rows[0]["c"] == 0


class TestScoreDistribution:
    def test_no_score_exceeds_one_hundred(self, source_shaped_data):
        client, _ = source_shaped_data
        rows = _q(client, "MATCH (e:EmergencyResponse) RETURN max(e.score) AS hi, min(e.score) AS lo")
        assert 0 <= rows[0]["lo"] and rows[0]["hi"] <= 100, (
            "SPAR scores are percentages. A maximum in the 2020s means a year "
            "is sitting in the score column (samyama-graph#1609)"
        )

    def test_every_assessment_carries_a_year(self, source_shaped_data):
        client, _ = source_shaped_data
        rows = _q(client, "MATCH (e:EmergencyResponse) WHERE NOT exists(e.year) RETURN count(*) AS c")
        assert rows[0]["c"] == 0

    def test_the_comma_bearing_capacity_name_survives_intact(self, source_shaped_data):
        client, _ = source_shaped_data
        rows = _q(client, """
            MATCH (e:EmergencyResponse {capacity_code: 'C01'})
            RETURN e.capacity_name AS n, e.score AS s, e.year AS y ORDER BY y
        """)
        assert len(rows) == 2
        assert rows[0]["n"] == C1_NAME, "the name was truncated at its comma"
        assert [(r["y"], r["s"]) for r in rows] == [(2021, 47), (2023, 40)]
