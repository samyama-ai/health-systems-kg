"""Tests for shared Cypher helpers."""

from etl.helpers import _escape, _q, _prop_str, Registry


class TestEscape:
    def test_plain(self):
        assert _escape("hello") == "hello"

    def test_quote(self):
        assert _escape("it's") == "it\\'s"


class TestQ:
    def test_none(self):
        assert _q(None) == "null"

    def test_int(self):
        assert _q(42) == "42"

    def test_string(self):
        assert _q("hello") == "'hello'"


class TestPropStr:
    def test_empty(self):
        assert _prop_str({}) == "{}"

    def test_mixed(self):
        r = _prop_str({"name": "WHO", "score": 85})
        assert "name: 'WHO'" in r
        assert "score: 85" in r


class TestRegistry:
    def test_all_fields(self):
        r = Registry()
        for f in ["countries", "health_facilities", "health_workforce",
                   "supply_chain", "policies", "funding_flows", "emergency_responses",
                   "located_in", "serves", "supplies", "policy_of", "funded_by", "capacity_for"]:
            assert isinstance(getattr(r, f), set)

    def test_dedup(self):
        r = Registry()
        r.countries.add("IND")
        r.countries.add("IND")
        assert len(r.countries) == 1
