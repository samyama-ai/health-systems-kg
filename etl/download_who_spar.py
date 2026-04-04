"""Download WHO SPAR (IHR capacity) data.

Usage: python -m etl.download_who_spar --data-dir data
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path

import requests

GHO_BASE = "https://ghoapi.azureedge.net/api"

# IHR SPAR capacity indicators
# SPAR v2 indicators (v1 IHRSPAR_CAPACITY* are deprecated/empty)
SPAR_INDICATORS = {
    "IHRSPAR2_C01": "C1 - Policy, legal and normative instruments",
    "IHRSPAR2_C02": "C2 - IHR coordination and National IHR Focal Point",
    "IHRSPAR2_C03": "C3 - Financing",
    "IHRSPAR2_C04": "C4 - Laboratory",
    "IHRSPAR2_C05": "C5 - Surveillance",
    "IHRSPAR2_C06": "C6 - Human resources",
    "IHRSPAR2_C07": "C7 - Health emergency management",
    "IHRSPAR2_C08": "C8 - Health services provision",
    "IHRSPAR2_C09": "C9 - Infection prevention and control",
    "IHRSPAR2_C10": "C10 - Risk communication and community engagement",
    "IHRSPAR2_C11": "C11 - Points of entry and border health",
    "IHRSPAR2_C12": "C12 - Zoonotic diseases",
    "IHRSPAR2_C13": "C13 - Food safety",
    "IHRSPAR2_C14": "C14 - Chemical events",
    "IHRSPAR2_C15": "C15 - Radiation emergencies",
}
# Also fetch the aggregate SDG indicator
AGGREGATE_INDICATOR = "SDGIHR2021"


def _fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json().get("value", [])
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
            else:
                print(f"  [ERROR] {url}: {e}")
                return []


def download_spar(data_dir: str | Path) -> dict:
    out_path = Path(data_dir) / "who_spar"
    out_path.mkdir(parents=True, exist_ok=True)

    # Countries (reuse WHO GHO country list)
    print("Downloading WHO country list...")
    countries_raw = _fetch_json(f"{GHO_BASE}/DIMENSION/COUNTRY/DimensionValues")
    countries = []
    for c in countries_raw:
        code = c.get("Code", "").strip()
        name = c.get("Title", "").strip()
        if code and name and len(code) == 3:
            countries.append({"iso_code": code, "name": name, "who_region": "", "income_level": ""})
    with open(out_path / "countries.json", "w") as f:
        json.dump(countries, f, indent=2)
    print(f"  {len(countries)} countries")

    # SPAR data
    print(f"Downloading SPAR capacities ({len(SPAR_INDICATORS)} indicators)...")
    all_rows = []
    for code, name in SPAR_INDICATORS.items():
        records = _fetch_json(f"{GHO_BASE}/{code}")
        for r in records:
            cc = str(r.get("SpatialDim", "")).strip()
            year = str(r.get("TimeDim", "")).strip()
            value = r.get("NumericValue")
            if cc and year and value is not None:
                cap_code = code.replace("IHRSPAR2_", "")
                all_rows.append({"country_code": cc, "capacity_code": cap_code,
                                 "capacity_name": name, "year": year, "score": int(value)})
        print(f"  {code}: {len(records)} records")
        time.sleep(0.3)

    csv_path = out_path / "spar.csv"
    if all_rows:
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["country_code", "capacity_code", "capacity_name", "year", "score"])
            w.writeheader()
            w.writerows(all_rows)
    print(f"  Total: {len(all_rows)} records")
    return {"countries": len(countries), "spar_records": len(all_rows)}


def main():
    parser = argparse.ArgumentParser(description="Download WHO SPAR data")
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()
    download_spar(args.data_dir)


if __name__ == "__main__":
    main()
