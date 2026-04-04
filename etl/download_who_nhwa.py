"""Download WHO health workforce density data from GHO API.

Fetches physician, nursing/midwifery, dentist, and pharmacist density
per 10,000 population from the WHO Global Health Observatory.

Usage:
    python -m etl.download_who_nhwa --data-dir data
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

import requests

GHO_BASE = "https://ghoapi.azureedge.net/api"

WORKFORCE_INDICATORS = {
    "HWF_0001": "physicians",
    "HWF_0006": "nursing_midwifery",
    "HWF_0003": "dentists",
    "HWF_0004": "pharmacists",
}


def _fetch_gho_all(code: str) -> list[dict]:
    """Fetch all records from WHO GHO OData endpoint with pagination."""
    records = []
    url = f"{GHO_BASE}/{code}"
    while url:
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            records.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
        except Exception as e:
            print(f"    [ERROR] {url}: {e}")
            break
    return records


def download_nhwa(data_dir: str | Path) -> dict:
    """Download WHO health workforce density data from GHO."""
    out_path = Path(data_dir) / "who_nhwa"
    out_path.mkdir(parents=True, exist_ok=True)

    print("Downloading WHO Health Workforce (NHWA) from GHO...")
    rows = []
    for code, profession in WORKFORCE_INDICATORS.items():
        records = _fetch_gho_all(code)
        count = 0
        for r in records:
            cc = str(r.get("SpatialDim", "")).strip()
            year = str(r.get("TimeDim", "")).strip()
            value = r.get("NumericValue")
            if cc and year and value is not None and len(cc) == 3:
                rows.append({
                    "country_code": cc,
                    "profession": profession,
                    "count": 0,
                    "density_per_10k": round(value, 2),
                    "year": year,
                })
                count += 1
        print(f"  {code} ({profession}): {count} records")
        time.sleep(0.3)

    csv_path = out_path / "nhwa.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["country_code", "profession", "count", "density_per_10k", "year"])
        w.writeheader()
        w.writerows(rows)
    print(f"  Total: {len(rows)} records → {csv_path}")
    return {"records": len(rows)}


def main():
    parser = argparse.ArgumentParser(description="Download WHO NHWA data")
    parser.add_argument("--data-dir", default="data", help="Output directory")
    args = parser.parse_args()
    download_nhwa(args.data_dir)


if __name__ == "__main__":
    main()
