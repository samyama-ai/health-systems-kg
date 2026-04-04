"""Download Global Fund disbursement data.

Usage: python -m etl.download_globalfund --data-dir data
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def download_globalfund(data_dir: str | Path) -> dict:
    out_path = Path(data_dir) / "globalfund"
    out_path.mkdir(parents=True, exist_ok=True)
    csv_path = out_path / "disbursements.csv"

    if csv_path.exists():
        with open(csv_path) as f:
            count = sum(1 for _ in csv.DictReader(f))
        print(f"  Found existing {csv_path}: {count} records")
        return {"records": count}

    print("  Global Fund: No pre-downloaded data.")
    print(f"  Place CSV at: {csv_path}")
    print("  Columns: country_code, donor, disease_component, amount_usd, year")
    return {"records": 0}


def main():
    parser = argparse.ArgumentParser(description="Download Global Fund data")
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()
    download_globalfund(args.data_dir)


if __name__ == "__main__":
    main()
