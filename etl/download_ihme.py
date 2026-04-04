"""Download IHME GHDx health expenditure data.

Usage: python -m etl.download_ihme --data-dir data
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def download_ihme(data_dir: str | Path) -> dict:
    out_path = Path(data_dir) / "ihme"
    out_path.mkdir(parents=True, exist_ok=True)
    csv_path = out_path / "expenditure.csv"

    if csv_path.exists():
        with open(csv_path) as f:
            count = sum(1 for _ in csv.DictReader(f))
        print(f"  Found existing {csv_path}: {count} records")
        return {"records": count}

    print("  IHME: No pre-downloaded data.")
    print(f"  Place CSV at: {csv_path}")
    print("  Columns: country_code, indicator, indicator_name, year, value")
    return {"records": 0}


def main():
    parser = argparse.ArgumentParser(description="Download IHME expenditure data")
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()
    download_ihme(args.data_dir)


if __name__ == "__main__":
    main()
