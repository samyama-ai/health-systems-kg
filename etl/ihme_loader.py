"""IHME GHDx health expenditure loader.

Creates FundingFlow nodes (with donor='IHME') linked to Country nodes.
"""

from __future__ import annotations

import csv
import os

from etl.helpers import Registry, batch_create_nodes, batch_create_edges_fast, create_index


def load_ihme(client, data_dir: str, registry: Registry, tenant: str = "default") -> dict:
    """Load IHME expenditure data. Expects: data_dir/ihme/expenditure.csv
    Columns: country_code, indicator, indicator_name, year, value
    """
    print("Health Systems KG: IHME GHDx (Health Expenditure)")
    create_index(client, "FundingFlow", "id", tenant)

    csv_path = os.path.join(data_dir, "ihme", "expenditure.csv")
    if not os.path.exists(csv_path):
        print("  No IHME data found, skipping")
        return {"source": "ihme", "funding_nodes": 0, "funded_by_edges": 0}

    node_batch, edge_batch = [], []
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            cc = row.get("country_code", "").strip()
            indicator = row.get("indicator", "").strip()
            indicator_name = row.get("indicator_name", "").strip()
            year = row.get("year", "").strip()
            value_str = row.get("value", "").strip()
            if not cc or not indicator or not year or not value_str or cc not in registry.countries:
                continue

            try:
                value = float(value_str)
            except ValueError:
                continue

            nid = f"FF-{cc}-IHME-{indicator}-{year}"
            if nid in registry.funding_flows:
                continue
            registry.funding_flows.add(nid)

            props = {"id": nid, "donor": "IHME", "indicator": indicator,
                     "indicator_name": indicator_name, "value": value,
                     "year": int(year) if year.isdigit() else year, "country_code": cc}
            node_batch.append(("FundingFlow", props))

            ek = f"{nid}|{cc}"
            if ek not in registry.funded_by:
                registry.funded_by.add(ek)
                edge_batch.append(("FundingFlow", "id", nid, "Country", "iso_code", cc, "FUNDED_BY", {}))

    nc = 0
    for i in range(0, len(node_batch), 100):
        chunk = node_batch[i:i+100]; batch_create_nodes(client, chunk, tenant); nc += len(chunk)
    ec = batch_create_edges_fast(client, edge_batch, tenant)
    print(f"  IHME: {nc} nodes, {ec} edges")
    return {"source": "ihme", "funding_nodes": nc, "funded_by_edges": ec}
