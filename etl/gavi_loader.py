"""GAVI vaccine supply chain loader.

Creates SupplyChain nodes linked to Country nodes via SUPPLIES edges.
"""

from __future__ import annotations

import csv
import os

from etl.helpers import Registry, batch_create_nodes, batch_create_edges_fast, create_index


def load_gavi(client, data_dir: str, registry: Registry, tenant: str = "default") -> dict:
    """Load GAVI supply data. Expects: data_dir/gavi/supply.csv
    Columns: country_code, vaccine_name, doses_shipped, doses_used, wastage_pct, year
    """
    print("Health Systems KG: GAVI (Vaccine Supply)")
    create_index(client, "SupplyChain", "id", tenant)

    csv_path = os.path.join(data_dir, "gavi", "supply.csv")
    if not os.path.exists(csv_path):
        print("  No GAVI data found, skipping")
        return {"source": "gavi", "supply_chain_nodes": 0, "supplies_edges": 0}

    node_batch, edge_batch = [], []
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            cc = row.get("country_code", "").strip()
            vaccine = row.get("vaccine_name", "").strip()
            year = row.get("year", "").strip()
            if not cc or not vaccine or not year or cc not in registry.countries:
                continue

            nid = f"SC-{cc}-{vaccine.replace(' ', '_')}-{year}"
            if nid in registry.supply_chain:
                continue
            registry.supply_chain.add(nid)

            props = {"id": nid, "vaccine_name": vaccine, "year": int(year) if year.isdigit() else year, "country_code": cc}
            for field, key in [("doses_shipped", "doses_shipped"), ("doses_used", "doses_used"), ("wastage_pct", "wastage_pct")]:
                val = row.get(field, "").strip()
                if val:
                    try: props[key] = float(val)
                    except ValueError: pass

            node_batch.append(("SupplyChain", props))
            ek = f"{nid}|{cc}"
            if ek not in registry.supplies:
                registry.supplies.add(ek)
                edge_batch.append(("SupplyChain", "id", nid, "Country", "iso_code", cc, "SUPPLIES", {}))

    nc = 0
    for i in range(0, len(node_batch), 100):
        chunk = node_batch[i:i+100]; batch_create_nodes(client, chunk, tenant); nc += len(chunk)
    ec = batch_create_edges_fast(client, edge_batch, tenant)
    print(f"  Supply chain: {nc} nodes, {ec} edges")
    return {"source": "gavi", "supply_chain_nodes": nc, "supplies_edges": ec}
