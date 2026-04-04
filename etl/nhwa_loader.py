"""WHO NHWA (National Health Workforce Accounts) loader.

Creates HealthWorkforce nodes linked to Country nodes via SERVES edges.
"""

from __future__ import annotations

import csv
import os

from etl.helpers import Registry, batch_create_nodes, batch_create_edges_fast, create_index


def load_nhwa(client, data_dir: str, registry: Registry, tenant: str = "default") -> dict:
    """Load WHO NHWA data. Expects: data_dir/who_nhwa/nhwa.csv
    Columns: country_code, profession, count, density_per_10k, year
    """
    print("Health Systems KG: WHO NHWA (Health Workforce)")
    create_index(client, "HealthWorkforce", "id", tenant)

    csv_path = os.path.join(data_dir, "who_nhwa", "nhwa.csv")
    if not os.path.exists(csv_path):
        print("  No NHWA data found, skipping")
        return {"source": "who_nhwa", "workforce_nodes": 0, "serves_edges": 0}

    node_batch, edge_batch = [], []
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            cc = row.get("country_code", "").strip()
            prof = row.get("profession", "").strip()
            year = row.get("year", "").strip()
            count_str = row.get("count", "").strip()
            density_str = row.get("density_per_10k", "").strip()
            if not cc or not prof or not year or cc not in registry.countries:
                continue

            nid = f"HW-{cc}-{prof}-{year}"
            if nid in registry.health_workforce:
                continue
            registry.health_workforce.add(nid)

            props = {"id": nid, "profession": prof, "year": int(year) if year.isdigit() else year, "country_code": cc}
            if count_str:
                try: props["count"] = int(float(count_str))
                except ValueError: pass
            if density_str:
                try: props["density_per_10k"] = float(density_str)
                except ValueError: pass

            node_batch.append(("HealthWorkforce", props))
            ek = f"{nid}|{cc}"
            if ek not in registry.serves:
                registry.serves.add(ek)
                edge_batch.append(("HealthWorkforce", "id", nid, "Country", "iso_code", cc, "SERVES", {}))

    nc = 0
    for i in range(0, len(node_batch), 100):
        chunk = node_batch[i:i+100]; batch_create_nodes(client, chunk, tenant); nc += len(chunk)
    ec = batch_create_edges_fast(client, edge_batch, tenant)
    print(f"  Workforce: {nc} nodes, {ec} edges")
    return {"source": "who_nhwa", "workforce_nodes": nc, "serves_edges": ec}
