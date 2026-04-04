"""Global Fund disbursements loader.

Creates FundingFlow nodes linked to Country nodes via FUNDED_BY edges.
"""

from __future__ import annotations

import csv
import os

from etl.helpers import Registry, batch_create_nodes, batch_create_edges_fast, create_index


def load_globalfund(client, data_dir: str, registry: Registry, tenant: str = "default") -> dict:
    """Load Global Fund data. Expects: data_dir/globalfund/disbursements.csv
    Columns: country_code, donor, disease_component, amount_usd, year
    """
    print("Health Systems KG: Global Fund (Disbursements)")
    create_index(client, "FundingFlow", "id", tenant)

    csv_path = os.path.join(data_dir, "globalfund", "disbursements.csv")
    if not os.path.exists(csv_path):
        print("  No Global Fund data found, skipping")
        return {"source": "globalfund", "funding_nodes": 0, "funded_by_edges": 0}

    node_batch, edge_batch = [], []
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            cc = row.get("country_code", "").strip()
            donor = row.get("donor", "").strip()
            component = row.get("disease_component", "").strip()
            amount_str = row.get("amount_usd", "").strip()
            year = row.get("year", "").strip()
            if not cc or not component or not year or cc not in registry.countries:
                continue

            nid = f"FF-{cc}-GF-{component.replace(' ', '_').replace('/', '_')}-{year}"
            if nid in registry.funding_flows:
                continue
            registry.funding_flows.add(nid)

            props = {"id": nid, "donor": donor or "Global Fund", "disease_component": component,
                     "year": int(year) if year.isdigit() else year, "country_code": cc}
            if amount_str:
                try: props["amount_usd"] = float(amount_str)
                except ValueError: pass

            node_batch.append(("FundingFlow", props))
            ek = f"{nid}|{cc}"
            if ek not in registry.funded_by:
                registry.funded_by.add(ek)
                edge_batch.append(("FundingFlow", "id", nid, "Country", "iso_code", cc, "FUNDED_BY", {}))

    nc = 0
    for i in range(0, len(node_batch), 100):
        chunk = node_batch[i:i+100]; batch_create_nodes(client, chunk, tenant); nc += len(chunk)
    ec = batch_create_edges_fast(client, edge_batch, tenant)
    print(f"  Global Fund: {nc} nodes, {ec} edges")
    return {"source": "globalfund", "funding_nodes": nc, "funded_by_edges": ec}
