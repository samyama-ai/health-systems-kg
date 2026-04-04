"""WHO SPAR (State Party Annual Report) loader.

Creates Country nodes and EmergencyResponse nodes for IHR capacity scores.
This is Phase 1 — creates Country nodes used by all subsequent phases.

Usage:
    from etl.spar_loader import load_spar
    load_spar(client, data_dir, registry)
"""

from __future__ import annotations

import csv
import json
import os

from etl.helpers import (
    Registry,
    batch_create_nodes,
    batch_create_edges_fast,
    create_index,
)


def load_spar(
    client,
    data_dir: str,
    registry: Registry,
    tenant: str = "default",
) -> dict:
    """Load WHO SPAR data into the graph.

    Expects:
      data_dir/who_spar/countries.json — country list
      data_dir/who_spar/spar.csv — IHR capacity scores
    Columns: country_code, capacity_code, capacity_name, year, score
    """
    print("Health Systems KG: WHO SPAR (IHR Capacities)")

    create_index(client, "Country", "iso_code", tenant)
    create_index(client, "Country", "name", tenant)
    create_index(client, "EmergencyResponse", "id", tenant)

    country_nodes = 0

    # ── Countries ──
    countries_path = os.path.join(data_dir, "who_spar", "countries.json")
    if os.path.exists(countries_path):
        with open(countries_path) as f:
            countries = json.load(f)
        batch = []
        for c in countries:
            code = c.get("iso_code", "").strip()
            name = c.get("name", "").strip()
            if not code or not name or code in registry.countries:
                continue
            registry.countries.add(code)
            props = {
                "iso_code": code,
                "name": name,
                "who_region": c.get("who_region", ""),
                "income_level": c.get("income_level", ""),
            }
            batch.append(("Country", props))
            if len(batch) >= 50:
                batch_create_nodes(client, batch, tenant)
                country_nodes += len(batch)
                batch = []
        if batch:
            batch_create_nodes(client, batch, tenant)
            country_nodes += len(batch)
        print(f"  Countries: {country_nodes}")

    # ── Emergency Response (SPAR capacities) ──
    csv_path = os.path.join(data_dir, "who_spar", "spar.csv")
    if not os.path.exists(csv_path):
        print("  No SPAR data found, skipping")
        return {"source": "who_spar", "country_nodes": country_nodes,
                "emergency_response_nodes": 0, "capacity_for_edges": 0}

    node_batch = []
    edge_batch = []

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country_code = row.get("country_code", "").strip()
            capacity_code = row.get("capacity_code", "").strip()
            capacity_name = row.get("capacity_name", "").strip()
            year = row.get("year", "").strip()
            score_str = row.get("score", "").strip()

            if not country_code or not capacity_code or not year or not score_str:
                continue
            if country_code not in registry.countries:
                continue

            try:
                score = int(float(score_str))
            except ValueError:
                continue

            nid = f"ER-{country_code}-{capacity_code}-{year}"
            if nid in registry.emergency_responses:
                continue
            registry.emergency_responses.add(nid)

            node_batch.append(("EmergencyResponse", {
                "id": nid,
                "capacity_code": capacity_code,
                "capacity_name": capacity_name,
                "year": int(year) if year.isdigit() else year,
                "score": score,
                "country_code": country_code,
            }))

            edge_key = f"{nid}|{country_code}"
            if edge_key not in registry.capacity_for:
                registry.capacity_for.add(edge_key)
                edge_batch.append((
                    "EmergencyResponse", "id", nid,
                    "Country", "iso_code", country_code,
                    "CAPACITY_FOR", {},
                ))

    node_count = 0
    for i in range(0, len(node_batch), 100):
        chunk = node_batch[i:i + 100]
        batch_create_nodes(client, chunk, tenant)
        node_count += len(chunk)

    edge_count = batch_create_edges_fast(client, edge_batch, tenant)
    print(f"  Emergency response: {node_count} nodes, {edge_count} edges")

    return {
        "source": "who_spar",
        "country_nodes": country_nodes,
        "emergency_response_nodes": node_count,
        "capacity_for_edges": edge_count,
    }
