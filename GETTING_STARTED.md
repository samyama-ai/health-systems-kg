# Getting Started — Health Systems Knowledge Graph

From `git clone` to your first answer. The **snapshot path** is the fastest (a few minutes).

---

## 1. Prerequisites

- **Python ≥ 3.10** (required by the `samyama` SDK; macOS ships 3.9 — use `python3.10`+).
- **git**
- **Docker** — to run the Samyama engine (needed for the snapshot import and for serving MCP / CLI / API).

## 2. Install

```bash
git clone https://github.com/samyama-ai/health-systems-kg.git
cd health-systems-kg
python3 -m venv .venv && source .venv/bin/activate     # Python >= 3.10
pip install -r requirements.txt
```

## 3. Run the engine (Docker)

```bash
docker run --rm -p 8080:8080 -p 6379:6379 public.ecr.aws/f9f6l5u4/samyama-graph:1.1.0
```

## 4. Load the graph — into the `health-systems` tenant

### Option A — snapshot (recommended, ~seconds)
```bash
curl -LO https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v6/health-systems.sgsnap
curl -X POST http://localhost:8080/api/tenants -H 'Content-Type: application/json' \
  -d '{"id":"health-systems","name":"Health Systems KG"}'
curl -X POST http://localhost:8080/api/tenants/health-systems/snapshot/import -F "file=@health-systems.sgsnap"
```

### Option B — build from source (downloads WHO SPAR/NHWA, Gavi, Global Fund, IHME)
```bash
python -m etl.download_who_spar --data-dir data
python -m etl.download_who_nhwa --data-dir data
python -m etl.download_gavi --data-dir data
python -m etl.download_globalfund --data-dir data
python -m etl.download_ihme --data-dir data
python -m etl.loader --data-dir data --url http://localhost:8080     # all phases → health-systems tenant
```
*(The loader defaults to the `health-systems` tenant; override with `--tenant`. Omit `--url` to build
an in-memory graph instead. Load a subset with `--phases spar nhwa`.)*

## 5. Ask your first question

Fastest is **Claude over MCP** — see **[docs/QUERYING.md](docs/QUERYING.md)**. Quick check over HTTP —
average IHR preparedness score by capacity area (2021):

```bash
curl -s -X POST http://localhost:8080/api/query -H 'Content-Type: application/json' -d '{
  "graph": "health-systems",
  "query": "MATCH (e:EmergencyResponse) WHERE e.year = 2021 AND e.score <= 100 RETURN e.capacity_name AS capacity, avg(e.score) AS avg_score ORDER BY avg_score DESC LIMIT 5"
}'
# → C5 Surveillance (80.6), C4 Laboratory (72.3), C8 Health services provision (71.7), ...
```

> **Note:** Country / capacity nodes carry vector embeddings, so `keys(n)` / `properties(n)` come back
> empty — but **explicit property access works** (`c.name`, `e.capacity_name`, `e.score`, `e.year`).
> The `score` column also carries some year-like values; filter `e.score <= 100` for true 0–100 scores.

## 6. The ETL pipeline

- Data sources: **WHO SPAR, WHO NHWA, Gavi, The Global Fund, IHME**.
- `etl/download_*.py` — one downloader per source.
- `etl/loader.py` — orchestrates the phases (`spar`, `nhwa`, `gavi`, `globalfund`, `ihme`) into the graph
  (Country, EmergencyResponse, …). Run `python -m etl.loader --help`.

## Next
- **[docs/QUERYING.md](docs/QUERYING.md)** — MCP (Claude), HTTP API, and the Samyama CLI
