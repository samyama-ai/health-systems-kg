# Querying the Health Systems KG

Ways to ask the graph questions, once it's loaded into the `health-systems` tenant on a running engine
(see [GETTING_STARTED.md](../GETTING_STARTED.md)). The HTTP and MCP examples below were run live and return
real results.

> **Heads-up:** Country / capacity nodes carry vector embeddings, so `keys(n)` / `properties(n)` come back
> empty. **Explicit property access works** (`c.name`, `e.capacity_name`, `e.score`, `e.year`). The
> `EmergencyResponse.score` column also holds some year-like values — filter `e.score <= 100` for the
> true 0–100 IHR scores.

---

## 1. Claude, over MCP (natural language)

```bash
# register this KG's MCP server with Claude Code (once), pointed at the running engine:
claude mcp add health-systems -- python -m mcp_server.server --url http://localhost:8080 --tenant health-systems

# start a new Claude Code session (MCP servers load at session start), then just ask:
#   "which IHR capacity areas score highest on average?"   → C5 Surveillance, C4 Laboratory, ...
#   "how prepared is each country for health emergencies?"
```

*(No engine? `python -m mcp_server.server --data-dir data` loads a graph in-memory and serves it.)*

## 2. HTTP API (`POST /api/query`) — recommended

```bash
curl -s -X POST http://localhost:8080/api/query -H 'Content-Type: application/json' -d '{
  "graph": "health-systems",
  "query": "MATCH (e:EmergencyResponse) WHERE e.year = 2021 AND e.score <= 100 RETURN e.capacity_name AS capacity, avg(e.score) AS avg_score ORDER BY avg_score DESC LIMIT 5"
}'
```
```json
{"columns":["capacity","avg_score"],
 "records":[["C5 - Surveillance",80.60],["C4 - Laboratory",72.26],["C8 - Health services provision",71.73],
            ["C7 - Health emergency management",70.31],["C10 - Risk communication and community engagement",66.69]]}
```

## 3. Samyama CLI (Redis wire protocol, `:6379`)

The engine also speaks the Redis wire protocol:

```bash
redis-cli -p 6379 GRAPH.QUERY health-systems \
  "MATCH (e:EmergencyResponse) WHERE e.year = 2021 AND e.score <= 100 RETURN e.capacity_name, avg(e.score) AS avg ORDER BY avg DESC LIMIT 5"
```

> **Note:** on the current engine build the RESP/`GRAPH.QUERY` path returns empty results for
> `EmergencyResponse` queries on this tenant that the HTTP API answers correctly (same class of issue as
> [samyama-graph#334](https://github.com/samyama-ai/samyama-graph/issues/334)). Until that's fixed, prefer
> the **HTTP API** or **MCP** for the health-systems KG.

---

## More queries
See the MCP tools (`--list-tools`) — `emergency_preparedness_ranking`, `country_health_capacity`,
`capacity_gap_analysis`, `workforce_density`, `funding_landscape`, `vaccine_supply_status` — for
higher-level, ready-made questions.
