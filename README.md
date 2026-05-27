# Health Systems Knowledge Graph

Health systems knowledge graph — WHO SPAR, NHWA, GAVI, Global Fund, IHME on Samyama.

> Part of the **Samyama** ecosystem — loaded into and queried via the graph engine at [samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph).
> This repo holds the loader and source-data specifics for the KG; `etl/` has the ingest scripts, `schema/` the node/edge shapes, `mcp_server/` the MCP exposure.

![Health-systems capacity-to-respond demo](demo/health-systems.gif)

## Demo

A narrated walkthrough (load WHO SPAR + NHWA → least-prepared countries → thinnest
physician workforce → cross the two for highest-risk countries):

```bash
python -m demo.demo                                              # run live
asciinema rec -c "python -m demo.demo" demo/health-systems.cast  # re-record
```

This README is a stub; see `docs/` for the full data-source references and ingest pipeline.
