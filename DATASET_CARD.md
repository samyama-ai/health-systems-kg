---
license: other
pretty_name: Health Systems Knowledge Graph
tags:
  - knowledge-graph
  - samyama
  - property-graph
  - health
language:
  - en
---

# Dataset Card for `health-systems-kg`

**Health systems knowledge graph — WHO SPAR, NHWA, GAVI, Global Fund, IHME on Samyama.**

> Part of the **Samyama** ecosystem. This card describes the dataset; the repository
> holds the loader and source-data specifics.

## Structure

_Not recorded in this repository's README._ Node labels, edge types and per-label
counts should be added here — a dataset card without them cannot be used to decide
whether the data fits a question.

## Provenance and licence

Apache 2.0 covers the loader. WHO SPAR and NHWA are **CC BY-NC-SA 3.0 IGO**; IHME's own
agreement is more restrictive and may not permit redistribution at all. Gavi and Global
Fund terms are unverified. **Ship the loader, not the graph.** See
[`DATA-LICENSES.md`](DATA-LICENSES.md).


## Freshness

**Refresh cadence:** No single cadence — this graph joins five sources with different
update rhythms. WHO SPAR is a country self-assessment submitted to WHO under the IHR
annually; WHO NHWA workforce indicators are also updated on roughly an annual cycle.
Gavi and Global Fund publish programme/grant data on their own reporting schedules
(not fixed here — see `DATA-LICENSES.md`, both rows "not re-verified"). IHME's GBD
burden-of-disease estimates ship in major study rounds roughly annually to
biennially, not continuously. In short: annually for the two WHO sources, unknown
for Gavi/Global Fund, and roughly annual/biennial for IHME.

**Data as of:** Unknown for the live data itself. This repository ships the loader,
not a vendored graph: `data/who_spar/*.csv`, `data/who_nhwa/*.csv`, `data/gavi/*.csv`,
`data/globalfund/*.csv` and `data/ihme/*.csv` are all `.gitignore`d (fetched by
`etl/download_*.py` at run time), so the graph's real age is whenever a user last ran
the loader against the live sources — there is no record of that in this repository.
The `etl/download_*.py` and `etl/*_loader.py` scripts themselves were last edited
2026-04-04 (initial ETL commit, `7cca45b`) and have not been touched since (a
2026-07-31 commit, `e4fea45`, changed the MCP/loader tenant default, not the
fetch/parse logic). Separately, the upstream licence terms pages for WHO SPAR and
WHO NHWA were last read 2026-09-18 (see `DATA-LICENSES.md`) — that is a licence
check, not a data fetch, and does not date the data itself.

## Reproducing

The loader in this repository rebuilds the graph from the upstream source. See the
README's Quick Start for the snapshot download and the from-source build.

## Citation

Please cite this repository if you use it. See [`CITATION.cff`](CITATION.cff) for
machine-readable metadata (CFF 1.2.0).

```bibtex
@misc{health_systems_kg_2026,
  title        = {Health Systems Knowledge Graph},
  author       = {Samyama},
  year         = {2026},
  howpublished = {\url{https://git.samyama.ai/Samyama.ai/health-systems-kg}}
}
```

**No DOI.** This release has not been deposited to Zenodo, so there is no DOI to
cite. Getting one is open work — it requires a human to make the Zenodo deposit
(KG-06).

## Known limitations

- Counts here are those stated by the repository README at the time this card was
  written; they are not re-measured by the card.
- Where a field above says *not recorded*, that is a gap in this repository rather
  than a property of the data.
