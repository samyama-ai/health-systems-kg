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


## Reproducing

The loader in this repository rebuilds the graph from the upstream source. See the
README's Quick Start for the snapshot download and the from-source build.

## Known limitations

- Counts here are those stated by the repository README at the time this card was
  written; they are not re-measured by the card.
- Where a field above says *not recorded*, that is a gap in this repository rather
  than a property of the data.
