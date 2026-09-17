# Data licences

`LICENSE` in this repository covers the **loader code**. It says nothing about the
upstream data this repository reads and, where a snapshot is published, redistributes.
That gap is what this file closes (samyama-cloud#97).

Each row records what the source's **own terms page** says, with the URL and the date it
was read. Where a source could not be re-verified it says so rather than guessing: an
unverified licence written down as fact is worse than the silence it replaces.

| Source | What we load | What its terms page says | Checked |
|---|---|---|---|
| [WHO SPAR (IHR core capacities)](https://www.who.int/about/policies/publishing/copyright) | Country capacity scores | **CC BY-NC-SA 3.0 IGO.** Copy, adapt and redistribute for **non-commercial** purposes, crediting WHO, with adaptations under the same terms. WHO's logo needs written permission and the data may not be used to promote a product or organisation. | 2026-09-18 |
| [WHO NHWA (health workforce)](https://www.who.int/about/policies/publishing/copyright) | Workforce indicators | **CC BY-NC-SA 3.0 IGO.** Copy, adapt and redistribute for **non-commercial** purposes, crediting WHO, with adaptations under the same terms. WHO's logo needs written permission and the data may not be used to promote a product or organisation. | 2026-09-18 |
| [Gavi](https://www.gavi.org/) | Immunisation programme data | **Not re-verified.** Gavi publishes country data without a single stated open licence; confirm before redistributing. | not checked |
| [The Global Fund](https://data.theglobalfund.org/) | Grant and disbursement data | **Not re-verified.** The Global Fund's data service states its own terms; confirm before redistributing. | not checked |
| [IHME](https://www.healthdata.org/About/terms-and-conditions) | Burden-of-disease estimates | **Not re-verified.** IHME data is released under its own Free-of-Charge Non-commercial User Agreement, which restricts redistribution; this is the row most likely to block a published snapshot. | not checked |

**Non-commercial at best, and three rows unverified.** WHO's two sources alone make the
graph CC BY-NC-SA 3.0 IGO. IHME's agreement is more restrictive still and may not permit
redistribution at all.

**Ship the loader.** A published snapshot needs the Gavi, Global Fund and IHME rows settled
first, and even then would be non-commercial and share-alike.

## How to read the "derived graph" line

A graph built from several sources carries **all** of their terms at once. The
restrictive ones win: one non-commercial source makes the join non-commercial, one
share-alike source makes the join share-alike. That is why the derived licence below is
not simply the most permissive source in the table.

## If you redistribute

- Keep the attributions named above with the data.
- State which snapshot version you took, so a reader can check it against the source.
- Re-read the terms pages: licences change, and the dates in this table are when we last
  looked.
