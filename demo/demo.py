"""Narrated terminal demo: Health-systems capacity-to-respond on Samyama.

Record with asciinema:
    asciinema rec -c "python -m demo.demo" demo/health-systems.cast

Loads WHO SPAR (IHR core-capacity scores) + WHO NHWA (health-workforce
densities) into a Samyama graph and walks through the question every
preparedness team asks: "what capacity exists to respond?"
"""

from __future__ import annotations

import time

from rich.console import Console
from rich.panel import Panel
from samyama import SamyamaClient

from etl.loader import load_health_systems

console = Console()
G = "default"


def pause(s: float = 1.4) -> None:
    time.sleep(s)


def step(title: str) -> None:
    console.print()
    console.rule(f"[bold cyan]{title}")
    pause(0.6)


def run(client, q, label):
    console.print(f"  [dim]cypher>[/dim] [yellow]{q}[/yellow]")
    rows = client.query(q, G).records
    one = len(rows) == 1 and len(rows[0]) == 1
    console.print(f"  [green]→[/green] {label}: [bold]{rows[0][0] if one else rows}[/bold]")
    pause()
    return rows


def main() -> None:
    console.print(Panel.fit(
        "[bold]Samyama · Health-Systems Knowledge Graph[/bold]\n"
        "\"What capacity exists to respond?\" — IHR preparedness + health workforce\n"
        "[dim]data: WHO SPAR + WHO NHWA · public indicators[/dim]",
        border_style="cyan",
    ))
    pause(1.2)

    step("1 · Load WHO SPAR + NHWA into Samyama")
    stats = load_health_systems(client := SamyamaClient.embedded(), "data",
                                phases=["spar", "nhwa"])
    console.print(f"  [green]loaded[/green] {stats['total_nodes']} nodes, "
                  f"{stats['total_edges']} edges")
    run(client, "MATCH (c:Country) RETURN count(c) AS countries", "countries covered")
    run(client, "MATCH (e:EmergencyResponse) "
                "RETURN count(DISTINCT e.capacity_name) AS caps",
        "IHR core capacities tracked")

    step("2 · Which countries are least prepared for a health emergency?")
    run(
        client,
        "MATCH (e:EmergencyResponse)-[:CAPACITY_FOR]->(c:Country) "
        "RETURN c.name AS country, round(avg(e.score)) AS spar "
        "ORDER BY spar ASC LIMIT 5",
        "lowest mean SPAR score",
    )

    step("3 · Where is the physician workforce thinnest?")
    run(
        client,
        "MATCH (w:HealthWorkforce {profession: \"physicians\"})-[:SERVES]->(c:Country) "
        "RETURN c.name AS country, min(w.density_per_10k) AS docs_per_10k "
        "ORDER BY docs_per_10k ASC LIMIT 5",
        "fewest doctors per 10k people",
    )

    step("4 · Cross the two: under-prepared AND under-staffed")
    console.print("  [dim]join SPAR preparedness with NHWA physician density per country…[/dim]")
    pause()
    run(
        client,
        "MATCH (e:EmergencyResponse)-[:CAPACITY_FOR]->(c:Country) "
        "WITH c, avg(e.score) AS spar "
        "MATCH (w:HealthWorkforce {profession: \"physicians\"})-[:SERVES]->(c) "
        "WITH c, spar, min(w.density_per_10k) AS docs "
        "WHERE spar < 50.0 AND docs < 5.0 "
        "RETURN c.name AS country, round(spar) AS spar, docs "
        "ORDER BY spar ASC LIMIT 5",
        "highest-risk: weak IHR capacity + scarce physicians",
    )

    console.print()
    console.print(Panel.fit(
        "[bold green]One Country.iso_code joins this to surveillance-kg & "
        "health-determinants-kg[/bold green] — outbreak signal, social drivers,\n"
        "and response capacity answered on a single engine, one Cypher query.",
        border_style="green",
    ))
    pause(1.5)


if __name__ == "__main__":
    main()
