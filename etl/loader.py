"""Health Systems KG — Main orchestrator.

Loads all phases (spar, nhwa, gavi, globalfund, ihme) sequentially,
sharing a single Registry for cross-phase deduplication.

Usage:
    python -m etl.loader --data-dir data
    python -m etl.loader --data-dir data --phases spar nhwa
    python -m etl.loader --url http://localhost:8080 --data-dir data
"""

from __future__ import annotations

import argparse
import time

from etl.helpers import Registry

ALL_PHASES = ["spar", "nhwa", "gavi", "globalfund", "ihme"]


def _run_phase(phase: str, client, data_dir: str, registry: Registry, *, tenant: str = "default") -> dict:
    if phase == "spar":
        from etl.spar_loader import load_spar
        return load_spar(client, data_dir, registry, tenant)
    elif phase == "nhwa":
        from etl.nhwa_loader import load_nhwa
        return load_nhwa(client, data_dir, registry, tenant)
    elif phase == "gavi":
        from etl.gavi_loader import load_gavi
        return load_gavi(client, data_dir, registry, tenant)
    elif phase == "globalfund":
        from etl.globalfund_loader import load_globalfund
        return load_globalfund(client, data_dir, registry, tenant)
    elif phase == "ihme":
        from etl.ihme_loader import load_ihme
        return load_ihme(client, data_dir, registry, tenant)
    else:
        raise ValueError(f"Unknown phase: {phase}")


def load_health_systems(
    client,
    data_dir: str = "data",
    phases: list[str] | None = None,
    tenant: str = "default",
) -> dict:
    if phases is None:
        phases = ALL_PHASES

    print(f"\n{'='*60}")
    print("Health Systems Knowledge Graph")
    print(f"Phases: {', '.join(phases)}")
    print(f"{'='*60}\n")

    t0 = time.time()
    registry = Registry()
    all_stats = []

    for phase in phases:
        if phase not in ALL_PHASES:
            print(f"  [WARN] Unknown phase '{phase}', skipping")
            continue
        phase_t0 = time.time()
        stats = _run_phase(phase, client, data_dir, registry, tenant=tenant)
        stats["phase_elapsed_s"] = round(time.time() - phase_t0, 1)
        all_stats.append(stats)
        print()

    elapsed = time.time() - t0

    total_nodes = sum(v for s in all_stats for k, v in s.items() if k.endswith("_nodes") and isinstance(v, int))
    total_edges = sum(v for s in all_stats for k, v in s.items() if k.endswith("_edges") and isinstance(v, int))

    print(f"{'='*60}")
    print(f"DONE: {total_nodes} nodes, {total_edges} edges in {elapsed:.1f}s")
    print(f"Registry: {len(registry.countries)} countries")
    print(f"{'='*60}\n")

    return {
        "phases": [s.get("source", "") for s in all_stats],
        "total_nodes": total_nodes,
        "total_edges": total_edges,
        "elapsed_s": round(elapsed, 1),
        "phase_stats": all_stats,
    }


def main():
    parser = argparse.ArgumentParser(description="Load Health Systems KG")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--phases", nargs="*", default=None, help=f"Choices: {ALL_PHASES}")
    parser.add_argument("--url", default=None, help="Remote Samyama server URL")
    parser.add_argument("--tenant", default="default")
    args = parser.parse_args()

    from samyama import SamyamaClient
    client = SamyamaClient.connect(args.url) if args.url else SamyamaClient.embedded()
    load_health_systems(client, args.data_dir, args.phases, args.tenant)


if __name__ == "__main__":
    main()
