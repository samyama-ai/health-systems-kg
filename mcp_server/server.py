"""Health Systems KG — MCP Server."""

from __future__ import annotations

import argparse
import os


def main(argv=None):
    parser = argparse.ArgumentParser(description="Health Systems KG MCP Server")
    parser.add_argument("--url", help="Remote Samyama server URL")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--phases", nargs="*", default=None)
    parser.add_argument("--tenant", default="default")
    args = parser.parse_args(argv)

    from samyama import SamyamaClient
    if args.url:
        client = SamyamaClient.connect(args.url)
    else:
        client = SamyamaClient.embedded()
        from etl.loader import load_health_systems
        load_health_systems(client, args.data_dir, args.phases, args.tenant)

    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    try:
        from samyama_mcp import SamyamaMCPServer, ToolConfig
        config = ToolConfig.from_yaml(config_path)
        server = SamyamaMCPServer(client, config=config, tenant=args.tenant)
        server.run()
    except ImportError:
        print("samyama-mcp-serve not available, falling back to REPL...")
        while True:
            try:
                cypher = input("cypher> ").strip()
                if not cypher or cypher.lower() in ("exit", "quit"):
                    break
                result = client.query(cypher, args.tenant)
                for row in result.records:
                    print(dict(zip(result.columns, row)))
            except (EOFError, KeyboardInterrupt):
                break


if __name__ == "__main__":
    main()
