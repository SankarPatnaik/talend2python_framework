"""Command line interface for running migrations."""
from __future__ import annotations

import argparse

from .config import MigrationConfig
from .migrator import migrate


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Migrate data from MSSQL to ORASS")
    parser.add_argument("config", help="Path to YAML migration configuration")
    args = parser.parse_args(argv)

    cfg = MigrationConfig.from_yaml(args.config)
    migrate(cfg)


if __name__ == "__main__":  # pragma: no cover - manual invocation
    main()
