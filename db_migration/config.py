"""Configuration models for database migration."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import yaml


@dataclass
class DBConnection:
    """Database connection information."""
    url: str


@dataclass
class TableMapping:
    """Mapping between source and target table names."""
    source: str
    target: str


@dataclass
class MigrationConfig:
    """Full configuration for a migration run."""
    source: DBConnection
    target: DBConnection
    tables: List[TableMapping]

    @classmethod
    def from_yaml(cls, path: str | Path) -> "MigrationConfig":
        """Load configuration from a YAML file."""
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        source = DBConnection(**raw["source"])
        target = DBConnection(**raw["target"])
        tables = [TableMapping(**t) for t in raw.get("tables", [])]
        return cls(source=source, target=target, tables=tables)
