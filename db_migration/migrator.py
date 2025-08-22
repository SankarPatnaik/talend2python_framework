"""Utilities to migrate data between databases."""
from __future__ import annotations

from typing import Iterable

from sqlalchemy import MetaData, Table, create_engine, select

from .config import MigrationConfig, TableMapping


def migrate(config: MigrationConfig) -> None:
    """Run the migration for all configured tables."""
    src_engine = create_engine(config.source.url)
    dst_engine = create_engine(config.target.url)

    for mapping in config.tables:
        migrate_table(src_engine, dst_engine, mapping)


def migrate_table(src_engine, dst_engine, mapping: TableMapping) -> None:
    """Copy a single table from source to target."""
    src_meta = MetaData()
    src_table = Table(mapping.source, src_meta, autoload_with=src_engine)

    dst_meta = MetaData()
    dst_table = Table(mapping.target, dst_meta)
    for col in src_table.columns:
        dst_table.append_column(col.copy())
    dst_meta.create_all(dst_engine)

    with src_engine.connect() as src_conn, dst_engine.begin() as dst_conn:
        result = src_conn.execute(select(src_table))
        rows: Iterable[dict] = [dict(r._mapping) for r in result]
        if rows:
            dst_conn.execute(dst_table.insert(), list(rows))
