from sqlalchemy import create_engine, text

from db_migration.config import MigrationConfig
from db_migration.migrator import migrate


def test_sqlite_migration(tmp_path):
    src_path = tmp_path / "src.db"
    dst_path = tmp_path / "dst.db"

    src_engine = create_engine(f"sqlite:///{src_path}")
    dst_engine = create_engine(f"sqlite:///{dst_path}")

    with src_engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(text("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')"))

    config = f"""
source:
  url: sqlite:///{src_path}

target:
  url: sqlite:///{dst_path}

tables:
  - source: users
    target: users_copy
"""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(config)

    cfg = MigrationConfig.from_yaml(cfg_file)
    migrate(cfg)

    with dst_engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM users_copy ORDER BY id"))
        rows = [tuple(r) for r in rows]

    assert rows == [(1, "Alice"), (2, "Bob")]
