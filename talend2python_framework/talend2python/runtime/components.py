from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import zipfile
import tarfile


@dataclass
class TMysqlConnection:
    """Simple representation of a MySQL connection."""

    host: str
    user: str
    password: str
    database: str
    port: int = 3306

    def engine(self):
        """Create a SQLAlchemy engine for the connection."""
        from sqlalchemy import create_engine  # Imported lazily

        url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(url)


@dataclass
class TMysqlInput:
    """Run a query against a MySQL database and return a DataFrame."""

    connection: TMysqlConnection
    query: str

    def read(self) -> pd.DataFrame:
        engine = self.connection.engine()
        return pd.read_sql_query(self.query, con=engine)


@dataclass
class TMysqlOutput:
    """Write a DataFrame to a MySQL table."""

    connection: TMysqlConnection
    table: str
    if_exists: str = "append"

    def write(self, df: pd.DataFrame) -> None:
        engine = self.connection.engine()
        df.to_sql(self.table, con=engine, if_exists=self.if_exists, index=False)


@dataclass
class TFileInputDelimited:
    """Read a delimited text file into a DataFrame."""

    path: str
    separator: str = ","
    header: bool = True

    def read(self) -> pd.DataFrame:
        header_row = 0 if self.header else None
        return pd.read_csv(self.path, sep=self.separator, header=header_row)


@dataclass
class TFileInputExcel:
    """Read an Excel file into a DataFrame."""

    path: str
    sheet: str | int = 0
    header: bool = True

    def read(self) -> pd.DataFrame:
        header_row = 0 if self.header else None
        return pd.read_excel(self.path, sheet_name=self.sheet, header=header_row)


@dataclass
class TFileList:
    """List files matching a pattern inside a directory."""

    directory: str
    pattern: str = "*"

    def list_files(self) -> List[str]:
        base = Path(self.directory)
        return [str(p) for p in base.glob(self.pattern)]


@dataclass
class TFileArchive:
    """Create an archive (zip or tar.gz) from a set of files."""

    files: List[str]
    archive_path: str
    format: str = "zip"

    def archive(self) -> None:
        if self.format == "zip":
            with zipfile.ZipFile(self.archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in self.files:
                    zf.write(f, arcname=Path(f).name)
        elif self.format in ("tar", "gztar", "tar.gz"):
            mode = "w:gz" if "gz" in self.format else "w"
            with tarfile.open(self.archive_path, mode) as tf:
                for f in self.files:
                    tf.add(f, arcname=Path(f).name)
        else:
            raise ValueError(f"Unsupported archive format: {self.format}")


@dataclass
class TRowGenerator:
    """Generate a DataFrame of synthetic data."""

    schema: Dict[str, Callable[[], Any]]
    num_rows: int

    def generate(self) -> pd.DataFrame:
        data = {col: [fn() for _ in range(self.num_rows)] for col, fn in self.schema.items()}
        return pd.DataFrame(data)


@dataclass
class TMsgBox:
    """Display a simple message."""

    message: str

    def show(self) -> None:
        print(self.message)


@dataclass
class TLogRow:
    """Print the head of a DataFrame for debugging."""

    n: int = 10

    def log(self, df: pd.DataFrame) -> None:
        print(df.head(self.n).to_string())


@dataclass
class TPreJob:
    """Run a list of callables before the main job."""

    tasks: List[Callable[[], Any]]

    def run(self) -> None:
        for task in self.tasks:
            task()


@dataclass
class TMap:
    """Select or compute new columns in a DataFrame."""

    mapping: Dict[str, str]

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame()
        for col, expr in self.mapping.items():
            if expr in {"__keep__", "__copy__"}:
                out[col] = df[col]
            else:
                out[col] = df.eval(expr)
        return out


@dataclass
class TJoin:
    """Join two DataFrames."""

    left_on: str
    right_on: str
    how: str = "inner"

    def join(self, left_df: pd.DataFrame, right_df: pd.DataFrame) -> pd.DataFrame:
        return left_df.merge(right_df, left_on=self.left_on, right_on=self.right_on, how=self.how)


@dataclass
class TJava:
    """Execute a snippet of Python code (analogous to Talend's tJava)."""

    code: str

    def run(self, context: Optional[Dict[str, Any]] = None) -> None:
        exec(self.code, {}, context if context is not None else {})


@dataclass
class TRunJob:
    """Sequentially execute another job represented by a callable."""

    job: Callable[..., Any]
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)

    def run(self) -> Any:
        return self.job(*self.args, **self.kwargs)

