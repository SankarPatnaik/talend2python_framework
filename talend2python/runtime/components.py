from __future__ import annotations

from dataclasses import dataclass, field, make_dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import zipfile
import tarfile
import time
import yaml


def register_components_from_yaml() -> None:
    """Dynamically create simple dataclasses for components defined in YAML.

    ``mapping/component_map.yaml`` enumerates supported components along with
    the configuration keys they expect.  To reduce manual boilerplate when new
    components are added, this function reads the mapping file and generates a
    minimal dataclass for any component not already present in this module.  A
    placeholder ``run`` method is provided so instances can be created without
    immediately implementing behaviour.
    """

    mapping_path = Path(__file__).resolve().parent.parent / "mapping" / "component_map.yaml"
    try:
        data = yaml.safe_load(mapping_path.read_text())
    except FileNotFoundError:
        return

    for comp_name, info in data.items():
        if comp_name in globals():
            continue
        params = info.get("params", {})
        fields = [(v, Optional[Any], field(default=None)) for v in params.values()]

        def run(self, *args, **kwargs):  # pragma: no cover - dynamic placeholder
            raise NotImplementedError(f"No runtime behaviour defined for {comp_name}")

        cls = make_dataclass(comp_name, fields, namespace={"run": run})
        globals()[comp_name] = cls


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




@dataclass
class TFileOutputDelimited:
    """Write a DataFrame to a delimited text file."""

    path: str
    separator: str = ","
    header: bool = True

    def write(self, df: pd.DataFrame) -> None:
        df.to_csv(self.path, sep=self.separator, index=False, header=self.header)


@dataclass
class TFileOutputExcel:
    """Write a DataFrame to an Excel file."""

    path: str
    sheet: str = "Sheet1"
    header: bool = True

    def write(self, df: pd.DataFrame) -> None:
        df.to_excel(self.path, sheet_name=self.sheet, index=False, header=self.header)


@dataclass
class TOracleConnection:
    """Simple representation of an Oracle connection."""

    host: str
    user: str
    password: str
    database: str
    port: int = 1521

    def engine(self):
        from sqlalchemy import create_engine
        url = (
            f"oracle+cx_oracle://{self.user}:{self.password}@{self.host}:{self.port}/"            f"?service_name={self.database}"
        )
        return create_engine(url)


@dataclass
class TOracleInput:
    """Run a query against an Oracle database and return a DataFrame."""

    connection: TOracleConnection
    query: str

    def read(self) -> pd.DataFrame:
        engine = self.connection.engine()
        return pd.read_sql_query(self.query, con=engine)


@dataclass
class TOracleOutput:
    """Write a DataFrame to an Oracle table."""

    connection: TOracleConnection
    table: str
    if_exists: str = "append"

    def write(self, df: pd.DataFrame) -> None:
        engine = self.connection.engine()
        df.to_sql(self.table, con=engine, if_exists=self.if_exists, index=False)


@dataclass
class TPostgresqlConnection:
    """Simple representation of a PostgreSQL connection."""

    host: str
    user: str
    password: str
    database: str
    port: int = 5432

    def engine(self):
        from sqlalchemy import create_engine
        url = (
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/"            f"{self.database}"
        )
        return create_engine(url)


@dataclass
class TPostgresqlInput:
    """Run a query against a PostgreSQL database and return a DataFrame."""

    connection: TPostgresqlConnection
    query: str

    def read(self) -> pd.DataFrame:
        engine = self.connection.engine()
        return pd.read_sql_query(self.query, con=engine)


@dataclass
class TPostgresqlOutput:
    """Write a DataFrame to a PostgreSQL table."""

    connection: TPostgresqlConnection
    table: str
    if_exists: str = "append"

    def write(self, df: pd.DataFrame) -> None:
        engine = self.connection.engine()
        df.to_sql(self.table, con=engine, if_exists=self.if_exists, index=False)


@dataclass
class TMSSqlConnection:
    """Simple representation of a Microsoft SQL Server connection."""

    host: str
    user: str
    password: str
    database: str
    port: int = 1433

    def engine(self):
        from sqlalchemy import create_engine
        url = (
            f"mssql+pyodbc://{self.user}:{self.password}@{self.host}:{self.port}/"            f"{self.database}?driver=ODBC+Driver+17+for+SQL+Server"
        )
        return create_engine(url)


@dataclass
class TMSSqlInput:
    """Run a query against an MSSQL database and return a DataFrame."""

    connection: TMSSqlConnection
    query: str

    def read(self) -> pd.DataFrame:
        engine = self.connection.engine()
        return pd.read_sql_query(self.query, con=engine)


@dataclass
class TMSSqlOutput:
    """Write a DataFrame to an MSSQL table."""

    connection: TMSSqlConnection
    table: str
    if_exists: str = "append"

    def write(self, df: pd.DataFrame) -> None:
        engine = self.connection.engine()
        df.to_sql(self.table, con=engine, if_exists=self.if_exists, index=False)


@dataclass
class TDB2Connection:
    """Simple representation of a DB2 connection."""

    host: str
    user: str
    password: str
    database: str
    port: int = 50000

    def engine(self):
        from sqlalchemy import create_engine
        url = (
            f"ibm_db_sa://{self.user}:{self.password}@{self.host}:{self.port}/"            f"{self.database}"
        )
        return create_engine(url)


@dataclass
class TDB2Input:
    """Run a query against a DB2 database and return a DataFrame."""

    connection: TDB2Connection
    query: str

    def read(self) -> pd.DataFrame:
        engine = self.connection.engine()
        return pd.read_sql_query(self.query, con=engine)


@dataclass
class TDB2Output:
    """Write a DataFrame to a DB2 table."""

    connection: TDB2Connection
    table: str
    if_exists: str = "append"

    def write(self, df: pd.DataFrame) -> None:
        engine = self.connection.engine()
        df.to_sql(self.table, con=engine, if_exists=self.if_exists, index=False)


@dataclass
class TExtractDelimitedFields:
    """Extract fields from a delimited string column."""

    column: str
    separator: str
    new_columns: List[str]

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        parts = df[self.column].str.split(self.separator, expand=True)
        for i, col in enumerate(self.new_columns):
            df[col] = parts[i]
        return df


@dataclass
class TAggregateRow:
    """Aggregate data based on specified columns."""

    group_by: List[str]
    aggregations: Dict[str, Any]

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.groupby(self.group_by).agg(self.aggregations).reset_index()


@dataclass
class TFlowMeter:
    """Monitor data flow rate and volume."""

    count: int = 0
    start_time: float = field(default_factory=time.time)

    def measure(self, df: pd.DataFrame) -> pd.DataFrame:
        self.count += len(df)
        return df

    def stats(self) -> Dict[str, float]:
        elapsed = time.time() - self.start_time
        rate = self.count / elapsed if elapsed > 0 else 0.0
        return {"rows": self.count, "elapsed": elapsed, "rows_per_sec": rate}


@dataclass
class TPostJob:
    """Run a list of callables after the main job."""

    tasks: List[Callable[[], Any]]

    def run(self) -> None:
        for task in self.tasks:
            task()


@dataclass
class TStatCatcher:
    """Collect job execution statistics."""

    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    rows: int = 0

    def add_rows(self, n: int) -> None:
        self.rows += n

    def stop(self) -> None:
        self.end_time = time.time()

    def stats(self) -> Dict[str, Any]:
        end = self.end_time if self.end_time is not None else time.time()
        duration = end - self.start_time
        return {
            "rows": self.rows,
            "start_time": self.start_time,
            "end_time": end,
            "duration": duration,
        }


@dataclass
class TGroovy:
    """Execute a snippet of Python code in place of Groovy."""

    code: str

    def run(self, context: Optional[Dict[str, Any]] = None) -> None:
        exec(self.code, {}, context if context is not None else {})


@dataclass
class TJavaFlex:
    """Execute start, main, and end code blocks."""

    start_code: str = ""
    main_code: str = ""
    end_code: str = ""

    def run(self, df: pd.DataFrame, context: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        ctx: Dict[str, Any] = {} if context is None else context
        if self.start_code:
            exec(self.start_code, {}, ctx)
        if self.main_code:
            for _, row in df.iterrows():
                ctx["row"] = row
                exec(self.main_code, {}, ctx)
        if self.end_code:
            exec(self.end_code, {}, ctx)
        return df


@dataclass
class TSetGlobalVar:
    """Set a variable in a context dictionary."""

    key: str
    value: Any

    def set(self, context: Dict[str, Any]) -> None:
        context[self.key] = self.value


@dataclass
class TDataprepRun:
    """Apply a Talend-like data preparation function."""

    prep: Callable[[pd.DataFrame], pd.DataFrame]

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.prep(df)


@dataclass
class TBigtableConnection:
    """Connection information for Google Bigtable."""

    project: str
    instance: str

    def instance_client(self):
        from google.cloud import bigtable

        client = bigtable.Client(project=self.project, admin=True)
        return client.instance(self.instance)


@dataclass
class TBigtableInput:
    """Read rows from a Bigtable table."""

    connection: TBigtableConnection
    table: str

    def read(self):
        instance = self.connection.instance_client()
        table = instance.table(self.table)
        rows = table.read_rows()
        return [row for row in rows]


@dataclass
class TBigtableOutput:
    """Write rows to a Bigtable table."""

    connection: TBigtableConnection
    table: str

    def write(self, rows: Dict[bytes, Dict[bytes, Dict[bytes, bytes]]]) -> None:
        instance = self.connection.instance_client()
        table = instance.table(self.table)
        for key, families in rows.items():
            row = table.direct_row(key)
            for family, columns in families.items():
                for column, value in columns.items():
                    row.set_cell(family, column, value)
            row.commit()


@dataclass
class TGoogleDriveConnection:
    """Connection wrapper for Google Drive."""

    credentials: Any

    def service(self):
        from googleapiclient.discovery import build

        return build("drive", "v3", credentials=self.credentials)


@dataclass
class TGoogleDriveUpload:
    """Upload a file to Google Drive."""

    connection: TGoogleDriveConnection
    filepath: str
    mimetype: str
    parent_id: Optional[str] = None

    def upload(self) -> Dict[str, Any]:
        service = self.connection.service()
        from googleapiclient.http import MediaFileUpload

        file_metadata = {"name": Path(self.filepath).name}
        if self.parent_id:
            file_metadata["parents"] = [self.parent_id]
        media = MediaFileUpload(self.filepath, mimetype=self.mimetype)
        file = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        return file


@dataclass
class TGoogleDriveDownload:
    """Download a file from Google Drive."""

    connection: TGoogleDriveConnection
    file_id: str
    dest_path: str

    def download(self) -> None:
        service = self.connection.service()
        request = service.files().get_media(fileId=self.file_id)
        from googleapiclient.http import MediaIoBaseDownload
        import io as _io

        with open(self.dest_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()


# Register dynamically generated components after explicit definitions
register_components_from_yaml()
