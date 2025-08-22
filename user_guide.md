# Talend2Python Framework – Interactive User Guide

## Table of Contents
- [Overview](#overview)
- [Quickstart](#quickstart)
- [Architecture](#architecture)
- [Adding a New Talend Component](#adding-a-new-talend-component)
- [Component Reference](#component-reference)
  - [Core ETL Components](#core-etl-components)
  - [Database Components](#database-components)
  - [File & Utility Components](#file--utility-components)
  - [Cloud & External Services](#cloud--external-services)
- [Further Help](#further-help)

## Overview
`talend2python` converts Talend `.item` jobs into runnable Python ETL pipelines using either **PySpark** or **Pandas** engines. The framework supports a growing subset of Talend components and exposes a simple path for extension.

## Quickstart
1. **Install** (prefer a virtual environment):
   ```bash
   pip install -e .
   ```
2. **Convert a Talend job** to PySpark or Pandas code:
   ```bash
   talend2py convert --input path/to/job.item --out build/job --engine pyspark
   # or
   talend2py convert --input path/to/job.item --out build/job_pandas --engine pandas
   ```
3. **Run the generated job** (example for PySpark):
   ```bash
   python build/job/main.py --input_csv data/input.csv --output_csv build/output.csv
   ```

## Architecture
```
Talend XML (.item)
       │
       ▼
Parser → Graph (nodes, edges) → Generator (Jinja2 templates) → Python/PySpark code
       │
       ▼
Runtime components for execution
```
- **Parser**: `parsers/talend_xml_parser.py` converts the XML into a graph of `Node` objects, remapping parameter names via `_KEY_MAP`.
- **Mapping**: `mapping/component_map.yaml` links Talend component names to generic actions and parameter keys.
- **Generators**: Jinja2 templates (`main_pandas.py.j2`, `main_pyspark.py.j2`) contain conditional blocks for each supported component type.
- **Runtime**: `runtime/components.py` defines Python classes implementing component behaviour.

## Adding a New Talend Component
1. **Map the component** in `mapping/component_map.yaml`:
   ```yaml
   tNewComponent:
     action: new_action
     params:
       foo_key: "foo"
       bar_key: "bar"
   ```
2. **Expose parameters** in the parser (`parsers/talend_xml_parser.py`). Extend `_KEY_MAP` for new `elementParameter` names so they appear in `Node.config`.
3. **Implement runtime behaviour** in `runtime/components.py`. Create a class or function following the dataclass pattern used by existing components.
4. **Update generators** (`templates/main_pandas.py.j2` and `templates/main_pyspark.py.j2`). Add a block such as:
   ```jinja
   {% elif s.type == "tNewComponent" %}
     {{ generate_new_component(s) }}
   {% endif %}
   ```
5. **Add tests** under `talend2python_framework/tests/` with a sample Talend job to verify parsing and code generation.
6. **Run conversion** to manually verify the new component:
   ```bash
   talend2py convert --input path/to/sample.item --out build/sample --engine pyspark
   ```

## Component Reference
### Core ETL Components
| Component | Purpose | Key Parameters | Notes |
|-----------|---------|----------------|-------|
| `tFileInputDelimited` | Read CSV-like files into a DataFrame | `file_path`, `separator`, `header` | Template + runtime class |
| `tFilterRow` | Row-level filtering using expressions | `filter_expr` | Templates |
| `tMap` | Select, rename, or derive columns via expressions | JSON `mapping` | Templates + runtime class |
| `tJoin` | Join two DataFrames | `left_on`, `right_on`, `join_type` | Templates + runtime class |
| `tLogRow` | Log the current DataFrame | n/a | Templates + runtime class |
| `tFileOutputDelimited` | Write DataFrame to CSV | `file_path`, `separator`, `header` | Templates + runtime class |

### Database Components
| Component Group | Description |
|-----------------|-------------|
| MySQL (`TMysqlConnection`, `TMysqlInput`, `TMysqlOutput`) | Connect, read, and write using SQLAlchemy |
| Oracle (`TOracleConnection`, `TOracleInput`, `TOracleOutput`) | Oracle database access |
| PostgreSQL (`TPostgresqlConnection`, `TPostgresqlInput`, `TPostgresqlOutput`) | PostgreSQL database access |
| MSSQL (`TMSSqlConnection`, `TMSSqlInput`, `TMSSqlOutput`) | Microsoft SQL Server operations |
| DB2 (`TDB2Connection`, `TDB2Input`, `TDB2Output`) | IBM DB2 access |

### File & Utility Components
| Component | Purpose |
|-----------|---------|
| `TFileInputExcel` | Load Excel sheets |
| `TFileList` | Enumerate files in a directory |
| `TFileArchive` | Create ZIP/TAR archives |
| `TFileOutputExcel` | Export DataFrame to Excel |
| `TRowGenerator` | Produce synthetic rows from callables |
| `TMsgBox` | Display a message |
| `TPreJob` / `TPostJob` | Run tasks before/after the job |
| `TJava` / `TGroovy` / `TJavaFlex` | Execute embedded Python code blocks |
| `TRunJob` | Invoke another callable job |
| `TSetGlobalVar` | Persist key–value pair in context |
| `TDataprepRun` | Apply a user-supplied preparation function |
| `TExtractDelimitedFields` | Split a column into new columns |
| `TAggregateRow` | Group and aggregate rows |
| `TFlowMeter` | Track row counts and rates |
| `TStatCatcher` | Capture job statistics |

### Cloud & External Services
| Component | Description |
|-----------|-------------|
| Bigtable (`TBigtableConnection`, `TBigtableInput`, `TBigtableOutput`) | Google Cloud Bigtable operations |
| Google Drive (`TGoogleDriveConnection`, `TGoogleDriveUpload`, `TGoogleDriveDownload`) | Upload/download files via Google Drive API |

## Further Help
- See the `examples/` directory for sample Talend jobs and configuration files.
- Explore tests in `talend2python_framework/tests/` to understand expected behaviour.
- Issues and contributions are welcome via GitHub.

