# talend2python

A **workable framework** to convert Talend jobs (XML `.item`) into **Python ETL pipelines** with **PySpark** or **Pandas** backends.

> ✅ This is a practical MVP that supports a useful subset of Talend components and shows how to extend to full coverage.

## Supported (MVP) Talend components

- `tFileInputDelimited` (CSV read)
- `tFilterRow` (row-level filter expressions)
- `tMap` (select/rename/derive columns – basic expressions)
- `tLogRow` (debug sink)
- `tFileOutputDelimited` (CSV write)

You can add more components by extending `mapping/component_map.yaml` and generator templates.

## Quickstart

```bash
# 1) Install (prefer venv or conda)
pip install -e .

# 2) Convert a Talend job to PySpark code
talend2py convert --input talend2python_framework/examples/jobs/sample_talend_job/sample_job.item --out build/sample_job --engine pyspark

# 3) Run the generated PySpark job (requires pyspark)
python build/sample_job/main.py --input_csv talend2python_framework/data/input.csv --output_csv build/output.csv

# 4) Or generate Pandas job
talend2py convert --input talend2python_framework/examples/jobs/sample_talend_job/sample_job.item --out build/sample_job_pandas --engine pandas
python build/sample_job_pandas/main.py --input_csv talend2python_framework/data/input.csv --output_csv build/output.csv
```

## CLI

```
talend2py convert --input <path to .item> --out <dir> --engine [pyspark|pandas]
```

## Project layout

```
talend2python/
  cli.py                  # Entry point: parse -> IR -> generate code
  parsers/talend_xml_parser.py
  ir/model.py             # Internal graph model (components + edges)
  mapping/component_map.yaml
  generators/pyspark_generator.py
  generators/pandas_generator.py
  templates/
      main_pyspark.py.j2
      main_pandas.py.j2
      components/
         filter.j2
         select_rename.j2
         log.j2
  runtime/
      io.py
      utils.py
talend2python_framework/examples/jobs/sample_talend_job/sample_job.item
talend2python_framework/tests/
```

## CI

- Runs `flake8` lint, `pytest`, and a sample conversion + smoke run on GitHub Actions.

## Extending coverage

- Add Talend component mapping to `mapping/component_map.yaml`
- Update generators to emit code blocks for new nodes
- Add tests in `talend2python_framework/tests/`

## MSSQL to ORASS Migration

This project also includes a lightweight framework to copy data from Microsoft SQL Server
to Oracle (ORASS) without any business transformation. Define connection details and
table mappings in a YAML file and run the migration with the `db-migrate` CLI.

```bash
db-migrate path/to/config.yaml
```

See `talend2python_framework/examples/mssql_to_orass.yaml` for a sample configuration
file.

## License

Apache-2.0
