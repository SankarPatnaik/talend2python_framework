"""
Command line interface for talend2python.

This script exposes a ``convert`` subcommand that accepts a Talend ``.item``
file or raw XML content and generates equivalent Python or PySpark code. It
utilises the ``parse_talend_item`` function to build an IR graph and then
delegates to either the pandas or PySpark generator based on the ``--engine``
option.
"""

import argparse
import pathlib

from .generators import pandas_generator, pyspark_generator
from .parsers.talend_xml_parser import parse_talend_item


def main():
    p = argparse.ArgumentParser(
        prog="talend2py", description="Convert Talend .item to Python ETL code"
        )
    sp = p.add_subparsers(dest="cmd", required=True)
    c = sp.add_parser("convert", help="Convert Talend job to Python code")
    grp = c.add_mutually_exclusive_group(required=True)
    grp.add_argument("--input", help="Path to Talend .item XML")
    grp.add_argument("--xml", help="Raw Talend job XML content")
    c.add_argument("--out", required=True, help="Output directory for generated code")
    c.add_argument(
        "--engine",
        default="pyspark",
        choices=["pyspark", "pandas"],
        help="Execution engine",
    )
    args = p.parse_args()

    if args.cmd == "convert":
        xml_source = args.input if args.input else args.xml
        graph = parse_talend_item(xml_source)
        out_dir = pathlib.Path(args.out)
        out_dir.mkdir(parents=True, exist_ok=True)
        if args.engine == "pyspark":
            result = pyspark_generator.generate(graph, out_dir)
        else:
            result = pandas_generator.generate(graph, out_dir)
        print(f"Generated {result['engine']} job at: {out_dir}")

