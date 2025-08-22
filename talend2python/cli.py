"""
Command line interface for talend2python.

This script exposes a ``convert`` subcommand that accepts a Talend .item file
and generates equivalent Python or PySpark code.  It utilises the
``parse_talend_item`` function to build an IR graph and then delegates to
either the pandas or PySpark generator based on the ``--engine`` option.
"""

import argparse
import pathlib
from .parsers.talend_xml_parser import parse_talend_item
from .generators import pyspark_generator, pandas_generator


def main():
    p = argparse.ArgumentParser(prog="talend2py", description="Convert Talend .item to Python ETL code")
    sp = p.add_subparsers(dest="cmd", required=True)
    c = sp.add_parser("convert", help="Convert Talend job to Python code")
    c.add_argument("--input", required=True, help="Path to Talend .item XML")
    c.add_argument("--out", required=True, help="Output directory for generated code")
    c.add_argument("--engine", default="pyspark", choices=["pyspark", "pandas"], help="Execution engine")
    args = p.parse_args()

    if args.cmd == "convert":
        graph = parse_talend_item(args.input)
        out_dir = pathlib.Path(args.out)
        out_dir.mkdir(parents=True, exist_ok=True)
        if args.engine == "pyspark":
            result = pyspark_generator.generate(graph, out_dir)
        else:
            result = pandas_generator.generate(graph, out_dir)
        print(f"Generated {result['engine']} job at: {out_dir}")