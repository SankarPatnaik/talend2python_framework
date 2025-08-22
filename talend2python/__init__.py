"""Talend to Python conversion package.

This package provides tools for parsing Talend job definitions (.item files)
into an intermediate representation (IR) and generating equivalent ETL code
for both pandas and PySpark engines.  The main entry point for end users
is the ``cli`` module which exposes a command line interface.
"""

from . import cli  # noqa: F401  # re‑export for convenience