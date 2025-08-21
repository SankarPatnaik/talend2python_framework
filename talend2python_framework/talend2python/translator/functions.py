"""
Expression translation utilities for PySpark.

This module provides helpers to convert Talend style expressions into
equivalent PySpark expressions.  A handful of common string functions and
the ternary operator are handled explicitly.  For any unrecognised
expression the raw string is passed to ``F.expr`` to be interpreted by
Spark SQL.
"""

from pyspark.sql import functions as F


def translate_expression(expr: str):
    """Translate a Talend‑style expression into a PySpark function call.

    Examples
    --------
    >>> translate_expression("col.toUpperCase()")
    Column<'upper(col)'>
    >>> translate_expression("(age > 18) ? 'adult' : 'minor'")
    Column<'CASE WHEN (age > 18) THEN 'adult' ELSE 'minor' END'>

    Parameters
    ----------
    expr : str
        The expression to translate.

    Returns
    -------
    pyspark.sql.column.Column
        A column expression suitable for use in DataFrame.select() or
        DataFrame.filter().
    """
    # Simple suffix mappings for commonly used methods
    replacements = {
        ".toUpperCase()": lambda col: F.upper(F.col(col)),
        ".toLowerCase()": lambda col: F.lower(F.col(col)),
        "StringHandling.TRIM": lambda col: F.trim(F.col(col)),
    }

    # Function‑style cases
    if expr.startswith("StringHandling.LEFT"):
        # StringHandling.LEFT(col, n)
        inside = expr[expr.find("(") + 1 : expr.rfind(")")]
        col, n = [s.strip() for s in inside.split(",")]
        return F.substring(F.col(col), 1, int(n))

    if expr.startswith("StringHandling.RIGHT"):
        # StringHandling.RIGHT(col, n)
        inside = expr[expr.find("(") + 1 : expr.rfind(")")]
        col, n = [s.strip() for s in inside.split(",")]
        return F.expr(f"substring({col}, length({col})-{int(n)}+1, {int(n)})")

    if expr.startswith("StringHandling.CONCAT"):
        # StringHandling.CONCAT(col1, col2, ...)
        inside = expr[expr.find("(") + 1 : expr.rfind(")")]
        cols = [F.col(c.strip()) for c in inside.split(",")]
        return F.concat(*cols)

    if "?" in expr and ":" in expr:
        # Ternary operator: (cond) ? a : b
        cond, rest = expr.split("?")
        a, b = rest.split(":")
        return F.when(F.expr(cond.strip()), F.expr(a.strip())).otherwise(F.expr(b.strip()))

    # Simple suffix mappings (toUpperCase, toLowerCase, trim)
    for key, func in replacements.items():
        if key in expr:
            # assume expr is like "col.toUpperCase()"
            col = expr.split(".")[0]
            return func(col)

    # Default: return raw expression wrapped in expr()
    return F.expr(expr)