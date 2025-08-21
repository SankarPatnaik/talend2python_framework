from pyspark.sql import functions as F

def translate_expression(expr: str):
    """
    Translate Talend-style expressions into PySpark equivalents.
    Example:
        .toUpperCase() -> F.upper(col("col"))
        (cond) ? a : b -> F.when(cond, a).otherwise(b)
    """
    # Simple mappings
    replacements = {
        ".toUpperCase()": lambda col: F.upper(F.col(col)),
        ".toLowerCase()": lambda col: F.lower(F.col(col)),
        "StringHandling.TRIM": lambda col: F.trim(F.col(col)),
    }

    # Handle function-style cases
    if expr.startswith("StringHandling.LEFT"):
        # StringHandling.LEFT(col, n)
        inside = expr[expr.find("(")+1:expr.rfind(")")]
        col, n = [s.strip() for s in inside.split(",")]
        return F.substring(F.col(col), 1, int(n))

    if expr.startswith("StringHandling.RIGHT"):
        # StringHandling.RIGHT(col, n)
        inside = expr[expr.find("(")+1:expr.rfind(")")]
        col, n = [s.strip() for s in inside.split(",")]
        return F.expr(f"substring({col}, length({col})-{int(n)}+1, {int(n)})")

    if expr.startswith("StringHandling.CONCAT"):
        # StringHandling.CONCAT(col1, col2, ...)
        inside = expr[expr.find("(")+1:expr.rfind(")")]
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

