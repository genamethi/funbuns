"""
Expression parsing for the webserver and partition_report CLI.

``parse_filter`` converts a short filter string (exact value, modular pattern,
range, or set) into a Polars expression against a named column. ``filter_min``
returns a lower bound implied by the expression where one can be derived.
"""

from __future__ import annotations

import re

import polars as pl


def filter_min(expr: str) -> int | None:
    """Return the minimum value implied by a filter expression, or None.

    Works for exact values, ranges, and lists. Returns None for modular
    patterns (e.g. 2*i+1) where a useful minimum can't be determined.
    """
    expr = expr.strip()
    expr = re.sub(
        r'(\d+)\s*\^\s*(\d+)',
        lambda m: str(int(m.group(1)) ** int(m.group(2))),
        expr,
    )
    if re.fullmatch(r'\d+', expr):
        return int(expr)
    m = re.fullmatch(r'(\d+)\s*\.\.\s*(\d+)', expr)
    if m:
        return int(m.group(1))
    if re.fullmatch(r'\d+(\s*,\s*\d+)+', expr):
        return min(int(x.strip()) for x in expr.split(','))
    return None


def parse_filter(expr: str, column: str) -> pl.Expr:
    """Parse a filter expression into a Polars boolean expression.

    Patterns:
        5        -> col == 5                 (exact)
        2*i      -> col % 2 == 0             (multiples)
        2*i+1    -> col % 2 == 1             (modular)
        3*i-1    -> col % 3 == 2             (modular, negative offset)
        3..10    -> col.is_between(3, 10)    (range)
        3,5,7    -> col.is_in([3, 5, 7])     (list)
    """
    expr = expr.strip()

    # Expand power notation: 10^9 -> 1000000000, 2^5 -> 32
    expr = re.sub(
        r'(\d+)\s*\^\s*(\d+)',
        lambda m: str(int(m.group(1)) ** int(m.group(2))),
        expr,
    )

    col = pl.col(column)

    if re.fullmatch(r'\d+', expr):
        return col == int(expr)

    m = re.fullmatch(r'(\d+)\s*\*\s*i\s*(?:([+-])\s*(\d+))?', expr)
    if m:
        k = int(m.group(1))
        if k == 0:
            raise ValueError("Multiplier cannot be 0")
        if m.group(2) and m.group(3):
            c = int(m.group(3))
            if m.group(2) == '-':
                c = -c
            return (col % k) == (c % k)
        return (col % k) == 0

    m = re.fullmatch(r'(\d+)\s*\.\.\s*(\d+)', expr)
    if m:
        return col.is_between(int(m.group(1)), int(m.group(2)))

    if re.fullmatch(r'\d+(\s*,\s*\d+)+', expr):
        values = [int(x.strip()) for x in expr.split(',')]
        return col.is_in(values)

    raise ValueError(f"Cannot parse filter: {expr!r}")
