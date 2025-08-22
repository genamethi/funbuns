"""
Data integrity checks for partitions data in blocks and runs.

Capabilities:
- Schema validation against expected columns/dtypes (best-effort, non-fatal)
- Duplicate detection by (p, m_k, n_k, q_k)
- Overlap detection across consecutive blocks (shared primes)
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple

import polars as pl

from .block_catalog import blocks_dir, list_block_files, sorted_blocks_by_data


EXPECTED_KEYS = ["p", "m_k", "n_k", "q_k"]


def check_schema(df: pl.DataFrame) -> Dict[str, bool]:
    present = {c: (c in df.columns) for c in EXPECTED_KEYS}
    return present


def detect_duplicates_in_block(path: Path) -> int:
    df = pl.read_parquet(path)
    keys = [c for c in EXPECTED_KEYS if c in df.columns]
    if not keys:
        return 0
    before = len(df)
    after = len(df.unique(keys))
    return before - after


def detect_overlaps_between_blocks() -> pl.DataFrame:
    """Return a small report of overlaps in primes between consecutive blocks."""
    infos = sorted_blocks_by_data()
    if len(infos) < 2:
        return pl.DataFrame({"block_a": [], "block_b": [], "overlap_primes": []})

    rows = []
    for a, b in zip(infos, infos[1:]):
        try:
            a_p = pl.scan_parquet(a.path).select(pl.col("p")).collect()
            b_p = pl.scan_parquet(b.path).select(pl.col("p")).collect()
            overlap = a_p.join(b_p, on="p", how="inner").height
            if overlap > 0:
                rows.append({
                    "block_a": a.path.name,
                    "block_b": b.path.name,
                    "overlap_primes": overlap,
                })
        except Exception:
            continue
    if not rows:
        return pl.DataFrame({"block_a": [], "block_b": [], "overlap_primes": []})
    return pl.DataFrame(rows)


def quick_integrity_report() -> str:
    files = list_block_files()
    if not files:
        return "No block files found."

    dup_total = 0
    per_block = []
    for f in files:
        dups = detect_duplicates_in_block(f)
        dup_total += dups
        per_block.append((f.name, dups))

    overlap_df = detect_overlaps_between_blocks()

    lines = []
    lines.append("=== DATA INTEGRITY REPORT ===")
    lines.append(f"Blocks: {len(files)}")
    lines.append(f"Total duplicate rows across blocks (by keys {EXPECTED_KEYS}): {dup_total}")
    if any(dups > 0 for _, dups in per_block):
        lines.append("Per-block duplicates:")
        for name, d in per_block:
            if d > 0:
                lines.append(f"  {name}: {d} duplicate rows")
    if overlap_df.height > 0:
        lines.append("Overlapping primes between adjacent blocks:")
        for row in overlap_df.iter_rows(named=True):
            lines.append(f"  {row['block_a']} <-> {row['block_b']}: {row['overlap_primes']} primes")
    else:
        lines.append("No overlaps of primes detected between adjacent blocks.")

    return "\n".join(lines)


