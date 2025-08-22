"""
Block catalog utilities: discover, sort, and summarize parquet blocks.

Design goals:
- Properly sort blocks based on intrinsic data (min/max prime), not just filenames
- Provide fast summaries used by resume logic and integrity checks
- Use pathlib for paths and Polars for vectorized aggregation
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import polars as pl

from .utils import get_data_dir


BLOCK_GLOB_PATTERN = "pp_b*.parquet"


@dataclass(frozen=True)
class BlockInfo:
    path: Path
    block_num: Optional[int]
    min_prime: Optional[int]
    max_prime: Optional[int]
    num_rows: Optional[int]
    num_unique_primes: Optional[int]


def _parse_block_filename(path: Path) -> Tuple[Optional[int], Optional[int]]:
    """Parse filename like pp_b001_p7249729.parquet -> (1, 7249729).

    Fallbacks to (None, None) if pattern does not match.
    """
    name = path.name
    try:
        # Expected formats: pp_bNNN_pMAX.parquet
        base = name.replace(".parquet", "")
        parts = base.split("_")
        # parts: ["pp", "bNNN", "pMAX"] or ["pp", "bNNN", "pMAX", ...]
        b_part = next((p for p in parts if p.startswith("b")), None)
        p_part = next((p for p in parts if p.startswith("p")), None)
        block_num = int(b_part[1:]) if b_part and b_part[1:].isdigit() else None
        max_prime = int(p_part[1:]) if p_part and p_part[1:].isdigit() else None
        return block_num, max_prime
    except Exception:
        return None, None


def blocks_dir() -> Path:
    """Return the directory where block files are stored."""
    return get_data_dir() / "blocks"


def list_block_files() -> List[Path]:
    """List all block parquet files (unsorted)."""
    bdir = blocks_dir()
    if not bdir.exists():
        return []
    return sorted(bdir.glob(BLOCK_GLOB_PATTERN))


def _fast_block_bounds(path: Path) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    """Compute (min_p, max_p, rows, unique_p) for a single block using Polars lazily.

    Returns (None, None, None, None) on error.
    """
    try:
        lf = pl.scan_parquet(path)
        stats = lf.select([
            pl.col("p").min().alias("min_p"),
            pl.col("p").max().alias("max_p"),
            pl.len().alias("rows"),
            pl.col("p").n_unique().alias("uniq"),
        ]).collect()
        return (
            int(stats["min_p"].item()),
            int(stats["max_p"].item()),
            int(stats["rows"].item()),
            int(stats["uniq"].item()),
        )
    except Exception:
        return None, None, None, None


def build_block_catalog(files: Optional[Iterable[Path]] = None) -> List[BlockInfo]:
    """Build a catalog of blocks with intrinsic stats for robust sorting."""
    if files is None:
        files = list_block_files()
    catalog: List[BlockInfo] = []
    for path in files:
        bnum, max_from_name = _parse_block_filename(path)
        min_p, max_p, rows, uniq = _fast_block_bounds(path)
        # If content-derived max is missing, fall back to filename-derived
        resolved_max = max_p if max_p is not None else max_from_name
        catalog.append(
            BlockInfo(
                path=path,
                block_num=bnum,
                min_prime=min_p,
                max_prime=resolved_max,
                num_rows=rows,
                num_unique_primes=uniq,
            )
        )
    return catalog


def sorted_blocks_by_data(files: Optional[Iterable[Path]] = None) -> List[BlockInfo]:
    """Return blocks sorted by min_prime then max_prime (content-derived)."""
    catalog = build_block_catalog(files)
    return sorted(
        catalog,
        key=lambda b: (
            float("inf") if b.min_prime is None else b.min_prime,
            float("inf") if b.max_prime is None else b.max_prime,
        ),
    )


def compute_resume_from_blocks() -> Tuple[int, int]:
    """Compute (last_prime, start_idx) across all blocks using a single lazy scan."""
    bdir = blocks_dir()
    files = list(bdir.glob(BLOCK_GLOB_PATTERN)) if bdir.exists() else []
    if not files:
        return 2, 0
    pattern = str(bdir / BLOCK_GLOB_PATTERN)
    result = (
        pl.scan_parquet(pattern)
        .select([
            pl.col("p").max().alias("last_prime"),
            pl.col("p").n_unique().alias("start_idx"),
        ])
        .collect()
    )
    last_prime = int(result["last_prime"].item())
    start_idx = int(result["start_idx"].item())
    return last_prime, start_idx


def block_summary_frame() -> pl.DataFrame:
    """Return a Polars DataFrame summary of all blocks (path, block_num, min, max, rows, uniq)."""
    cat = build_block_catalog()
    if not cat:
        return pl.DataFrame({
            "path": pl.Series([], pl.Utf8),
            "block_num": pl.Series([], pl.Int64),
            "min_prime": pl.Series([], pl.Int64),
            "max_prime": pl.Series([], pl.Int64),
            "rows": pl.Series([], pl.Int64),
            "unique_primes": pl.Series([], pl.Int64),
        })
    return pl.DataFrame({
        "path": [str(b.path) for b in cat],
        "block_num": [b.block_num for b in cat],
        "min_prime": [b.min_prime for b in cat],
        "max_prime": [b.max_prime for b in cat],
        "rows": [b.num_rows for b in cat],
        "unique_primes": [b.num_unique_primes for b in cat],
    }).with_columns([
        pl.col("block_num").cast(pl.Int64, strict=False),
        pl.col("min_prime").cast(pl.Int64, strict=False),
        pl.col("max_prime").cast(pl.Int64, strict=False),
        pl.col("rows").cast(pl.Int64, strict=False),
        pl.col("unique_primes").cast(pl.Int64, strict=False),
    ])


