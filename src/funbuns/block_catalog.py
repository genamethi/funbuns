"""
Block catalog utilities: discover, sort, and summarize parquet blocks.

Design goals:
- Properly sort blocks based on intrinsic data (min/max prime), not just filenames
- Provide fast summaries used by resume logic and integrity checks
- Use pathlib for paths and Polars for vectorized aggregation
- Cache catalog to avoid re-scanning 20K+ block files on every invocation
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import polars as pl

from .utils import get_data_dir


BLOCK_GLOB_PATTERN = "pp_b*.parquet"
_CACHE_FILENAME = ".block_catalog_cache.json"


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
    import re
    m = re.match(r'pp_b(\d+)_p(\d+)\.parquet$', path.name)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def blocks_dir() -> Path:
    """Return the directory where block files are stored."""
    return get_data_dir() / "blocks"


def list_block_files() -> List[Path]:
    """List all block parquet files sorted by max_prime from filename."""
    bdir = blocks_dir()
    if not bdir.exists():
        return []
    files = list(bdir.glob(BLOCK_GLOB_PATTERN))
    return sorted(files, key=lambda f: _parse_block_filename(f)[1] or 0)


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


# ---------------------------------------------------------------------------
# Catalog cache: avoids re-scanning all block files on every bmgr invocation.
# Keyed on the set of (filename, file_size) pairs. If any block is added,
# removed, or rewritten, the cache invalidates and a full rebuild happens.
# ---------------------------------------------------------------------------

def _cache_fingerprint(files: List[Path]) -> dict[str, int]:
    """Build a fingerprint: {filename: file_size_bytes} for cache validity."""
    return {f.name: f.stat().st_size for f in files}


def _load_catalog_cache(bdir: Path, files: List[Path]) -> Optional[List[BlockInfo]]:
    """Load cached catalog if it matches the current set of block files."""
    cache_path = bdir / _CACHE_FILENAME
    if not cache_path.exists():
        return None
    try:
        with open(cache_path) as f:
            data = json.load(f)
        fingerprint = _cache_fingerprint(files)
        if data.get("fingerprint") != fingerprint:
            return None
        catalog = []
        for entry in data["blocks"]:
            catalog.append(BlockInfo(
                path=bdir / entry["name"],
                block_num=entry.get("block_num"),
                min_prime=entry.get("min_prime"),
                max_prime=entry.get("max_prime"),
                num_rows=entry.get("num_rows"),
                num_unique_primes=entry.get("num_unique_primes"),
            ))
        return catalog
    except Exception:
        return None


def _save_catalog_cache(bdir: Path, files: List[Path], catalog: List[BlockInfo]):
    """Persist catalog to cache file."""
    cache_path = bdir / _CACHE_FILENAME
    data = {
        "fingerprint": _cache_fingerprint(files),
        "blocks": [
            {
                "name": b.path.name,
                "block_num": b.block_num,
                "min_prime": b.min_prime,
                "max_prime": b.max_prime,
                "num_rows": b.num_rows,
                "num_unique_primes": b.num_unique_primes,
            }
            for b in catalog
        ],
    }
    try:
        with open(cache_path, "w") as f:
            json.dump(data, f)
    except Exception:
        pass  # cache is best-effort


def build_block_catalog(files: Optional[Iterable[Path]] = None) -> List[BlockInfo]:
    """Build a catalog of blocks with intrinsic stats for robust sorting.

    Uses a file-based cache keyed on block filenames and sizes. If the cache
    matches, returns instantly without scanning any parquet data.
    """
    if files is not None:
        file_list = list(files)
        # Explicit file list: no caching (caller controls the set)
        return _build_catalog_uncached(file_list)

    file_list = list_block_files()
    bdir = blocks_dir()

    cached = _load_catalog_cache(bdir, file_list)
    if cached is not None:
        return cached

    catalog = _build_catalog_uncached(file_list)
    _save_catalog_cache(bdir, file_list, catalog)
    return catalog


def _build_catalog_uncached(files: List[Path]) -> List[BlockInfo]:
    """Scan each block file for metadata (no cache)."""
    catalog: List[BlockInfo] = []
    for path in files:
        bnum, max_from_name = _parse_block_filename(path)
        min_p, max_p, rows, uniq = _fast_block_bounds(path)
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




