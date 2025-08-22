"""
Run ingester: convert data/runs/*.parquet into properly sized, deduplicated blocks.

Responsibilities:
- Read all pending run files lazily
- Deduplicate on (p, m_k, n_k, q_k)
- Append into the last block if it has capacity, else create new blocks of target size
- Remove processed run files after successful integration

Notes:
- Uses content-derived prime ranges for block naming and ordering
- Coordinates with block_catalog for directory and discovery helpers
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple

import polars as pl

from .utils import get_data_dir
from .block_catalog import blocks_dir, list_block_files


def runs_dir() -> Path:
    return get_data_dir() / "runs"


def _read_all_runs() -> Optional[pl.DataFrame]:
    rdir = runs_dir()
    files = list(rdir.glob("*.parquet")) if rdir.exists() else []
    if not files:
        return None
    # Lazy read then collect once; schema assumed consistent with blocks
    return pl.scan_parquet([str(f) for f in files]).collect()


def _dedup(df: pl.DataFrame) -> pl.DataFrame:
    # Ensure core uniqueness keys; tolerate absence by intersecting available columns
    cols = [c for c in ["p", "m_k", "n_k", "q_k"] if c in df.columns]
    if not cols:
        return df.unique()
    return df.unique(cols)


def integrate_runs_into_blocks(target_prime_count: int = 500_000, verbose: bool = True, delete_run_files: bool = False) -> bool:
    """Integrate all run files into blocks. Returns True if any work was done."""
    data = _read_all_runs()
    if data is None or len(data) == 0:
        if verbose:
            print("No run files found to integrate.")
        return False

    # Ensure schema normalization (in case of any stray columns)
    keep_cols = [c for c in ["p","m_k","n_k","q_k"] if c in data.columns]
    data = data.select(keep_cols).sort("p")
    data = _dedup(data)

    bdir = blocks_dir()
    bdir.mkdir(exist_ok=True)

    # Determine existing last block capacity
    existing_blocks = list_block_files()
    last_block = existing_blocks[-1] if existing_blocks else None

    last_block_primes = 0
    if last_block is not None:
        last_df = pl.read_parquet(last_block)
        last_block_primes = int(last_df.select(pl.col("p").n_unique()).item())

    unique_primes = data.select("p").unique().sort("p")

    # Append to last block if space remains
    if last_block is not None and last_block_primes < target_prime_count:
        space_remaining = target_prime_count - last_block_primes
        primes_to_append = min(space_remaining, len(unique_primes))
        if primes_to_append > 0:
            append_primes = unique_primes.slice(0, primes_to_append)
            min_p = int(append_primes.select(pl.col("p").min()).item())
            max_p = int(append_primes.select(pl.col("p").max()).item())
            append_rows = (
                data.filter((pl.col("p") >= min_p) & (pl.col("p") <= max_p))
                .unique([c for c in ["p", "m_k", "n_k", "q_k"] if c in data.columns])
            )
            combined = pl.concat([pl.read_parquet(last_block), append_rows]).sort("p")
            combined.write_parquet(last_block)
            # Rename to reflect new max prime
            block_idx = len(existing_blocks)
            new_name = f"pp_b{block_idx:03d}_p{max_p}.parquet"
            new_path = bdir / new_name
            if new_path != last_block:
                Path(last_block).rename(new_path)
                last_block = new_path
            # Shrink unique_primes by consumed count
            unique_primes = unique_primes.slice(primes_to_append)

    # Create new blocks for remaining primes
    remaining = len(unique_primes)
    if remaining > 0:
        start_idx = len(existing_blocks) if last_block is None else len(existing_blocks)
        # If we appended above, last_block is updated but count remains len(existing_blocks)
        block_cursor = start_idx
        while len(unique_primes) > 0:
            slice_size = min(target_prime_count, len(unique_primes))
            slice_primes = unique_primes.head(slice_size)
            min_p = int(slice_primes.select(pl.col("p").min()).item())
            max_p = int(slice_primes.select(pl.col("p").max()).item())
            block_rows = (
                data.filter((pl.col("p") >= min_p) & (pl.col("p") <= max_p))
                .unique([c for c in ["p", "m_k", "n_k", "q_k"] if c in data.columns])
            )
            out_path = bdir / f"pp_b{block_cursor + 1:03d}_p{max_p}.parquet"
            block_rows.write_parquet(out_path)
            block_cursor += 1
            unique_primes = unique_primes.slice(slice_size)

    # Optionally remove run files after success
    if delete_run_files:
        rdir = runs_dir()
        files = list(rdir.glob("*.parquet")) if rdir.exists() else []
        for f in files:
            f.unlink()

    if verbose:
        total_integrated = int(data.select(pl.col('p').n_unique()).item())
        print(f"Integrated {total_integrated:,} primes from runs into blocks.")
    return True


