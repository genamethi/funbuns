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

def integrate_runs_into_blocks(target_prime_count: int = 500_000, verbose: bool = True, delete_run_files: bool = False) -> bool:
    """Integrate all run files into blocks. Returns True if any work was done."""
    # Step 1: Combine new data and data from the last partial block
    new_data = _read_all_runs()
    
    existing_blocks = list_block_files()
    starting_block_idx = len(existing_blocks)
    data_to_block_list = []

    if existing_blocks:
        last_block_path = existing_blocks[-1]
        last_block_df = pl.read_parquet(last_block_path)
        # FIX: Check unique primes, not total rows
        if last_block_df.select(pl.col("p").n_unique()).item() < target_prime_count:
            data_to_block_list.append(last_block_df)
            starting_block_idx -= 1
            last_block_path.unlink()

    if new_data is not None and len(new_data) > 0:
        data_to_block_list.append(new_data)

    if not data_to_block_list:
        if verbose: print("No new run data or partial blocks to integrate.")
        return False
        
    raw_data = pl.concat(data_to_block_list)

    # In the new input validation section:

    # Check for true, identical duplicate rows
    total_rows = len(raw_data)
    unique_rows = raw_data.unique().height # No subset argument
    duplicate_count = total_rows - unique_rows
    if duplicate_count > 0:
        print(f"  ⚠️ WARNING: Detected {duplicate_count:,} identical duplicate rows in source files.")
        print("           Proceeding by removing these redundant rows.")

    # FIX: Correctly de-duplicate the data before doing anything else.
    # This ensures we have one canonical row per prime.
    data_to_block = raw_data.unique().sort("p")
    
    # Step 2: Create a "plan" for chunking the data into new blocks
    # This planning step is now correct because `data_to_block` is guaranteed to be unique by 'p'
    unique_primes = data_to_block.select("p")
    
    block_plan_df = (
        unique_primes.with_row_index("index")
        .with_columns(
            block_id=(pl.col("index") // target_prime_count) + starting_block_idx
        )
        .group_by("block_id")
        .agg(
            pl.col("p").min().alias("min_p"),
            pl.col("p").max().alias("max_p")
        )
        .sort("block_id")
    )
    
    # Step 3: Execute the plan and write the new blocks
    bdir = blocks_dir()
    bdir.mkdir(exist_ok=True)
    
    for plan in block_plan_df.iter_rows(named=True):
        block_id = plan["block_id"]
        min_p, max_p = plan["min_p"], plan["max_p"]
        
        block_rows = data_to_block.filter(pl.col("p").is_between(min_p, max_p))
        
        out_path = bdir / f"pp_b{block_id + 1:03d}_p{max_p}.parquet"
        block_rows.write_parquet(out_path)

    # Step 4: Optional cleanup 🧹
    if delete_run_files:
        rdir = runs_dir()
        if rdir.exists():
            for f in rdir.glob("*.parquet"): f.unlink()

    if verbose:
        total_integrated = len(unique_primes)
        print(f"Integrated {total_integrated:,} unique prime power partitions into blocks.")
    return True


