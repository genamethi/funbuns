"""
Run ingester: convert data/runs/*.parquet into properly sized, deduplicated blocks.

Responsibilities:
- Read pending run files in memory-bounded batches
- Deduplicate on (p, m_k, n_k, q_k)
- Append into the last block if it has capacity, else create new blocks of target size
- Remove processed run files after successful integration

Notes:
- Uses content-derived prime ranges for block naming and ordering
- Coordinates with block_catalog for directory and discovery helpers
- Memory-adaptive: processes run files in sorted batches to avoid OOM
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple

import polars as pl

from .utils import get_data_dir
from .block_catalog import blocks_dir, list_block_files


def runs_dir() -> Path:
    return get_data_dir() / "runs"


def _sorted_run_files() -> List[Path]:
    """List run files sorted by name (which embeds timestamp)."""
    rdir = runs_dir()
    if not rdir.exists():
        return []
    return sorted(rdir.glob("*.parquet"))


def _estimate_run_batch_size(files: List[Path], memory_limit_mb: int = 512) -> int:
    """Estimate how many run files to load per batch based on file sizes.

    Aims to keep each batch under memory_limit_mb when loaded into memory.
    Polars DataFrames from parquet are roughly 2-4x the file size on disk.
    """
    if not files:
        return 1
    total_size = sum(f.stat().st_size for f in files)
    avg_size = total_size / len(files)
    expansion_factor = 4  # conservative: parquet → in-memory
    files_per_batch = max(1, int((memory_limit_mb * 1024 * 1024) / (avg_size * expansion_factor)))
    return files_per_batch


def integrate_runs_into_blocks(target_prime_count: int = 500_000,
                               verbose: bool = True,
                               delete_run_files: bool = False,
                               memory_limit_mb: int = 512) -> bool:
    """Integrate all run files into blocks. Returns True if any work was done.

    Processes run files in memory-bounded batches to avoid OOM on long runs.
    Each batch is sorted, deduplicated, and written as blocks before the next
    batch is loaded.
    """
    run_files = _sorted_run_files()
    existing_blocks = list_block_files()
    starting_block_idx = len(existing_blocks)

    # Check if there's a partial last block to absorb
    last_block_df = None
    last_block_primes = 0
    if existing_blocks:
        last_block_path = existing_blocks[-1]
        last_block_meta = pl.scan_parquet(last_block_path).select(
            pl.col("p").n_unique().alias("uniq")
        ).collect()
        last_block_uniq = last_block_meta["uniq"].item()
        if last_block_uniq < target_prime_count:
            last_block_df = pl.read_parquet(last_block_path)
            last_block_primes = last_block_uniq
            starting_block_idx -= 1
            last_block_path.unlink()

    if not run_files and last_block_df is None:
        if verbose:
            print("No new run data or partial blocks to integrate.")
        return False

    # Determine batch size based on available memory
    batch_size = _estimate_run_batch_size(run_files, memory_limit_mb)
    total_files = len(run_files)
    total_integrated = 0

    if verbose and total_files > batch_size:
        print(f"  Processing {total_files} run files in batches of {batch_size} "
              f"(~{memory_limit_mb}MB memory limit)")

    bdir = blocks_dir()
    bdir.mkdir(exist_ok=True)

    # Process run files in batches
    file_batches = [run_files[i:i + batch_size] for i in range(0, max(len(run_files), 1), batch_size)]
    if not run_files:
        file_batches = [[]]  # still need to process the partial last block

    processed_files: List[Path] = []

    for batch_idx, file_batch in enumerate(file_batches):
        data_parts = []

        # On the first batch, include the partial last block
        if batch_idx == 0 and last_block_df is not None:
            data_parts.append(last_block_df)
            last_block_df = None  # free memory

        # Read this batch of run files
        for f in file_batch:
            try:
                data_parts.append(pl.read_parquet(f))
                processed_files.append(f)
            except Exception as e:
                if verbose:
                    print(f"  Warning: skipping corrupt run file {f.name}: {e}")

        if not data_parts:
            continue

        raw_data = pl.concat(data_parts)
        del data_parts  # free list references

        # Deduplicate
        before = raw_data.height
        data_to_block = raw_data.unique().sort("p")
        del raw_data
        after = data_to_block.height
        if before > after and verbose:
            print(f"  Batch {batch_idx + 1}: removed {before - after:,} duplicate rows")

        # Assign block IDs via a prime-rank mapping, then partition
        unique_primes = data_to_block.select("p").unique().sort("p")
        n_primes = unique_primes.height
        total_integrated += n_primes

        # Build prime → block_id mapping
        prime_block_map = (
            unique_primes.with_row_index("index")
            .with_columns(
                block_id=(pl.col("index") // target_prime_count) + starting_block_idx
            )
            .select("p", "block_id")
        )

        # Join block_id onto data and partition — single pass, no repeated filters
        tagged = data_to_block.join(prime_block_map, on="p", how="left")
        partitions = tagged.partition_by("block_id", as_dict=True)

        for block_id_val, block_df in sorted(partitions.items()):
            block_id = block_df["block_id"][0]
            max_p = block_df["p"].max()
            out_path = bdir / f"pp_b{block_id + 1:03d}_p{max_p}.parquet"
            block_df.drop("block_id").write_parquet(
                out_path, compression="zstd", compression_level=1,
                row_group_size=min(block_df.height, 100_000)
            )

        # Advance the block index for the next batch
        if partitions:
            starting_block_idx = max(
                part["block_id"][0] for part in partitions.values()
            ) + 1

        del data_to_block, unique_primes, prime_block_map, tagged, partitions

        if verbose and total_files > batch_size:
            files_done = min((batch_idx + 1) * batch_size, total_files)
            print(f"  Batch {batch_idx + 1}: {files_done}/{total_files} files processed")

    # Cleanup run files
    if delete_run_files:
        for f in processed_files:
            try:
                f.unlink()
            except FileNotFoundError:
                pass

    if verbose:
        new_primes = total_integrated - last_block_primes
        if last_block_primes > 0:
            print(f"Integrated {new_primes:,} new primes into blocks "
                  f"({last_block_primes:,} re-packed from partial last block, "
                  f"{total_integrated:,} total in affected blocks).")
        else:
            print(f"Integrated {new_primes:,} new primes into blocks.")
    return True
