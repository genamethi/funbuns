"""
One-shot compactor: legacy blocks/*.parquet -> iceberg funbuns.{primes,decompositions}.

Reads legacy block files in true p-order (via block_catalog), accumulates
them into batches of ~50M primes, and calls iceberg_schema.write_batch for
each batch. Resumes from the current iceberg state on restart. Does not
touch legacy blocks — verification and retirement are separate steps.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import polars as pl
from tqdm import tqdm

from funbuns import iceberg_schema as isch
from funbuns.block_catalog import BlockInfo, sorted_blocks_by_data
from funbuns.utils import JournalWriter


DEFAULT_BATCH_TARGET = 50_000_000


def _resume_state(cat) -> tuple[int, int]:
    """Return (next_batch_id, resume_p) from current iceberg state. Both 0 if empty.

    Reads per-file upper_bound statistics from the primes table manifest via
    tbl.inspect.files(). Metadata-only — does not open data files.
    """
    try:
        tbl = cat.load_table(isch.PRIMES_IDENT)
    except Exception:
        return 0, 0
    if tbl.current_snapshot() is None:
        return 0, 0
    files = pl.from_arrow(tbl.inspect.files().select(["readable_metrics"]))
    if files.height == 0:
        return 0, 0
    bounds = files.select(
        pl.col("readable_metrics").struct.field("batch_id").struct.field("upper_bound").max().alias("max_batch"),
        pl.col("readable_metrics").struct.field("p").struct.field("upper_bound").max().alias("max_p"),
    )
    max_batch = bounds["max_batch"].item()
    max_p = bounds["max_p"].item()
    if max_batch is None or max_p is None:
        return 0, 0
    return int(max_batch) + 1, int(max_p)


def _select_blocks(all_blocks: list[BlockInfo], resume_p: int, start_from: int | None) -> list[BlockInfo]:
    cutoff = start_from if start_from is not None else resume_p
    return [
        b for b in all_blocks
        if b.min_prime is not None and b.max_prime is not None and b.max_prime > cutoff
    ]


def _build_batch_frames(
    block_paths: list[Path],
    *,
    p_floor: int,
    batch_id: int,
) -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    """
    Concat legacy blocks, filter by p > p_floor, compute iceberg-shaped frames.
    Returns (primes_df, decomp_df, source_stats).
    """
    lf = (
        pl.scan_parquet([str(p) for p in block_paths])
        .filter(pl.col("p") > p_floor)
        .unique(subset=["p", "m_k", "n_k", "q_k"])
    )
    src = lf.collect()

    decomp = (
        src.filter(pl.col("q_k") > 0)
        .with_columns(
            pl.col("p").cast(pl.Int64),
            pl.col("m_k").cast(pl.Int32),
            pl.col("n_k").cast(pl.Int32),
            pl.col("q_k").cast(pl.Int64),
            pl.lit(batch_id, dtype=pl.Int32).alias("batch_id"),
        )
        .select(["p", "m_k", "n_k", "q_k", "batch_id"])
    )

    primes = (
        src.group_by("p")
        .agg((pl.col("q_k") > 0).sum().cast(pl.Int32).alias("k"))
        .with_columns(
            pl.col("p").cast(pl.Int64),
            pl.lit(batch_id, dtype=pl.Int32).alias("batch_id"),
        )
        .select(["p", "k", "batch_id"])
    )

    source_stats = {
        "source_rows": src.height,
        "source_distinct_primes": primes.height,
        "source_k_sum": int(primes["k"].sum()),
        "source_p_min": int(primes["p"].min()),
        "source_p_max": int(primes["p"].max()),
    }
    return primes, decomp, source_stats


def _assemble_and_flush(
    cat,
    selected: list[BlockInfo],
    *,
    batch_target: int,
    next_batch_id: int,
    resume_p: int,
    max_batches: int | None,
    stop_at: int | None,
    dry_run: bool,
    journal: JournalWriter,
) -> None:
    pbar = tqdm(total=len(selected), desc="blocks", smoothing=0.1, unit="block")
    idx = 0
    batch_id = next_batch_id
    p_floor = resume_p
    batches_done = 0
    grand_total_primes = 0
    grand_total_decomp = 0

    while idx < len(selected):
        if max_batches is not None and batches_done >= max_batches:
            journal.log("compactor", "max_batches_reached", batches_done=batches_done)
            break
        if stop_at is not None and p_floor >= stop_at:
            journal.log("compactor", "stop_at_reached", p_floor=p_floor, stop_at=stop_at)
            break

        accumulated: list[BlockInfo] = []
        accumulated_primes = 0
        while idx < len(selected) and accumulated_primes < batch_target:
            b = selected[idx]
            accumulated.append(b)
            accumulated_primes += b.num_unique_primes or 0
            idx += 1

        if not accumulated:
            break

        block_paths = [b.path for b in accumulated]
        t0 = time.perf_counter()
        primes_df, decomp_df, src_stats = _build_batch_frames(
            block_paths, p_floor=p_floor, batch_id=batch_id
        )
        t_build = time.perf_counter() - t0

        journal.log(
            "compactor",
            "batch_assembled",
            batch_id=batch_id,
            blocks=len(accumulated),
            first_block=accumulated[0].path.name,
            last_block=accumulated[-1].path.name,
            p_floor=p_floor,
            build_secs=round(t_build, 2),
            **src_stats,
        )

        if dry_run:
            pbar.update(len(accumulated))
            pbar.set_postfix_str(
                f"batch {batch_id} dry-run primes={primes_df.height:,}"
            )
            p_floor = src_stats["source_p_max"]
            batch_id += 1
            batches_done += 1
            continue

        t0 = time.perf_counter()
        result = isch.write_batch(
            cat,
            batch_id=batch_id,
            primes_df=primes_df,
            decomp_df=decomp_df,
        )
        t_write = time.perf_counter() - t0

        grand_total_primes += result.primes_rows
        grand_total_decomp += result.decomp_rows

        journal.log(
            "compactor",
            "batch_committed",
            batch_id=batch_id,
            primes_rows=result.primes_rows,
            decomp_rows=result.decomp_rows,
            primes_files=[p.name for p in result.primes_files],
            decomp_files=[p.name for p in result.decomp_files],
            primes_snapshot_id=result.primes_snapshot_id,
            decomp_snapshot_id=result.decomp_snapshot_id,
            write_secs=round(t_write, 2),
            grand_total_primes=grand_total_primes,
            grand_total_decomp=grand_total_decomp,
        )

        pbar.update(len(accumulated))
        pbar.set_postfix_str(
            f"batch {batch_id} p≤{src_stats['source_p_max']:,} "
            f"primes={result.primes_rows:,} "
            f"decomp={result.decomp_rows:,}"
        )

        p_floor = src_stats["source_p_max"]
        batch_id += 1
        batches_done += 1

    pbar.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--batch-target", type=int, default=DEFAULT_BATCH_TARGET,
                    help="target primes per batch (default: 50_000_000)")
    ap.add_argument("--max-batches", type=int, default=None,
                    help="stop after N batches (testing)")
    ap.add_argument("--warehouse", type=Path, default=None,
                    help="override iceberg warehouse root (for scratch runs)")
    ap.add_argument("--dry-run", action="store_true",
                    help="build + validate frames, skip write_batch")
    ap.add_argument("--start-from", type=int, default=None,
                    help="force starting p (override resume logic)")
    ap.add_argument("--stop-at", type=int, default=None,
                    help="stop once p_floor >= this value (bounded reruns)")
    ap.add_argument("--start-batch-id", type=int, default=None,
                    help="force next batch_id (override resume logic)")
    args = ap.parse_args(argv)

    journal = JournalWriter(name="iceberg_compactor")
    journal.log(
        "compactor", "start",
        batch_target=args.batch_target,
        max_batches=args.max_batches,
        warehouse=str(args.warehouse) if args.warehouse else None,
        dry_run=args.dry_run,
        start_from=args.start_from,
        stop_at=args.stop_at,
        start_batch_id=args.start_batch_id,
    )

    cat = isch.open_catalog(warehouse_root=args.warehouse)
    isch.ensure_tables(cat)

    next_batch_id, resume_p = _resume_state(cat)
    if args.start_batch_id is not None:
        next_batch_id = args.start_batch_id
    all_blocks = sorted_blocks_by_data()
    selected = _select_blocks(all_blocks, resume_p=resume_p, start_from=args.start_from)

    print(
        f"iceberg state: next_batch_id={next_batch_id}, resume_p={resume_p:,}\n"
        f"legacy blocks: {len(all_blocks):,} total, {len(selected):,} selected for compaction\n"
        f"batch target:  {args.batch_target:,} primes"
    )
    if not selected:
        print("nothing to do")
        journal.log("compactor", "done", reason="no_blocks_selected")
        return 0

    try:
        _assemble_and_flush(
            cat,
            selected,
            batch_target=args.batch_target,
            next_batch_id=next_batch_id,
            resume_p=resume_p if args.start_from is None else args.start_from,
            max_batches=args.max_batches,
            stop_at=args.stop_at,
            dry_run=args.dry_run,
            journal=journal,
        )
    except Exception as e:
        journal.log("compactor", "error", error=str(e), type=type(e).__name__)
        raise

    journal.log("compactor", "done", reason="exhausted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
