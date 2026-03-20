"""
Lightweight remainder profiling with incremental aggregation.

Processes block data one file at a time, computing per-block summaries and
merging into running accumulators. No full remainder profiles are stored --
only compact aggregates. Uses the Rust full_profile function for a single
factorization per remainder row (replacing 13 separate calls).

Running accumulators live in data/remainder_agg/:
  manifest.json          -- which blocks have been processed
  stats.json             -- obstruction rate, Erdos-Kac running sums
  omega.parquet          -- {omega, count}
  near_misses.parquet    -- top 10K by dominant_share
  filtration.parquet     -- {v_2..v_13, count}
"""

import json
import time
from math import log, sqrt
from pathlib import Path
from typing import Optional

import numpy as np
import polars as pl

from .utils import get_data_dir, JournalWriter


_FILTRATION_PRIMES = [2, 3, 5, 7, 11, 13]
_NEAR_MISS_K = 10_000


def _block_files() -> list[Path]:
    return sorted(get_data_dir().joinpath("blocks").glob("pp_b*.parquet"))


def _agg_dir() -> Path:
    d = get_data_dir() / "remainder_agg"
    d.mkdir(exist_ok=True)
    return d


def _load_manifest(agg_dir: Path) -> dict:
    mf = agg_dir / "manifest.json"
    if mf.exists():
        with open(mf) as f:
            return json.load(f)
    return {"blocks": {}, "version": 1}


def _save_manifest(agg_dir: Path, manifest: dict):
    with open(agg_dir / "manifest.json", "w") as f:
        json.dump(manifest, f)


def _load_stats(agg_dir: Path) -> dict:
    sf = agg_dir / "stats.json"
    if sf.exists():
        with open(sf) as f:
            return json.load(f)
    return {
        "total_primes": 0,
        "obstructed_primes": 0,
        "total_remainder_rows": 0,
        "ek_n": 0,
        "ek_omega_sum": 0.0,
        "ek_omega_sq_sum": 0.0,
        "ek_loglogr_sum": 0.0,
        "ek_loglogr_sq_sum": 0.0,
    }


def _save_stats(agg_dir: Path, stats: dict):
    with open(agg_dir / "stats.json", "w") as f:
        json.dump(stats, f, indent=2)


def _load_omega(agg_dir: Path) -> pl.DataFrame:
    p = agg_dir / "omega.parquet"
    if p.exists():
        return pl.read_parquet(p)
    return pl.DataFrame({"omega": pl.Series([], dtype=pl.UInt8),
                         "count": pl.Series([], dtype=pl.UInt32)})


def _save_omega(agg_dir: Path, df: pl.DataFrame):
    df.write_parquet(agg_dir / "omega.parquet", compression="zstd")


def _load_near_misses(agg_dir: Path) -> pl.DataFrame:
    p = agg_dir / "near_misses.parquet"
    if p.exists():
        return pl.read_parquet(p)
    return pl.DataFrame()


def _save_near_misses(agg_dir: Path, df: pl.DataFrame):
    df.write_parquet(agg_dir / "near_misses.parquet", compression="zstd")


def _load_filtration(agg_dir: Path) -> pl.DataFrame:
    p = agg_dir / "filtration.parquet"
    if p.exists():
        return pl.read_parquet(p)
    return pl.DataFrame()


def _save_filtration(agg_dir: Path, df: pl.DataFrame):
    df.write_parquet(agg_dir / "filtration.parquet", compression="zstd")


# ---------------------------------------------------------------------------
# Block analysis using full_profile
# ---------------------------------------------------------------------------

def _analyze_block(block_path: Path,
                   filtration_primes: list[int]) -> pl.DataFrame:
    """Analyze one block: obstructed primes -> expand remainders -> full_profile.

    Returns DataFrame with columns: p, m, r, omega, big_omega, dominant_q,
    dominant_exp, dominant_share, mu, is_prime_power, v_2..v_13.
    """
    from .native_expr import full_profile

    # Find obstructed primes in this block
    primes_lf = (
        pl.scan_parquet(str(block_path))
        .group_by('p')
        .agg((pl.col('m_k') == 0).all().alias('is_obstructed'))
        .filter(pl.col('is_obstructed'))
        .select('p')
    )

    # Expand to (p, m, r) triples
    remainders = (
        primes_lf
        .with_columns(
            pl.col('p').log(base=2).floor().cast(pl.Int32).alias('max_m')
        )
        .with_columns(
            pl.int_ranges(pl.lit(1), pl.col('max_m') + 1).alias('m')
        )
        .explode('m')
        .with_columns(
            (pl.col('p') - pl.lit(2).cast(pl.Int64).pow(pl.col('m'))).alias('r')
        )
        .filter(pl.col('r') > 0)
        .drop('max_m')
    )

    # Apply full_profile (single factorization per row)
    remainders = remainders.with_columns(
        full_profile(pl.col('r'), filtration_primes=filtration_primes)
        .alias('_fp')
    )
    # Unnest the struct into individual columns
    remainders = remainders.with_columns(
        pl.col('_fp').struct.unnest()
    ).drop('_fp')

    return remainders.collect()


def _compute_block_summaries(block_df: pl.DataFrame,
                             filtration_primes: list[int]) -> dict:
    """Compute all per-block summaries from an analyzed block DataFrame.

    Returns dict with keys: n_primes, n_rows, omega_df, near_miss_df,
    filtration_df, ek_stats.
    """
    n_primes = block_df['p'].n_unique()
    n_rows = block_df.height

    # Omega distribution
    omega_df = (
        block_df
        .group_by('omega')
        .agg(pl.len().cast(pl.UInt32).alias('count'))
    )

    # Near-miss: best per prime (highest dominant_share)
    near_miss_df = (
        block_df
        .sort('dominant_share', descending=True)
        .group_by('p')
        .first()
    )

    # Filtration patterns
    v_cols = [f'v_{ell}' for ell in filtration_primes]
    existing_v = [c for c in v_cols if c in block_df.columns]
    if existing_v:
        filtration_df = (
            block_df
            .select(existing_v)
            .group_by(existing_v)
            .agg(pl.len().cast(pl.UInt32).alias('count'))
        )
    else:
        filtration_df = pl.DataFrame()

    # Erdos-Kac running sums
    ek_subset = block_df.filter(pl.col('r') > 2)
    if ek_subset.height > 0:
        omegas = ek_subset['omega'].to_numpy().astype(float)
        rs = ek_subset['r'].to_numpy().astype(float)
        llr = np.log(np.log(np.maximum(rs, 3.0)))
        ek_stats = {
            'ek_n': int(len(omegas)),
            'ek_omega_sum': float(omegas.sum()),
            'ek_omega_sq_sum': float((omegas ** 2).sum()),
            'ek_loglogr_sum': float(llr.sum()),
            'ek_loglogr_sq_sum': float((llr ** 2).sum()),
        }
    else:
        ek_stats = {
            'ek_n': 0, 'ek_omega_sum': 0.0, 'ek_omega_sq_sum': 0.0,
            'ek_loglogr_sum': 0.0, 'ek_loglogr_sq_sum': 0.0,
        }

    return {
        'n_primes': n_primes,
        'n_rows': n_rows,
        'omega_df': omega_df,
        'near_miss_df': near_miss_df,
        'filtration_df': filtration_df,
        'ek_stats': ek_stats,
    }


def _merge_omega(running: pl.DataFrame, block: pl.DataFrame) -> pl.DataFrame:
    if running.height == 0:
        return block
    if block.height == 0:
        return running
    return (
        pl.concat([running, block])
        .group_by('omega')
        .agg(pl.col('count').sum())
        .sort('omega')
    )


def _merge_near_misses(running: pl.DataFrame, block: pl.DataFrame,
                        k: int = _NEAR_MISS_K) -> pl.DataFrame:
    if running.height == 0:
        return block.sort('dominant_share', descending=True).head(k)
    if block.height == 0:
        return running
    return (
        pl.concat([running, block])
        .sort('dominant_share', descending=True)
        .head(k)
    )


def _merge_filtration(running: pl.DataFrame, block: pl.DataFrame) -> pl.DataFrame:
    if running.height == 0:
        return block
    if block.height == 0:
        return running
    v_cols = [c for c in block.columns if c.startswith('v_')]
    return (
        pl.concat([running, block])
        .group_by(v_cols)
        .agg(pl.col('count').sum())
        .sort('count', descending=True)
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_remainder_analysis(limit: Optional[int] = None,
                           verbose: bool = False,
                           filtration_primes: Optional[list[int]] = None):
    """Run incremental remainder profiling across all blocks.

    Processes each unprocessed block, computes summaries, and merges into
    running accumulators in data/remainder_agg/. Resumable: blocks already
    in the manifest are skipped.
    """
    # Check native plugin
    try:
        from .native_expr import _lib_path
        _lib_path()
    except (ImportError, TypeError):
        raise ImportError("Rust plugin required. Build with: "
                          "cd funbuns_native && cargo build --release")

    if filtration_primes is None:
        filtration_primes = list(_FILTRATION_PRIMES)

    journal = JournalWriter()
    agg_dir = _agg_dir()
    block_files = _block_files()

    if not block_files:
        print("No block data found.")
        return

    print("=== Remainder Profiling (incremental) ===\n")

    # Load existing state
    manifest = _load_manifest(agg_dir)
    stats = _load_stats(agg_dir)
    omega_agg = _load_omega(agg_dir)
    near_misses = _load_near_misses(agg_dir)
    filtration_agg = _load_filtration(agg_dir)

    done_blocks = set(manifest.get("blocks", {}).keys())
    pending = [f for f in block_files if f.stem not in done_blocks]

    if done_blocks:
        print(f"  {len(done_blocks)} blocks already processed "
              f"({stats['obstructed_primes']} obstructed primes)")

    if not pending:
        print("  All blocks already processed.")
        _print_report(stats, omega_agg, near_misses, filtration_primes)
        return

    print(f"  {len(pending)} blocks to process\n")

    journal.log("remainder", "run_start",
                pending=len(pending), done=len(done_blocks))
    t0 = time.perf_counter()
    new_primes = 0

    for i, block_path in enumerate(pending):
        print(f"  [{i+1}/{len(pending)}] {block_path.name}", end="", flush=True)
        bt = time.perf_counter()

        block_df = _analyze_block(block_path, filtration_primes)

        if block_df.height == 0:
            print(" -- no obstructed primes", flush=True)
            manifest["blocks"][block_path.stem] = {"n_primes": 0, "n_rows": 0}
            _save_manifest(agg_dir, manifest)
            continue

        summaries = _compute_block_summaries(block_df, filtration_primes)
        del block_df

        # Merge into running accumulators
        n_p = summaries['n_primes']
        n_r = summaries['n_rows']
        stats['obstructed_primes'] += n_p
        stats['total_remainder_rows'] += n_r
        for k in ('ek_n', 'ek_omega_sum', 'ek_omega_sq_sum',
                   'ek_loglogr_sum', 'ek_loglogr_sq_sum'):
            stats[k] += summaries['ek_stats'][k]

        omega_agg = _merge_omega(omega_agg, summaries['omega_df'])
        near_misses = _merge_near_misses(near_misses, summaries['near_miss_df'])
        filtration_agg = _merge_filtration(filtration_agg, summaries['filtration_df'])

        # Update manifest
        elapsed_block = time.perf_counter() - bt
        manifest["blocks"][block_path.stem] = {
            "n_primes": n_p, "n_rows": n_r,
        }
        new_primes += n_p

        print(f" -- {n_p} primes, {n_r} rows, {elapsed_block:.1f}s", flush=True)

        # Periodic save (every 50 blocks)
        if (i + 1) % 50 == 0:
            _save_all(agg_dir, manifest, stats, omega_agg, near_misses, filtration_agg)
            journal.log("remainder", "checkpoint",
                        blocks_done=len(manifest["blocks"]),
                        obstructed=stats['obstructed_primes'])

        if limit is not None and new_primes >= limit:
            break

    # Final save
    # Count total primes from block data
    stats['total_primes'] = sum(
        m.get('n_primes', 0) for m in manifest['blocks'].values()
    )
    _save_all(agg_dir, manifest, stats, omega_agg, near_misses, filtration_agg)

    blocks_done = len(manifest["blocks"]) - len(done_blocks)
    elapsed = time.perf_counter() - t0
    print(f"\n  Processed {blocks_done} blocks in {elapsed:.1f}s "
          f"({new_primes} new obstructed primes)")

    journal.log("remainder", "run_complete",
                blocks_processed=len(pending),
                new_primes=new_primes, elapsed_s=round(elapsed, 2))

    _print_report(stats, omega_agg, near_misses, filtration_primes)


def _save_all(agg_dir, manifest, stats, omega_agg, near_misses, filtration_agg):
    _save_manifest(agg_dir, manifest)
    _save_stats(agg_dir, stats)
    if omega_agg.height > 0:
        _save_omega(agg_dir, omega_agg)
    if near_misses.height > 0:
        _save_near_misses(agg_dir, near_misses)
    if filtration_agg.height > 0:
        _save_filtration(agg_dir, filtration_agg)


def _print_report(stats: dict, omega_agg: pl.DataFrame,
                  near_misses: pl.DataFrame,
                  filtration_primes: list[int]):
    """Print summary report from accumulators."""
    print("\n--- Results ---\n")

    total = stats.get('obstructed_primes', 0)
    rows = stats.get('total_remainder_rows', 0)
    print(f"Obstructed primes: {total:,}")
    print(f"Total remainder rows: {rows:,}")

    # Erdos-Kac
    ek_n = stats.get('ek_n', 0)
    if ek_n > 0:
        emp_mean = stats['ek_omega_sum'] / ek_n
        emp_std = sqrt(max(stats['ek_omega_sq_sum'] / ek_n - emp_mean ** 2, 0))
        pred_mean = stats['ek_loglogr_sum'] / ek_n
        pred_std = sqrt(max(stats['ek_loglogr_sq_sum'] / ek_n - pred_mean ** 2, 0.01))
        print(f"\nErdos-Kac comparison ({ek_n:,} samples):")
        print(f"  Empirical:  mean={emp_mean:.4f}, std={emp_std:.4f}")
        print(f"  Predicted:  mean={pred_mean:.4f}, std={pred_std:.4f}")

    # Omega distribution
    if omega_agg.height > 0:
        total_omega = int(omega_agg['count'].sum())
        print(f"\nAlmost-primality distribution ({total_omega:,} remainders):")
        for row in omega_agg.sort('omega').iter_rows(named=True):
            k = row['omega']
            cnt = row['count']
            pct = 100 * cnt / total_omega if total_omega > 0 else 0
            print(f"  omega={k}: {cnt:>10,} ({pct:5.2f}%)")

    # Near misses
    if near_misses.height > 0:
        print(f"\nTop 10 nearest misses (of {near_misses.height:,} saved):")
        for row in near_misses.head(10).iter_rows(named=True):
            line = f"  p={row['p']}: r={row['r']}, share={row['dominant_share']:.4f}"
            if 'dominant_q' in near_misses.columns:
                line += f", dominant={row['dominant_q']}^{row['dominant_exp']}"
            print(line)

    # Filtration
    print(f"\nFiltration primes: {filtration_primes}")
    print(f"Saved to: data/remainder_agg/")
