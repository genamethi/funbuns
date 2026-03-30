"""
l-adic analysis: valuation profiles and gap-filling deep dives.

The core analysis computes v_ell(r, l) for small filtration primes l on
remainders r = p - 2^m of obstructed primes. This uses only trial division
(Rust v_ell plugin), NOT full factorization.

Full factorization / remainder profiling lives in remainder.py.
Zipf analysis lives in zipf.py.
Spectral analysis lives in spectral.py.
"""

import json
import time
import traceback
import polars as pl
import numpy as np
from math import log, floor
from pathlib import Path
from typing import Optional
from .utils import get_data_dir, JournalWriter


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def _block_pattern() -> str:
    return str(get_data_dir() / "blocks" / "pp_b*.parquet")


def _obstructed_primes_lazy() -> pl.LazyFrame:
    """Lazy frame of obstructed prime values (those with no decomposition)."""
    return (
        pl.scan_parquet(_block_pattern())
        .group_by('p')
        .agg((pl.col('m_k') == 0).all().alias('is_obstructed'))
        .filter(pl.col('is_obstructed'))
        .select('p')
    )


def _all_primes_lazy() -> pl.LazyFrame:
    """Lazy frame of all unique primes from block data."""
    return (
        pl.scan_parquet(_block_pattern())
        .select('p')
        .unique()
    )


def classify_remainder(omega: int) -> str:
    """Classify by number of distinct prime factors (scalar version)."""
    if omega == 0:
        return 'unit'
    if omega == 1:
        return 'prime_power'
    if omega == 2:
        return 'semiprime'
    return f'{omega}-almost-prime'


# ---------------------------------------------------------------------------
# l-adic valuation (Python fallback)
# ---------------------------------------------------------------------------

def v_ell(n: int, ell: int) -> int:
    """l-adic valuation: largest k such that ell^k | n."""
    if n == 0:
        return -1
    if ell < 2:
        raise ValueError(f"ell must be prime, got {ell}")
    k = 0
    while n % ell == 0:
        n //= ell
        k += 1
    return k


def valuation_profile(n: int, primes: list[int]) -> list[int]:
    """Compute v_ell(n) for each ell in primes."""
    return [v_ell(n, ell) for ell in primes]


# ---------------------------------------------------------------------------
# Gap-filling: deep analysis of a single obstructed prime (uses SageMath)
# ---------------------------------------------------------------------------

def compute_remainder_profile(p: int, m: int) -> dict:
    """Full factorization and arithmetic profile of r = p - 2^m."""
    r = p - (1 << m)
    if r <= 0:
        return {'p': p, 'm': m, 'r': r, 'omega': 0, 'big_omega': 0,
                'dominant_q': 0, 'dominant_exp': 0, 'dominant_share': 0.0,
                'mu': 0, 'is_prime_power': False, 'factorization': ''}
    if r == 1:
        return {'p': p, 'm': m, 'r': 1, 'omega': 0, 'big_omega': 0,
                'dominant_q': 1, 'dominant_exp': 0, 'dominant_share': 1.0,
                'mu': 1, 'is_prime_power': False, 'factorization': '1'}

    from sage.all import factor, ZZ
    factors = list(factor(ZZ(r)))
    om = len(factors)
    big_om = sum(e for _, e in factors)
    log_r = log(r) if r > 1 else 1.0

    shares = [(q, e, e * log(q) / log_r) for q, e in factors]
    dom_q, dom_e, dom_share = max(shares, key=lambda t: t[2])

    squarefree = all(e == 1 for _, e in factors)
    mu = ((-1) ** om) if squarefree else 0
    is_pp = (om == 1)

    fac_str = '\u00b7'.join(
        f"{q}^{e}" if e > 1 else str(q) for q, e in factors
    )

    return {
        'p': int(p), 'm': int(m), 'r': int(r),
        'omega': om, 'big_omega': big_om,
        'dominant_q': int(dom_q), 'dominant_exp': int(dom_e),
        'dominant_share': round(dom_share, 6),
        'mu': mu, 'is_prime_power': is_pp,
        'factorization': fac_str,
    }


def gap_filling_analysis(p: int) -> pl.DataFrame:
    """For a single obstructed prime, enumerate ALL m values, factorize
    each r = p - 2^m, and characterize the full obstruction surface.
    """
    max_m = int(floor(log(p) / log(2)))
    rows = []
    for m in range(1, max_m + 1):
        profile = compute_remainder_profile(p, m)
        profile['class'] = classify_remainder(profile['omega'])
        rows.append(profile)

    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(rows)
    return df.sort('dominant_share', descending=True)


# ---------------------------------------------------------------------------
# Factorization lattice (uses SageMath)
# ---------------------------------------------------------------------------

def factorization_lattice(n: int) -> dict:
    """Compute the divisor lattice structure of n."""
    if n <= 1:
        return {'factors': [], 'divisor_count': 1, 'lattice_dimension': 0,
                'lattice_points': 1, 'betti_0': 1, 'euler_char': 1}

    from sage.all import factor, ZZ
    factors = list(factor(ZZ(n)))
    exponents = [e for _, e in factors]
    omega = len(factors)
    tau = 1
    for e in exponents:
        tau *= (e + 1)

    prod_e = 1
    sum_e = 0
    for e in exponents:
        prod_e *= e
        sum_e += e
    euler_char = prod_e * ((-1) ** (sum_e - omega)) if omega > 0 else 1

    return {
        'factors': [(int(p), int(e)) for p, e in factors],
        'divisor_count': tau,
        'lattice_dimension': omega,
        'lattice_points': tau,
        'betti_0': 1,
        'euler_char': euler_char,
    }


# ---------------------------------------------------------------------------
# Persistence / I/O
# ---------------------------------------------------------------------------

def save_analysis(df: pl.DataFrame, name: str = "ladic_analysis") -> Path:
    """Save analysis DataFrame to parquet in the data directory."""
    data_dir = get_data_dir()
    out = data_dir / f"{name}.parquet"
    df.write_parquet(out)
    print(f"Analysis saved: {out} ({df.height} rows)")
    return out


def load_analysis(name: str = "ladic_analysis") -> pl.DataFrame:
    """Load a previously saved analysis DataFrame."""
    data_dir = get_data_dir()
    path = data_dir / f"{name}.parquet"
    return pl.read_parquet(path)


# ---------------------------------------------------------------------------
# Block-level l-adic valuation analysis (uses Rust v_ell only, no factorize)
# ---------------------------------------------------------------------------

_FILTRATION_PRIMES = [2, 3, 5, 7, 11, 13]


def _block_files() -> list[Path]:
    return sorted(get_data_dir().joinpath("blocks").glob("pp_b*.parquet"))


def _ladic_agg_dir() -> Path:
    d = get_data_dir() / "ladic_agg"
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


def _analyze_block_ladic(block_path: Path,
                         filtration_primes: list[int]) -> pl.DataFrame:
    """Analyze one block: obstructed primes -> remainders -> v_ell only.

    Returns DataFrame with columns: p, m, r, v_2, v_3, v_5, v_7, v_11, v_13.
    No factorization, no GMP primality testing.
    """
    from .native_expr import v_ell as v_ell_expr

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

    # Apply v_ell for each filtration prime (trial division only)
    remainders = remainders.with_columns([
        v_ell_expr(pl.col('r'), ell=ell).alias(f'v_{ell}')
        for ell in filtration_primes
    ])

    return remainders.collect()


def _merge_filtration(running: pl.DataFrame, block: pl.DataFrame) -> pl.DataFrame:
    """Merge filtration pattern counts."""
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
# CLI entry point (--ladic)
# ---------------------------------------------------------------------------

def run_ladic_analysis(limit: Optional[int] = None,
                       verbose: bool = False,
                       filtration_primes: Optional[list[int]] = None):
    """Run l-adic valuation analysis across all blocks.

    Computes v_ell(r, l) for filtration primes on remainders r = p - 2^m
    of obstructed primes. Uses Rust v_ell (trial division only), NOT
    full_profile / factorize. Incremental with manifest-based resume.
    """
    if filtration_primes is None:
        filtration_primes = list(_FILTRATION_PRIMES)

    journal = JournalWriter()
    agg_dir = _ladic_agg_dir()
    block_files = _block_files()

    if not block_files:
        block_dir = get_data_dir().joinpath("blocks")
        journal.log("ladic", "no_block_data",
                    data_dir=str(get_data_dir()),
                    block_dir=str(block_dir),
                    block_dir_exists=block_dir.exists())
        print(f"No block data found. (checked {block_dir})")
        return

    print("=== l-adic Valuation Analysis ===\n")

    # Load existing state
    manifest = _load_manifest(agg_dir)
    done_blocks = set(manifest.get("blocks", {}).keys())

    # Load running filtration accumulator
    filt_path = agg_dir / "filtration.parquet"
    filtration_agg = pl.read_parquet(filt_path) if filt_path.exists() else pl.DataFrame()

    pending = [f for f in block_files if f.stem not in done_blocks]

    total_primes = sum(m.get('n_primes', 0) for m in manifest.get('blocks', {}).values())
    if done_blocks:
        print(f"  {len(done_blocks)} blocks already processed "
              f"({total_primes} obstructed primes)")

    if not pending:
        print("  All blocks already processed.")
        _print_ladic_report(filtration_agg, filtration_primes, total_primes)
        return

    print(f"  {len(pending)} blocks to process\n")

    journal.log("ladic", "run_start",
                pending=len(pending), done=len(done_blocks),
                n_block_files=len(block_files))
    t0 = time.perf_counter()
    new_primes = 0
    current_block = None

    try:
        for i, block_path in enumerate(pending):
            current_block = block_path.name
            print(f"  [{i+1}/{len(pending)}] {current_block}", end="", flush=True)
            bt = time.perf_counter()

            block_df = _analyze_block_ladic(block_path, filtration_primes)

            n_p = block_df['p'].n_unique() if block_df.height > 0 else 0
            n_r = block_df.height

            if n_r == 0:
                print(" -- no obstructed primes", flush=True)
                manifest["blocks"][block_path.stem] = {"n_primes": 0, "n_rows": 0}
                _save_manifest(agg_dir, manifest)
                continue

            # Aggregate filtration patterns from this block
            v_cols = [f'v_{ell}' for ell in filtration_primes]
            block_filt = (
                block_df
                .select(v_cols)
                .group_by(v_cols)
                .agg(pl.len().cast(pl.UInt32).alias('count'))
            )
            del block_df

            filtration_agg = _merge_filtration(filtration_agg, block_filt)

            elapsed_block = time.perf_counter() - bt
            manifest["blocks"][block_path.stem] = {"n_primes": n_p, "n_rows": n_r}
            new_primes += n_p

            print(f" -- {n_p} primes, {n_r} rows, {elapsed_block:.1f}s", flush=True)

            # Periodic save (every 50 blocks)
            if (i + 1) % 50 == 0:
                _save_manifest(agg_dir, manifest)
                if filtration_agg.height > 0:
                    filtration_agg.write_parquet(filt_path, compression="zstd")
                journal.log("ladic", "checkpoint",
                            blocks_done=len(manifest["blocks"]),
                            primes=total_primes + new_primes,
                            current_block=current_block)

            if limit is not None and new_primes >= limit:
                break

    except Exception as exc:
        try:
            _save_manifest(agg_dir, manifest)
            if filtration_agg.height > 0:
                filtration_agg.write_parquet(filt_path, compression="zstd")
        except Exception:
            pass
        elapsed = time.perf_counter() - t0
        journal.log("ladic", "run_error",
                    error=str(exc),
                    error_type=type(exc).__name__,
                    traceback=traceback.format_exc(),
                    current_block=current_block,
                    blocks_done=len(manifest["blocks"]),
                    elapsed_s=round(elapsed, 2))
        raise

    # Final save
    _save_manifest(agg_dir, manifest)
    if filtration_agg.height > 0:
        filtration_agg.write_parquet(filt_path, compression="zstd")

    elapsed = time.perf_counter() - t0
    blocks_done = len(manifest["blocks"]) - len(done_blocks)
    print(f"\n  Processed {blocks_done} blocks in {elapsed:.1f}s "
          f"({new_primes} new obstructed primes)")

    journal.log("ladic", "run_complete",
                blocks_processed=blocks_done,
                new_primes=new_primes, elapsed_s=round(elapsed, 2))

    _print_ladic_report(filtration_agg, filtration_primes, total_primes + new_primes)


def _print_ladic_report(filtration_agg: pl.DataFrame,
                        filtration_primes: list[int],
                        total_primes: int):
    """Print summary of l-adic filtration patterns."""
    print(f"\n--- l-adic Filtration Report ---\n")
    print(f"Filtration primes: {filtration_primes}")
    print(f"Total obstructed primes: {total_primes:,}")

    if filtration_agg.height == 0:
        return

    total_rows = int(filtration_agg['count'].sum())
    print(f"Total remainder rows: {total_rows:,}")
    print(f"Distinct valuation patterns: {filtration_agg.height:,}")

    # Top patterns by frequency
    top = filtration_agg.sort('count', descending=True).head(20)
    print(f"\nTop 20 valuation patterns:")
    v_cols = [c for c in top.columns if c.startswith('v_')]
    for row in top.iter_rows(named=True):
        pattern = ", ".join(f"v_{c.split('_')[1]}={row[c]}" for c in v_cols)
        print(f"  ({pattern}): {row['count']:>12,}")

    # Per-prime valuation distributions
    for ell in filtration_primes:
        col = f'v_{ell}'
        if col in filtration_agg.columns:
            dist = (
                filtration_agg
                .group_by(col)
                .agg(pl.col('count').sum())
                .sort(col)
            )
            print(f"\n  v_{ell} distribution:")
            for row in dist.iter_rows(named=True):
                print(f"    v_{ell}={row[col]:>2}: {row['count']:>12,}")

    print(f"\nSaved to: {get_data_dir() / 'ladic_agg'}/")

