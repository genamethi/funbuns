"""
l-adic analysis: gap-filling deep dives and SageMath-backed factorization.

This module retains the single-prime deep-dive analysis (gap_filling_analysis)
and SageMath-backed utilities (factorization_lattice, compute_remainder_profile).

Block-by-block processing has moved to remainder.py (incremental accumulators).
Zipf analysis has moved to zipf.py (bounded-memory Counter).
Spectral analysis has moved to spectral.py (obstruction indicator, FFT, clocks).
"""

import polars as pl
import numpy as np
from math import log, floor
from pathlib import Path
from typing import Optional
from .utils import get_data_dir


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
# CLI entry point (--ladic)
# ---------------------------------------------------------------------------

def run_ladic_analysis(limit: Optional[int] = None,
                       verbose: bool = False,
                       filtration_primes: Optional[list[int]] = None):
    """Run l-adic analysis: delegates to remainder.py for block processing,
    zipf.py for Zipf fitting. Kept for backwards compatibility with --ladic flag.
    """
    print("=== l-adic Diophantine Analysis ===\n")
    print("Note: --ladic now delegates to --remainder and --zipf.\n")

    # Run remainder profiling
    from .remainder import run_remainder_analysis
    run_remainder_analysis(limit=limit, verbose=verbose,
                           filtration_primes=filtration_primes)

    # Run Zipf analysis
    print()
    from .zipf import run_zipf_analysis
    run_zipf_analysis(verbose=verbose)
