"""
ℓ-adic Diophantine analysis for prime power partitions.

Analyzes the equation p = 2^m + q^n through the lens of ℓ-adic valuations,
near-miss metrics, and probabilistic number theory. Focuses on understanding
obstructed primes (those with no prime power decomposition) and the arithmetic
structure of composite remainders r = p - 2^m.
"""

import polars as pl
import numpy as np
from math import log, sqrt, floor
from pathlib import Path
from typing import Optional
from .utils import get_data_dir


# ---------------------------------------------------------------------------
# Factorization helpers (SageMath, with optional Rust fast path)
# ---------------------------------------------------------------------------

def _sage_factor(n: int) -> list[tuple[int, int]]:
    """Factor n using SageMath. Returns list of (prime, exponent) pairs."""
    from sage.all import factor, ZZ
    return list(factor(ZZ(n)))


def _sage_is_prime(n: int) -> bool:
    from sage.all import is_prime
    return is_prime(n)


def _has_native() -> bool:
    """Check if the Rust plugin is available."""
    try:
        from . import native_expr  # noqa: F401
        return True
    except ImportError:
        return False


_NATIVE_AVAILABLE = None


def _use_native() -> bool:
    global _NATIVE_AVAILABLE
    if _NATIVE_AVAILABLE is None:
        _NATIVE_AVAILABLE = _has_native()
    return _NATIVE_AVAILABLE


def compute_remainder_columns_native(df: pl.DataFrame) -> pl.DataFrame:
    """Batch-compute omega, big_omega, dominant_share, mu, is_prime_power
    using the Rust plugin on a column named 'r'. Much faster than row-by-row
    SageMath for large DataFrames.

    Falls back to SageMath if the plugin is not built.
    """
    if not _use_native():
        raise ImportError("Rust plugin not available. Build with: pixi run build-native")

    from .native_expr import omega, big_omega, dominant_share, mobius, is_prime_power

    return df.with_columns([
        omega(pl.col('r')).alias('omega'),
        big_omega(pl.col('r')).alias('big_omega'),
        dominant_share(pl.col('r')).alias('dominant_share'),
        mobius(pl.col('r')).alias('mu'),
        is_prime_power(pl.col('r')).alias('is_prime_power'),
    ])


# ---------------------------------------------------------------------------
# Core remainder analysis
# ---------------------------------------------------------------------------

def compute_remainder_profile(p: int, m: int) -> dict:
    """Full factorization and arithmetic profile of r = p - 2^m.

    Returns dict with omega, big_omega, dominant_share, factorization, etc.
    """
    r = p - (1 << m)
    if r <= 0:
        return {'p': p, 'm': m, 'r': r, 'omega': 0, 'big_omega': 0,
                'dominant_q': 0, 'dominant_exp': 0, 'dominant_share': 0.0,
                'mu': 0, 'is_prime_power': False, 'factorization': ''}
    if r == 1:
        return {'p': p, 'm': m, 'r': 1, 'omega': 0, 'big_omega': 0,
                'dominant_q': 1, 'dominant_exp': 0, 'dominant_share': 1.0,
                'mu': 1, 'is_prime_power': False, 'factorization': '1'}

    factors = _sage_factor(r)
    omega = len(factors)
    big_omega = sum(e for _, e in factors)
    log_r = log(r) if r > 1 else 1.0

    # Dominant prime: the one contributing the largest share of log(r)
    shares = [(q, e, e * log(q) / log_r) for q, e in factors]
    dom_q, dom_e, dom_share = max(shares, key=lambda t: t[2])

    # Möbius function: (-1)^omega if squarefree, else 0
    squarefree = all(e == 1 for _, e in factors)
    mu = ((-1) ** omega) if squarefree else 0

    is_pp = (omega == 1)

    fac_str = '·'.join(
        f"{q}^{e}" if e > 1 else str(q) for q, e in factors
    )

    return {
        'p': int(p), 'm': int(m), 'r': int(r),
        'omega': omega, 'big_omega': big_omega,
        'dominant_q': int(dom_q), 'dominant_exp': int(dom_e),
        'dominant_share': round(dom_share, 6),
        'mu': mu, 'is_prime_power': is_pp,
        'factorization': fac_str,
    }


def classify_remainder(omega: int) -> str:
    """Classify by number of distinct prime factors."""
    if omega == 0:
        return 'unit'
    if omega == 1:
        return 'prime_power'
    if omega == 2:
        return 'semiprime'
    return f'{omega}-almost-prime'


# ---------------------------------------------------------------------------
# ℓ-adic valuation
# ---------------------------------------------------------------------------

def v_ell(n: int, ell: int) -> int:
    """ℓ-adic valuation: largest k such that ell^k | n."""
    if n == 0:
        return -1  # conventionally infinite
    if ell < 2:
        raise ValueError(f"ell must be prime, got {ell}")
    k = 0
    while n % ell == 0:
        n //= ell
        k += 1
    return k


def valuation_profile(n: int, primes: list[int]) -> list[int]:
    """Compute v_ℓ(n) for each ℓ in primes."""
    return [v_ell(n, ell) for ell in primes]


# ---------------------------------------------------------------------------
# Batch analysis over existing block data
# ---------------------------------------------------------------------------

ANALYSIS_SCHEMA = {
    'p': pl.Int64,
    'm': pl.Int64,
    'r': pl.Int64,
    'omega': pl.UInt8,
    'big_omega': pl.UInt8,
    'dominant_q': pl.Int64,
    'dominant_exp': pl.UInt8,
    'dominant_share': pl.Float64,
    'mu': pl.Int8,
    'is_prime_power': pl.Boolean,
    'factorization': pl.Utf8,
}


def analyze_obstructed_primes(limit: Optional[int] = None,
                              verbose: bool = False) -> pl.DataFrame:
    """Analyze all obstructed primes (zero-row entries) from block data.

    For each obstructed prime p, enumerates all valid m in [1, floor(log2(p))],
    computes r = p - 2^m, and returns a full factorization profile for every
    remainder.  This reveals the "nearest miss" — the remainder closest to
    being a prime power.

    Args:
        limit: Max number of obstructed primes to analyze (None = all).
        verbose: Print progress.

    Returns:
        DataFrame with ANALYSIS_SCHEMA columns plus 'class' (Utf8).
    """
    data_dir = get_data_dir()
    block_pattern = str(data_dir / "blocks" / "pp_b*.parquet")

    # Obstructed primes: those with a zero-row (m_k == 0)
    obstructed = (
        pl.scan_parquet(block_pattern)
        .filter(pl.col('m_k') == 0)
        .select('p')
        .unique()
        .sort('p')
    )
    if limit is not None:
        obstructed = obstructed.head(limit)

    obstructed_primes = obstructed.collect()['p'].to_list()

    if verbose:
        print(f"Found {len(obstructed_primes)} obstructed primes to analyze")

    # Build (p, m, r) triples first
    pm_rows = []
    for p in obstructed_primes:
        max_m = int(floor(log(p) / log(2)))
        for m in range(1, max_m + 1):
            pm_rows.append({'p': int(p), 'm': int(m), 'r': int(p - (1 << m))})

    if not pm_rows:
        return pl.DataFrame(schema=ANALYSIS_SCHEMA)

    # Try native fast path: compute omega/big_omega/dominant_share/mu/is_prime_power
    # in Rust on the full column at once, then fill in remaining columns via SageMath
    if _use_native():
        if verbose:
            print(f"  Using Rust plugin for batch valuation ({len(pm_rows)} remainders)")
        base_df = pl.DataFrame(pm_rows)
        base_df = compute_remainder_columns_native(base_df)
        # Still need factorization string and dominant_q/dominant_exp from SageMath
        fac_strs = []
        dom_qs = []
        dom_exps = []
        for i, r in enumerate(base_df['r'].to_list()):
            if r <= 1:
                fac_strs.append(str(r) if r == 1 else '')
                dom_qs.append(r)
                dom_exps.append(0)
            else:
                factors = _sage_factor(r)
                fac_strs.append('·'.join(
                    f"{q}^{e}" if e > 1 else str(q) for q, e in factors
                ))
                log_r = log(r)
                shares = [(int(q), int(e), e * log(q) / log_r) for q, e in factors]
                dq, de, _ = max(shares, key=lambda t: t[2])
                dom_qs.append(dq)
                dom_exps.append(de)
            if verbose and (i + 1) % 5000 == 0:
                print(f"  factorized {i + 1}/{len(pm_rows)}")

        base_df = base_df.with_columns([
            pl.Series('dominant_q', dom_qs, dtype=pl.Int64),
            pl.Series('dominant_exp', dom_exps, dtype=pl.UInt8),
            pl.Series('factorization', fac_strs, dtype=pl.Utf8),
        ])
        df = base_df.select(list(ANALYSIS_SCHEMA.keys()))
    else:
        # Pure SageMath fallback
        rows: list[dict] = []
        for i, pm in enumerate(pm_rows):
            profile = compute_remainder_profile(pm['p'], pm['m'])
            rows.append(profile)
            if verbose and (i + 1) % 500 == 0:
                print(f"  analyzed {i + 1}/{len(pm_rows)} remainders")
        df = pl.DataFrame(rows, schema=ANALYSIS_SCHEMA)

    df = df.with_columns(
        pl.col('omega').map_elements(classify_remainder, return_dtype=pl.Utf8).alias('class')
    )
    return df


def analyze_all_remainders(limit: Optional[int] = None,
                           verbose: bool = False) -> pl.DataFrame:
    """Analyze remainders for ALL primes (both obstructed and successful).

    Useful for comparing the distribution of omega(r) across the full dataset.
    Only processes primes up to `limit` count.
    """
    data_dir = get_data_dir()
    block_pattern = str(data_dir / "blocks" / "pp_b*.parquet")

    primes_lf = (
        pl.scan_parquet(block_pattern)
        .select('p')
        .unique()
        .sort('p')
    )
    if limit is not None:
        primes_lf = primes_lf.head(limit)

    prime_list = primes_lf.collect()['p'].to_list()

    if verbose:
        print(f"Analyzing remainders for {len(prime_list)} primes")

    rows: list[dict] = []
    for i, p in enumerate(prime_list):
        max_m = int(floor(log(p) / log(2)))
        for m in range(1, max_m + 1):
            rows.append(compute_remainder_profile(p, m))
        if verbose and (i + 1) % 1000 == 0:
            print(f"  {i + 1}/{len(prime_list)}")

    if not rows:
        return pl.DataFrame(schema=ANALYSIS_SCHEMA)

    df = pl.DataFrame(rows, schema=ANALYSIS_SCHEMA)
    df = df.with_columns(
        pl.col('omega').map_elements(classify_remainder, return_dtype=pl.Utf8).alias('class')
    )
    return df


# ---------------------------------------------------------------------------
# Near-miss analysis
# ---------------------------------------------------------------------------

def find_nearest_misses(analysis_df: pl.DataFrame) -> pl.DataFrame:
    """For each obstructed prime, find the remainder with the highest
    dominant_share (closest to being a prime power).

    Returns one row per prime: the "best" remainder.
    """
    return (
        analysis_df
        .filter(~pl.col('is_prime_power'))
        .sort('dominant_share', descending=True)
        .group_by('p')
        .first()
        .sort('dominant_share', descending=True)
    )


def near_miss_distribution(analysis_df: pl.DataFrame) -> pl.DataFrame:
    """Distribution of dominant_share values across all remainders.

    Bins dominant_share into [0, 0.1), [0.1, 0.2), ..., [0.9, 1.0], [1.0].
    """
    return (
        analysis_df
        .with_columns(
            (pl.col('dominant_share') * 10).floor().cast(pl.UInt8)
            .clip(0, 10).alias('share_bin')
        )
        .group_by('share_bin')
        .agg(pl.len().alias('count'))
        .sort('share_bin')
    )


# ---------------------------------------------------------------------------
# ℓ-adic filtration
# ---------------------------------------------------------------------------

def compute_ladic_filtration(analysis_df: pl.DataFrame,
                             primes: list[int]) -> pl.DataFrame:
    """Add v_ℓ(r) columns for each prime ℓ in the list.

    Creates columns named 'v_2', 'v_3', 'v_5', etc.
    """
    r_values = analysis_df['r'].to_list()

    for ell in primes:
        col_name = f'v_{ell}'
        vals = [v_ell(r, ell) if r > 0 else -1 for r in r_values]
        analysis_df = analysis_df.with_columns(
            pl.Series(col_name, vals, dtype=pl.Int16)
        )

    return analysis_df


def filtration_summary(analysis_df: pl.DataFrame,
                       primes: list[int]) -> pl.DataFrame:
    """Group remainders by their valuation pattern across the given primes.

    Returns counts for each distinct (v_2, v_3, v_5, ...) pattern.
    """
    v_cols = [f'v_{ell}' for ell in primes]
    existing = [c for c in v_cols if c in analysis_df.columns]
    if not existing:
        return pl.DataFrame({'pattern': [], 'count': []})

    return (
        analysis_df
        .group_by(existing)
        .agg(pl.len().alias('count'))
        .sort('count', descending=True)
    )


# ---------------------------------------------------------------------------
# Local obstruction detection
# ---------------------------------------------------------------------------

def local_obstruction_check(p: int, moduli: Optional[list[int]] = None) -> dict:
    """Check if p = 2^m + q^n has solutions mod each modulus.

    For each modulus M, checks all m in [1, ord_2(M)] and tests whether
    any residue class r = p - 2^m mod M can be a prime power mod M.

    Returns dict {modulus: has_local_solution}.
    """
    if moduli is None:
        moduli = [3, 5, 7, 8, 9, 11, 13, 16, 25]

    results = {}
    for M in moduli:
        # Compute all possible 2^m mod M (cyclic)
        powers_mod = set()
        pw = 1
        for _ in range(M + 1):  # 2^m mod M cycles with period | phi(M)
            pw = (pw * 2) % M
            powers_mod.add(pw)

        # For each 2^m mod M, check if p - 2^m mod M can be q^n for some prime q, n>=1
        has_solution = False
        for tw in powers_mod:
            r_mod = (p - tw) % M
            if r_mod <= 1:
                continue
            # Check: is r_mod a prime power residue mod M?
            # i.e., does there exist prime q < M and n >= 1 with q^n ≡ r_mod (mod M)?
            for q in range(2, M):
                if not _sage_is_prime(q):
                    continue
                qn = q
                for _ in range(1, 64):
                    if qn % M == r_mod:
                        has_solution = True
                        break
                    qn = (qn * q) % M
                    if qn == 0:
                        break
                if has_solution:
                    break
            if has_solution:
                break

        results[M] = has_solution

    return results


def find_congruence_obstructions(obstructed_primes: list[int],
                                 moduli: Optional[list[int]] = None,
                                 verbose: bool = False) -> pl.DataFrame:
    """For a set of obstructed primes, find congruence classes that
    concentrate obstructions.

    Returns DataFrame with columns: modulus, residue, obstructed_count, total_in_class.
    """
    if moduli is None:
        moduli = [3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 24, 30]

    rows = []
    for M in moduli:
        # Count obstructed primes in each residue class mod M
        residue_counts: dict[int, int] = {}
        for p in obstructed_primes:
            res = p % M
            residue_counts[res] = residue_counts.get(res, 0) + 1

        for res, count in sorted(residue_counts.items()):
            rows.append({
                'modulus': M,
                'residue': res,
                'obstructed_count': count,
                'fraction': round(count / len(obstructed_primes), 4),
            })

    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Gap-filling: deep analysis of a single obstructed prime
# ---------------------------------------------------------------------------

def gap_filling_analysis(p: int) -> pl.DataFrame:
    """For a single obstructed prime, enumerate ALL m values, factorize
    each r = p - 2^m, and characterize the full obstruction surface.

    Returns DataFrame with one row per m value, sorted by dominant_share desc.
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
# Probabilistic number theory
# ---------------------------------------------------------------------------

def erdos_kac_analysis(analysis_df: pl.DataFrame) -> dict:
    """Compare the empirical distribution of omega(r) to the Erdős–Kac
    prediction: omega(n) ~ Normal(log log n, sqrt(log log n)).

    Returns:
        dict with keys: empirical_mean, empirical_std,
        predicted_mean, predicted_std, ks_statistic, deviation_by_k
    """
    r_vals = analysis_df.filter(pl.col('r') > 2)

    omega_vals = r_vals['omega'].to_numpy().astype(float)
    r_numpy = r_vals['r'].to_numpy().astype(float)

    if len(omega_vals) == 0:
        return {}

    # Predicted mean and std from Erdős–Kac
    log_log_r = np.log(np.log(np.maximum(r_numpy, 3.0)))
    predicted_mean = np.mean(log_log_r)
    predicted_std = np.mean(np.sqrt(np.maximum(log_log_r, 0.01)))

    empirical_mean = float(np.mean(omega_vals))
    empirical_std = float(np.std(omega_vals))

    # Per-k deviation: P(omega = k) empirical vs predicted normal
    max_k = int(np.max(omega_vals))
    deviation_by_k = {}
    from scipy.stats import norm
    for k in range(1, max_k + 1):
        emp_frac = float(np.mean(omega_vals == k))
        # Predicted P(omega = k) ~ Phi((k+0.5 - mu)/sigma) - Phi((k-0.5 - mu)/sigma)
        pred_frac = float(
            norm.cdf((k + 0.5 - predicted_mean) / predicted_std)
            - norm.cdf((k - 0.5 - predicted_mean) / predicted_std)
        )
        deviation_by_k[k] = {
            'empirical': round(emp_frac, 6),
            'predicted': round(pred_frac, 6),
            'ratio': round(emp_frac / pred_frac, 4) if pred_frac > 1e-10 else None,
        }

    # KS statistic (simple version)
    sorted_omega = np.sort(omega_vals)
    n = len(sorted_omega)
    ecdf = np.arange(1, n + 1) / n
    theoretical_cdf = norm.cdf((sorted_omega - predicted_mean) / predicted_std)
    ks_stat = float(np.max(np.abs(ecdf - theoretical_cdf)))

    return {
        'empirical_mean': round(empirical_mean, 4),
        'empirical_std': round(empirical_std, 4),
        'predicted_mean': round(predicted_mean, 4),
        'predicted_std': round(predicted_std, 4),
        'ks_statistic': round(ks_stat, 6),
        'n_samples': len(omega_vals),
        'deviation_by_k': deviation_by_k,
    }


def density_by_almost_primality(analysis_df: pl.DataFrame) -> pl.DataFrame:
    """For k = 1, 2, 3, ..., compute the fraction of remainders that are
    k-almost-prime (omega(r) = k).

    Includes the Hardy–Ramanujan comparison: the expected fraction of
    integers near N with omega = k is approximately:
      (log log N)^{k-1} / ((k-1)! * log N)
    """
    r_positive = analysis_df.filter(pl.col('r') > 1)
    total = r_positive.height
    if total == 0:
        return pl.DataFrame()

    median_r = r_positive['r'].median()
    log_log_N = log(max(log(max(median_r, 3)), 1))
    log_N = log(max(median_r, 3))

    counts = (
        r_positive
        .group_by('omega')
        .agg(pl.len().alias('count'))
        .sort('omega')
    )

    from math import factorial
    rows = []
    for row in counts.iter_rows(named=True):
        k = row['omega']
        cnt = row['count']
        emp = cnt / total
        # Hardy–Ramanujan density approximation
        if k >= 1:
            hr_pred = (log_log_N ** (k - 1)) / (factorial(k - 1) * log_N)
        else:
            hr_pred = 0.0
        rows.append({
            'k': k,
            'count': cnt,
            'empirical_fraction': round(emp, 6),
            'hardy_ramanujan_pred': round(hr_pred, 6),
            'ratio': round(emp / hr_pred, 4) if hr_pred > 1e-10 else None,
        })

    return pl.DataFrame(rows)


def log_depth_distribution(analysis_df: pl.DataFrame) -> pl.DataFrame:
    """Distribution of dominant_share = max(v_ℓ(r)·log(ℓ)) / log(r).

    Prime powers have dominant_share = 1.0. The distribution of this metric
    reveals how "spread out" the factorizations are — a measure of distance
    from the prime-power manifold in the factorization lattice.
    """
    r_positive = analysis_df.filter(pl.col('r') > 1)

    return (
        r_positive
        .select('dominant_share', 'omega', 'is_prime_power')
        .with_columns(
            (pl.col('dominant_share') * 20).round(0).cast(pl.UInt8)
            .clip(0, 20).alias('share_vingtile')
        )
        .group_by('share_vingtile')
        .agg([
            pl.len().alias('count'),
            pl.col('omega').mean().alias('mean_omega'),
            pl.col('is_prime_power').sum().alias('prime_power_count'),
        ])
        .sort('share_vingtile')
    )


# ---------------------------------------------------------------------------
# Factorization lattice (ported from ppparts concepts)
# ---------------------------------------------------------------------------

def factorization_lattice(n: int) -> dict:
    """Compute the divisor lattice structure of n.

    Returns dict with:
      - factors: list of (p, e) pairs
      - divisor_count: tau(n)
      - lattice_dimension: omega(n) — the "rank" of the lattice
      - lattice_points: number of points = product(e_i + 1)
      - betti_0: connected components (always 1 for n > 1)
      - euler_char: Euler characteristic of the order complex
    """
    if n <= 1:
        return {'factors': [], 'divisor_count': 1, 'lattice_dimension': 0,
                'lattice_points': 1, 'betti_0': 1, 'euler_char': 1}

    factors = _sage_factor(n)
    exponents = [e for _, e in factors]
    omega = len(factors)
    tau = 1
    for e in exponents:
        tau *= (e + 1)

    # Euler characteristic of the order complex of the divisor poset
    # For a product of chains, chi = product(e_i) * (-1)^(sum(e_i) - omega)
    # This is the reduced Euler characteristic of the proper part
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


def lattice_analysis(analysis_df: pl.DataFrame) -> pl.DataFrame:
    """Compute factorization lattice invariants for each remainder.

    Adds columns: divisor_count, lattice_dim, euler_char.
    """
    r_vals = analysis_df['r'].to_list()

    divisor_counts = []
    lattice_dims = []
    euler_chars = []

    for r in r_vals:
        if r <= 1:
            divisor_counts.append(1)
            lattice_dims.append(0)
            euler_chars.append(1)
        else:
            lat = factorization_lattice(r)
            divisor_counts.append(lat['divisor_count'])
            lattice_dims.append(lat['lattice_dimension'])
            euler_chars.append(lat['euler_char'])

    return analysis_df.with_columns([
        pl.Series('divisor_count', divisor_counts, dtype=pl.Int32),
        pl.Series('lattice_dim', lattice_dims, dtype=pl.UInt8),
        pl.Series('euler_char', euler_chars, dtype=pl.Int32),
    ])


# ---------------------------------------------------------------------------
# Zipf / Zeta distribution analysis
# ---------------------------------------------------------------------------

def zipf_analysis_q(verbose: bool = False) -> pl.DataFrame:
    """Zipf analysis on q_k frequencies from block data.

    Ranks prime bases q by how often they appear as the base in p = 2^m + q^n,
    then fits Zipf (P(rank=k) ~ k^{-s}) and Mandelbrot (~ (k+b)^{-a}) models.

    The Zipf distribution is the discrete case of the zeta distribution:
      P(X = k) = k^{-s} / ζ(s)
    where ζ(s) is the Riemann zeta function. Deviations of the fitted s from
    the "random integer" baseline (s ≈ 1 for Zipf on factor frequencies)
    reveal how the additive constraint (subtracting 2^m from primes) distorts
    the multiplicative structure.

    Returns DataFrame with columns: rank, q, count, log_rank, log_count,
        zipf_pred, mandelbrot_pred
    """
    data_dir = get_data_dir()
    block_pattern = str(data_dir / "blocks" / "pp_b*.parquet")

    q_freq = (
        pl.scan_parquet(block_pattern)
        .filter(pl.col('q_k') > 0)
        .group_by('q_k')
        .agg(pl.len().alias('count'))
        .sort('count', descending=True)
        .collect(streaming=True)
        .with_row_index('rank', offset=1)
    )

    if q_freq.height == 0:
        return pl.DataFrame()

    # Add log columns for linear regression in log-log space
    q_freq = q_freq.with_columns([
        pl.col('rank').cast(pl.Float64).log().alias('log_rank'),
        pl.col('count').cast(pl.Float64).log().alias('log_count'),
    ])

    # Fit Zipf: log(count) = -s * log(rank) + c
    # Simple OLS in log-log space
    log_r = q_freq['log_rank'].to_numpy()
    log_c = q_freq['log_count'].to_numpy()

    n = len(log_r)
    sum_x = log_r.sum()
    sum_y = log_c.sum()
    sum_xy = (log_r * log_c).sum()
    sum_xx = (log_r * log_r).sum()

    s_zipf = -(n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
    c_zipf = (sum_y + s_zipf * sum_x) / n

    # Zipf predictions
    zipf_pred = np.exp(c_zipf) * q_freq['rank'].to_numpy().astype(float) ** (-s_zipf)

    # Fit Mandelbrot: log(count) = -a * log(rank + b) + c
    # Grid search over b, then linear fit for a and c
    best_b = 0.0
    best_r2 = -np.inf
    best_a = s_zipf
    best_c_m = c_zipf

    for b_try in np.arange(0.0, 5.1, 0.25):
        log_rb = np.log(q_freq['rank'].to_numpy().astype(float) + b_try)
        sx = log_rb.sum()
        sy = sum_y
        sxy = (log_rb * log_c).sum()
        sxx = (log_rb * log_rb).sum()

        a_try = -(n * sxy - sx * sy) / (n * sxx - sx * sx)
        c_try = (sy + a_try * sx) / n

        pred = c_try - a_try * log_rb
        ss_res = ((log_c - pred) ** 2).sum()
        ss_tot = ((log_c - log_c.mean()) ** 2).sum()
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0

        if r2 > best_r2:
            best_r2 = r2
            best_b = b_try
            best_a = a_try
            best_c_m = c_try

    mandelbrot_pred = np.exp(best_c_m) * (q_freq['rank'].to_numpy().astype(float) + best_b) ** (-best_a)

    q_freq = q_freq.with_columns([
        pl.Series('zipf_pred', zipf_pred),
        pl.Series('mandelbrot_pred', mandelbrot_pred),
    ])

    if verbose:
        print(f"  Zipf fit: s = {s_zipf:.4f} (pure Zipf exponent)")
        print(f"  Mandelbrot fit: a = {best_a:.4f}, b = {best_b:.2f}, R² = {best_r2:.6f}")
        print(f"  Zeta interpretation: P(X=k) = k^{{-{s_zipf:.4f}}} / ζ({s_zipf:.4f})")
        print(f"  Top 10 q values by frequency:")
        for row in q_freq.head(10).iter_rows(named=True):
            print(f"    rank {row['rank']:3d}: q={row['q_k']:>8d}, count={row['count']:>10,}")

    return q_freq, {'s_zipf': round(s_zipf, 6),
                    'a_mandelbrot': round(best_a, 6),
                    'b_mandelbrot': round(best_b, 4),
                    'r2_mandelbrot': round(best_r2, 6),
                    'n_primes': n}


def zipf_analysis_factors(analysis_df: pl.DataFrame,
                          verbose: bool = False) -> tuple[pl.DataFrame, dict]:
    """Zipf analysis on prime factor frequencies in composite remainders.

    For each remainder r = p - 2^m, its prime factors contribute to a frequency
    table. This is Satz's "text" analogy: remainders are "sentences" and their
    prime factors are "words." The frequency-rank relationship reveals whether
    the additive structure (subtracting 2^m) preserves or distorts the
    Zipf law that holds for prime factors of generic integers.

    Stratifying by obstructed vs. non-obstructed primes tests whether
    the obstruction mechanism has a Zipf signature.
    """
    # Parse factorizations to count prime factor frequencies
    factor_counts: dict[int, int] = {}
    for fac_str in analysis_df['factorization'].to_list():
        if not fac_str:
            continue
        for term in fac_str.split('·'):
            if '^' in term:
                base = int(term.split('^')[0])
                exp = int(term.split('^')[1])
            else:
                base = int(term)
                exp = 1
            factor_counts[base] = factor_counts.get(base, 0) + exp

    if not factor_counts:
        return pl.DataFrame(), {}

    # Build ranked DataFrame
    ranked = sorted(factor_counts.items(), key=lambda x: -x[1])
    df = pl.DataFrame({
        'rank': list(range(1, len(ranked) + 1)),
        'prime': [p for p, _ in ranked],
        'frequency': [c for _, c in ranked],
    }).with_columns([
        pl.col('rank').cast(pl.Float64).log().alias('log_rank'),
        pl.col('frequency').cast(pl.Float64).log().alias('log_freq'),
    ])

    # Fit Zipf exponent
    log_r = df['log_rank'].to_numpy()
    log_f = df['log_freq'].to_numpy()
    n = len(log_r)

    sum_x = log_r.sum()
    sum_y = log_f.sum()
    sum_xy = (log_r * log_f).sum()
    sum_xx = (log_r * log_r).sum()

    s = -(n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
    c = (sum_y + s * sum_x) / n

    zipf_pred = np.exp(c) * df['rank'].to_numpy().astype(float) ** (-s)
    df = df.with_columns(pl.Series('zipf_pred', zipf_pred))

    # R² for quality
    pred_log = c - s * log_r
    ss_res = ((log_f - pred_log) ** 2).sum()
    ss_tot = ((log_f - log_f.mean()) ** 2).sum()
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    params = {
        's_zipf': round(float(s), 6),
        'r2': round(r2, 6),
        'n_distinct_factors': n,
        'total_factor_occurrences': sum(factor_counts.values()),
    }

    if verbose:
        print(f"  Factor Zipf: s = {s:.4f}, R² = {r2:.6f}")
        print(f"  {n} distinct primes, {sum(factor_counts.values()):,} total occurrences")
        print(f"  Top 10:")
        for row in df.head(10).iter_rows(named=True):
            print(f"    rank {row['rank']:3d}: p={row['prime']:>6d}, freq={row['frequency']:>8,}")

    return df, params


# ---------------------------------------------------------------------------
# Spectral / harmonic analysis ("wall of clocks")
# ---------------------------------------------------------------------------

def prime_clock_phases(p: int, max_m: Optional[int] = None) -> np.ndarray:
    """Compute the phase of 2^m mod p for m = 1, ..., max_m.

    This is the "clock hand position" for prime p at each power of 2.
    The phase is 2^m / p (fractional part), so it lives in [0, 1).
    When the phase lands near a value where p - 2^m is a prime power,
    the clock has "struck" a resonance.
    """
    if max_m is None:
        max_m = int(floor(log(p) / log(2)))
    phases = np.zeros(max_m)
    tw = 1
    for m in range(1, max_m + 1):
        tw = (tw * 2) % p
        phases[m - 1] = tw / p
    return phases


def obstruction_indicator(verbose: bool = False) -> pl.DataFrame:
    """Build the obstruction indicator function χ(p).

    χ(p) = 1 if p is obstructed (no prime power decomposition), 0 otherwise.
    Returns DataFrame with columns: p, chi, log_p.
    """
    data_dir = get_data_dir()
    block_pattern = str(data_dir / "blocks" / "pp_b*.parquet")

    indicator = (
        pl.scan_parquet(block_pattern)
        .group_by('p')
        .agg(
            (pl.col('m_k') == 0).all().cast(pl.UInt8).alias('chi')
        )
        .sort('p')
        .collect(streaming=True)
        .with_columns(
            pl.col('p').cast(pl.Float64).log().alias('log_p')
        )
    )

    if verbose:
        total = indicator.height
        obstructed = indicator.filter(pl.col('chi') == 1).height
        print(f"  Obstruction indicator: {obstructed}/{total} primes obstructed "
              f"({100 * obstructed / total:.2f}%)")

    return indicator


def spectral_analysis_obstruction(indicator_df: pl.DataFrame,
                                  n_frequencies: int = 200,
                                  verbose: bool = False) -> pl.DataFrame:
    """Fourier analysis of the obstruction indicator function.

    Computes the discrete Fourier transform of χ(p) evaluated at
    log(p) (the natural scale for prime distribution). Peaks in the
    power spectrum at frequency γ correspond to oscillatory terms
    in the explicit formula — i.e., nontrivial zeros ρ = 1/2 + iγ
    of the Riemann zeta function.

    If obstructed primes correlate with specific zeros, those
    frequencies will have anomalous power.

    Returns DataFrame: frequency, power, phase.
    """
    chi = indicator_df['chi'].to_numpy().astype(float)
    log_p = indicator_df['log_p'].to_numpy()

    # Subtract mean to get oscillatory part
    chi_centered = chi - chi.mean()
    N = len(chi)

    # Test frequencies spanning the range where low-lying zeta zeros live
    # The first few imaginary parts of nontrivial zeros:
    # γ₁ ≈ 14.135, γ₂ ≈ 21.022, γ₃ ≈ 25.011, γ₄ ≈ 30.425, γ₅ ≈ 32.935
    max_freq = 60.0
    freqs = np.linspace(0.5, max_freq, n_frequencies)

    powers = np.zeros(n_frequencies)
    phases = np.zeros(n_frequencies)

    # Compute the "Fourier coefficient" at each frequency γ:
    #   F(γ) = Σ_p χ(p) · exp(-i γ log p) / √N
    # This directly probes the explicit formula oscillations.
    for i, gamma in enumerate(freqs):
        # Complex exponential evaluated at log(p) with frequency gamma
        exp_vals = np.exp(-1j * gamma * log_p)
        coeff = np.sum(chi_centered * exp_vals) / sqrt(N)
        powers[i] = float(np.abs(coeff) ** 2)
        phases[i] = float(np.angle(coeff))

    result = pl.DataFrame({
        'frequency': freqs.tolist(),
        'power': powers.tolist(),
        'phase': phases.tolist(),
    })

    if verbose:
        # Find peaks
        sorted_by_power = result.sort('power', descending=True).head(10)
        print(f"  Top 10 spectral peaks in obstruction indicator:")
        print(f"  {'freq':>8s}  {'power':>10s}  note")
        known_zeros = {14.135: 'γ₁', 21.022: 'γ₂', 25.011: 'γ₃',
                       30.425: 'γ₄', 32.935: 'γ₅', 37.586: 'γ₆',
                       40.919: 'γ₇', 43.327: 'γ₈', 48.005: 'γ₉', 49.774: 'γ₁₀'}
        for row in sorted_by_power.iter_rows(named=True):
            f = row['frequency']
            p = row['power']
            # Check proximity to known zeros
            note = ''
            for gamma, label in known_zeros.items():
                if abs(f - gamma) < 0.5:
                    note = f'  <- near {label} = {gamma}'
                    break
            print(f"  {f:8.3f}  {p:10.4f}{note}")

    return result


def clock_superposition(primes: list[int], t_range: np.ndarray) -> np.ndarray:
    """Compute the superposition of prime clocks at continuous "time" t.

    S(t) = Σ_p exp(2πi t / p)

    This is the sum of unit-frequency oscillators, one per prime,
    with period p. The modulus |S(t)| measures constructive interference.
    When |S(t)| is large, the clock hands are aligned. When |S(t)| is small,
    they're spread out (destructive interference).

    The connection to ζ: consider S(t) evaluated at t such that t/p ≈ k
    for many primes simultaneously — this is related to the von Mangoldt
    explicit formula's oscillatory terms.

    Args:
        primes: List of primes to superpose.
        t_range: Array of t values to evaluate.

    Returns:
        Array of |S(t)|² values (power of the superposition).
    """
    result = np.zeros(len(t_range), dtype=complex)
    for p in primes:
        result += np.exp(2j * np.pi * t_range / p)
    return np.abs(result) ** 2


def clock_analysis(limit: int = 1000,
                   n_points: int = 2000,
                   verbose: bool = False) -> pl.DataFrame:
    """Run the clock superposition analysis using primes from block data.

    Evaluates S(t) over a range and identifies constructive interference
    peaks. These peaks correspond to values of t where many prime
    "clocks" align — the resonances of the prime distribution.

    Returns DataFrame: t, power, log_power.
    """
    data_dir = get_data_dir()
    block_pattern = str(data_dir / "blocks" / "pp_b*.parquet")

    primes = (
        pl.scan_parquet(block_pattern)
        .select('p')
        .unique()
        .sort('p')
        .head(limit)
        .collect()['p']
        .to_list()
    )

    if verbose:
        print(f"  Superposing {len(primes)} prime clocks (p from {primes[0]} to {primes[-1]})")

    # Evaluate over a range where interesting structure appears
    # Use log-spaced points to capture both fine and coarse structure
    max_p = primes[-1]
    t_range = np.linspace(1, max_p * 2, n_points)

    power = clock_superposition(primes, t_range)

    result = pl.DataFrame({
        't': t_range.tolist(),
        'power': power.tolist(),
        'log_power': np.log(np.maximum(power, 1e-10)).tolist(),
        'normalized_power': (power / len(primes)).tolist(),
    })

    if verbose:
        top = result.sort('power', descending=True).head(5)
        baseline = len(primes)  # expected power for random phases
        print(f"  Baseline (random phases): {baseline:.1f}")
        print(f"  Top 5 constructive interference peaks:")
        for row in top.iter_rows(named=True):
            ratio = row['power'] / baseline
            print(f"    t={row['t']:.1f}, power={row['power']:.1f} ({ratio:.1f}x baseline)")

    return result


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
# CLI entry point
# ---------------------------------------------------------------------------

def run_ladic_analysis(limit: Optional[int] = None,
                       verbose: bool = False,
                       filtration_primes: Optional[list[int]] = None):
    """Run the full ℓ-adic analysis pipeline.

    1. Identify obstructed primes from block data
    2. Factorize all remainders
    3. Compute near-miss scores
    4. ℓ-adic filtration
    5. Erdős–Kac comparison
    6. Zipf / zeta distribution analysis
    7. Save results
    """
    if filtration_primes is None:
        filtration_primes = [2, 3, 5, 7, 11, 13]

    print("=== ℓ-adic Diophantine Analysis ===\n")

    # Step 1: Zipf analysis on q_k from block data (no factorization needed)
    print("[1/7] Zipf / zeta analysis on q_k frequencies...")
    try:
        q_zipf_df, q_zipf_params = zipf_analysis_q(verbose=verbose)
        if q_zipf_params:
            s = q_zipf_params['s_zipf']
            print(f"  Zipf exponent s = {s:.4f}")
            print(f"  Zeta interpretation: P(rank=k) = k^{{-{s:.4f}}} / ζ({s:.4f})")
            print(f"  Mandelbrot: a={q_zipf_params['a_mandelbrot']:.4f}, "
                  f"b={q_zipf_params['b_mandelbrot']:.2f}, "
                  f"R²={q_zipf_params['r2_mandelbrot']:.6f}")
    except Exception as e:
        print(f"  Zipf analysis skipped: {e}")
        q_zipf_df = pl.DataFrame()
        q_zipf_params = {}
    print()

    # Step 2: Analyze obstructed primes
    print("[2/7] Analyzing obstructed primes...")
    analysis = analyze_obstructed_primes(limit=limit, verbose=verbose)
    print(f"  {analysis.height} remainder rows from "
          f"{analysis['p'].n_unique()} obstructed primes\n")

    if analysis.height == 0:
        print("No obstructed primes found in block data.")
        return

    # Step 3: Near-miss analysis
    print("[3/7] Finding nearest misses...")
    misses = find_nearest_misses(analysis)
    top = misses.head(10)
    print("  Top 10 nearest misses (highest dominant_share):")
    for row in top.iter_rows(named=True):
        print(f"    p={row['p']}: r={row['r']} = {row['factorization']}, "
              f"share={row['dominant_share']:.4f}")
    print()

    # Step 4: ℓ-adic filtration
    print(f"[4/7] Computing ℓ-adic filtration for ℓ ∈ {filtration_primes}...")
    analysis = compute_ladic_filtration(analysis, filtration_primes)
    filt_summary = filtration_summary(analysis, filtration_primes)
    print(f"  {filt_summary.height} distinct valuation patterns\n")

    # Step 5: Probabilistic number theory
    print("[5/7] Erdős–Kac comparison...")
    try:
        ek = erdos_kac_analysis(analysis)
        print(f"  Empirical: mean={ek.get('empirical_mean')}, "
              f"std={ek.get('empirical_std')}")
        print(f"  Predicted: mean={ek.get('predicted_mean')}, "
              f"std={ek.get('predicted_std')}")
        print(f"  KS statistic: {ek.get('ks_statistic')}")
    except Exception as e:
        print(f"  Erdős–Kac skipped (needs scipy): {e}")
        ek = {}
    print()

    # Step 6: Density by almost-primality
    print("[6/7] Almost-primality density...")
    density = density_by_almost_primality(analysis)
    if density.height > 0:
        for row in density.iter_rows(named=True):
            k = row['k']
            emp = row['empirical_fraction']
            hr = row['hardy_ramanujan_pred']
            ratio = row.get('ratio', '—')
            label = classify_remainder(k)
            print(f"    {label:20s}: {emp:.4f} (HR pred: {hr:.4f}, ratio: {ratio})")
    print()

    # Step 7: Zipf on factor frequencies in composite remainders
    print("[7/7] Zipf analysis on prime factor frequencies in remainders...")
    try:
        fac_zipf_df, fac_zipf_params = zipf_analysis_factors(analysis, verbose=verbose)
        if fac_zipf_params:
            print(f"  Factor Zipf exponent s = {fac_zipf_params['s_zipf']:.4f}, "
                  f"R² = {fac_zipf_params['r2']:.6f}")
            if q_zipf_params:
                delta = abs(q_zipf_params['s_zipf'] - fac_zipf_params['s_zipf'])
                print(f"  Exponent delta (q_k vs factors): {delta:.4f}")
                if delta < 0.1:
                    print("  -> Exponents match: additive structure preserves Zipf law")
                else:
                    print("  -> Exponents differ: p - 2^m distorts multiplicative structure")
    except Exception as e:
        print(f"  Factor Zipf skipped: {e}")
        fac_zipf_df = pl.DataFrame()
        fac_zipf_params = {}
    print()

    # Save
    out = save_analysis(analysis)
    save_analysis(misses, "nearest_misses")
    save_analysis(filt_summary, "filtration_summary")
    save_analysis(density, "density_almost_prime")
    if q_zipf_df.height > 0:
        save_analysis(q_zipf_df, "zipf_q_frequencies")
    if fac_zipf_df.height > 0:
        save_analysis(fac_zipf_df, "zipf_factor_frequencies")
    print(f"\nAll results saved under {get_data_dir()}/")
    return analysis
