"""
ℓ-adic Diophantine analysis for prime power partitions.

Analyzes the equation p = 2^m + q^n through the lens of ℓ-adic valuations,
near-miss metrics, and probabilistic number theory. Focuses on understanding
obstructed primes (those with no prime power decomposition) and the arithmetic
structure of composite remainders r = p - 2^m.

Architecture:
  - Block data (data/blocks/) is the source of truth: {p, m_k, n_k, q_k}
  - Remainders r = p - 2^m are computed lazily from blocks
  - Rust plugin (native_expr) handles vectorized arithmetic: omega, big_omega,
    dominant_share, dominant_q, dominant_exp, mu, is_prime_power,
    largest_prime_factor, v_ell
  - SageMath factor() only used for single-prime deep dives (gap_filling)
"""

import polars as pl
import numpy as np
from math import log, sqrt, floor
from pathlib import Path
from typing import Optional
from .utils import get_data_dir


# ---------------------------------------------------------------------------
# Rust plugin check
# ---------------------------------------------------------------------------

_NATIVE_AVAILABLE = None


def _use_native() -> bool:
    global _NATIVE_AVAILABLE
    if _NATIVE_AVAILABLE is None:
        try:
            from .native_expr import _lib_path
            _lib_path()
            _NATIVE_AVAILABLE = True
        except (ImportError, TypeError):
            _NATIVE_AVAILABLE = False
    return _NATIVE_AVAILABLE


# ---------------------------------------------------------------------------
# Lazy remainder generation from block data
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


def _expand_remainders(primes_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Expand a LazyFrame of primes into (p, m, r) triples.

    For each prime p, generates m = 1..floor(log2(p)) and r = p - 2^m,
    filtering to r > 0.
    """
    return (
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


def _apply_native_columns(lf: pl.LazyFrame,
                          filtration_primes: Optional[list[int]] = None,
                          ) -> pl.LazyFrame:
    """Add arithmetic columns via Rust plugin on column 'r'.

    If filtration_primes is given, also adds v_ℓ(r) columns for each ℓ.
    """
    from .native_expr import (omega, big_omega, dominant_share, dominant_q,
                              dominant_exp, mobius, is_prime_power, v_ell)

    cols = [
        omega(pl.col('r')).alias('omega'),
        big_omega(pl.col('r')).alias('big_omega'),
        dominant_q(pl.col('r')).alias('dominant_q'),
        dominant_exp(pl.col('r')).alias('dominant_exp'),
        dominant_share(pl.col('r')).alias('dominant_share'),
        mobius(pl.col('r')).alias('mu'),
        is_prime_power(pl.col('r')).alias('is_prime_power'),
    ]
    if filtration_primes:
        for ell in filtration_primes:
            cols.append(v_ell(pl.col('r'), ell=ell).alias(f'v_{ell}'))
    return lf.with_columns(cols)


def _classify_remainder_expr() -> pl.Expr:
    """Polars expression to classify by omega(r)."""
    return (
        pl.when(pl.col('omega') == 0).then(pl.lit('unit'))
        .when(pl.col('omega') == 1).then(pl.lit('prime_power'))
        .when(pl.col('omega') == 2).then(pl.lit('semiprime'))
        .otherwise(pl.col('omega').cast(pl.Utf8) + pl.lit('-almost-prime'))
        .alias('class')
    )


# ---------------------------------------------------------------------------
# Block-by-block analysis (memory-bounded)
# ---------------------------------------------------------------------------

def _analyze_block(block_path: Path, obstructed_only: bool = True,
                   filtration_primes: Optional[list[int]] = None,
                   ) -> pl.DataFrame:
    """Analyze a single block file: find target primes, expand remainders,
    apply Rust plugin. Returns a DataFrame for this block only.

    For obstructed_only=True, only processes primes with m_k == 0.
    If filtration_primes is given, v_ℓ(r) columns are included.
    """
    primes_lf = pl.scan_parquet(str(block_path))

    if obstructed_only:
        # Obstructed primes have a single row [p, 0, 0, 0]
        primes_lf = (
            primes_lf
            .group_by('p')
            .agg((pl.col('m_k') == 0).all().alias('is_obstructed'))
            .filter(pl.col('is_obstructed'))
            .select('p')
        )
    else:
        primes_lf = primes_lf.select('p').unique()

    remainders = _expand_remainders(primes_lf)
    remainders = _apply_native_columns(remainders, filtration_primes)
    remainders = remainders.with_columns(_classify_remainder_expr())

    return remainders.collect()


def analyze_obstructed_primes(limit: Optional[int] = None,
                              verbose: bool = False) -> pl.DataFrame:
    """Analyze obstructed primes block-by-block, returning per-block near-misses.

    Instead of holding all ~650M remainder rows in memory, processes each block
    independently and keeps only the best near-miss per prime (one row per prime).

    Returns:
        DataFrame with the best near-miss per obstructed prime:
        p, m, r, omega, big_omega, dominant_share, mu, is_prime_power, class.
    """
    if not _use_native():
        raise ImportError("Rust plugin required. Build with: pixi run build-native")

    block_files = sorted(get_data_dir().joinpath("blocks").glob("pp_b*.parquet"))
    if not block_files:
        return pl.DataFrame()

    near_miss_chunks: list[pl.DataFrame] = []
    total_primes = 0

    for i, block_path in enumerate(block_files):
        if verbose:
            print(f"  Block {i + 1}/{len(block_files)}: {block_path.name}", end="", flush=True)

        block_df = _analyze_block(block_path, obstructed_only=True)

        if block_df.height == 0:
            if verbose:
                print(" — no obstructed primes")
            continue

        n_primes = block_df['p'].n_unique()
        total_primes += n_primes

        # Keep only best near-miss per prime (highest dominant_share)
        best = (
            block_df
            .sort('dominant_share', descending=True)
            .group_by('p')
            .first()
        )
        near_miss_chunks.append(best)

        if verbose:
            print(f" — {n_primes} obstructed primes")

        # Free block data
        del block_df

        if limit is not None and total_primes >= limit:
            break

    if not near_miss_chunks:
        return pl.DataFrame()

    df = pl.concat(near_miss_chunks)

    if limit is not None:
        keep_primes = df.select('p').unique().sort('p').head(limit)['p']
        df = df.filter(pl.col('p').is_in(keep_primes))

    return df


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
# ℓ-adic valuation (Python fallback)
# ---------------------------------------------------------------------------

def v_ell(n: int, ell: int) -> int:
    """ℓ-adic valuation: largest k such that ell^k | n."""
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
    """Compute v_ℓ(n) for each ℓ in primes."""
    return [v_ell(n, ell) for ell in primes]


# ---------------------------------------------------------------------------
# Near-miss analysis
# ---------------------------------------------------------------------------

def find_nearest_misses(analysis_df: pl.DataFrame) -> pl.DataFrame:
    """For each obstructed prime, find the remainder with the highest
    dominant_share (closest to being a prime power).
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
    """Distribution of dominant_share values across all remainders."""
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
# ℓ-adic filtration (SageMath Zp for valuations)
# ---------------------------------------------------------------------------

def compute_ladic_filtration(analysis_df: pl.DataFrame,
                             primes: list[int]) -> pl.DataFrame:
    """Add v_ℓ(r) columns using SageMath Zp (PARI-backed).

    Creates columns named 'v_2', 'v_3', 'v_5', etc.
    """
    from sage.all import Zp

    r_list = analysis_df['r'].to_list()
    new_cols = []

    for ell in primes:
        R = Zp(ell, prec=64, type='fixed-mod')
        vals = [int(R(abs(r)).valuation()) if r > 0 else -1 for r in r_list]
        new_cols.append(pl.Series(f'v_{ell}', vals, dtype=pl.Int16))

    return analysis_df.with_columns(new_cols)


def filtration_summary(analysis_df: pl.DataFrame,
                       primes: list[int]) -> pl.DataFrame:
    """Group remainders by their valuation pattern across the given primes."""
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
# Probabilistic number theory
# ---------------------------------------------------------------------------

def erdos_kac_analysis(analysis_df: pl.DataFrame) -> dict:
    """Compare the empirical distribution of omega(r) to the Erdos-Kac
    prediction: omega(n) ~ Normal(log log n, sqrt(log log n)).
    """
    r_vals = analysis_df.filter(pl.col('r') > 2)

    omega_vals = r_vals['omega'].to_numpy().astype(float)
    r_numpy = r_vals['r'].to_numpy().astype(float)

    if len(omega_vals) == 0:
        return {}

    log_log_r = np.log(np.log(np.maximum(r_numpy, 3.0)))
    predicted_mean = np.mean(log_log_r)
    predicted_std = np.mean(np.sqrt(np.maximum(log_log_r, 0.01)))

    empirical_mean = float(np.mean(omega_vals))
    empirical_std = float(np.std(omega_vals))

    max_k = int(np.max(omega_vals))
    deviation_by_k = {}
    from scipy.stats import norm
    for k in range(1, max_k + 1):
        emp_frac = float(np.mean(omega_vals == k))
        pred_frac = float(
            norm.cdf((k + 0.5 - predicted_mean) / predicted_std)
            - norm.cdf((k - 0.5 - predicted_mean) / predicted_std)
        )
        deviation_by_k[k] = {
            'empirical': round(emp_frac, 6),
            'predicted': round(pred_frac, 6),
            'ratio': round(emp_frac / pred_frac, 4) if pred_frac > 1e-10 else None,
        }

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
    k-almost-prime (omega(r) = k), with Hardy-Ramanujan comparison.
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
    """Distribution of dominant_share = max(v_ℓ(r)*log(ℓ)) / log(r)."""
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


def lattice_analysis(analysis_df: pl.DataFrame) -> pl.DataFrame:
    """Compute factorization lattice invariants for each remainder."""
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

def zipf_analysis_q(verbose: bool = False) -> tuple[pl.DataFrame, dict]:
    block_pattern = _block_pattern()

    q_freq = (
        pl.scan_parquet(block_pattern)
        .filter(pl.col('q_k') > 0)
        .group_by('q_k')
        .agg(pl.len().alias('count'))
        .sort('count', descending=True)
        .collect(engine="streaming")
        .with_row_index('rank', offset=1)
    )

    if q_freq.height == 0:
        return pl.DataFrame(), {}

    q_freq = q_freq.with_columns([
        pl.col('rank').cast(pl.Float64).log().alias('log_rank'),
        pl.col('count').cast(pl.Float64).log().alias('log_count'),
    ])

    log_r = q_freq['log_rank'].to_numpy()
    log_c = q_freq['log_count'].to_numpy()

    n = len(log_r)
    sum_x = log_r.sum()
    sum_y = log_c.sum()
    sum_xy = (log_r * log_c).sum()
    sum_xx = (log_r * log_r).sum()

    s_zipf = -(n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
    c_zipf = (sum_y + s_zipf * sum_x) / n

    zipf_pred = np.exp(c_zipf) * q_freq['rank'].to_numpy().astype(float) ** (-s_zipf)

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
        print(f"  Mandelbrot fit: a = {best_a:.4f}, b = {best_b:.2f}, R\u00b2 = {best_r2:.6f}")
        print(f"  Zeta interpretation: P(X=k) = k^{{-{s_zipf:.4f}}} / \u03b6({s_zipf:.4f})")
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

    NOTE: This requires factorization strings. Only available when analysis_df
    contains a 'factorization' column (e.g. from gap_filling or legacy data).
    """
    if 'factorization' not in analysis_df.columns:
        if verbose:
            print("  Factor Zipf skipped: no factorization column "
                  "(available via --ladic-gap for single primes)")
        return pl.DataFrame(), {}

    factor_counts: dict[int, int] = {}
    for fac_str in analysis_df['factorization'].to_list():
        if not fac_str:
            continue
        for term in fac_str.split('\u00b7'):
            if '^' in term:
                base = int(term.split('^')[0])
                exp = int(term.split('^')[1])
            else:
                base = int(term)
                exp = 1
            factor_counts[base] = factor_counts.get(base, 0) + exp

    if not factor_counts:
        return pl.DataFrame(), {}

    ranked = sorted(factor_counts.items(), key=lambda x: -x[1])
    df = pl.DataFrame({
        'rank': list(range(1, len(ranked) + 1)),
        'prime': [p for p, _ in ranked],
        'frequency': [c for _, c in ranked],
    }).with_columns([
        pl.col('rank').cast(pl.Float64).log().alias('log_rank'),
        pl.col('frequency').cast(pl.Float64).log().alias('log_freq'),
    ])

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
        print(f"  Factor Zipf: s = {s:.4f}, R\u00b2 = {r2:.6f}")
        print(f"  {n} distinct primes, {sum(factor_counts.values()):,} total occurrences")
        print(f"  Top 10:")
        for row in df.head(10).iter_rows(named=True):
            print(f"    rank {row['rank']:3d}: p={row['prime']:>6d}, freq={row['frequency']:>8,}")

    return df, params


# ---------------------------------------------------------------------------
# Spectral / harmonic analysis
# ---------------------------------------------------------------------------

def prime_clock_phases(p: int, max_m: Optional[int] = None) -> np.ndarray:
    """Compute the phase of 2^m mod p for m = 1, ..., max_m."""
    if max_m is None:
        max_m = int(floor(log(p) / log(2)))
    phases = np.zeros(max_m)
    tw = 1
    for m in range(1, max_m + 1):
        tw = (tw * 2) % p
        phases[m - 1] = tw / p
    return phases


def obstruction_indicator(verbose: bool = False) -> pl.DataFrame:
    """Build the obstruction indicator function chi(p)."""
    block_pattern = _block_pattern()

    indicator = (
        pl.scan_parquet(block_pattern)
        .group_by('p')
        .agg(
            (pl.col('m_k') == 0).all().cast(pl.UInt8).alias('chi')
        )
        .sort('p')
        .collect(engine="streaming")
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
    """Fourier analysis of the obstruction indicator function."""
    chi = indicator_df['chi'].to_numpy().astype(float)
    log_p = indicator_df['log_p'].to_numpy()

    chi_centered = chi - chi.mean()
    N = len(chi)

    max_freq = 60.0
    freqs = np.linspace(0.5, max_freq, n_frequencies)

    powers = np.zeros(n_frequencies)
    phases = np.zeros(n_frequencies)

    for i, gamma in enumerate(freqs):
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
        sorted_by_power = result.sort('power', descending=True).head(10)
        print(f"  Top 10 spectral peaks in obstruction indicator:")
        print(f"  {'freq':>8s}  {'power':>10s}  note")
        known_zeros = {14.135: '\u03b3\u2081', 21.022: '\u03b3\u2082', 25.011: '\u03b3\u2083',
                       30.425: '\u03b3\u2084', 32.935: '\u03b3\u2085', 37.586: '\u03b3\u2086',
                       40.919: '\u03b3\u2087', 43.327: '\u03b3\u2088', 48.005: '\u03b3\u2089', 49.774: '\u03b3\u2081\u2080'}
        for row in sorted_by_power.iter_rows(named=True):
            f = row['frequency']
            p = row['power']
            note = ''
            for gamma, label in known_zeros.items():
                if abs(f - gamma) < 0.5:
                    note = f'  <- near {label} = {gamma}'
                    break
            print(f"  {f:8.3f}  {p:10.4f}{note}")

    return result


def clock_superposition(primes: list[int], t_range: np.ndarray) -> np.ndarray:
    """Compute the superposition of prime clocks: S(t) = sum_p exp(2*pi*i*t/p)."""
    result = np.zeros(len(t_range), dtype=complex)
    for p in primes:
        result += np.exp(2j * np.pi * t_range / p)
    return np.abs(result) ** 2


def clock_analysis(limit: int = 1000,
                   n_points: int = 2000,
                   verbose: bool = False) -> pl.DataFrame:
    """Run the clock superposition analysis using primes from block data."""
    block_pattern = _block_pattern()

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
        baseline = len(primes)
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

def _ladic_block_out_dir() -> Path:
    """Directory for per-block ladic summaries (resumable intermediate data)."""
    d = get_data_dir() / "ladic_blocks"
    d.mkdir(exist_ok=True)
    return d


def _process_and_save_block(block_path: Path, out_dir: Path,
                            filtration_primes: list[int],
                            verbose: bool) -> Optional[Path]:
    """Process one block: compute all remainders with full arithmetic profile.

    Saves per block:
      - ladic_{name}.parquet: ALL remainders per obstructed prime with
        omega, big_omega, dominant_q, dominant_exp, dominant_share, mu,
        is_prime_power, class, and v_ℓ(r) filtration columns.
      - stats_{name}.parquet: aggregate stats for quick reporting
      - omega_{name}.parquet: omega distribution counts

    Returns ladic output path, or None if no obstructed primes.
    """
    block_name = block_path.stem
    out_path = out_dir / f"ladic_{block_name}.parquet"
    stats_path = out_dir / f"stats_{block_name}.parquet"

    # Skip if already computed (resumable) — recompute if schema is stale
    # Version marker: bump this to force recompute (e.g. after fixing 2^m overflow)
    _CACHE_VERSION_MARKER = 'r_v2'
    if out_path.exists() and stats_path.exists():
        try:
            schema = pl.read_parquet_schema(out_path)
            cached_cols = set(schema.keys())
            need = {'dominant_q', 'dominant_exp', 'v_2', _CACHE_VERSION_MARKER}
            if need.issubset(cached_cols):
                print(" \u2014 cached", flush=True)
                return out_path
            else:
                print(" \u2014 recomputing (stale cache)", end="", flush=True)
        except Exception:
            pass  # corrupt cache, recompute

    block_df = _analyze_block(block_path, obstructed_only=True,
                              filtration_primes=filtration_primes)

    if block_df.height == 0:
        # Write empty marker so we don't reprocess
        pl.DataFrame({
            'n_primes': [0], 'n_rows': [0],
            'ek_n': [0], 'ek_omega_sum': [0.0], 'ek_omega_sq_sum': [0.0],
            'ek_loglogr_sum': [0.0], 'ek_loglogr_sq_sum': [0.0],
        }).write_parquet(stats_path, compression="zstd")
        print(" \u2014 no obstructed primes", flush=True)
        return None

    n_primes = block_df['p'].n_unique()

    # Add Zp filtration columns
    from sage.all import Zp
    for ell in filtration_primes:
        col_name = f'v_{ell}'
        if col_name not in block_df.columns:
            R = Zp(ell, prec=64, type='fixed-mod')
            r_list = block_df['r'].to_list()
            vals = [int(R(abs(r)).valuation()) if r > 0 else -1 for r in r_list]
            block_df = block_df.with_columns(
                pl.Series(col_name, vals, dtype=pl.Int16))

    # Save ALL remainders (full per-remainder data)
    block_df = block_df.with_columns(pl.lit(True).alias(_CACHE_VERSION_MARKER))
    block_df.write_parquet(out_path, compression="zstd", compression_level=1)

    # Omega counts (for quick aggregation without reading full data)
    omega_df = (
        block_df.group_by('omega')
        .agg(pl.len().alias('count'))
    )

    # Erdos-Kac running sums
    ek_subset = block_df.filter(pl.col('r') > 2)
    if ek_subset.height > 0:
        omegas = ek_subset['omega'].to_numpy().astype(float)
        rs = ek_subset['r'].to_numpy().astype(float)
        llr = np.log(np.log(np.maximum(rs, 3.0)))
        ek_row = {
            'ek_n': int(len(omegas)),
            'ek_omega_sum': float(omegas.sum()),
            'ek_omega_sq_sum': float((omegas ** 2).sum()),
            'ek_loglogr_sum': float(llr.sum()),
            'ek_loglogr_sq_sum': float((llr ** 2).sum()),
        }
    else:
        ek_row = {
            'ek_n': 0, 'ek_omega_sum': 0.0, 'ek_omega_sq_sum': 0.0,
            'ek_loglogr_sum': 0.0, 'ek_loglogr_sq_sum': 0.0,
        }

    stats_df = pl.DataFrame({
        'n_primes': [n_primes],
        'n_rows': [block_df.height],
        **{k: [v] for k, v in ek_row.items()},
    })

    stats_df.write_parquet(stats_path, compression="zstd")
    omega_df.write_parquet(
        out_dir / f"omega_{block_name}.parquet", compression="zstd")

    print(f" \u2014 {n_primes} obstructed primes, {block_df.height} rows", flush=True)

    del block_df
    return out_path


def _newest_block_mtime(data_dir: Path) -> float:
    """Return the newest mtime among block files."""
    blocks = data_dir.joinpath("blocks").glob("pp_b*.parquet")
    return max((f.stat().st_mtime for f in blocks), default=0.0)


def run_ladic_analysis(limit: Optional[int] = None,
                       verbose: bool = False,
                       filtration_primes: Optional[list[int]] = None):
    """Run the full l-adic analysis pipeline, block by block.

    Resumable: each block's results are saved to data/ladic_blocks/.
    On re-run, already-processed blocks are skipped.

    Steps:
      1. Zipf / zeta analysis on q_k frequencies (invalidated when blocks change)
      2. Block-by-block: all remainders, stats, filtration (saved per block)
      3. Aggregate saved results and report
    """
    if not _use_native():
        raise ImportError("Rust plugin required. Build with: pixi run build-native")

    if filtration_primes is None:
        filtration_primes = [2, 3, 5, 7, 11, 13]

    data_dir = get_data_dir()
    out_dir = _ladic_block_out_dir()

    print("=== \u2113-adic Diophantine Analysis ===\n")

    # Step 1: Zipf analysis on q_k (invalidate if blocks are newer)
    print("[1/4] Zipf / zeta analysis on q_k frequencies...")
    zipf_path = data_dir / "zipf_q_frequencies.parquet"
    zipf_stale = (not zipf_path.exists()
                  or _newest_block_mtime(data_dir) > zipf_path.stat().st_mtime)
    if not zipf_stale:
        print("  Cached \u2014 loading previous results.")
        q_zipf_df = pl.read_parquet(zipf_path)
    else:
        if zipf_path.exists():
            print("  Block data changed \u2014 recomputing...")
        try:
            q_zipf_df, q_zipf_params = zipf_analysis_q(verbose=verbose)
            if q_zipf_df.height > 0:
                save_analysis(q_zipf_df, "zipf_q_frequencies")
            if q_zipf_params:
                s = q_zipf_params['s_zipf']
                print(f"  Zipf exponent s = {s:.4f}")
                print(f"  Mandelbrot: a={q_zipf_params['a_mandelbrot']:.4f}, "
                      f"b={q_zipf_params['b_mandelbrot']:.2f}, "
                      f"R\u00b2={q_zipf_params['r2_mandelbrot']:.6f}")
        except Exception as e:
            print(f"  Zipf analysis skipped: {e}")
            q_zipf_df = pl.DataFrame()
    print()

    # Step 2: Block-by-block analysis (resumable)
    print("[2/4] Analyzing obstructed primes block by block...")
    block_files = sorted(data_dir.joinpath("blocks").glob("pp_b*.parquet"))
    if not block_files:
        print("No block data found.")
        return

    # Count already-completed blocks (limit is relative to these)
    already_done = 0
    already_stems: list[str] = []
    for block_path in block_files:
        ladic_path = out_dir / f"ladic_{block_path.stem}.parquet"
        stats_path = out_dir / f"stats_{block_path.stem}.parquet"
        if ladic_path.exists() and stats_path.exists():
            try:
                cached_cols = set(pl.read_parquet_schema(ladic_path).keys())
                if {'dominant_q', 'dominant_exp', 'v_2', 'r_v2'}.issubset(cached_cols):
                    s = pl.read_parquet(stats_path)
                    already_done += s['n_primes'][0]
                    already_stems.append(block_path.stem)
                    continue
            except Exception:
                pass
        break  # stop at first uncached block (sequential processing)

    if already_stems:
        print(f"  {len(already_stems)} blocks already cached ({already_done} obstructed primes)")

    new_primes = 0
    processed_stems: list[str] = list(already_stems)
    for i, block_path in enumerate(block_files):
        if block_path.stem in already_stems:
            continue

        print(f"  Block {i + 1}/{len(block_files)}: {block_path.name}", end="", flush=True)

        _process_and_save_block(block_path, out_dir, filtration_primes, verbose)
        processed_stems.append(block_path.stem)

        # Check stats for prime count (limit is relative to new primes)
        stats_path = out_dir / f"stats_{block_path.stem}.parquet"
        if stats_path.exists():
            s = pl.read_parquet(stats_path)
            new_primes += s['n_primes'][0]

        if limit is not None and new_primes >= limit:
            break

    total_primes = already_done + new_primes
    print(f"  {total_primes} total obstructed primes ({new_primes} new)\n")

    # Step 3: Aggregate results from processed blocks only
    print("[3/4] Aggregating results...")

    ladic_files = [out_dir / f"ladic_{stem}.parquet"
                   for stem in processed_stems
                   if (out_dir / f"ladic_{stem}.parquet").exists()]
    stats_files = [out_dir / f"stats_{stem}.parquet"
                   for stem in processed_stems
                   if (out_dir / f"stats_{stem}.parquet").exists()]
    omega_files = [out_dir / f"omega_{stem}.parquet"
                   for stem in processed_stems
                   if (out_dir / f"omega_{stem}.parquet").exists()]

    if not ladic_files:
        print("No obstructed primes found.")
        return

    # Best near-miss per prime -- process block by block to avoid OOM.
    # Each prime lives in exactly one block, so per-block group_by('p')
    # is the only dedup needed; concat is a simple stack.
    print("  Extracting near-misses per block...", flush=True)
    miss_chunks: list[pl.DataFrame] = []
    for f in ladic_files:
        chunk = (
            pl.scan_parquet(str(f))
            .sort('dominant_share', descending=True)
            .group_by('p')
            .first()
            .collect()
        )
        miss_chunks.append(chunk)
    misses = pl.concat(miss_chunks).sort('dominant_share', descending=True)
    del miss_chunks
    print(f"  {misses.height} obstructed primes with near-miss data")

    top = misses.head(10)
    print("  Top 10 nearest misses (highest dominant_share):")
    for row in top.iter_rows(named=True):
        line = f"    p={row['p']}: r={row['r']}, share={row['dominant_share']:.4f}"
        if 'dominant_q' in misses.columns:
            line += f", dominant={row['dominant_q']}^{row['dominant_exp']}"
        print(line)
    print()

    # Aggregate stats (tiny files, no OOM risk)
    all_stats = pl.scan_parquet([str(f) for f in stats_files]).collect()
    total_rows = int(all_stats['n_rows'].sum())
    ek_n = int(all_stats['ek_n'].sum())
    ek_omega_sum = float(all_stats['ek_omega_sum'].sum())
    ek_omega_sq_sum = float(all_stats['ek_omega_sq_sum'].sum())
    ek_loglogr_sum = float(all_stats['ek_loglogr_sum'].sum())
    ek_loglogr_sq_sum = float(all_stats['ek_loglogr_sq_sum'].sum())

    # Omega counts (tiny files, no OOM risk)
    if omega_files:
        omega_agg = (
            pl.scan_parquet([str(f) for f in omega_files])
            .group_by('omega')
            .agg(pl.col('count').sum())
            .sort('omega')
            .collect()
        )
    else:
        omega_agg = pl.DataFrame()

    # Filtration patterns -- aggregate block by block to avoid OOM
    v_cols = [f'v_{ell}' for ell in filtration_primes]
    print("  Aggregating filtration patterns...", flush=True)
    filt_chunks: list[pl.DataFrame] = []
    for f in ladic_files:
        chunk = (
            pl.scan_parquet(str(f))
            .select(v_cols)
            .group_by(v_cols)
            .agg(pl.len().alias('count'))
            .collect()
        )
        filt_chunks.append(chunk)
    filt_agg = (
        pl.concat(filt_chunks)
        .group_by(v_cols)
        .agg(pl.col('count').sum())
        .sort('count', descending=True)
    )
    del filt_chunks

    print(f"  {total_rows} total remainder rows analyzed")

    # Step 4: Report
    print("\n[4/4] Results\n")

    # Erdos-Kac
    print("Erdos-Kac comparison:")
    if ek_n > 0:
        emp_mean = ek_omega_sum / ek_n
        emp_std = sqrt(max(ek_omega_sq_sum / ek_n - emp_mean ** 2, 0))
        pred_mean = ek_loglogr_sum / ek_n
        pred_std = sqrt(max(ek_loglogr_sq_sum / ek_n - pred_mean ** 2, 0.01))
        print(f"  Empirical: mean={emp_mean:.4f}, std={emp_std:.4f}")
        print(f"  Predicted (log log r): mean={pred_mean:.4f}, std={pred_std:.4f}")
        print(f"  Samples: {ek_n}")
    print()

    # Density
    print("Almost-primality density:")
    if omega_agg.height > 0:
        total_positive = int(omega_agg.filter(pl.col('omega') > 0)['count'].sum())
        if total_positive > 0:
            mid_block = block_files[len(block_files) // 2]
            median_p = pl.scan_parquet(str(mid_block)).select('p').first().collect().item()
            median_r = max(median_p // 2, 3)
            log_log_N = log(max(log(max(median_r, 3)), 1))
            log_N = log(max(median_r, 3))

            from math import factorial
            for row in omega_agg.filter(pl.col('omega') > 0).iter_rows(named=True):
                k = row['omega']
                cnt = row['count']
                emp = cnt / total_positive
                hr_pred = (log_log_N ** (k - 1)) / (factorial(k - 1) * log_N) if k >= 1 else 0.0
                ratio = round(emp / hr_pred, 4) if hr_pred > 1e-10 else None
                label = classify_remainder(k)
                print(f"    {label:20s}: {emp:.4f} (HR pred: {hr_pred:.4f}, ratio: {ratio})")
    print()

    # Filtration
    print(f"\u2113-adic filtration for \u2113 \u2208 {filtration_primes}:")
    print(f"  {filt_agg.height} distinct valuation patterns\n")

    # Save aggregated results
    print("Saving aggregated results...")
    save_analysis(misses, "nearest_misses")
    if filt_agg.height > 0:
        save_analysis(filt_agg, "filtration_summary")
    if omega_agg.height > 0:
        save_analysis(omega_agg, "density_almost_prime")

    print(f"\nAll results saved under {data_dir}/")
    print(f"Per-block data in {out_dir}/ (full per-remainder data, delete to recompute)")
    return misses
