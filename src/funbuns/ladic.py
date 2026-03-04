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
# Factorization helpers (SageMath)
# ---------------------------------------------------------------------------

def _sage_factor(n: int) -> list[tuple[int, int]]:
    """Factor n using SageMath. Returns list of (prime, exponent) pairs."""
    from sage.all import factor, ZZ
    return list(factor(ZZ(n)))


def _sage_is_prime(n: int) -> bool:
    from sage.all import is_prime
    return is_prime(n)


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

    rows: list[dict] = []
    for i, p in enumerate(obstructed_primes):
        max_m = int(floor(log(p) / log(2)))
        for m in range(1, max_m + 1):
            profile = compute_remainder_profile(p, m)
            rows.append(profile)
        if verbose and (i + 1) % 500 == 0:
            print(f"  analyzed {i + 1}/{len(obstructed_primes)} primes")

    if not rows:
        return pl.DataFrame(schema=ANALYSIS_SCHEMA)

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
    6. Save results
    """
    if filtration_primes is None:
        filtration_primes = [2, 3, 5, 7, 11, 13]

    print("=== ℓ-adic Diophantine Analysis ===\n")

    # Step 1: Analyze obstructed primes
    print("[1/5] Analyzing obstructed primes...")
    analysis = analyze_obstructed_primes(limit=limit, verbose=verbose)
    print(f"  {analysis.height} remainder rows from "
          f"{analysis['p'].n_unique()} obstructed primes\n")

    if analysis.height == 0:
        print("No obstructed primes found in block data.")
        return

    # Step 2: Near-miss analysis
    print("[2/5] Finding nearest misses...")
    misses = find_nearest_misses(analysis)
    top = misses.head(10)
    print("  Top 10 nearest misses (highest dominant_share):")
    for row in top.iter_rows(named=True):
        print(f"    p={row['p']}: r={row['r']} = {row['factorization']}, "
              f"share={row['dominant_share']:.4f}")
    print()

    # Step 3: ℓ-adic filtration
    print(f"[3/5] Computing ℓ-adic filtration for ℓ ∈ {filtration_primes}...")
    analysis = compute_ladic_filtration(analysis, filtration_primes)
    filt_summary = filtration_summary(analysis, filtration_primes)
    print(f"  {filt_summary.height} distinct valuation patterns\n")

    # Step 4: Probabilistic number theory
    print("[4/5] Erdős–Kac comparison...")
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

    # Step 5: Density by almost-primality
    print("[5/5] Almost-primality density...")
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

    # Save
    out = save_analysis(analysis)
    save_analysis(misses, "nearest_misses")
    save_analysis(filt_summary, "filtration_summary")
    save_analysis(density, "density_almost_prime")
    print(f"\nAll results saved under {get_data_dir()}/")
    return analysis
