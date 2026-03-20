"""
Spectral / harmonic analysis of the obstruction indicator.

Analyses:
  - chi(p): obstruction indicator (1 if p has no 2^m + q^n decomposition, 0 otherwise)
  - FFT of chi against log(p) -- probes connection to zeta zeros
  - Prime clock superposition: S(t) = |sum_p exp(2*pi*i*t/p)|^2

Mathematical context:
  The obstruction indicator chi(p) encodes which primes fail to decompose as
  2^m + q^n. Its Fourier transform against log(p) probes the explicit formula
  from analytic number theory (von Mangoldt). Peaks near the imaginary parts
  of zeta zeros (gamma_1 = 14.135, gamma_2 = 21.022, ...) would indicate
  that the obstruction pattern is correlated with the distribution of primes
  via the zeros of zeta(s).
"""

import numpy as np
import polars as pl
from math import floor, log, sqrt
from pathlib import Path
from typing import Optional

from .utils import get_data_dir, JournalWriter


def _block_pattern() -> str:
    return str(get_data_dir() / "blocks" / "pp_b*.parquet")


def obstruction_indicator(verbose: bool = False) -> pl.DataFrame:
    """Build the obstruction indicator function chi(p).

    Returns DataFrame with columns: p, chi (UInt8), log_p (Float64).
    chi = 1 if p is obstructed (no decomposition), 0 otherwise.
    """
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
    """Fourier analysis of the obstruction indicator function.

    Computes the Lomb-Scargle-style periodogram of chi(p) against log(p),
    looking for peaks near the imaginary parts of zeta zeros.
    """
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


def clock_superposition(primes: list[int], t_range: np.ndarray) -> np.ndarray:
    """Compute the superposition of prime clocks: S(t) = |sum_p exp(2*pi*i*t/p)|^2."""
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
        print(f"  Superposing {len(primes)} prime clocks "
              f"(p from {primes[0]} to {primes[-1]})")

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
            print(f"    t={row['t']:.1f}, power={row['power']:.1f} "
                  f"({ratio:.1f}x baseline)")

    return result


def save_analysis(df: pl.DataFrame, name: str) -> Path:
    """Save analysis DataFrame to parquet in the data directory."""
    data_dir = get_data_dir()
    out = data_dir / f"{name}.parquet"
    df.write_parquet(out)
    print(f"Analysis saved: {out} ({df.height} rows)")
    return out
