"""
Zipf / Mandelbrot fitting for prime factor frequencies in remainders.

Bounded-memory approach: track q values up to Q_MAX (default 1M, ~78K primes).
The tail (q > Q_MAX) is counted but not itemized. The Zipf fit uses only the
tracked entries -- the tail doesn't affect the power-law parameters.

Mathematical context (Satz, arXiv:2403.12773):
  P(X = k) = k^{-s} / zeta(s)
  The fitted Zipf exponent s IS the Riemann zeta parameter. If our exponent
  matches Satz's prediction for random integers, the additive structure from
  2^m subtraction is "invisible" to multiplicative structure. If it differs,
  that's a deep result.
"""

import json
import time
from collections import Counter
from pathlib import Path

import numpy as np
import polars as pl

from .utils import get_data_dir, JournalWriter


def _block_files() -> list[Path]:
    """Sorted list of block parquet files."""
    return sorted(get_data_dir().joinpath("blocks").glob("pp_b*.parquet"))


def scan_q_frequencies(q_max: int = 1_000_000,
                       verbose: bool = False) -> tuple[Counter, int, int]:
    """Stream block files, accumulate q_k frequencies. O(pi(q_max)) memory.

    Returns (q_counts, tail_count, total_hits) where:
      - q_counts: Counter of {q_value: count} for q <= q_max
      - tail_count: total occurrences of q > q_max
      - total_hits: total non-zero q_k rows seen
    """
    journal = JournalWriter()
    q_counts: Counter = Counter()
    tail_count = 0
    total_hits = 0

    block_files = _block_files()
    n_files = len(block_files)

    if n_files == 0:
        print("No block files found.")
        return q_counts, 0, 0

    journal.log("zipf", "scan_start", q_max=q_max, n_blocks=n_files)
    t0 = time.perf_counter()

    for i, f in enumerate(block_files):
        # Read only the q_k column from this block
        qk = pl.read_parquet(f, columns=["q_k"])

        # Filter to hits (q_k > 0), group within this file
        freqs = (
            qk.lazy()
            .filter(pl.col("q_k") > 0)
            .group_by("q_k")
            .agg(pl.len().alias("n"))
            .collect()
        )

        for row in freqs.iter_rows():
            q_val, count = row[0], row[1]
            total_hits += count
            if q_val <= q_max:
                q_counts[q_val] += count
            else:
                tail_count += count

        del qk, freqs

        if verbose and (i + 1) % 200 == 0:
            elapsed = time.perf_counter() - t0
            print(f"  [{i+1}/{n_files}] {len(q_counts):,} tracked q values, "
                  f"tail={tail_count:,}, {elapsed:.1f}s")

    elapsed = time.perf_counter() - t0
    print(f"Scanned {n_files} blocks in {elapsed:.1f}s: "
          f"{len(q_counts):,} tracked q values (q <= {q_max:,}), "
          f"tail={tail_count:,}, total_hits={total_hits:,}")

    journal.log("zipf", "scan_complete",
                n_blocks=n_files, tracked_q=len(q_counts),
                tail_count=tail_count, total_hits=total_hits,
                elapsed_s=round(elapsed, 2))

    return q_counts, tail_count, total_hits


def fit_zipf(q_counts: Counter, verbose: bool = False) -> dict:
    """Fit Zipf and Mandelbrot models to q frequency data.

    Returns dict with keys:
      s_zipf, c_zipf,
      a_mandelbrot, b_mandelbrot, c_mandelbrot, r2_mandelbrot,
      n_tracked, top_k (list of dicts)
    """
    if not q_counts:
        return {}

    # Build rank-frequency array from Counter (sorted by count descending)
    items = q_counts.most_common()
    n = len(items)
    ranks = np.arange(1, n + 1, dtype=np.float64)
    counts = np.array([c for _, c in items], dtype=np.float64)
    q_vals = np.array([q for q, _ in items], dtype=np.int64)

    log_r = np.log(ranks)
    log_c = np.log(counts)

    # Pure Zipf: log(count) = c - s * log(rank)
    sum_x = log_r.sum()
    sum_y = log_c.sum()
    sum_xy = (log_r * log_c).sum()
    sum_xx = (log_r * log_r).sum()

    denom = n * sum_xx - sum_x * sum_x
    s_zipf = -(n * sum_xy - sum_x * sum_y) / denom
    c_zipf = (sum_y + s_zipf * sum_x) / n

    # Mandelbrot: log(count) = c - a * log(rank + b), grid search over b
    best_b = 0.0
    best_r2 = -np.inf
    best_a = s_zipf
    best_c_m = c_zipf

    for b_try in np.arange(0.0, 5.1, 0.25):
        log_rb = np.log(ranks + b_try)
        sx = log_rb.sum()
        sxy = (log_rb * log_c).sum()
        sxx = (log_rb * log_rb).sum()

        d = n * sxx - sx * sx
        a_try = -(n * sxy - sx * sum_y) / d
        c_try = (sum_y + a_try * sx) / n

        pred = c_try - a_try * log_rb
        ss_res = ((log_c - pred) ** 2).sum()
        ss_tot = ((log_c - log_c.mean()) ** 2).sum()
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

        if r2 > best_r2:
            best_r2 = r2
            best_b = b_try
            best_a = a_try
            best_c_m = c_try

    # Top K for reporting
    top_k = [
        {"rank": int(i + 1), "q": int(q_vals[i]), "count": int(counts[i])}
        for i in range(min(30, n))
    ]

    params = {
        "s_zipf": round(float(s_zipf), 6),
        "c_zipf": round(float(c_zipf), 6),
        "a_mandelbrot": round(float(best_a), 6),
        "b_mandelbrot": round(float(best_b), 4),
        "c_mandelbrot": round(float(best_c_m), 6),
        "r2_mandelbrot": round(float(best_r2), 6),
        "n_tracked": n,
        "top_k": top_k,
    }

    if verbose:
        print(f"\n  Zipf fit: s = {s_zipf:.4f}")
        print(f"  Mandelbrot fit: a = {best_a:.4f}, b = {best_b:.2f}, "
              f"R^2 = {best_r2:.6f}")
        print(f"  Zeta interpretation: P(X=k) = k^{{-{s_zipf:.4f}}} "
              f"/ zeta({s_zipf:.4f})")
        print(f"  Tracked {n:,} distinct q values")
        print(f"\n  Top 10 q values by frequency:")
        for entry in top_k[:10]:
            print(f"    rank {entry['rank']:3d}: q={entry['q']:>8,}, "
                  f"count={entry['count']:>10,}")

    return params


def run_zipf_analysis(q_max: int = 1_000_000, verbose: bool = False):
    """CLI entry point for --zipf."""
    journal = JournalWriter()

    print("=== Zipf / Mandelbrot Analysis ===\n")

    q_counts, tail_count, total_hits = scan_q_frequencies(q_max, verbose)
    if not q_counts:
        return

    params = fit_zipf(q_counts, verbose=verbose)
    params["tail_count"] = tail_count
    params["total_hits"] = total_hits
    params["q_max"] = q_max

    # Save parameters
    data_dir = get_data_dir()
    params_file = data_dir / "zipf_params.json"
    with open(params_file, "w") as f:
        json.dump(params, f, indent=2)
    print(f"\nSaved fit parameters to {params_file}")

    # Save rank-frequency table (small: ~78K rows)
    items = q_counts.most_common()
    top_df = pl.DataFrame({
        "rank": list(range(1, len(items) + 1)),
        "q_k": [q for q, _ in items],
        "count": [c for _, c in items],
    }).with_columns([
        pl.col("rank").cast(pl.UInt32),
        pl.col("q_k").cast(pl.Int64),
        pl.col("count").cast(pl.UInt32),
    ])
    top_file = data_dir / "zipf_top_k.parquet"
    top_df.write_parquet(top_file, compression="zstd")
    print(f"Saved rank-frequency table ({len(items):,} rows) to {top_file}")

    # Print summary
    print(f"\n--- Summary ---")
    print(f"  Zipf exponent s = {params['s_zipf']:.4f}")
    print(f"  Mandelbrot (a={params['a_mandelbrot']:.4f}, "
          f"b={params['b_mandelbrot']:.2f}, R^2={params['r2_mandelbrot']:.6f})")
    print(f"  Tracked: {params['n_tracked']:,} distinct q values (q <= {q_max:,})")
    print(f"  Tail (q > {q_max:,}): {tail_count:,} occurrences")
    print(f"  Total hits: {total_hits:,}")

    journal.log("zipf", "fit_complete", **{k: v for k, v in params.items()
                                           if k != "top_k"})
