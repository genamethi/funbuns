#!/usr/bin/env python3
"""Verify prime coverage across all block files.

Checks:
1. Total unique primes vs prime_pi(max_p) — is data complete?
2. Specific prime lookups — does a given prime exist anywhere?
3. Contiguity around overlap regions — are "gap" primes in other blocks?

Usage:
    pixi run python scripts/verify_coverage.py
    pixi run python scripts/verify_coverage.py --check 29949157297
    pixi run python scripts/verify_coverage.py --sample-region 29940000000 29960000000
"""

import argparse
import glob
import os
import re
import sys
from pathlib import Path

import polars as pl
from tqdm import tqdm


def parse_max_prime(path):
    m = re.search(r'pp_b\d+_p(\d+)\.parquet', os.path.basename(path))
    return int(m.group(1)) if m else 0


def all_blocks():
    from funbuns.utils import get_data_dir
    bdir = get_data_dir() / "blocks"
    paths = sorted(glob.glob(str(bdir / "pp_b*.parquet")), key=parse_max_prime)
    return paths


def global_count(blocks):
    """Count total unique primes across all blocks, compare to prime_pi."""
    from sage.all import prime_pi

    # Get global min/max from first/last blocks
    first = pl.read_parquet(blocks[0], columns=['p'])
    last = pl.read_parquet(blocks[-1], columns=['p'])
    global_min = first['p'].min()
    global_max = last['p'].max()

    print(f"Blocks: {len(blocks)}")
    print(f"Global range: [{global_min:,}, {global_max:,}]")

    # Count unique primes across ALL blocks using streaming
    # Can't hold all primes in memory. Instead, sum per-block unique counts
    # and subtract shared primes at overlap boundaries.
    per_block_sum = 0
    shared_total = 0
    prev_primes = None
    prev_name = None

    for b in tqdm(blocks, desc="Counting", unit="block"):
        cur = set(pl.read_parquet(b, columns=['p'])['p'].unique().to_list())
        per_block_sum += len(cur)

        if prev_primes is not None:
            shared = len(prev_primes & cur)
            if shared > 0:
                shared_total += shared
                print(f"  Shared: {os.path.basename(prev_name)} ∩ "
                      f"{os.path.basename(b)} = {shared:,}")

        prev_primes = cur
        prev_name = b

    unique_total = per_block_sum - shared_total

    # Compare to prime_pi
    expected = int(prime_pi(global_max))
    if global_min > 2:
        expected -= int(prime_pi(global_min - 1))

    delta = unique_total - expected
    print(f"\nPer-block sum:  {per_block_sum:,}")
    print(f"Shared primes:  {shared_total:,}")
    print(f"Unique total:   {unique_total:,}")
    print(f"Expected (π):   {expected:,}")
    print(f"Delta:          {delta:+,}")

    if delta == 0:
        print("COMPLETE — all primes present")
    elif delta > 0:
        print(f"OVERCOUNT — {delta:,} extra (likely cross-block overlap not adjacent)")
    else:
        print(f"MISSING — {-delta:,} primes absent")

    return unique_total, expected


def check_prime(blocks, target):
    """Check if a specific prime exists in any block."""
    print(f"Searching for p={target:,} across {len(blocks)} blocks...")
    found_in = []
    for b in blocks:
        df = pl.read_parquet(b, columns=['p'])
        if df.filter(pl.col('p') == target).height > 0:
            found_in.append(os.path.basename(b))

    if found_in:
        print(f"  FOUND in: {', '.join(found_in)}")
    else:
        print(f"  NOT FOUND in any block")
        # Check which block's range contains it
        for b in blocks:
            df = pl.read_parquet(b, columns=['p'])
            if df['p'].min() <= target <= df['p'].max():
                uniq = df['p'].unique().sort()
                below = uniq.filter(uniq < target)
                above = uniq.filter(uniq > target)
                print(f"  In range of {os.path.basename(b)} "
                      f"[{df['p'].min():,} .. {df['p'].max():,}] "
                      f"but absent")
                if below.len() > 0 and above.len() > 0:
                    print(f"    Nearest below: {below[-1]:,}  "
                          f"Nearest above: {above[0]:,}  "
                          f"Gap: {above[0] - below[-1]:,}")
    return found_in


def sample_region(blocks, lo, hi):
    """Collect all unique primes in [lo, hi] from all blocks, check contiguity."""
    from sage.all import prime_pi, prime_range

    print(f"Region [{lo:,}, {hi:,}]")
    all_primes = set()
    contributing = []

    for b in tqdm(blocks, desc="Scanning", unit="block"):
        df = pl.read_parquet(b, columns=['p'])
        region = df.filter((pl.col('p') >= lo) & (pl.col('p') <= hi))
        primes = set(region['p'].unique().to_list())
        if primes:
            contributing.append((os.path.basename(b), len(primes)))
            all_primes |= primes

    print(f"\nContributing blocks: {len(contributing)}")
    for name, n in contributing:
        print(f"  {name}: {n:,} primes")

    expected = set(int(p) for p in prime_range(lo, hi + 1))
    missing = sorted(expected - all_primes)
    extra = sorted(all_primes - expected)

    print(f"\nExpected primes in region: {len(expected):,}")
    print(f"Found across all blocks:   {len(all_primes):,}")
    print(f"Missing: {len(missing):,}")
    print(f"Extra:   {len(extra):,}")

    if missing:
        print(f"First 10 missing: {missing[:10]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify prime coverage")
    parser.add_argument("--check", type=int, help="Check if a specific prime exists")
    parser.add_argument("--sample-region", nargs=2, type=int, metavar=("LO", "HI"),
                        help="Verify all primes in [LO, HI]")
    parser.add_argument("--global-count", action="store_true",
                        help="Count total unique primes vs prime_pi")
    args = parser.parse_args()

    blocks = all_blocks()
    if not blocks:
        print("No blocks found")
        sys.exit(1)

    if args.check:
        check_prime(blocks, args.check)
    elif args.sample_region:
        sample_region(blocks, args.sample_region[0], args.sample_region[1])
    elif args.global_count:
        global_count(blocks)
    else:
        # Default: check the two failing primes, then sample one gap region
        print("=== Checking specific primes ===")
        check_prime(blocks, 29949157297)
        print()
        check_prime(blocks, 20420788651)
        print()
        print("=== Sampling gap region ===")
        sample_region(blocks, 29946764521, 29954721887)
