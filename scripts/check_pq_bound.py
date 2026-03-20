#!/usr/bin/env python
"""Check the at-most-2 solutions bound for 2^m + q^n = p per (p, q) pair.

For coprime (a, b), the equation a^x + b^y = c has at most 2 solutions (x, y).
Here a=2, b=q (prime), c=p, so for each (p, q) pair we expect at most 2 (m, n).

Scans all block files and reports:
  - Distribution of solution counts per (p, q) pair
  - Any counterexamples (3+ solutions)
  - All pairs achieving the maximum of 2
"""
import polars as pl
from pathlib import Path
import sys

data_dir = Path("data/blocks")
block_files = sorted(data_dir.glob("pp_b*.parquet"))

if not block_files:
    print("No block files found in data/blocks/")
    sys.exit(1)

print(f"Scanning {len(block_files)} blocks...\n")

max_seen = 0
total_pairs = 0
two_solution_pairs: list[dict] = []
violations: list[dict] = []
dist: dict[int, int] = {}

for i, bf in enumerate(block_files):
    df = pl.read_parquet(str(bf))
    decomposed = df.filter(pl.col('m_k') > 0)
    if decomposed.height == 0:
        continue

    per_pq = (
        decomposed
        .group_by(['p', 'q_k'])
        .agg(
            pl.len().alias('n_solutions'),
            pl.col('m_k').alias('ms'),
            pl.col('n_k').alias('ns'),
        )
    )

    for n in per_pq['n_solutions'].to_list():
        dist[n] = dist.get(n, 0) + 1

    block_max = per_pq['n_solutions'].max()
    if block_max > max_seen:
        max_seen = block_max

    # Collect pairs with 2+ solutions
    interesting = per_pq.filter(pl.col('n_solutions') >= 2)
    for row in interesting.iter_rows(named=True):
        entry = {
            'p': row['p'], 'q': row['q_k'],
            'n_solutions': row['n_solutions'],
            'ms': row['ms'], 'ns': row['ns'],
        }
        if row['n_solutions'] == 2:
            two_solution_pairs.append(entry)
        if row['n_solutions'] > 2:
            violations.append(entry)

    total_pairs += per_pq.height

    if (i + 1) % 25 == 0:
        print(f"  {i + 1}/{len(block_files)} blocks, {total_pairs} (p,q) pairs so far, "
              f"max solutions seen: {max_seen}", flush=True)

print(f"\nDone. {total_pairs} total (p, q) pairs across {len(block_files)} blocks.\n")

print("Distribution of solutions per (p, q) pair:")
for n in sorted(dist):
    print(f"  {n} solution(s): {dist[n]:>12,} pairs")

print(f"\nMaximum solutions per (p, q) pair: {max_seen}")

if violations:
    print(f"\n*** VIOLATIONS: {len(violations)} pairs with >2 solutions! ***")
    for v in violations[:20]:
        print(f"  p={v['p']}, q={v['q']}: {v['n_solutions']} solutions")
        for m, n in zip(v['ms'], v['ns']):
            print(f"    2^{m} + {v['q']}^{n} = {(1 << m) + v['q']**n}")
else:
    print("\nNo violations: bound of 2 solutions per (p, q) pair holds.")

print(f"\n{len(two_solution_pairs)} pairs achieve exactly 2 solutions.")
if two_solution_pairs:
    print("Examples:")
    for entry in two_solution_pairs[:20]:
        p, q = entry['p'], entry['q']
        pairs_str = ", ".join(
            f"(m={m}, n={n})" for m, n in zip(entry['ms'], entry['ns'])
        )
        print(f"  p={p}, q={q}: {pairs_str}")
