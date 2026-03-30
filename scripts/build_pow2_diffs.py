"""
Build a table of differences 2^m - 2^k and their factorizations.

For each pair (m, k) with 3 <= m <= MAX_EXP and 1 <= k <= m-2,
compute 2^m - 2^k = 2^k * (2^{m-k} - 1).

The odd part is always a Mersenne number M_d = 2^d - 1 where d = m - k.
Its factorization tells us which residue conditions propagate: if q | p - 2^k
and q | 2^m - 2^k, then q | p - 2^m, so a single small prime can poison
multiple candidate m values for an obstructed prime.

Output: data/power_of_two_diffs.parquet
Schema: m (u8), k (u8), d (u8), diff (u64), v2 (u8),
        odd_part (u64), factorization (str), odd_prime_factors (list[u64])
"""
from sage.all import factor, ZZ
import polars as pl

MAX_EXP = 40

rows = []
for m in range(3, MAX_EXP + 1):
    for k in range(1, m - 1):  # k from 1 to m-2
        diff = ZZ(2) ** m - ZZ(2) ** k
        f = factor(diff)
        odd_primes = sorted([int(p) for p, _e in f if p != 2])
        rows.append({
            "m": int(m),
            "k": int(k),
            "d": int(m - k),
            "diff": int(diff),
            "v2": int(k),
            "odd_part": int(diff >> k),
            "factorization": str(f),
            "odd_prime_factors": [int(p) for p in odd_primes],
        })
    print(f"m={m:2d}: {m-2:2d} differences computed")

df = pl.DataFrame(rows).cast({
    "m": pl.UInt8, "k": pl.UInt8, "d": pl.UInt8,
    "diff": pl.UInt64, "v2": pl.UInt8, "odd_part": pl.UInt64,
})

print(f"\nTotal rows: {df.height}")
print(f"\nSample (m=10):")
print(df.filter(pl.col("m") == 10))

# Unique Mersenne numbers and their factorizations
mersenne = (
    df.select("d", "odd_part", "factorization")
    .unique(subset=["d"])
    .sort("d")
)
print(f"\nUnique Mersenne numbers 2^d - 1 (d=2..{MAX_EXP - 1}):")
print(mersenne)

output_path = "data/power_of_two_diffs.parquet"
df.write_parquet(output_path)
print(f"\nWritten to {output_path}")
