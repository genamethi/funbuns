"""
Analysis of maximum covering moduli and k-value structure.

Three questions:
1. For the 22 unconditional obstruction classes, what's the algebraic proof?
2. How does k correlate with coverage structure (gaps → opportunities)?
3. Is there a universal bound on the maximum modulus needed?
"""
from math import gcd
import polars as pl
import duckdb

CACHE_DIR = "/media/extssd/research/dioph.pp/data/covering"

# ── Load cached aggregate ───────────────────────────────────────────────

agg = pl.read_parquet(f"{CACHE_DIR}/mod255255_aggregate.parquet")
print(f"Loaded {agg.height:,} residue groups")

# ── Q1: Algebraic structure of the 22 unconditional classes ─────────────

print("=" * 70)
print("ALGEBRAIC STRUCTURE OF 22 UNCONDITIONAL OBSTRUCTION CLASSES")
print("=" * 70)

perfect = agg.filter(
    (pl.col("n_obs") == pl.col("total")) & (pl.col("total") > 100)
)

# For each, show the CRT residue mod 255255 and the coverage explanation
print("\nResidues mod 255255 (via CRT from the 6 components):")
for row in perfect.sort("r3", "r5", "r7", "r11", "r13", "r17").iter_rows(named=True):
    r3, r5, r7, r11, r13, r17 = row["r3"], row["r5"], row["r7"], row["r11"], row["r13"], row["r17"]

    # q=3 parity: r3=1 → even m, r3=2 → odd m
    q3_parity = "even" if r3 == 1 else "odd"
    # q=11 parity: check mask
    q11_parity = "even" if row["mask11"] == 1365 else "odd"

    # q=5 covers which mod-4 class?
    q5_map = {1: "m≡0(4)", 2: "m≡1(4)", 3: "m≡3(4)", 4: "m≡2(4)"}
    q5_class = q5_map.get(r5, "none")

    # q=7 covers which mod-3 class?
    q7_map = {1: "m≡0(3)", 2: "m≡1(3)", 4: "m≡2(3)"}
    q7_class = q7_map.get(r7, "inactive")

    # q=13 covers which position mod 12?
    q13_positions = {1:0, 2:1, 3:4, 4:2, 5:9, 6:5, 7:11, 8:3, 9:8, 10:10, 11:7, 12:6}
    q13_pos = q13_positions.get(r13, -1)

    # q=17 covers which mod-4 class?
    q17_map = {1: "m≡0(4)", 2: "m≡1(4)", 4: "m≡2(4)", 8: "m≡3(4)",
               9: "m≡3(4)", 13: "m≡2(4)", 15: "m≡1(4)", 16: "m≡0(4)"}
    q17_class = q17_map.get(r17, "none")

    print(f"\n  ({r3},{r5},{r7},{r11},{r13},{r17}) — {row['total']:,} primes")
    print(f"    q=3: {q3_parity} m | q=11: {q11_parity} m → {'FULL' if q3_parity != q11_parity else 'SAME'} parity coverage")
    print(f"    q=5: {q5_class} | q=7: {q7_class} | q=13: m≡{q13_pos}(12) | q=17: {q17_class}")

# KEY: since all 22 have opposite parity, {3,11} cover everything.
# Why does this force 100% obstruction?
print("\n" + "=" * 70)
print("WHY OPPOSITE PARITY → 100% OBSTRUCTION")
print("=" * 70)
print("""
When q=3 and q=11 cover opposite parities, every m has either:
  - 3 | p - 2^m  (one parity), or
  - 11 | p - 2^m (other parity)

For p - 2^m to be a prime power q^n:
  - If 3 | p - 2^m: need p - 2^m = 3^n. This means 3 || p - 2^m would
    give q^n = 3·(something), which is only 3^n if "something" = 3^{n-1}.
  - If 11 | p - 2^m: need p - 2^m = 11^n similarly.

The 100% rate means: at every m, the remainder has an ADDITIONAL factor
beyond the covering prime. The question is: what forces that second factor?

Hypothesis: the additional primes (5, 7, 13, 17) guarantee a second
factor at every position. When 3|r AND 5|r, r has ≥2 distinct factors
(unless r=15^j... but 15 is not a prime power).

At each m, we need: covering prime q_1 divides r AND some OTHER prime
also divides r. The combined coverage of {3,5,7,11,13,17} may ensure
at least TWO primes divide every remainder.
""")

# ── Check: at each Z/12Z position, how many covering primes hit it? ────

print("=" * 70)
print("MULTI-COVERAGE: how many primes cover each position?")
print("=" * 70)

for row in perfect.head(5).iter_rows(named=True):
    r = f"({row['r3']},{row['r5']},{row['r7']},{row['r11']},{row['r13']},{row['r17']})"
    masks = [row[f"mask{q}"] for q in [3, 5, 7, 11, 13, 17]]
    primes_list = [3, 5, 7, 11, 13, 17]

    print(f"\n  Group {r}:")
    for pos in range(12):
        covering = [q for q, m in zip(primes_list, masks) if m & (1 << pos)]
        marker = " **" if len(covering) >= 2 else " ← SINGLE"
        print(f"    m≡{pos:>2d} (mod 12): covered by {covering}{marker}")

# ── Q2: k distribution by residual gap count ───────────────────────────

print(f"\n{'='*70}")
print("k DISTRIBUTION BY RESIDUAL GAP COUNT")
print(f"{'='*70}")

con = duckdb.connect("data/funbuns.duckdb", read_only=True)

# Get k distribution grouped by mod-255255 coverage tiers
k_by_gaps = con.execute("""
    SELECT
        k,
        count(*) as cnt
    FROM partition_counts
    WHERE p > 17
    GROUP BY k
    ORDER BY k
""").pl()

print("\nOverall k distribution:")
for row in k_by_gaps.iter_rows(named=True):
    if row["cnt"] > 1000:
        print(f"  k={row['k']:>3d}: {row['cnt']:>12,}")

# k distribution per residual gap tier
# We need to join partition_counts with our aggregate.
# Since we can't join directly (too many rows), compute in DuckDB
# by reconstructing the residual gap from modular residues.

# Register the aggregate as a table
con2 = duckdb.connect("data/funbuns.duckdb", read_only=True)

# Build a smaller lookup: (r3,r5,r7,r11,r13,r17) → residual_gaps
gap_lookup = agg.select("r3", "r5", "r7", "r11", "r13", "r17", "residual_gaps")
con2.register("gap_lookup", gap_lookup.to_arrow())

k_by_gap_tier = con2.execute("""
    SELECT
        gl.residual_gaps,
        pc.k,
        count(*) as cnt
    FROM partition_counts pc
    JOIN gap_lookup gl
        ON CAST(pc.p % 3 AS INT) = gl.r3
        AND CAST(pc.p % 5 AS INT) = gl.r5
        AND CAST(pc.p % 7 AS INT) = gl.r7
        AND CAST(pc.p % 11 AS INT) = gl.r11
        AND CAST(pc.p % 13 AS INT) = gl.r13
        AND CAST(pc.p % 17 AS INT) = gl.r17
    WHERE pc.p > 17
    GROUP BY gl.residual_gaps, pc.k
    ORDER BY gl.residual_gaps, pc.k
""").pl()
con2.close()

print("\nk distribution by residual gap tier:")
for gaps in sorted(k_by_gap_tier["residual_gaps"].unique().to_list()):
    tier = k_by_gap_tier.filter(pl.col("residual_gaps") == gaps)
    total = tier["cnt"].sum()
    mean_k = (tier["k"].cast(pl.Float64) * tier["cnt"]).sum() / total
    max_k = tier["k"].max()

    # Show top k values
    top_ks = tier.sort("k", descending=True).head(5)
    top_str = ", ".join(f"k={r['k']}:{r['cnt']:,}" for r in top_ks.iter_rows(named=True))

    print(f"\n  {gaps} gaps: mean_k={mean_k:.3f}, max_k={max_k}, total={total:,}")
    print(f"    Highest: {top_str}")

# ── Q3: k_max vs p (upper envelope) ───────────────────────────────────

print(f"\n{'='*70}")
print("k_max vs p RANGE (upper envelope)")
print(f"{'='*70}")

con3 = duckdb.connect("data/funbuns.duckdb", read_only=True)

# Bin by order of magnitude
k_max_by_range = con3.execute("""
    SELECT
        CAST(floor(log2(p)) AS INT) as log2_p,
        max(k) as k_max,
        avg(k) as k_mean,
        count(*) as n_primes
    FROM partition_counts
    WHERE p > 17
    GROUP BY 1
    ORDER BY 1
""").pl()
con3.close()

print(f"\n  {'log2(p)':>8s}  {'k_max':>5s}  {'k_mean':>8s}  {'n_primes':>12s}  {'k_max/log2(p)':>13s}")
for row in k_max_by_range.iter_rows(named=True):
    log2p = row["log2_p"]
    kmax = row["k_max"]
    kmean = row["k_mean"]
    n = row["n_primes"]
    ratio = kmax / log2p if log2p > 0 else 0
    print(f"  {log2p:>8d}  {kmax:>5d}  {kmean:>8.3f}  {n:>12,}  {ratio:>13.4f}")
