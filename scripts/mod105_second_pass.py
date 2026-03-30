"""
Second-pass obstruction classifier: extend backbone {3,5,7} with helpers {11,13,17}.

For each prime q with ord(2,q) = d, coverage of Z/12Z is determined by
gcd(d, 12):
  q=3  (d=2,  gcd=2):  covers one parity — 6 positions
  q=5  (d=4,  gcd=4):  covers one mod-4 class — 3 positions
  q=7  (d=3,  gcd=3):  covers one mod-3 class — 4 positions
  q=11 (d=10, gcd=2):  covers one parity — 6 positions (like q=3!)
  q=13 (d=12, gcd=12): covers exactly ONE specific position mod 12
  q=17 (d=8,  gcd=4):  covers one mod-4 class — 3 positions

Strategy: GROUP BY (p%3, p%5, p%7, p%11, p%13, p%17) in DuckDB,
pull 92K aggregate rows into Polars, join with coverage bitmask lookups,
compute residual gaps.
"""
from math import gcd
import polars as pl
import duckdb

# ── Coverage bitmask lookups ────────────────────────────────────────────

PRIMES = [
    (3, 2), (5, 4), (7, 3),     # backbone
    (11, 10), (13, 12), (17, 8), # helpers
]

def build_coverage_lookup(q, d):
    """For each residue r mod q, compute bitmask of Z/12Z positions covered."""
    rows = []
    for r in range(q):
        mask = 0
        if r != 0:
            for k0 in range(1, d + 1):
                if pow(2, k0, q) == r:
                    n_pos = 12 // gcd(12, d)
                    for j in range(n_pos):
                        pos = (k0 + j * d) % 12
                        mask |= (1 << pos)
                    break
        rows.append({"r": r, "mask": mask})
    return rows

lookups = {}
for q, d in PRIMES:
    rows = build_coverage_lookup(q, d)
    lookups[q] = {r["r"]: r["mask"] for r in rows}
    # Show coverage
    print(f"q={q:>2d} (ord={d:>2d}, gcd(d,12)={gcd(d,12):>2d}):")
    for r in rows:
        if r["mask"]:
            positions = [i for i in range(12) if r["mask"] & (1 << i)]
            print(f"  r≡{r['r']:>2d}: covers m≡{positions} mod 12 (mask={r['mask']:>4d})")
    print()

# Verify: q=11 has same parity structure as q=3
print("q=11 parity structure:")
for r, mask in lookups[11].items():
    if mask:
        positions = [i for i in range(12) if mask & (1 << i)]
        parity = "even" if positions[0] % 2 == 0 else "odd"
        print(f"  p≡{r:>2d} mod 11 → covers {parity} m")
print()

# ── DuckDB aggregation ─────────────────────────────────────────────────

print("=" * 70)
print("QUERYING DuckDB: GROUP BY (p%3, p%5, p%7, p%11, p%13, p%17)")
print("=" * 70)

con = duckdb.connect("data/funbuns.duckdb", read_only=True)

agg = con.execute("""
    SELECT
        CAST(p % 3 AS INT) as r3,
        CAST(p % 5 AS INT) as r5,
        CAST(p % 7 AS INT) as r7,
        CAST(p % 11 AS INT) as r11,
        CAST(p % 13 AS INT) as r13,
        CAST(p % 17 AS INT) as r17,
        count(*) as total,
        sum(CASE WHEN k = 0 THEN 1 ELSE 0 END) as n_obs
    FROM partition_counts
    WHERE p > 17
    GROUP BY 1, 2, 3, 4, 5, 6
""").pl()
con.close()

print(f"\nGroups: {agg.height:,}")
print(f"Total primes: {agg['total'].sum():,}")
print(f"Total obstructed: {agg['n_obs'].sum():,}")

# ── Compute coverage bitmasks per group ─────────────────────────────────

def compute_combined_mask(r3, r5, r7, r11, r13, r17):
    return (lookups[3].get(r3, 0) | lookups[5].get(r5, 0) | lookups[7].get(r7, 0) |
            lookups[11].get(r11, 0) | lookups[13].get(r13, 0) | lookups[17].get(r17, 0))

# Vectorize: build mask from components
agg = agg.with_columns([
    pl.col("r3").replace_strict(lookups[3], default=0).alias("mask3"),
    pl.col("r5").replace_strict(lookups[5], default=0).alias("mask5"),
    pl.col("r7").replace_strict(lookups[7], default=0).alias("mask7"),
    pl.col("r11").replace_strict(lookups[11], default=0).alias("mask11"),
    pl.col("r13").replace_strict(lookups[13], default=0).alias("mask13"),
    pl.col("r17").replace_strict(lookups[17], default=0).alias("mask17"),
])

agg = agg.with_columns(
    (pl.col("mask3") | pl.col("mask5") | pl.col("mask7") |
     pl.col("mask11") | pl.col("mask13") | pl.col("mask17")).alias("combined_mask"),
)

FULL_MASK = (1 << 12) - 1  # 0xFFF = all 12 positions covered

agg = agg.with_columns([
    (pl.col("combined_mask") == FULL_MASK).alias("fully_covered"),
    # Count residual gaps
    pl.col("combined_mask").map_elements(
        lambda m: 12 - bin(m).count('1'), return_dtype=pl.UInt8
    ).alias("residual_gaps"),
    # Backbone-only mask for comparison
    (pl.col("mask3") | pl.col("mask5") | pl.col("mask7")).alias("backbone_mask"),
    (pl.col("n_obs").cast(pl.Float64) / pl.col("total")).alias("obs_rate"),
])

agg = agg.with_columns(
    pl.col("backbone_mask").map_elements(
        lambda m: 12 - bin(m).count('1'), return_dtype=pl.UInt8
    ).alias("backbone_gaps"),
)

# ── Cache to SSD ───────────────────────────────────────────────────────

CACHE_DIR = "/media/extssd/research/dioph.pp/data/covering"
import os
os.makedirs(CACHE_DIR, exist_ok=True)

# Stage 1: coverage lookup tables (one per prime)
for q, d in PRIMES:
    rows = build_coverage_lookup(q, d)
    pl.DataFrame(rows).write_parquet(f"{CACHE_DIR}/coverage_q{q}.parquet")

# Stage 2: the 92K-row aggregate with all masks and rates
agg.write_parquet(f"{CACHE_DIR}/mod255255_aggregate.parquet")

# Stage 3: the power-of-two difference table (copy from local)
import shutil
local_diffs = "data/power_of_two_diffs.parquet"
if os.path.exists(local_diffs):
    shutil.copy2(local_diffs, f"{CACHE_DIR}/power_of_two_diffs.parquet")

print(f"\nCached to {CACHE_DIR}/")
for f in sorted(os.listdir(CACHE_DIR)):
    size = os.path.getsize(f"{CACHE_DIR}/{f}")
    print(f"  {f}: {size:,} bytes")

# ── Results ─────────────────────────────────────────────────────────────

print(f"\n{'='*70}")
print("RESIDUAL GAP DISTRIBUTION (backbone + helpers {11,13,17})")
print(f"{'='*70}")

gap_summary = (
    agg.group_by("residual_gaps")
    .agg(
        pl.col("total").sum().alias("n_primes"),
        pl.col("n_obs").sum().alias("n_obstructed"),
    )
    .with_columns(
        (pl.col("n_obstructed").cast(pl.Float64) / pl.col("n_primes")).alias("obs_rate"),
    )
    .sort("residual_gaps")
)

for row in gap_summary.iter_rows(named=True):
    g = row["residual_gaps"]
    n = row["n_primes"]
    nobs = row["n_obstructed"]
    rate = row["obs_rate"]
    print(f"  {g} residual gaps: {nobs:>12,} / {n:>12,} = {rate:.6f}")

# Compare: backbone-only vs with helpers
print(f"\n{'='*70}")
print("IMPROVEMENT: BACKBONE ONLY vs BACKBONE + HELPERS")
print(f"{'='*70}")

comparison = (
    agg.group_by("backbone_gaps", "residual_gaps")
    .agg(
        pl.col("total").sum().alias("n_primes"),
        pl.col("n_obs").sum().alias("n_obstructed"),
    )
    .with_columns(
        (pl.col("n_obstructed").cast(pl.Float64) / pl.col("n_primes")).alias("obs_rate"),
    )
    .sort("backbone_gaps", "residual_gaps")
)

for bg in sorted(agg["backbone_gaps"].unique().to_list()):
    subset = comparison.filter(pl.col("backbone_gaps") == bg)
    print(f"\n  Backbone {bg} gaps:")
    for row in subset.iter_rows(named=True):
        rg = row["residual_gaps"]
        n = row["n_primes"]
        nobs = row["n_obstructed"]
        rate = row["obs_rate"]
        direction = "→" if rg < bg else "="
        print(f"    {direction} {rg} residual: {nobs:>11,} / {n:>11,} = {rate:.6f}")

# Fully-covered groups: what's their obstruction rate?
print(f"\n{'='*70}")
print("FULLY COVERED GROUPS (all 12 positions of Z/12Z)")
print(f"{'='*70}")

fc = agg.filter(pl.col("fully_covered"))
n_fc_primes = fc["total"].sum()
n_fc_obs = fc["n_obs"].sum()
print(f"  Groups: {fc.height:,}")
print(f"  Primes: {n_fc_primes:,}")
print(f"  Obstructed: {n_fc_obs:,}")
if n_fc_primes > 0:
    print(f"  Rate: {n_fc_obs / n_fc_primes:.6f}")

# Not fully covered: how many gaps and what rates?
nfc = agg.filter(~pl.col("fully_covered"))
n_nfc_primes = nfc["total"].sum()
n_nfc_obs = nfc["n_obs"].sum()
print(f"\n  NOT fully covered:")
print(f"  Groups: {nfc.height:,}")
print(f"  Primes: {n_nfc_primes:,}")
print(f"  Obstructed: {n_nfc_obs:,}")
if n_nfc_primes > 0:
    print(f"  Rate: {n_nfc_obs / n_nfc_primes:.6f}")

# Most obstructed groups
print(f"\n{'='*70}")
print("TOP 20 MOST-OBSTRUCTED RESIDUE GROUPS")
print(f"{'='*70}")

top = agg.filter(pl.col("total") > 1000).sort("obs_rate", descending=True).head(20)
for row in top.iter_rows(named=True):
    r = f"({row['r3']},{row['r5']},{row['r7']},{row['r11']},{row['r13']},{row['r17']})"
    gaps = row["residual_gaps"]
    print(f"  {r:>25s}: {row['n_obs']:>8,}/{row['total']:>8,} = {row['obs_rate']:.6f}  [{gaps} gaps]")

print(f"\n{'='*70}")
print("BOTTOM 20 LEAST-OBSTRUCTED RESIDUE GROUPS")
print(f"{'='*70}")

bot = agg.filter(pl.col("total") > 1000).sort("obs_rate").head(20)
for row in bot.iter_rows(named=True):
    r = f"({row['r3']},{row['r5']},{row['r7']},{row['r11']},{row['r13']},{row['r17']})"
    gaps = row["residual_gaps"]
    print(f"  {r:>25s}: {row['n_obs']:>8,}/{row['total']:>8,} = {row['obs_rate']:.6f}  [{gaps} gaps]")

# ── 100% obstruction groups ─────────────────────────────────────────────

print(f"\n{'='*70}")
print("100% OBSTRUCTION GROUPS (every prime in class is obstructed)")
print(f"{'='*70}")

perfect = agg.filter(
    (pl.col("n_obs") == pl.col("total")) & (pl.col("total") > 100)
).sort("total", descending=True)

n_perfect = perfect.height
n_perfect_primes = perfect["total"].sum()
print(f"  Count: {n_perfect:,} groups")
print(f"  Primes: {n_perfect_primes:,}")
print(f"  Share of all obstructed: {n_perfect_primes / 1_130_500_573:.4f}")
print()

# What's the coverage structure of these groups?
print("Coverage structure of 100% groups:")
perfect_gap_dist = perfect.group_by("residual_gaps").agg(
    pl.len().alias("n_groups"),
    pl.col("total").sum().alias("n_primes"),
)
for row in perfect_gap_dist.sort("residual_gaps").iter_rows(named=True):
    print(f"  {row['residual_gaps']} residual gaps: {row['n_groups']:>5,} groups, {row['n_primes']:>10,} primes")

# Analyze the parity structure: do 3 and 11 cover opposite parities?
print(f"\nParity alignment (q=3 vs q=11) in 100% groups:")
perfect_with_parity = perfect.with_columns([
    # q=3: p≡1 mod 3 → even, p≡2 mod 3 → odd
    pl.when(pl.col("r3") == 1).then(pl.lit("even")).otherwise(pl.lit("odd")).alias("q3_parity"),
    # q=11: check if mask covers even (mask & 0x555 = 0x555) or odd
    pl.when(pl.col("mask11") == 1365).then(pl.lit("even")).otherwise(
        pl.when(pl.col("mask11") == 2730).then(pl.lit("odd")).otherwise(pl.lit("none"))
    ).alias("q11_parity"),
])

perfect_with_parity = perfect_with_parity.with_columns(
    (pl.col("q3_parity") != pl.col("q11_parity")).alias("opposite_parity"),
)

n_opposite = perfect_with_parity.filter(pl.col("opposite_parity")).height
n_same = perfect_with_parity.filter(~pl.col("opposite_parity")).height
print(f"  Opposite parity (3 and 11 cover ALL m): {n_opposite:,} groups")
print(f"  Same parity: {n_same:,} groups")

# Show a few representative 100% groups with their coverage explanation
print(f"\nRepresentative 100% groups (first 10):")
for row in perfect.head(10).iter_rows(named=True):
    r = f"({row['r3']},{row['r5']},{row['r7']},{row['r11']},{row['r13']},{row['r17']})"
    mask = row["combined_mask"]
    positions = [i for i in range(12) if mask & (1 << i)]
    gaps = [i for i in range(12) if not (mask & (1 << i))]
    print(f"  {r:>25s}: {row['total']:>7,} primes, covered={positions}, gaps={gaps}")

# Near-100% groups (>99%)
print(f"\n{'='*70}")
print("NEAR-100% OBSTRUCTION (>99%)")
print(f"{'='*70}")

near_perfect = agg.filter(
    (pl.col("obs_rate") > 0.99) & (pl.col("obs_rate") < 1.0) & (pl.col("total") > 100)
)
n_near = near_perfect.height
n_near_primes = near_perfect["total"].sum()
n_near_obs = near_perfect["n_obs"].sum()
print(f"  Groups: {n_near:,}")
print(f"  Primes: {n_near_primes:,}")
print(f"  Obstructed: {n_near_obs:,}")
if n_near_primes > 0:
    print(f"  Rate: {n_near_obs / n_near_primes:.6f}")

# Rate distribution histogram
print(f"\n{'='*70}")
print("OBSTRUCTION RATE DISTRIBUTION (all groups with >1000 primes)")
print(f"{'='*70}")

viable = agg.filter(pl.col("total") > 1000)
bins = [0, 0.01, 0.02, 0.05, 0.10, 0.20, 0.30, 0.50, 0.70, 0.90, 0.99, 1.0, 1.01]
labels = ["0-1%", "1-2%", "2-5%", "5-10%", "10-20%", "20-30%", "30-50%",
          "50-70%", "70-90%", "90-99%", "99-100%", "100%"]

for i in range(len(bins) - 1):
    lo, hi = bins[i], bins[i + 1]
    if i == len(bins) - 2:  # exact 100%
        bucket = viable.filter(pl.col("obs_rate") == 1.0)
    else:
        bucket = viable.filter((pl.col("obs_rate") >= lo) & (pl.col("obs_rate") < hi))
    n_groups = bucket.height
    n_primes = bucket["total"].sum()
    n_obs = bucket["n_obs"].sum()
    if n_groups > 0:
        print(f"  {labels[i]:>8s}: {n_groups:>6,} groups, {n_primes:>13,} primes, {n_obs:>12,} obstructed")
