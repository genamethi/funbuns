"""
Vectorized mod-105 obstruction classifier.

For each coprime residue class r mod 105 = 3·5·7, precompute which
positions in Z/12Z = Z/lcm(2,3,4)Z the backbone {3,5,7} covers.
Then apply as a single Polars join to classify all primes at once.

The backbone leaves 2-6 gaps per period-12 block depending on r.
For primes with max_m >= 12, these gaps always need additional primes.
For smaller primes, the backbone may suffice if gaps fall outside [1, max_m].
"""
from math import gcd, log2
from collections import Counter
import polars as pl
import duckdb

# ── Precompute discrete log tables ──────────────────────────────────────

BACKBONE = {
    3: {"ord": 2},   # 2^1≡2, 2^2≡1 mod 3
    7: {"ord": 3},   # 2^1≡2, 2^2≡4, 2^3≡1 mod 7
    5: {"ord": 4},   # 2^1≡2, 2^2≡4, 2^3≡3, 2^4≡1 mod 5
}

for q, info in BACKBONE.items():
    d = info["ord"]
    # dlog[r] = k such that 2^k ≡ r mod q (1-indexed), or None
    dlog = {}
    for k in range(1, d + 1):
        r = pow(2, k, q)
        dlog[r] = k  # k mod d is the residue class of m
    info["dlog"] = dlog

print("Discrete log tables:")
for q, info in BACKBONE.items():
    print(f"  q={q} (ord={info['ord']}): {info['dlog']}")
print()

# ── Build lookup table: r mod 105 → backbone coverage of Z/12Z ─────────

LCM12 = 12  # lcm(2, 3, 4)

lookup_rows = []
for r in range(105):
    if gcd(r, 105) != 1:
        continue

    covered = set()
    active_primes = []

    for q, info in BACKBONE.items():
        d = info["ord"]
        r_q = r % q
        if r_q in info["dlog"]:
            k0 = info["dlog"][r_q]
            # q covers m ≡ k0 mod d, which in Z/12Z is:
            positions = set()
            for j in range(LCM12 // d):
                positions.add((k0 + j * d) % LCM12)
            covered |= positions
            active_primes.append(q)

    gaps = sorted(set(range(LCM12)) - covered)
    # Convert Z/12Z gaps to actual m-values: gap 0 means m ≡ 0 mod 12, i.e. m=12,24,...
    # gap j means m ≡ j mod 12, i.e. m = j, j+12, j+24, ...
    # But m starts at 1, so gap 0 → first occurrence at m=12
    first_gap_m = min((g if g > 0 else 12) for g in gaps) if gaps else 999

    lookup_rows.append({
        "r_mod105": r,
        "backbone_covered": len(covered),
        "backbone_gaps": len(gaps),
        "gap_positions_mod12": gaps,
        "first_gap_m": first_gap_m,
        "active_backbone": active_primes,
    })

lookup = pl.DataFrame(lookup_rows)

print(f"Backbone coverage over Z/12Z for {lookup.height} coprime residue classes mod 105:")
print()
coverage_dist = lookup.group_by("backbone_gaps").len().sort("backbone_gaps")
for row in coverage_dist.iter_rows(named=True):
    n_gaps = row["backbone_gaps"]
    count = row["len"]
    print(f"  {n_gaps} gaps: {count} residue classes")

print()
print("Detail by gap count:")
for n_gaps in sorted(lookup["backbone_gaps"].unique().to_list()):
    subset = lookup.filter(pl.col("backbone_gaps") == n_gaps)
    print(f"\n  {n_gaps} gaps per period-12 block ({subset.height} classes):")
    for row in subset.sort("r_mod105").iter_rows(named=True):
        r = row["r_mod105"]
        gaps = row["gap_positions_mod12"]
        active = row["active_backbone"]
        first_m = row["first_gap_m"]
        print(f"    r≡{r:>3d}: gaps at m≡{gaps} mod 12, first gap m={first_m}, active={active}")

# ── Apply to actual primes via DuckDB (push computation down) ──────────

print(f"\n{'='*70}")
print("APPLYING TO PARTITION DATA")
print(f"{'='*70}")

# Build the lookup as a small table we can register in DuckDB
lookup_for_db = lookup.select("r_mod105", "backbone_gaps", "first_gap_m")

con = duckdb.connect("data/funbuns.duckdb", read_only=True)

# Register the lookup table
con.register("backbone_lookup", lookup_for_db.to_arrow())

# Do all aggregation inside DuckDB
total_stats = con.execute("""
    SELECT count(*) as total,
           sum(CASE WHEN k = 0 THEN 1 ELSE 0 END) as n_obstructed
    FROM partition_counts WHERE p > 7
""").fetchone()
print(f"\n  Total primes: {total_stats[0]:,}")
print(f"  Obstructed (k=0): {total_stats[1]:,}")
print(f"  Non-obstructed (k>0): {total_stats[0] - total_stats[1]:,}")

# Gap distribution by obstruction status
print(f"\n{'='*70}")
print("BACKBONE GAP RATES BY OBSTRUCTION STATUS")
print(f"{'='*70}")

gap_rates = con.execute("""
    SELECT bl.backbone_gaps,
           count(*) as total,
           sum(CASE WHEN pc.k = 0 THEN 1 ELSE 0 END) as n_obstructed
    FROM partition_counts pc
    JOIN backbone_lookup bl ON (pc.p % 105) = bl.r_mod105
    WHERE pc.p > 7
    GROUP BY bl.backbone_gaps
    ORDER BY bl.backbone_gaps
""").pl()

for row in gap_rates.iter_rows(named=True):
    n_gaps = row["backbone_gaps"]
    total = row["total"]
    n_obs = row["n_obstructed"]
    obs_rate = n_obs / total if total > 0 else 0
    print(f"  {n_gaps} gaps: {n_obs:>10,} obstructed / {total:>12,} total = {obs_rate:.6f}")

# Residue-level obstruction rates
print(f"\n{'='*70}")
print("OBSTRUCTION RATE BY RESIDUE CLASS MOD 105")
print(f"{'='*70}")

res_stats = con.execute("""
    SELECT pc.p % 105 as r_mod105,
           bl.backbone_gaps,
           count(*) as total,
           sum(CASE WHEN pc.k = 0 THEN 1 ELSE 0 END) as n_obstructed,
           sum(CASE WHEN pc.k = 0 THEN 1.0 ELSE 0.0 END) / count(*) as obs_rate
    FROM partition_counts pc
    JOIN backbone_lookup bl ON (pc.p % 105) = bl.r_mod105
    WHERE pc.p > 7
    GROUP BY pc.p % 105, bl.backbone_gaps
    ORDER BY obs_rate DESC
""").pl()

print(f"\nTop 15 most-obstructed residue classes:")
for row in res_stats.head(15).iter_rows(named=True):
    r = row["r_mod105"]
    total = row["total"]
    n_obs = row["n_obstructed"]
    rate = row["obs_rate"]
    gaps = row["backbone_gaps"]
    print(f"  r≡{r:>3d} (mod 105): {n_obs:>9,}/{total:>10,} = {rate:.6f}  [{gaps} backbone gaps]")

print(f"\nBottom 15 least-obstructed residue classes:")
bottom = res_stats.sort("obs_rate")
for row in bottom.head(15).iter_rows(named=True):
    r = row["r_mod105"]
    total = row["total"]
    n_obs = row["n_obstructed"]
    rate = row["obs_rate"]
    gaps = row["backbone_gaps"]
    print(f"  r≡{r:>3d} (mod 105): {n_obs:>9,}/{total:>10,} = {rate:.6f}  [{gaps} backbone gaps]")

con.close()
