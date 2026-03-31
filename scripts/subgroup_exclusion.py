"""
Subgroup exclusion proof for the 22 unconditional obstruction classes.

At single-coverage positions (only q covers), if p - 2^m = q^n then
q^n must match the forced residue mod each non-covering prime ℓ.
But q generates a proper subgroup ⟨q⟩ ⊂ (Z/ℓZ)*, and the forced
residue may lie OUTSIDE this subgroup → contradiction → obstruction.

Strategy: for each (class, position, m mod period_ℓ), find ONE blocking ℓ.
We check each blocking prime independently — no need for a global LCM period.
"""
from sage.all import Mod, ZZ, crt
import polars as pl

CACHE_DIR = "/media/extssd/research/dioph.pp/data/covering"
agg = pl.read_parquet(f"{CACHE_DIR}/mod255255_aggregate.parquet")

perfect = agg.filter(
    (pl.col("n_obs") == pl.col("total")) & (pl.col("total") > 100)
)

COVERING_PRIMES = [3, 5, 7, 11, 13, 17]

# ── Build blocking prime candidates ──────────────────────────────────

def compute_subgroup(q, ell):
    """Compute ⟨q⟩ mod ℓ as a frozenset."""
    sg = set()
    val = 1
    for _ in range(ell - 1):
        val = (val * q) % ell
        sg.add(val)
    return frozenset(sg)

def find_blocking_primes(q_cover, max_ell=200):
    """Find primes ℓ where ⟨q_cover⟩ is a proper subgroup of (Z/ℓZ)*."""
    from sage.all import prime_range
    blockers = []
    for ell in prime_range(3, max_ell):
        ell = int(ell)
        if ell == q_cover or ell == 2:
            continue
        sg = compute_subgroup(q_cover, ell)
        index = (ell - 1) // len(sg)
        if index > 1:
            blockers.append((ell, sg, index))
    return blockers

print("Building blocking prime tables...")
blocking_primes = {}
for q_cover in [3, 11]:
    blockers = find_blocking_primes(q_cover, max_ell=200)
    blocking_primes[q_cover] = blockers
    print(f"\n  q={q_cover}: {len(blockers)} blocking primes available")
    for ell, sg, index in blockers[:8]:
        print(f"    ℓ={ell:>3d}: |⟨{q_cover}⟩|={len(sg):>3d}, index={index}")

# ── For each blocking prime ℓ, precompute ord(2,ℓ) ──────────────────

ord2 = {}
for q_cover in [3, 11]:
    for ell, sg, idx in blocking_primes[q_cover]:
        if ell not in ord2:
            ord2[ell] = int(Mod(2, ell).multiplicative_order())

# ── Verify subgroup exclusion for all 22 classes ───────────────────────

print(f"\n{'='*70}")
print("SUBGROUP EXCLUSION VERIFICATION")
print(f"{'='*70}")

all_verified = True
class_details = []

for row in perfect.sort("r3", "r5", "r7").iter_rows(named=True):
    r3, r5, r7, r11, r13, r17 = (
        row["r3"], row["r5"], row["r7"], row["r11"], row["r13"], row["r17"]
    )
    masks = {q: row[f"mask{q}"] for q in COVERING_PRIMES}

    r_crt = int(crt([r3, r5, r7, r11, r13, r17], [3, 5, 7, 11, 13, 17]))
    label = f"({r3},{r5},{r7},{r11},{r13},{r17})"

    print(f"\n  Class {label}, p ≡ {r_crt} (mod 255255):")

    class_ok = True
    ells_used = set()

    for pos in range(12):
        covering = [q for q in COVERING_PRIMES if masks[q] & (1 << pos)]

        if len(covering) >= 2:
            continue  # multi-coverage: automatically blocked

        if len(covering) == 0:
            print(f"    m≡{pos:>2d}: NOT COVERED — ERROR")
            class_ok = False
            continue

        q_cover = covering[0]

        # For each blocking prime ℓ, the residue (r_crt - 2^m) mod ℓ
        # depends on m mod ord(2,ℓ). Since m ≡ pos (mod 12), the distinct
        # values of m mod ord(2,ℓ) are: {(pos + 12*j) mod ord(2,ℓ) : j ≥ 0},
        # which has size ord(2,ℓ) / gcd(12, ord(2,ℓ)).
        #
        # Strategy: try blocking primes in order. For each ℓ, check which
        # m-classes (mod ord(2,ℓ)) are blocked. Accumulate until all
        # m-classes (mod global_period) are covered.
        #
        # Instead of computing a global LCM, we track blocked m-residues
        # per ℓ independently and verify completeness.

        # The set of distinct (m mod 12) residues is just {pos}.
        # But 2^m mod ℓ has period ord(2,ℓ), so the distinct residues of
        # (r_crt - 2^m) mod ℓ for m ≡ pos (mod 12) cycle with period
        # lcm(12, ord(2,ℓ)) / 12 = ord(2,ℓ) / gcd(12, ord(2,ℓ)).
        #
        # We need: for EVERY m ≡ pos (mod 12), at least one ℓ blocks it.
        # Equivalently: for every j ∈ Z, some ℓ has (r_crt - 2^{pos+12j}) mod ℓ ∉ ⟨q⟩.
        #
        # The residue depends only on j mod (ord(2,ℓ) / gcd(12, ord(2,ℓ))),
        # i.e., on j mod (period_ℓ) where period_ℓ = ord(2,ℓ) / gcd(12, ord(2,ℓ)).
        # (Because 2^{pos+12j} mod ℓ = 2^pos · (2^12)^j mod ℓ, and 2^12 has
        # order ord(2,ℓ)/gcd(12,ord(2,ℓ)) in (Z/ℓZ)*.)
        #
        # So each ℓ partitions j into period_ℓ classes and blocks some subset.
        # We need the union of blocked classes (mod their respective periods)
        # to cover ALL of Z.
        #
        # This is itself a covering system problem! But a small one.
        # Practical approach: compute global_period = lcm of all period_ℓ
        # for blocking primes we use, then check all j in [0, global_period).

        from math import gcd, lcm

        # Collect per-ℓ blocking data
        ell_block_data = []  # (ell, period_ℓ, blocked_j_set)
        for ell, sg, index in blocking_primes[q_cover]:
            d = ord2[ell]
            g = gcd(12, d)
            period_ell = d // g  # period of 2^{12j} mod ℓ

            # Check each j mod period_ell
            blocked_j = set()
            for j in range(period_ell):
                m = pos + 12 * j
                if m == 0:
                    m = 12 * period_ell  # m ≥ 1
                r_ell = (r_crt - pow(2, m, ell)) % ell
                if r_ell == 0 or r_ell not in sg:
                    blocked_j.add(j)

            if blocked_j:
                ell_block_data.append((ell, period_ell, blocked_j))

        # Greedy covering: accumulate blocking primes until all j are covered.
        # Use a small global period — start with the first few blocking primes.
        # Compute incrementally.
        global_period = 1
        covered_j = set()  # covered j mod global_period

        # Sort by most blocking first (heuristic: higher index, lower period)
        ell_block_data.sort(key=lambda x: -len(x[2]) / x[1])

        used_ells = []
        for ell, per, blk in ell_block_data:
            # Expand covered_j to new global period
            new_gp = lcm(global_period, per)
            if new_gp > 10**6:
                break  # safety: period too large, skip this ℓ
            if new_gp != global_period:
                new_covered = set()
                for j0 in covered_j:
                    for k in range(new_gp // global_period):
                        new_covered.add(j0 + k * global_period)
                covered_j = new_covered
                global_period = new_gp

            # Add newly blocked j values
            for jb in blk:
                for k in range(global_period // per):
                    covered_j.add((jb + k * per) % global_period)

            used_ells.append(ell)

            # Check if fully covered
            if len(covered_j) == global_period:
                break

        if len(covered_j) == global_period:
            ells_used.update(used_ells)
            ell_str = ", ".join(f"ℓ={e}" for e in used_ells)
            print(f"    m≡{pos:>2d}: q={q_cover}, BLOCKED (period {global_period}) "
                  f"by {ell_str}")
        else:
            uncovered = global_period - len(covered_j)
            print(f"    m≡{pos:>2d}: q={q_cover}, UNBLOCKED "
                  f"({uncovered}/{global_period} j-classes uncovered)")
            class_ok = False

    if class_ok:
        print(f"    ✓ ALL positions verified (primes used: {sorted(ells_used)})")
        class_details.append((label, r_crt, True, sorted(ells_used)))
    else:
        print(f"    ✗ INCOMPLETE")
        class_details.append((label, r_crt, False, sorted(ells_used)))
        all_verified = False

# ── Summary ───────────────────────────────────────────────────────────

print(f"\n{'='*70}")
n_ok = sum(1 for _, _, ok, _ in class_details if ok)
if all_verified:
    print("ALL 22 CLASSES FULLY VERIFIED BY SUBGROUP EXCLUSION")
else:
    print(f"{n_ok}/22 CLASSES VERIFIED, {22 - n_ok} NEED ADDITIONAL ARGUMENTS")
print(f"{'='*70}")

all_ells = set()
for _, _, ok, ells in class_details:
    all_ells.update(ells)
print(f"\nBlocking primes used across all classes: {sorted(all_ells)}")
if all_ells:
    print(f"Maximum blocking prime needed: {max(all_ells)}")

# ── Proof structure ───────────────────────────────────────────────────

if all_verified:
    print(f"""
THEOREM: For r in the 22 residue classes mod 255255 listed above,
every prime p ≡ r (mod 255255) with p > 255255 is obstructed (k = 0).

PROOF STRUCTURE:

Lemma 1 (Multi-coverage). If q₁ ≠ q₂ are distinct primes with
  q₁ | n and q₂ | n, then n is not a prime power.

Lemma 2 (Covering completeness). For each of the 22 classes, the
  covering system {{q_i, ord(2,q_i), k_i}} covers all of Z/12Z.

Lemma 3 (Subgroup exclusion). For each single-coverage position
  (pos, q) in each class, and for each m ≡ pos (mod 12), there
  exists a prime ℓ ∈ {sorted(all_ells)} such that
  (r - 2^m) mod ℓ ∉ ⟨q⟩ ⊂ (Z/ℓZ)*.
  [Finite verification over all 22 × single-positions × period.]

Combining: for any m, either multi-coverage (Lemma 1) or
subgroup exclusion (Lemma 3) prevents p - 2^m from being
a prime power. □
""")
