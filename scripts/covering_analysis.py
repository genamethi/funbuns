"""
Covering-system analysis of obstructed primes.

For each obstructed prime p, find the minimal set of (q, k_0) pairs
such that q | p - 2^{k_0} and the arithmetic progressions
{k_0 + j * ord(2, q) : j >= 0} cover all m in [1, floor(log2(p))].

This connects to Erdős-Selfridge covering systems: the obstruction
structure is exactly a covering of [1, max_m] by arithmetic progressions
with moduli = multiplicative orders of 2 mod small primes.
"""
import math
from sage.all import factor, ZZ, Mod, multiplicative_order
import polars as pl
import duckdb

# Precompute ord(2, q) for small odd primes
# These are primes q where q | 2^d - 1 for some d <= 39
SMALL_PRIMES = [3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47,
                53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107,
                109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167,
                173, 179, 181, 191, 193, 197, 199]

ord2 = {}
for q in SMALL_PRIMES:
    ord2[q] = int(multiplicative_order(Mod(2, q)))

print("Multiplicative orders ord(2, q):")
print("  " + "  ".join(f"q={q}: ord={ord2[q]}" for q in SMALL_PRIMES[:20]))
print()

# Group by order to see the covering structure
from collections import defaultdict
by_order = defaultdict(list)
for q in SMALL_PRIMES:
    by_order[ord2[q]].append(q)
print("Primes grouped by ord(2, q):")
for d in sorted(by_order):
    print(f"  ord = {d:3d}: {by_order[d]}")
print()

# Load obstructed primes
con = duckdb.connect("data/funbuns.duckdb", read_only=True)
obstructed = con.execute("""
    SELECT p FROM partition_counts WHERE k = 0 ORDER BY p LIMIT 500
""").pl()
con.close()

print(f"Loaded {obstructed.height} obstructed primes")
print()

def find_covering(p):
    """
    For an obstructed prime p, find which (q, k_0) pairs cover [1, max_m].

    Returns:
        covering: list of (q, ord, k_0, covered_set) tuples
        uncovered: set of m values not covered by any small prime
    """
    p = int(p)
    max_m = int(math.floor(math.log2(p)))
    target = set(range(1, max_m + 1))

    # For each small prime q, find which m values it covers
    candidates = []
    for q in SMALL_PRIMES:
        d = ord2[q]
        # Find k_0: the smallest k >= 1 where q | p - 2^k
        remainder = (p - 2) % q
        # Actually, check each residue class mod d
        for k0 in range(1, d + 1):
            if (p - (1 << k0)) % q == 0:
                # This q covers {k0, k0+d, k0+2d, ...} intersect [1, max_m]
                covered = set(range(k0, max_m + 1, d))
                if covered & target:
                    candidates.append((q, d, k0, covered & target))
                break  # only need one starting point per q

    # Greedy set cover: pick the (q, k_0) that covers the most uncovered m values
    covering = []
    uncovered = set(target)
    # Sort candidates by coverage size (descending)
    while uncovered:
        best = None
        best_new = 0
        for cand in candidates:
            q, d, k0, covered = cand
            new_coverage = len(covered & uncovered)
            if new_coverage > best_new:
                best = cand
                best_new = new_coverage
        if best is None or best_new == 0:
            break
        covering.append(best)
        uncovered -= best[3]

    return covering, uncovered

# Analyze each obstructed prime
results = []
for p_val in obstructed["p"].to_list():
    p = int(p_val)
    if p < 5:
        continue
    covering, uncovered = find_covering(p)
    max_m = int(math.floor(math.log2(p)))

    results.append({
        "p": p,
        "max_m": max_m,
        "num_covering_primes": len(covering),
        "covering_primes": [q for q, _, _, _ in covering],
        "covering_orders": [d for _, d, _, _ in covering],
        "uncovered_m": sorted(uncovered),
        "fully_covered": len(uncovered) == 0,
    })

# Summary statistics
total = len(results)
fully_covered = sum(1 for r in results if r["fully_covered"])
print(f"\nCovering analysis for {total} obstructed primes (p >= 5):")
print(f"  Fully covered by small primes: {fully_covered}/{total}")
print(f"  Partially covered: {total - fully_covered}/{total}")
print()

# Distribution of covering set sizes
from collections import Counter
size_dist = Counter(r["num_covering_primes"] for r in results if r["fully_covered"])
print("Covering set size distribution (fully covered primes):")
for size in sorted(size_dist):
    print(f"  {size} primes needed: {size_dist[size]} obstructed primes")
print()

# Show detailed covering for first 20
print("=" * 70)
print("DETAILED COVERING ANALYSIS")
print("=" * 70)
for r in results[:30]:
    p = r["p"]
    max_m = r["max_m"]
    status = "COVERED" if r["fully_covered"] else f"GAPS at m={r['uncovered_m']}"
    print(f"\np = {p}  (max_m = {max_m})  [{status}]")

    covering, _ = find_covering(p)
    remaining = set(range(1, max_m + 1))
    for q, d, k0, covered in covering:
        newly_covered = covered & remaining
        m_list = sorted(newly_covered)
        remaining -= newly_covered
        print(f"  q={q:>4d} (ord={d:>2d}), k₀={k0}: covers m = {m_list}")

# What primes appear most often in minimal covering sets?
covering_freq = Counter()
for r in results:
    if r["fully_covered"]:
        for q in r["covering_primes"]:
            covering_freq[q] += 1

print(f"\n{'='*70}")
print(f"COVERING PRIME FREQUENCY (in minimal covering sets)")
print(f"{'='*70}")
for q, count in covering_freq.most_common(20):
    print(f"  q={q:>4d} (ord={ord2[q]:>2d}): in {count:>3d}/{fully_covered} minimal covers ({100*count/fully_covered:.1f}%)")

# Now the key question: what's the minimal covering system?
# For primes up to 2^20 (~10^6), max_m = 19.
# Which set of small primes can cover [1, 19]?
print(f"\n{'='*70}")
print("COVERING SYSTEM STRUCTURE")
print("=" * 70)
print("\nFor max_m = 19 (primes up to ~500K):")
print("Need to cover [1, 19] with arithmetic progressions mod ord(2, q)")
print()
print("Available moduli from small primes:")
for d in sorted(by_order):
    if d <= 20:
        print(f"  mod {d:>2d}: primes {by_order[d]}")

# The lcm structure
from sage.all import lcm
orders = sorted(set(ord2[q] for q in SMALL_PRIMES))
print(f"\nDistinct orders: {orders}")
print(f"LCM of all orders: {lcm(orders)}")

# Check: can {3, 7, 5} alone cover [1, 19]?
# 3 has ord 2: covers all even or all odd positions
# 7 has ord 3: covers 1/3 of positions
# 5 has ord 4: covers 1/4 of positions
print("\nMinimal covering attempt with {3, 7, 5}:")
print("  q=3 (mod 2): covers alternating m values")
print("  q=7 (mod 3): covers every 3rd m value")
print("  q=5 (mod 4): covers every 4th m value")
print("  LCM(2,3,4) = 12, so this system repeats with period 12")
print("  Positions mod 12 that need covering: all of {0,1,...,11}")
print()

# Check if {2,3,4} can cover
# Period 2 covers: {r_3} and {r_3 + 1} mod 2 (one residue class)
# Period 3 covers: one residue class mod 3
# Period 4 covers: one residue class mod 4
# Together: can they cover all of Z/12Z?
# mod 2 covers 6 positions, mod 3 covers 4, mod 4 covers 3
# Best case: 6 + 4 + 3 = 13 > 12, so possible with overlap
for r3 in range(2):
    for r7 in range(3):
        for r5 in range(4):
            covered = set()
            for x in range(12):
                if x % 2 == r3 or x % 3 == r7 or x % 4 == r5:
                    covered.add(x)
            if len(covered) == 12:
                print(f"  Full cover: r≡{r3}(mod 2), r≡{r7}(mod 3), r≡{r5}(mod 4)")

print("\nSo (3, 7, 5) can form a covering system for Z/12Z,")
print("but not all residue class combinations work.")
print("The actual residue classes depend on p mod lcm(3,7,5) = p mod 105.")

# For each obstructed prime, check p mod 105
print(f"\n{'='*70}")
print("RESIDUE STRUCTURE: p mod 105 for obstructed primes")
print("=" * 70)
mod105 = Counter()
for r in results:
    mod105[r["p"] % 105] += 1
print("p mod 105 distribution:")
for res in sorted(mod105):
    if mod105[res] >= 2:
        print(f"  p ≡ {res:>3d} (mod 105): {mod105[res]:>3d} primes")
