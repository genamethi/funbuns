"""
Covering map analysis for prime power partitions.

For a prime p, shows:
  1. The Mersenne-based covering map: which small primes q cover which m positions
  2. Uncovered positions: the only candidates for p = 2^m + q^n decompositions
  3. Computed decompositions at uncovered positions
  4. Cross-check: Mersenne factors governing the m-gap between co-parent pairs

The covering propagation rule: if q | (p - 2^k), then q also divides
p - 2^{k + j*ord(2,q)} for all j >= 0.  This is because
2^{k+d} - 2^k = 2^k * (2^d - 1) = 2^k * M_d, and q | M_d iff ord(2,q) | d.

Usage:
    pixi run python scripts/covering_map.py <prime>
    pixi run python scripts/covering_map.py --validate N   # spot-check N primes from data
    pixi run python scripts/covering_map.py --mersenne      # show M_n factor / order table
"""

import sys
import math
from collections import defaultdict
from sage.all import (Integer, factor, is_prime, is_prime_power,
                      multiplicative_order, Mod, prime_pi, previous_prime)


# ── Mersenne factor table ──────────────────────────────────────────────────
# Precompute: for each d in 1..40, the prime factors of M_d = 2^d - 1.
# Also extract ord(2, q) for each unique prime q dividing any M_d.

def build_mersenne_table(d_max=40):
    """Build Mersenne factor table and multiplicative order lookup."""
    mersenne_factors = {}  # d → list of prime factors of M_d
    ord2 = {}              # q → ord(2, q)

    for d in range(1, d_max + 1):
        M = Integer(2**d - 1)
        if M == 1:
            mersenne_factors[d] = []
            continue
        primes = [int(p) for p, _ in factor(M)]
        mersenne_factors[d] = primes
        for q in primes:
            if q not in ord2:
                ord2[q] = int(multiplicative_order(Mod(2, q)))

    return mersenne_factors, ord2


# Cache at module level
_MERSENNE, _ORD2 = None, None

def get_tables(d_max=40):
    global _MERSENNE, _ORD2
    if _MERSENNE is None:
        _MERSENNE, _ORD2 = build_mersenne_table(d_max)
    return _MERSENNE, _ORD2


# ── Covering map for a single prime ───────────────────────────────────────

def compute_covering(p, d_max=40):
    """
    For prime p, compute which m positions in [1, floor(log2(p))] are covered
    by each small prime q (factors of M_d for d <= d_max).

    Returns:
        coverage: dict q → set of m values covered by q
        uncovered: set of m values not covered by any q
        max_m: floor(log2(p))
    """
    p = int(p)
    _, ord2 = get_tables(d_max)
    max_m = int(math.floor(math.log2(p)))
    all_m = set(range(1, max_m + 1))

    coverage = {}
    covered_union = set()

    # For each covering prime q, find which m values it covers
    for q, d in sorted(ord2.items(), key=lambda x: x[1]):
        # Find starting positions: k in [1, d] where q | (p - 2^k)
        for k in range(1, min(d, max_m) + 1):
            if (p - (1 << k)) % q == 0:
                # q covers the AP {k, k+d, k+2d, ...} ∩ [1, max_m]
                covered = set(range(k, max_m + 1, d))
                if covered:
                    if q not in coverage:
                        coverage[q] = set()
                    coverage[q] |= covered
                    covered_union |= covered

    uncovered = all_m - covered_union
    return coverage, uncovered, max_m


def classify_positions(coverage, max_m):
    """
    Classify each m position by coverage multiplicity:
      - multi-covered (2+ primes): decomposition IMPOSSIBLE (remainder composite)
      - singly-covered (1 prime q): decomposition only possible as q^n
      - uncovered (0 primes): decomposition possible for any prime power

    Returns dict m → (multiplicity, covering_primes)
    """
    result = {}
    for m in range(1, max_m + 1):
        covering_qs = [q for q, ms in coverage.items() if m in ms]
        result[m] = (len(covering_qs), covering_qs)
    return result


def compute_decompositions(p):
    """
    Compute all decompositions p = 2^m + q^n where q is odd prime, m,n >= 1.
    Returns list of (m, n, q) tuples.
    """
    p = int(p)
    max_m = int(math.floor(math.log2(p)))
    decomps = []

    for m in range(1, max_m + 1):
        r = p - (1 << m)
        if r <= 0:
            break
        if r == 1:
            continue  # 1 is not a prime power
        if is_prime_power(Integer(r)):
            # Factor to get q^n
            f = factor(Integer(r))
            q_val = int(f[0][0])
            n_val = int(f[0][1])
            if q_val == 2:
                continue  # q must be odd
            decomps.append((m, n_val, q_val))

    return decomps


def show_mersenne_table():
    """Print the Mersenne factor / order table."""
    mersenne, ord2 = get_tables()

    # Group primes by their order
    by_order = defaultdict(list)
    for q, d in sorted(ord2.items()):
        by_order[d].append(q)

    print("Mersenne numbers M_d = 2^d - 1 and their prime factors")
    print("=" * 78)
    print(f"{'d':>3}  {'M_d':>15}  {'Factors':<40}  {'Primitive'}")
    print("-" * 78)
    for d in range(1, 41):
        M = 2**d - 1
        factors = mersenne[d]
        # Primitive = primes whose order is exactly d (not a divisor)
        primitive = [q for q in factors if ord2.get(q) == d]
        prim_str = ", ".join(str(q) for q in primitive) if primitive else "-"
        fac_str = ", ".join(str(q) for q in factors) if factors else "1"
        if len(fac_str) > 38:
            fac_str = fac_str[:35] + "..."
        print(f"{d:>3}  {M:>15}  {fac_str:<40}  {prim_str}")

    print()
    print("Primes grouped by ord(2, q):")
    print("-" * 50)
    for d in sorted(by_order):
        qs = by_order[d]
        qs_str = ", ".join(str(q) for q in qs[:8])
        if len(qs) > 8:
            qs_str += f" ... ({len(qs)} total)"
        print(f"  ord {d:>2}: {qs_str}")


def analyze_prime(p):
    """Full covering map analysis for a single prime."""
    p = int(p)
    if not is_prime(Integer(p)):
        print(f"Error: {p} is not prime")
        return

    _, ord2 = get_tables()
    coverage, uncovered, max_m = compute_covering(p)
    decomps = compute_decompositions(p)

    print(f"Covering map for p = {p}")
    print(f"  max_m = {max_m} (log2(p) = {math.log2(p):.2f})")
    print(f"  k = {len(decomps)} decompositions")
    print()

    # Classify positions by coverage multiplicity
    positions = classify_positions(coverage, max_m)
    decomp_by_m = {m: (n, q) for m, n, q in decomps}

    # Show coverage map as a grid
    print(f"{'m':>4}  {'status':^7}  {'covered by':.<42}  {'remainder':>15}  {'decomposition'}")
    print("-" * 100)

    for m in range(1, max_m + 1):
        r = p - (1 << m)
        if r <= 0:
            r_str = "-"
            dec_str = ""
        else:
            r_str = str(r)
            if m in decomp_by_m:
                n, q = decomp_by_m[m]
                dec_str = f"= {q}^{n}"
            else:
                dec_str = ""

        mult, covering_qs = positions[m]
        covering_qs_sorted = sorted(covering_qs, key=lambda q: ord2.get(q, 999))

        if mult == 0:
            status = "OPEN"
            cov_str = "-"
        elif mult == 1:
            status = f"q={covering_qs[0]}"
            cov_str = f"{covering_qs[0]}(d={ord2[covering_qs[0]]})"
        else:
            status = f"x{mult}"
            cov_str = ", ".join(f"{q}(d={ord2[q]})" for q in covering_qs_sorted[:5])
            if len(covering_qs_sorted) > 5:
                cov_str += f" +{len(covering_qs_sorted)-5}"

        print(f"{m:>4}  {status:^7}  {cov_str:<42}  {r_str:>15}  {dec_str}")

    # Summary
    print()
    n_multi = sum(1 for m in range(1, max_m+1) if positions[m][0] >= 2)
    n_single = sum(1 for m in range(1, max_m+1) if positions[m][0] == 1)
    n_open = sum(1 for m in range(1, max_m+1) if positions[m][0] == 0)
    print(f"Position classification ({max_m} total):")
    print(f"  Multi-covered (blocked):     {n_multi:>3}  -- remainder has 2+ prime factors, no decomposition possible")
    print(f"  Singly-covered (restricted): {n_single:>3}  -- decomposition only as power of the covering prime")
    print(f"  Uncovered (open):            {n_open:>3}  -- any prime power remainder possible")
    print(f"  Decompositions found:        {len(decomps):>3}")

    print(f"\nActive covering primes:")
    active_qs = sorted(coverage.keys(), key=lambda q: ord2.get(q, 999))
    for q in active_qs:
        ms = sorted(coverage[q])
        print(f"  q={q:>7} (ord={ord2[q]:>2}): covers {len(ms):>2} positions: {ms}")

    if decomps:
        print(f"\nDecomposition consistency check:")
        for m, n, q in decomps:
            mult, covering_qs = positions[m]
            if mult == 0:
                # Open position: any prime power OK
                status = "OK (open position)"
            elif mult == 1:
                # Singly covered: decomposition must be a power of the covering prime
                if q == covering_qs[0]:
                    status = f"OK (singly covered by q={q})"
                else:
                    status = f"!! ANOMALY: covered by {covering_qs[0]} but decomp uses q={q} !!"
            else:
                # Multi-covered: should be impossible
                status = f"!! ANOMALY: multi-covered (x{mult}) but decomposition exists !!"
            print(f"  p = 2^{m} + {q}^{n}  [{status}]")

    # Mersenne connection between co-parent pairs
    if len(decomps) >= 2:
        print(f"\n  Co-parent Mersenne structure:")
        print(f"  For edges q1->p and q2->p with m-gap d = |m1-m2|:")
        print(f"  q2^n2 - q1^n1 = 2^min(m1,m2) * M_d")
        print()
        mersenne, _ = get_tables()
        for i in range(len(decomps)):
            for j in range(i+1, len(decomps)):
                m1, n1, q1 = decomps[i]
                m2, n2, q2 = decomps[j]
                d = abs(m1 - m2)
                if d == 0:
                    continue
                M_d_factors = mersenne.get(d, [])
                diff = q2**n2 - q1**n1
                m_min = min(m1, m2)
                M_d = 2**d - 1
                # Verify: diff = 2^m_min * M_d? No:
                # diff = 2^m1 - 2^m2 if m1 > m2, but actually
                # q2^n2 - q1^n1 = (p - 2^m2) - (p - 2^m1) = 2^m1 - 2^m2
                # So if m1 > m2: diff_expected = 2^m2 * (2^(m1-m2) - 1) = 2^m2 * M_d
                if m1 > m2:
                    expected = (1 << m2) * M_d
                else:
                    expected = -((1 << m1) * (2**(m2-m1) - 1))
                actual = q2**n2 - q1**n1
                check = "OK" if actual == expected else "MISMATCH"
                fac_str = "*".join(str(f) for f in M_d_factors) if M_d_factors else str(M_d)
                print(f"    ({q1}^{n1}, {q2}^{n2}): d={d}, M_{d} = {fac_str}  [{check}]")


def validate_from_data(n_primes):
    """Spot-check N primes from the data against covering predictions."""
    import duckdb
    import random

    con = duckdb.connect("data/funbuns.duckdb", read_only=True)

    # Get a sample of primes with various k values
    sample = con.execute(f"""
        SELECT p, k FROM partition_counts
        WHERE p > 100
        ORDER BY random()
        LIMIT {n_primes}
    """).fetchall()
    con.close()

    if not sample:
        print("No data found in DuckDB. Run 'funbuns-admin db sync' first.")
        return

    print(f"Validating {len(sample)} primes against covering predictions")
    print("=" * 70)

    mismatches = 0
    for p_val, k_data in sample:
        p = int(p_val)
        _, uncovered, max_m = compute_covering(p)
        decomps = compute_decompositions(p)
        k_computed = len(decomps)

        # Check: k from data should match computed k
        if k_data != k_computed:
            print(f"  MISMATCH p={p}: data k={k_data}, computed k={k_computed}")
            mismatches += 1
            continue

        # Check: all decomposition m values should be at uncovered positions
        bad_positions = [m for m, n, q in decomps if m not in uncovered]
        if bad_positions:
            print(f"  ANOMALY p={p}: decompositions at covered positions m={bad_positions}")
            mismatches += 1
        else:
            k_str = f"k={k_computed}"
            uncov_str = f"{len(uncovered)}/{max_m} uncovered"
            print(f"  OK p={p:>12}: {k_str:>5}, {uncov_str}")

    print(f"\n{len(sample) - mismatches}/{len(sample)} passed, {mismatches} issues")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    if sys.argv[1] == "--mersenne":
        show_mersenne_table()
    elif sys.argv[1] == "--validate":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        validate_from_data(n)
    else:
        p = int(sys.argv[1])
        analyze_prime(p)
