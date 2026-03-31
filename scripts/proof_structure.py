"""
Examine the algebraic mechanism at single-coverage positions
in the 22 unconditional obstruction classes.

At these positions, only q=3 (or q=11) divides p - 2^m.
For 100% obstruction, p - 2^m must NOT be a prime power.
Since 3 | p - 2^m, the only prime power possibility is 3^n.
So we need to show: p - 2^m is never a power of 3 at these positions.

Equivalently: p ≢ 2^m (mod 3^2) at the right residue, or more generally,
the 3-adic valuation v_3(p - 2^m) is bounded and there's always a cofactor.
"""
from sage.all import Mod, ZZ, factor, is_prime_power as sage_is_pp
import polars as pl

CACHE_DIR = "/media/extssd/research/dioph.pp/data/covering"
agg = pl.read_parquet(f"{CACHE_DIR}/mod255255_aggregate.parquet")

perfect = agg.filter(
    (pl.col("n_obs") == pl.col("total")) & (pl.col("total") > 100)
)

# For each 100% group, identify single-coverage positions and analyze
# what additional congruences force compositeness

PRIMES_LIST = [3, 5, 7, 11, 13, 17]

print("=" * 70)
print("SINGLE-COVERAGE POSITION ANALYSIS")
print("=" * 70)

for row in perfect.head(5).iter_rows(named=True):
    r3, r5, r7, r11, r13, r17 = row["r3"], row["r5"], row["r7"], row["r11"], row["r13"], row["r17"]
    residues = {"r3": r3, "r5": r5, "r7": r7, "r11": r11, "r13": r13, "r17": r17}
    masks = {q: row[f"mask{q}"] for q in PRIMES_LIST}

    label = f"({r3},{r5},{r7},{r11},{r13},{r17})"
    print(f"\nGroup {label}:")

    for pos in range(12):
        covering = [q for q in PRIMES_LIST if masks[q] & (1 << pos)]
        if len(covering) != 1:
            continue

        q_cover = covering[0]
        # At position m ≡ pos (mod 12), only q_cover divides p - 2^m.
        # For p - 2^m to be a prime power, it must be q_cover^n.
        # We know p ≡ r (mod 255255).
        # So p - 2^m ≡ r - 2^m (mod 255255).
        # At this position, m ≡ pos (mod 12), so 2^m mod q is fixed for each q.

        # Compute r - 2^pos mod each prime to see what other factors are forced
        print(f"\n  m ≡ {pos} (mod 12): covered only by q={q_cover}")

        # The remainder p - 2^m ≡ 0 (mod q_cover) by construction.
        # Check: is it also ≡ 0 mod any OTHER prime (not in our set)?

        # For the first few actual m values of this form, check the remainder
        # using a concrete small prime from this class.
        # CRT: find smallest prime p ≡ (r3, r5, r7, r11, r13, r17) mod (3,5,7,11,13,17)
        from sage.all import CRT, crt
        r_mod255255 = int(crt([r3, r5, r7, r11, r13, r17], [3, 5, 7, 11, 13, 17]))
        print(f"    p ≡ {r_mod255255} (mod 255255)")

        # For m ≡ pos mod 12, 2^m mod 255255 cycles with period lcm(ord(2,q) for q | 255255)
        # Since 255255 = 3·5·7·11·13·17, and lcm(2,4,3,10,12,8) = 120
        # So 2^m mod 255255 has period 120.
        # For m ≡ pos mod 12, the values 2^m mod 255255 cycle through 10 values
        # (since 120/12 = 10)

        remainders_mod = []
        for j in range(10):  # 10 distinct m values mod 120 with m ≡ pos mod 12
            m = pos + 12 * j
            if m == 0:
                m = 120  # m starts at 1 in practice, but pos=0 means m=12,24,...
            pow2m = pow(2, m, 255255)
            remainder_mod = (r_mod255255 - pow2m) % 255255
            remainders_mod.append((m, remainder_mod))

        # Factor each remainder mod 255255
        print(f"    Remainders p - 2^m mod 255255 for m ≡ {pos} (mod 12):")
        for m, rem in remainders_mod:
            if rem == 0:
                print(f"      m={m:>3d}: 255255 | p - 2^m (rare)")
            else:
                f = factor(ZZ(rem))
                # Check: is q_cover a factor?
                has_cover = any(p == q_cover for p, _ in f)
                other_factors = [int(p) for p, _ in f if p != q_cover and p > 2]
                print(f"      m={m:>3d}: p-2^m ≡ {rem:>6d} (mod 255255) = {f}  "
                      f"{'✓' if has_cover else '✗'} q={q_cover}"
                      f"  others: {other_factors}")

# Now the KEY question: at single-coverage positions, does p - 2^m
# always have a factor that's forced by the congruence class?

print(f"\n\n{'='*70}")
print("PROOF STRUCTURE: FORCED COFACTORS AT SINGLE-COVERAGE POSITIONS")
print("=" * 70)

# For each of the 22 classes, for each single-coverage position,
# determine which ADDITIONAL prime always divides p - 2^m.
# This happens when: for all m ≡ pos (mod 12), the remainder
# p - 2^m ≡ 0 (mod q_extra) for some fixed q_extra.

print("""
At a single-coverage position m ≡ pos (mod 12) with covering prime q:
  - q | p - 2^m by the covering condition
  - For p - 2^m to be q^n, we need v_q(p - 2^m) = n and no other prime factor

The congruence p ≡ r (mod 255255) fixes p - 2^m mod each small prime.
If q' | p - 2^m for ALL m ≡ pos (mod 12), then q' is a "forced cofactor"
and p - 2^m is always divisible by both q and q', hence never q^n.

Checking for forced cofactors...
""")

for row in perfect.iter_rows(named=True):
    r3, r5, r7, r11, r13, r17 = row["r3"], row["r5"], row["r7"], row["r11"], row["r13"], row["r17"]
    masks = {q: row[f"mask{q}"] for q in PRIMES_LIST}
    label = f"({r3},{r5},{r7},{r11},{r13},{r17})"

    r_crt = int(crt([r3, r5, r7, r11, r13, r17], [3, 5, 7, 11, 13, 17]))

    single_positions = []
    for pos in range(12):
        covering = [q for q in PRIMES_LIST if masks[q] & (1 << pos)]
        if len(covering) == 1:
            single_positions.append((pos, covering[0]))

    # For each single position, check which primes ALWAYS divide the remainder
    # We check primes up to 200
    from sage.all import prime_range
    check_primes = [int(p) for p in prime_range(2, 200)]

    forced_cofactors = {}
    for pos, q_cover in single_positions:
        forced = []
        for q_check in check_primes:
            if q_check == q_cover:
                continue
            # Does q_check | p - 2^m for ALL m ≡ pos (mod 12)?
            ord_q = int(Mod(2, q_check).multiplicative_order()) if q_check > 2 else 1
            period = 12 * ord_q  # LCM(12, ord_q) but 12 is enough context

            all_divisible = True
            for j in range(max(1, ord_q)):  # check one full period
                m = pos + 12 * j
                if m == 0:
                    m = 12 * ord_q  # avoid m=0
                rem = (r_crt - pow(2, m, q_check)) % q_check
                if rem != 0:
                    all_divisible = False
                    break

            if all_divisible:
                forced.append(q_check)

        forced_cofactors[pos] = (q_cover, forced)

    # Display
    all_locked = True
    print(f"\n  {label} (p ≡ {r_crt} mod 255255):")
    for pos, q_cover in single_positions:
        cofactors = forced_cofactors[pos][1]
        if cofactors:
            print(f"    m≡{pos:>2d}: q={q_cover} + forced cofactors {cofactors[:5]}  ← LOCKED")
        else:
            print(f"    m≡{pos:>2d}: q={q_cover} + NO forced cofactor found  ← OPEN")
            all_locked = False

    if all_locked:
        print(f"    → ALL single positions locked by forced cofactors")
    else:
        print(f"    → SOME positions need deeper analysis")
