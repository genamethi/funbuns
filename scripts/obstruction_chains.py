"""
For each obstructed prime p (k=0), check the chain of residues p - 2^m
against the factorizations of 2^m - 2^k differences.

The hypothesis: obstructed primes fail because for every candidate m,
p - 2^m has at least two distinct odd prime factors (not a prime power),
and these factors are "explained" by shared divisibility with differences
between powers of two.

Specifically, if q | 2^{m} - 2^{k} and q | p - 2^{k}, then q | p - 2^{m}.
So a single small prime poisoning p - 2^{k} cascades to other m values.
"""
import math
import polars as pl
from sage.all import factor, ZZ, is_prime_power as sage_is_prime_power

# Load the difference table
diffs = pl.read_parquet("data/power_of_two_diffs.parquet")

# Collect all unique odd primes that appear in the difference table
all_odd_primes = set()
for row in diffs.select("odd_prime_factors").to_series():
    all_odd_primes.update(row)
all_odd_primes = sorted(all_odd_primes)
print(f"Unique odd primes in difference table: {len(all_odd_primes)}")
print(f"Smallest 20: {all_odd_primes[:20]}")

# Load some obstructed primes from the database
import duckdb
con = duckdb.connect("data/funbuns.duckdb", read_only=True)
obstructed = con.execute("""
    SELECT p FROM partition_counts WHERE k = 0 ORDER BY p LIMIT 200
""").pl()
con.close()

print(f"\nFirst 10 obstructed primes: {obstructed['p'].head(10).to_list()}")

# For each obstructed prime, build its residue profile
results = []
for p_val in obstructed["p"].to_list()[:50]:  # start with first 50
    p = int(p_val)
    max_m = int(math.floor(math.log2(p)))

    # For each valid m (where 2^m < p), compute p - 2^m and its factorization
    chain = []
    for m in range(1, max_m + 1):
        remainder = p - (1 << m)
        if remainder <= 0:
            break
        f = factor(ZZ(remainder))
        odd_factors = sorted([int(q) for q, _e in f if q != 2])
        is_pp = bool(sage_is_prime_power(ZZ(remainder)))

        # Which primes from our difference table divide this remainder?
        # For each k < m, check if any odd prime factor of 2^m - 2^k
        # also divides p - 2^k (propagation check)
        propagations = []
        for k in range(1, m):
            diff_row = diffs.filter(
                (pl.col("m") == m) & (pl.col("k") == k)
            )
            if diff_row.height == 0:
                continue
            diff_odd_primes = diff_row["odd_prime_factors"][0]
            remainder_at_k = p - (1 << k)
            shared = [int(q) for q in diff_odd_primes if remainder_at_k % q == 0]
            if shared:
                propagations.append((k, shared))

        chain.append({
            "m": m,
            "remainder": remainder,
            "factorization": str(f),
            "odd_factors": odd_factors,
            "is_prime_power": is_pp,
            "propagations": propagations,
        })

    # Summary: for this prime, which small primes explain the obstruction?
    all_blocking_primes = set()
    for step in chain:
        if not step["is_prime_power"]:
            all_blocking_primes.update(step["odd_factors"])

    results.append({
        "p": p,
        "max_m": max_m,
        "chain": chain,
        "blocking_primes": sorted(all_blocking_primes),
        "num_blocking": len(all_blocking_primes),
    })

# Print detailed analysis for a few primes
for r in results[:10]:
    p = r["p"]
    print(f"\n{'='*60}")
    print(f"p = {p}  (max_m = {r['max_m']})")
    print(f"Blocking primes (union of odd factors across all non-pp remainders): {r['blocking_primes']}")
    for step in r["chain"]:
        pp_marker = "  PP" if step["is_prime_power"] else " !PP"
        prop_str = ""
        if step["propagations"]:
            props = [f"k={k}:{qs}" for k, qs in step["propagations"]]
            prop_str = f"  propagated from: {', '.join(props)}"
        print(f"  m={step['m']:2d}: {p} - 2^{step['m']} = {step['remainder']:>10d} = {step['factorization']:<30s}{pp_marker}{prop_str}")

# Now look at the propagation structure: for each obstructed prime,
# which primes at which k values cascade to block other m values?
print(f"\n{'='*60}")
print("PROPAGATION SUMMARY")
print(f"{'='*60}")
for r in results[:20]:
    p = r["p"]
    # Build a map: for each k, which primes at that k propagate forward?
    k_poison = {}
    for step in r["chain"]:
        for k, shared in step["propagations"]:
            if k not in k_poison:
                k_poison[k] = set()
            k_poison[k].update(shared)

    if k_poison:
        poison_str = "; ".join(f"k={k}: {sorted(qs)}" for k, qs in sorted(k_poison.items()))
        print(f"p={p:>8d}: {poison_str}")

# Frequency of blocking primes across all obstructed primes
from collections import Counter
prime_freq = Counter()
for r in results:
    for q in r["blocking_primes"]:
        prime_freq[q] += 1

print(f"\n{'='*60}")
print(f"BLOCKING PRIME FREQUENCY (across {len(results)} obstructed primes)")
print(f"{'='*60}")
for q, count in prime_freq.most_common(30):
    print(f"  q={q:>8d}: appears in {count:>3d}/{len(results)} obstructed primes ({100*count/len(results):.1f}%)")
