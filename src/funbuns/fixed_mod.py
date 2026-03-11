"""
Fixed-modulus ring analysis for prime power partitions.

Reduces the exponential Diophantine equation p = 2^m + q^n to polynomial
congruences mod M by precomputing power residue sets. Identifies local
obstructions via Hensel lifting and combines them via CRT.

Key idea: {2^m mod M : m >= 1} is finite and periodic. So is
{q^n mod M : q prime, n >= 1} for each M. Their sumset determines which
residue classes of p can possibly admit decompositions. Primes in the
complement are *locally obstructed* at M.
"""

import polars as pl
from math import gcd, log
from pathlib import Path
from typing import Optional
from .utils import get_data_dir


# ---------------------------------------------------------------------------
# Pure-Python arithmetic (no SageMath needed for local checks)
# ---------------------------------------------------------------------------

def _primes_up_to(n: int) -> list[int]:
    """Simple sieve of Eratosthenes."""
    if n < 2:
        return []
    sieve = [True] * (n + 1)
    sieve[0] = sieve[1] = False
    for i in range(2, int(n**0.5) + 1):
        if sieve[i]:
            for j in range(i * i, n + 1, i):
                sieve[j] = False
    return [i for i, v in enumerate(sieve) if v]


def power_residues_mod(base: int, modulus: int) -> set[int]:
    """Compute {base^m mod modulus : m >= 1}.

    This is a finite cyclic set since base^m mod M is eventually periodic.
    """
    residues = set()
    pw = base % modulus
    for _ in range(modulus + 1):
        residues.add(pw)
        pw = (pw * base) % modulus
    return residues


def prime_power_residues_mod(modulus: int) -> set[int]:
    """Compute {q^n mod modulus : q prime, n >= 1}.

    By Dirichlet's theorem, every coprime residue class mod M contains
    primes. So the union of {r^n mod M : n >= 1} over all coprime r
    gives the full prime power residue set. For prime power moduli
    M = ell^k where (Z/M)* is cyclic, a primitive root generates
    everything, so the result is just the coprime residues.

    For general M, we use the structure:
    - Coprime residues: union of power residues for a small set of
      generators of (Z/M)*
    - Prime divisors of M: add their power residues separately
    """
    residues: set[int] = set()

    # Factor modulus to determine group structure
    prime_divisors = []
    temp = modulus
    for p in range(2, int(temp**0.5) + 2):
        if temp <= 1:
            break
        if temp % p == 0:
            prime_divisors.append(p)
            while temp % p == 0:
                temp //= p
    if temp > 1:
        prime_divisors.append(temp)

    # For each coprime residue class, power_residues_mod gives its
    # cyclic subgroup. But iterating all coprime residues is O(M) which
    # is expensive for large M. Instead, use generators.
    #
    # For M = ell^k (odd prime power): (Z/M)* is cyclic, so one
    # primitive root generates all coprime residues as powers.
    # For M = 2^k: (Z/M)* = Z/2 x Z/2^{k-2} for k >= 3.
    # For general M: use CRT decomposition.
    #
    # In all cases, the union of {g^n : n >= 1} over generators g
    # gives all coprime residues (since every coprime r is g^a for
    # some generator, and r^n = g^{an}).

    # Fast path: just compute {r : 1 <= r < M, gcd(r, M) = 1}
    # This IS the set of prime power residues mod M (by Dirichlet + group theory:
    # for any coprime r, r = q^1 mod M for some prime q in that residue class,
    # and all higher powers r^n are also achievable as prime powers).
    for r in range(1, modulus):
        if gcd(r, modulus) == 1:
            residues.add(r)

    # Also include prime divisors of M and their powers
    # (e.g., 2 divides 4, and 2^n mod 4 gives {0, 2})
    for p in prime_divisors:
        residues |= power_residues_mod(p, modulus)

    return residues


def achievable_residues(modulus: int) -> set[int]:
    """Compute {2^m + q^n mod modulus : m >= 1, q prime, n >= 1}.

    This is the set of residue classes mod M that can possibly admit
    a decomposition p = 2^m + q^n.
    """
    two_residues = power_residues_mod(2, modulus)
    qn_residues = prime_power_residues_mod(modulus)
    return {(t + r) % modulus for t in two_residues for r in qn_residues}


_achievable_cache: dict[int, set[int]] = {}


def local_has_solution(p: int, modulus: int) -> bool:
    """Check if p = 2^m + q^n has a solution mod `modulus`.

    Uses memoized achievable residue sets.
    """
    if modulus not in _achievable_cache:
        _achievable_cache[modulus] = achievable_residues(modulus)
    return (p % modulus) in _achievable_cache[modulus]


def local_solutions_series(primes: pl.Series, modulus: int) -> pl.Series:
    """For each prime p, does p = 2^m + q^n have a solution mod `modulus`?

    Returns Boolean series. Precomputes residue set once, then checks
    membership for each p in O(1).
    """
    achieved = achievable_residues(modulus)
    vals = [(p % modulus) in achieved for p in primes.to_list()]
    return pl.Series(f"sol_mod_{modulus}", vals, dtype=pl.Boolean)


# ---------------------------------------------------------------------------
# Obstruction depth via Hensel lifting (uses SageMath Zp)
# ---------------------------------------------------------------------------

def obstruction_depth(p: int, ell: int, max_prec: int = 20,
                      max_modulus: int = 10000) -> int:
    """Find smallest k such that p = 2^m + q^n has no solution mod ell^k.

    Uses successive power-of-ell moduli: ell, ell^2, ..., up to
    min(ell^max_prec, max_modulus). Returns the first k where no
    solution exists, or the effective max precision + 1 if solutions
    persist at all levels.

    Does NOT use SageMath -- pure Python modular arithmetic suffices here
    since we only need residue set computations.
    """
    for k in range(1, max_prec + 1):
        M = ell ** k
        if M > max_modulus:
            return k  # can't go deeper, treat as max
        if not local_has_solution(p, M):
            return k
    return max_prec + 1


def obstruction_depths_multi(p: int, primes: list[int],
                             max_prec: int = 20) -> dict[int, int]:
    """Compute obstruction depth at each prime ell for a single p."""
    return {ell: obstruction_depth(p, ell, max_prec) for ell in primes}


# ---------------------------------------------------------------------------
# CRT combination of local obstructions
# ---------------------------------------------------------------------------

def _lcm(a: int, b: int) -> int:
    return a * b // gcd(a, b)


def crt_obstruction_classes(moduli: list[int]) -> dict:
    """Combine local obstructions via CRT.

    For each modulus M, compute the set of residues mod M that are
    *not achievable* (locally obstructed). Then combine via CRT to find
    residue classes mod lcm(moduli) that are obstructed at ANY modulus
    (a single local obstruction suffices to block a global solution).

    Returns dict with:
      - 'modulus': the combined modulus (lcm)
      - 'obstructed_classes': set of residues mod combined modulus
      - 'per_modulus': dict mapping each M to its obstructed residue set
      - 'density': fraction of residue classes that are obstructed
    """
    per_modulus = {}
    for M in moduli:
        achieved = achievable_residues(M)
        obstructed = set(range(M)) - achieved
        per_modulus[M] = obstructed

    # Combine via CRT: a residue r mod L is structurally obstructed
    # if r mod M_i is in the obstructed set for EVERY M_i.
    L = moduli[0]
    for M in moduli[1:]:
        L = _lcm(L, M)

    # Only consider moduli that actually have obstructed classes
    active_moduli = [M for M in moduli if per_modulus[M]]
    combined_obstructed = set()
    if active_moduli:
        for r in range(L):
            # r is structurally obstructed if it's obstructed at ANY active modulus
            if any((r % M) in per_modulus[M] for M in active_moduli):
                combined_obstructed.add(r)

    return {
        'modulus': L,
        'obstructed_classes': combined_obstructed,
        'per_modulus': per_modulus,
        'density': len(combined_obstructed) / L if L > 0 else 0.0,
    }


# ---------------------------------------------------------------------------
# Block-by-block obstruction scan
# ---------------------------------------------------------------------------

def _block_pattern() -> str:
    return str(get_data_dir() / "blocks" / "pp_b*.parquet")


def _fixed_mod_out_dir() -> Path:
    d = get_data_dir() / "fixed_mod_blocks"
    d.mkdir(exist_ok=True)
    return d


def _scan_block(block_path: Path, moduli: list[int],
                verbose: bool = False) -> Optional[pl.DataFrame]:
    """For each obstructed prime in a block, check local solvability
    at each modulus.

    Returns DataFrame with columns: p, sol_mod_{M} for each M.
    """
    block = pl.scan_parquet(str(block_path))

    # Find obstructed primes (those where all m_k == 0)
    obstructed = (
        block
        .group_by('p')
        .agg((pl.col('m_k') == 0).all().alias('is_obstructed'))
        .filter(pl.col('is_obstructed'))
        .select('p')
        .sort('p')
        .collect()
    )

    if obstructed.height == 0:
        return None

    primes_series = obstructed['p']

    # Compute local solvability for each modulus
    result = obstructed.clone()
    for M in moduli:
        result = result.with_columns(local_solutions_series(primes_series, M))

    return result


def scan_obstructions(moduli: Optional[list[int]] = None,
                      limit: Optional[int] = None,
                      verbose: bool = False) -> pl.DataFrame:
    """Scan all blocks for local solvability of obstructed primes.

    For each obstructed prime, computes whether p = 2^m + q^n has a
    solution mod each modulus. Primes where local_has_solution is False
    are *locally obstructed* at that modulus.

    Args:
        moduli: List of moduli to check. Default: ell^k for small primes.
        limit: Stop after processing this many obstructed primes.
        verbose: Print progress per block.

    Returns DataFrame with p and boolean columns per modulus.
    """
    if moduli is None:
        # Default: prime powers up to moderate size
        moduli = [3, 4, 5, 7, 8, 9, 11, 13, 16, 25, 27, 49]

    data_dir = get_data_dir()
    block_files = sorted(data_dir.joinpath("blocks").glob("pp_b*.parquet"))
    if not block_files:
        print("No block data found.")
        return pl.DataFrame()

    all_results: list[pl.DataFrame] = []
    total_primes = 0

    for i, block_path in enumerate(block_files):
        if verbose:
            print(f"  Block {i + 1}/{len(block_files)}: {block_path.name}",
                  end="", flush=True)

        result = _scan_block(block_path, moduli, verbose)
        if result is not None:
            all_results.append(result)
            total_primes += result.height
            if verbose:
                print(f" ({result.height} obstructed)")
        else:
            if verbose:
                print(" (no obstructed primes)")

        if limit is not None and total_primes >= limit:
            break

    if not all_results:
        return pl.DataFrame()

    combined = pl.concat(all_results)
    if limit is not None:
        combined = combined.head(limit)

    return combined


# ---------------------------------------------------------------------------
# Depth analysis: Hensel lifting across a set of primes
# ---------------------------------------------------------------------------

def depth_analysis(primes_to_check: list[int],
                   lifting_primes: Optional[list[int]] = None,
                   max_prec: int = 12,
                   verbose: bool = False) -> pl.DataFrame:
    """Compute obstruction depth at each lifting prime for a list of primes.

    For each obstructed prime p and each lifting prime ell, finds the
    smallest k such that p = 2^m + q^n has no solution mod ell^k.

    Args:
        primes_to_check: List of (obstructed) primes to analyze.
        lifting_primes: Primes ell for Hensel lifting. Default: [2,3,5,7,11,13].
        max_prec: Maximum precision (k) for lifting.
        verbose: Print progress.

    Returns DataFrame with p and depth_ell columns.
    """
    if lifting_primes is None:
        lifting_primes = [2, 3, 5, 7, 11, 13]

    rows = []
    for i, p in enumerate(primes_to_check):
        if verbose and (i + 1) % 100 == 0:
            print(f"  Depth analysis: {i + 1}/{len(primes_to_check)}", flush=True)

        row = {'p': p}
        depths = obstruction_depths_multi(p, lifting_primes, max_prec)
        for ell, d in depths.items():
            row[f'depth_{ell}'] = d
        rows.append(row)

    schema = {'p': pl.Int64}
    for ell in lifting_primes:
        schema[f'depth_{ell}'] = pl.Int32

    return pl.DataFrame(rows, schema=schema)


# ---------------------------------------------------------------------------
# CLI entry point: run_fixed_mod_analysis
# ---------------------------------------------------------------------------

def run_fixed_mod_analysis(limit: Optional[int] = None,
                           verbose: bool = False) -> None:
    """Main entry point for fixed-modulus analysis.

    Steps:
      1. Scan blocks for local solvability at various moduli
      2. Compute obstruction depths via Hensel lifting (sample)
      3. CRT combination to find structural obstruction classes
      4. Report and save results
    """
    data_dir = get_data_dir()

    print("=== Fixed-Modulus Ring Analysis ===\n")

    # Step 1: Local solvability scan
    scan_moduli = [3, 4, 5, 7, 8, 9, 11, 13, 16, 25, 27, 49]
    print(f"[1/3] Local solvability scan (moduli: {scan_moduli})...")

    scan_df = scan_obstructions(moduli=scan_moduli, limit=limit, verbose=verbose)
    if scan_df.height == 0:
        print("No obstructed primes found.")
        return

    # Report: for each modulus, how many obstructed primes are locally obstructed
    print(f"\n  {scan_df.height} obstructed primes scanned\n")
    print(f"  {'Modulus':>8}  {'Locally obstructed':>20}  {'Fraction':>10}")
    print("  " + "-" * 44)
    for M in scan_moduli:
        col = f"sol_mod_{M}"
        if col in scan_df.columns:
            n_obstructed = int(scan_df.filter(~pl.col(col)).height)
            frac = n_obstructed / scan_df.height
            print(f"  {M:>8}  {n_obstructed:>20}  {frac:>10.4f}")
    print()

    # Step 2: Hensel lifting on a sample
    lifting_primes = [2, 3, 5, 7, 11, 13]
    sample_size = min(scan_df.height, 500)
    sample_primes = scan_df['p'].head(sample_size).to_list()

    print(f"[2/3] Obstruction depth via Hensel lifting "
          f"(sample={sample_size}, ell in {lifting_primes})...")
    depth_df = depth_analysis(sample_primes, lifting_primes,
                              max_prec=12, verbose=verbose)

    # Report depth distributions
    print(f"\n  Obstruction depth distribution (k where first blocked at ell^k):")
    for ell in lifting_primes:
        col = f"depth_{ell}"
        counts = depth_df.group_by(col).len().sort(col)
        finite = counts.filter(pl.col(col) <= 12)
        n_finite = int(finite['len'].sum()) if finite.height > 0 else 0
        print(f"  ell={ell}: {n_finite}/{sample_size} locally obstructed "
              f"(depths: {dict(zip(finite[col].to_list(), finite['len'].to_list()))})")
    print()

    # Step 3: CRT combination
    # Use prime moduli where we see actual obstructions
    crt_moduli = []
    for M in scan_moduli:
        col = f"sol_mod_{M}"
        if col in scan_df.columns:
            n_obs = int(scan_df.filter(~pl.col(col)).height)
            if n_obs > 0:
                crt_moduli.append(M)

    print(f"[3/3] CRT combination of locally-obstructing moduli: {crt_moduli}...")
    if crt_moduli:
        crt = crt_obstruction_classes(crt_moduli)
        n_classes = len(crt['obstructed_classes'])
        print(f"  Combined modulus: {crt['modulus']}")
        print(f"  Structurally obstructed classes: {n_classes}")
        print(f"  Density: {crt['density']:.6f}")

        if n_classes > 0 and n_classes <= 50:
            print(f"  Classes: {sorted(crt['obstructed_classes'])}")

        # Check how many scanned primes fall in CRT-obstructed classes
        L = crt['modulus']
        obs_classes = crt['obstructed_classes']
        structural = scan_df.with_columns(
            pl.col('p').map_elements(
                lambda p: (p % L) in obs_classes,
                return_dtype=pl.Boolean
            ).alias('crt_obstructed')
        )
        n_structural = int(structural.filter(pl.col('crt_obstructed')).height)
        print(f"  Primes in CRT-obstructed classes: {n_structural}/{scan_df.height} "
              f"({n_structural / scan_df.height:.4f})")
    else:
        print("  No moduli show local obstructions.")
        crt = None
    print()

    # Save results
    print("Saving results...")
    out_dir = _fixed_mod_out_dir()

    scan_df.write_parquet(str(data_dir / "fixed_mod_scan.parquet"))
    depth_df.write_parquet(str(data_dir / "fixed_mod_depths.parquet"))

    print(f"\nResults saved under {data_dir}/")
    print(f"  fixed_mod_scan.parquet: local solvability per modulus")
    print(f"  fixed_mod_depths.parquet: Hensel lifting depths")
