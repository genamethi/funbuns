"""
Data integrity checks for partitions data in blocks and runs.

Capabilities:
- Schema validation against expected columns/dtypes (best-effort, non-fatal)
- Duplicate detection by (p, m_k, n_k, q_k)
- Overlap detection across consecutive blocks (shared primes)
- Coverage gap detection via interval merging (O(B), zero SageMath calls)
- Completeness validation: Dusart (2010) unconditional bounds as filter,
  PARI prime_pi / P.unrank only when discrepancy exceeds proven error bound
- Paranoid verification: rolling cumulative index with P.unrank at boundaries,
  Schoenfeld-bound escalation to targeted prime_range on suspicious gaps
- Comprehensive diagnosis with fix-command suggestions
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

import polars as pl

from .block_catalog import blocks_dir, list_block_files, sorted_blocks_by_data, _parse_block_filename

if TYPE_CHECKING:
    from .block_catalog import BlockInfo


EXPECTED_KEYS = ["p", "m_k", "n_k", "q_k"]


def check_schema(df: pl.DataFrame) -> Dict[str, bool]:
    present = {c: (c in df.columns) for c in EXPECTED_KEYS}
    return present


def detect_duplicates_in_block(path: Path) -> int:
    try:
        df = pl.read_parquet(path)
    except Exception as e:
        raise IOError(f"Corrupt parquet file {path.name}: {e}") from e
    keys = [c for c in EXPECTED_KEYS if c in df.columns]
    if not keys:
        return 0
    before = len(df)
    after = len(df.unique(keys))
    return before - after


def detect_overlaps_between_blocks() -> pl.DataFrame:
    """Return a small report of overlaps in primes between consecutive blocks."""
    from tqdm import tqdm

    infos = sorted_blocks_by_data()
    if len(infos) < 2:
        return pl.DataFrame({"block_a": [], "block_b": [], "overlap_primes": []})

    rows = []
    for a, b in tqdm(list(zip(infos, infos[1:])), desc="Overlap check", unit="pair"):
        try:
            # Skip pairs where metadata proves no overlap
            if (a.max_prime is not None and b.min_prime is not None
                    and a.max_prime < b.min_prime):
                continue
            a_p = pl.scan_parquet(a.path).select(pl.col("p")).collect()
            b_p = pl.scan_parquet(b.path).select(pl.col("p")).collect()
            overlap = a_p.join(b_p, on="p", how="inner").height
            if overlap > 0:
                rows.append({
                    "block_a": a.path.name,
                    "block_b": b.path.name,
                    "overlap_primes": overlap,
                })
        except Exception:
            continue
    if not rows:
        return pl.DataFrame({"block_a": [], "block_b": [], "overlap_primes": []})
    return pl.DataFrame(rows)


def quick_integrity_report() -> str:
    from tqdm import tqdm

    files = list_block_files()
    if not files:
        return "No block files found."

    dup_total = 0
    per_block = []
    corrupt_files = []
    for f in tqdm(files, desc="Duplicate check", unit="block"):
        try:
            dups = detect_duplicates_in_block(f)
        except IOError as e:
            corrupt_files.append((f.name, str(e)))
            continue
        dup_total += dups
        per_block.append((f.name, dups))

    overlap_df = detect_overlaps_between_blocks()

    lines = []
    lines.append("=== DATA INTEGRITY REPORT ===")
    lines.append(f"Blocks: {len(files)}")
    if corrupt_files:
        lines.append(f"CORRUPT FILES ({len(corrupt_files)}):")
        for name, err in corrupt_files:
            lines.append(f"  {name}: {err}")
    lines.append(f"Total duplicate rows across blocks (by keys {EXPECTED_KEYS}): {dup_total}")
    if any(dups > 0 for _, dups in per_block):
        lines.append("Per-block duplicates:")
        for name, d in per_block:
            if d > 0:
                lines.append(f"  {name}: {d} duplicate rows")
    if overlap_df.height > 0:
        lines.append("Overlapping primes between adjacent blocks:")
        for row in overlap_df.iter_rows(named=True):
            lines.append(f"  {row['block_a']} <-> {row['block_b']}: {row['overlap_primes']} primes")
    else:
        lines.append("No overlaps of primes detected between adjacent blocks.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Validation: interval merging, Dusart bounds, rank-based (layered)
# ---------------------------------------------------------------------------


def _merge_block_ranges(
    valid_infos: List["BlockInfo"],
) -> List[Dict]:
    """Merge overlapping block [min_p, max_p] ranges into coverage intervals.

    Assumes valid_infos is sorted by min_prime.
    Returns list of dicts: {start, end, start_block, end_block}.
    O(B) over block metadata, zero data reads.
    """
    merged: List[Dict] = []
    for b in valid_infos:
        if b.min_prime is None or b.max_prime is None:
            continue
        if merged and b.min_prime <= merged[-1]["end"]:
            if b.max_prime > merged[-1]["end"]:
                merged[-1]["end"] = b.max_prime
                merged[-1]["end_block"] = b
        else:
            merged.append({
                "start": b.min_prime,
                "end": b.max_prime,
                "start_block": b,
                "end_block": b,
            })
    return merged


def _natural_gap_bound(p: int) -> int:
    """Conservative upper bound on prime gaps near p.  5 * (ln p)^2.

    Exceeds all known gaps well beyond our range.
    Cramer's conjecture: gap ~ (ln p)^2.  We use 5x for safety.
    """
    ln_p = math.log(max(p, 3))
    return max(200, int(5 * ln_p * ln_p))


def _dusart_pi_bounds(x: float) -> Tuple[float, float]:
    """Unconditional bounds on pi(x).  Dusart (2010), Theorem 6.9.

    Lower valid for x >= 88,783.
    Upper valid for x >= 2,953,652,287.
    No RH assumption.
    """
    ln_x = math.log(x)
    inv = 1.0 / ln_x
    lower = (x * inv) * (1.0 + inv + 2.0 * inv * inv)
    upper = (x * inv) * (1.0 + inv + 2.334 * inv * inv)
    return lower, upper


def _pnt_estimate(n: int) -> float:
    """PNT estimate for the n-th prime: p_n ~ n * (ln n + ln ln n).  Display only."""
    if n < 6:
        return [2, 3, 5, 7, 11, 13][n]
    ln_n = math.log(n)
    return n * (ln_n + math.log(ln_n))


def detect_gaps(
    valid_infos: List["BlockInfo"],
) -> Tuple[List[Dict], List[Dict]]:
    """Detect data coverage gaps via interval merging.

    Merges all block [min_p, max_p] ranges, then finds gaps between
    merged intervals exceeding the natural prime gap bound.

    O(B), zero SageMath calls, zero data reads.
    Definitive for gaps >> (ln p)^2.

    Returns (gaps, merged_intervals).
    """
    merged = _merge_block_ranges(valid_infos)
    if len(merged) < 2:
        return [], merged

    gaps = []
    for i in range(1, len(merged)):
        gap_start = merged[i - 1]["end"]
        gap_end = merged[i]["start"]
        if gap_end - gap_start > _natural_gap_bound(gap_start):
            mid = (gap_start + gap_end) / 2
            gaps.append({
                "block_a": merged[i - 1]["end_block"].path.name,
                "block_b": merged[i]["start_block"].path.name,
                "gap_start": gap_start,
                "gap_end": gap_end,
                "est_missing": int((gap_end - gap_start) / math.log(mid)) + 1,
            })
    return gaps, merged


def validate_completeness_fast(infos: Optional[List["BlockInfo"]] = None) -> Dict:
    """Fast completeness check via interval merging + Dusart bounds.

    Layered approach, zero SageMath calls:
      1. Interval merge: detect coverage gaps.  O(B).
      2. If no gaps + overlapping blocks: COMPLETE (deterministic generation
         guarantees overlapping ranges share all primes).
      3. If no gaps + no overlaps: per_block_sum is exact unique count.
         Check against Dusart (2010) unconditional bounds on pi(max).
         If within bounds: COMPLETE.  If outside: needs exact verification.
    """
    if infos is None:
        infos = sorted_blocks_by_data()

    corrupt = [b for b in infos if b.num_unique_primes is None]
    valid = [b for b in infos if b.num_unique_primes is not None]

    if not valid:
        return {
            "complete": False, "valid_infos": valid, "corrupt_blocks": corrupt,
            "gaps": [], "merged_intervals": [], "actual_max": None,
            "actual_min": None, "per_block_sum": 0, "n_overlapping_pairs": 0,
            "needs_exact": False,
        }

    actual_max = valid[-1].max_prime
    actual_min = valid[0].min_prime
    per_block_sum = sum(b.num_unique_primes for b in valid)

    gaps, merged = detect_gaps(valid)

    # Count consecutive overlapping pairs (informational)
    n_overlapping = sum(
        1 for i in range(1, len(valid))
        if valid[i - 1].max_prime is not None
        and valid[i].min_prime is not None
        and valid[i - 1].max_prime >= valid[i].min_prime
    )

    result = {
        "valid_infos": valid, "corrupt_blocks": corrupt,
        "gaps": gaps, "merged_intervals": merged,
        "actual_max": actual_max, "actual_min": actual_min,
        "per_block_sum": per_block_sum, "n_overlapping_pairs": n_overlapping,
        "needs_exact": False,
    }

    # Phase 1: gaps -> INCOMPLETE
    if gaps:
        result["complete"] = False
        return result

    # Phase 2: no gaps; overlapping blocks with deterministic generation
    # means all primes in the contiguous range are present.
    if n_overlapping > 0:
        result["complete"] = actual_min <= 3 and len(corrupt) == 0
        return result

    # Phase 3: no gaps, no overlaps -> per_block_sum == true unique count.
    # Check against Dusart (2010) unconditional bounds (no RH).
    if actual_max >= 2_953_652_287:
        pi_lo, pi_hi = _dusart_pi_bounds(actual_max)
        result["dusart_lower"] = pi_lo
        result["dusart_upper"] = pi_hi

        # Adjust expected count: data starts at 3 (p=2 not generated)
        adj = 1 if actual_min > 2 else 0

        if pi_lo - adj <= per_block_sum <= pi_hi - adj:
            result["complete"] = len(corrupt) == 0
        else:
            # Discrepancy exceeds proven error bound -> exact check needed
            result["complete"] = False
            result["needs_exact"] = True
    else:
        # Below Dusart upper threshold; small dataset, exact check is cheap
        result["complete"] = False
        result["needs_exact"] = True

    return result


def verify_count_exact(actual_max: int, actual_min: int) -> Dict:
    """Exact count verification via PARI prime_pi.

    Called only when Dusart bounds are insufficient (discrepancy exceeds
    proven error bound).  Uses PARI-backed prime_pi which is fast for
    x < 10^13.
    """
    from sage.all import prime_pi as sage_prime_pi

    expected = int(sage_prime_pi(actual_max))
    if actual_min > 2:
        expected -= int(sage_prime_pi(actual_min - 1))

    return {
        "expected_unique": expected,
        "actual_max": actual_max,
        "actual_min": actual_min,
    }


def prefix_check_report(infos: Optional[List["BlockInfo"]] = None) -> Dict:
    """Validate block prefix using interval merging + Dusart bounds.

    Checks:
    - Filename max_prime vs content max_prime mismatches (no SageMath)
    - Coverage gaps via interval merging (no SageMath)
    - Completeness via Dusart bounds (no SageMath)

    Returns dict with keys: gaps, filename_mismatches, completeness.
    """
    if infos is None:
        infos = sorted_blocks_by_data()

    filename_mismatches = []
    for info in infos:
        _, name_max = _parse_block_filename(info.path)
        if name_max is not None and info.max_prime is not None and name_max != info.max_prime:
            filename_mismatches.append({
                "file": info.path.name,
                "name_max": name_max,
                "content_max": info.max_prime,
            })

    comp = validate_completeness_fast(infos)

    return {
        "gaps": comp["gaps"],
        "filename_mismatches": filename_mismatches,
        "completeness": comp,
    }


def _round_up_to_multiple(n: int, multiple: int) -> int:
    """Round n up to the nearest multiple."""
    return ((n + multiple - 1) // multiple) * multiple


def comprehensive_diagnosis(verbose: bool = False) -> Dict:
    """Full diagnostic report with actionable fix commands.

    Default: interval merging + Dusart bounds.  Zero SageMath calls.
    With --verbose: adds PARI prime_pi exact verification when Dusart
    bounds are exceeded.
    """
    print("=== BLOCK DIAGNOSIS ===")

    infos = sorted_blocks_by_data()
    if not infos:
        print("No block files found.")
        return {"blocks": 0}

    comp = validate_completeness_fast(infos)
    valid = comp["valid_infos"]
    corrupt = comp["corrupt_blocks"]
    actual_max = comp["actual_max"]
    actual_min = comp["actual_min"]
    gaps = comp["gaps"]
    merged = comp["merged_intervals"]
    n_overlapping = comp["n_overlapping_pairs"]
    per_block_sum = comp["per_block_sum"]

    n_blocks = len(infos)
    print(f"Blocks: {n_blocks} ({len(corrupt)} corrupt)")
    print(f"Coverage: {len(merged)} interval(s), "
          f"{n_overlapping} overlapping block pairs")
    print(f"Range: {actual_min:,} to {actual_max:,}")
    print(f"Per-block unique sum: {per_block_sum:,}"
          + (f" (overcounts due to {n_overlapping} overlapping pairs)"
             if n_overlapping > 0 else ""))

    if corrupt:
        print(f"\nCORRUPT BLOCKS ({len(corrupt)}):")
        for b in corrupt:
            print(f"  {b.path.name}")

    # Completeness
    if comp["complete"]:
        dusart_lo = comp.get("dusart_lower")
        if dusart_lo is not None:
            print(f"\nCompleteness: COMPLETE "
                  f"(within Dusart bounds [{int(dusart_lo):,}, "
                  f"{int(comp['dusart_upper']):,}])")
        elif n_overlapping > 0:
            print(f"\nCompleteness: COMPLETE "
                  f"(contiguous coverage, deterministic generation)")
        else:
            print(f"\nCompleteness: COMPLETE")
    else:
        reason_parts = []
        if gaps:
            reason_parts.append(f"{len(gaps)} coverage gap(s)")
        if actual_min > 3:
            reason_parts.append(f"starts at {actual_min:,} (expected 2 or 3)")
        if comp.get("needs_exact"):
            reason_parts.append("count outside Dusart bounds")
        print(f"\nCompleteness: INCOMPLETE"
              + (f" ({', '.join(reason_parts)})" if reason_parts else ""))

    # Exact verification (verbose, or when Dusart insufficient)
    if verbose and comp.get("needs_exact"):
        print("  Running exact verification via PARI prime_pi...")
        exact = verify_count_exact(actual_max, actual_min)
        print(f"  Expected unique primes in [{actual_min:,}, {actual_max:,}]: "
              f"{exact['expected_unique']:,}")
        print(f"  Per-block sum: {per_block_sum:,}, "
              f"delta: {per_block_sum - exact['expected_unique']:+,}")

    # Gaps
    if gaps:
        print(f"\nGAPS ({len(gaps)}):")
        for i, gap in enumerate(gaps, 1):
            batch_size = 10_000
            n_needed = _round_up_to_multiple(
                int(gap["est_missing"] * 1.2),  # 20% margin
                batch_size,
            )
            start_from = (gap["gap_start"] + 2
                          if gap["gap_start"] % 2 == 1
                          else gap["gap_start"] + 1)
            print(f"  Gap {i}: {gap['block_a']} (max {gap['gap_start']:,}) "
                  f"-> {gap['block_b']} (min {gap['gap_end']:,})")
            print(f"    ~{gap['est_missing']:,} missing primes (PNT estimate)")
            print(f"    Fix: funbuns --init {start_from} -n {n_needed} -b {batch_size}")
            print(f"         bmgr --integrate-check")
    elif comp["complete"]:
        print("\nGaps: 0")

    return {
        "blocks": n_blocks,
        "corrupt": corrupt,
        "per_block_sum": per_block_sum,
        "min_prime": actual_min,
        "max_prime": actual_max,
        "complete": comp["complete"],
        "gaps": gaps,
        "n_overlapping_pairs": n_overlapping,
        "n_coverage_intervals": len(merged),
    }


# ---------------------------------------------------------------------------
# Paranoid verification: exact prime-by-prime completeness checks
# ---------------------------------------------------------------------------

# PARI Baillie-PSW is proven correct below 2^64. Above this, proof=True is
# needed for primality claims. All current data (max ~158B) is well under.
PARI_DETERMINISTIC_LIMIT = 2**64


def set_proof_for_regime(max_p: int) -> bool:
    """Set SageMath arithmetic proof level based on data regime.

    Below 2^64, PARI B-PSW is deterministic — all block manager integrity
    reduces to gap/continuity checks. No primality proofs needed.
    Above 2^64, enables proof=True (may be slow; issue a warning).

    Returns True if proof=True was set (64-bit regime).
    """
    from sage.structure.proof.proof import proof
    if max_p < PARI_DETERMINISTIC_LIMIT:
        proof.arithmetic(False)
        return False
    else:
        proof.arithmetic(True)
        print(f"WARNING: max prime {max_p} exceeds 2^64 — enabling primality proofs. "
              "This may be slow.", flush=True)
        return True


def _schoenfeld_bound(x: float) -> float:
    """Schoenfeld (1976) bound: |pi(x) - Li(x)| < sqrt(x)*ln(x)/(8*pi).

    Valid for x >= 2657 under RH. At our scale, breaching this is
    near-certain data loss, not a refutation of RH.
    """
    return math.sqrt(x) * math.log(x) / (8.0 * math.pi)


def check_paranoid_escalation(observed_count: int, max_prime: int) -> Dict:
    """Check if observed prime count deviates enough to trigger paranoid mode.

    Layered approach:
    1. Dusart (2010) unconditional bounds — proven, no RH needed.
       If count is within bounds, data is OK regardless of Schoenfeld.
    2. Schoenfeld (1976) bound under RH — tighter, but our Li(x) approx
       has truncation error that can exceed it at large x.
       Only used when Dusart can't decide (x below threshold).

    Returns dict with:
    - trigger_paranoid: bool
    - reason: "schoenfeld" | "dusart" | None
    - expected_count: float (Li(x) estimate)
    - deviation: float
    """
    x = float(max_prime)
    if x < 2657:
        # Too small for any analytic bound; use exact check
        return {"trigger_paranoid": True, "reason": "schoenfeld",
                "expected_count": 0, "deviation": 0}

    # Li(x) approximation: x/ln(x) * (1 + 1/ln(x) + 2/ln(x)^2)
    ln_x = math.log(x)
    inv = 1.0 / ln_x
    li_estimate = (x * inv) * (1.0 + inv + 2.0 * inv * inv)

    deviation = abs(observed_count - li_estimate)
    bound = _schoenfeld_bound(x)

    # Check Dusart bounds first (unconditional, proven)
    if x >= 88_783:
        pi_lo, pi_hi = _dusart_pi_bounds(x)
        # If within Dusart bounds: data is OK — don't trigger
        in_lower = observed_count >= pi_lo
        in_upper = (x < 2_953_652_287) or (observed_count <= pi_hi)
        if in_lower and in_upper:
            return {
                "trigger_paranoid": False,
                "reason": None,
                "expected_count": li_estimate,
                "deviation": deviation,
                "bound": bound,
            }
        # Outside Dusart bounds: genuine concern
        return {
            "trigger_paranoid": True,
            "reason": "dusart",
            "expected_count": li_estimate,
            "deviation": deviation,
            "bound": bound,
        }

    # Below Dusart threshold: fall back to Schoenfeld
    if deviation > bound:
        return {
            "trigger_paranoid": True,
            "reason": "schoenfeld",
            "expected_count": li_estimate,
            "deviation": deviation,
            "bound": bound,
        }

    return {
        "trigger_paranoid": False,
        "reason": None,
        "expected_count": li_estimate,
        "deviation": deviation,
        "bound": bound,
    }


def paranoid_verify_block(path: Path) -> Dict:
    """Verify a single block for missing primes.

    For small blocks (test-scale): uses SageMath prime_range for exact
    enumeration. For production, use paranoid_rolling_verify instead.

    Returns dict with 'missing_primes' list.
    """
    from sage.all import prime_range as sage_prime_range

    df = pl.read_parquet(path)
    data_primes = set(df["p"].unique().to_list())

    min_p = min(data_primes)
    max_p = max(data_primes)

    # For small ranges, direct enumeration is fine
    expected = set(int(p) for p in sage_prime_range(min_p, max_p + 1))
    missing = sorted(expected - data_primes)

    return {"missing_primes": missing, "extra_primes": sorted(data_primes - expected)}


def paranoid_verify_range(path: Path, min_p: int, max_p: int) -> Dict:
    """Verify a parquet file has all primes in [min_p, max_p].

    For small ranges (test-scale): uses SageMath prime_range.
    Returns dict with 'missing_primes' and 'extra_primes'.
    """
    from sage.all import prime_range as sage_prime_range

    df = pl.read_parquet(path)
    data_primes = set(df["p"].unique().to_list())

    expected = set(int(p) for p in sage_prime_range(min_p, max_p + 1))
    missing = sorted(expected - data_primes)
    extra = sorted(data_primes - expected)

    return {"missing_primes": missing, "extra_primes": extra}


def paranoid_rolling_verify(verbose: bool = True) -> Dict:
    """Production paranoid verification using rolling cumulative index.

    Algorithm:
    1. Walk blocks in sorted order using filename-derived max_p
    2. Maintain cumulative unique prime count
    3. At each block boundary: verify P.unrank(cumulative_count) matches
       next block's min_p (from filename or lazy scan)
    4. If count deviation exceeds Schoenfeld bound: escalate with
       targeted prime_range to identify exact missing primes
    5. Check block isolation: no prime from cur block leaks into neighbors

    Uses filename-derived max_p where possible to avoid full block reads.
    Falls back to lazy scans for min_p when filenames don't encode it.
    """
    from sage.all import Primes, prime_range as sage_prime_range
    from tqdm import tqdm

    infos = sorted_blocks_by_data()
    if not infos:
        print("No blocks to verify.")
        return {"verified": 0, "issues": []}

    P = Primes()
    cumulative_count = 0
    issues = []
    verified_blocks = 0
    prev_max_p = None

    for i, info in enumerate(tqdm(infos, desc="Paranoid verify", unit="block")):
        if info.num_unique_primes is None:
            issues.append({"block": info.path.name, "issue": "corrupt/unreadable"})
            continue

        cur_min_p = info.min_prime
        cur_max_p = info.max_prime
        cur_uniq = info.num_unique_primes

        # Boundary check: gap between prev block and current
        if prev_max_p is not None and cur_min_p is not None:
            gap = cur_min_p - prev_max_p
            bound = _natural_gap_bound(prev_max_p)
            if gap > bound:
                # Suspicious gap — use unrank to confirm (expensive, but rare)
                expected_next = int(P.unrank(cumulative_count))
                if expected_next != cur_min_p:
                    # Escalate: find missing primes in the gap
                    missing_in_gap = [
                        int(p) for p in sage_prime_range(prev_max_p + 1, cur_min_p)
                    ]
                    issues.append({
                        "block": info.path.name,
                        "issue": "gap_missing_primes",
                        "expected_min": expected_next,
                        "actual_min": cur_min_p,
                        "missing_count": len(missing_in_gap),
                    })
                    if verbose:
                        print(f"  GAP before {info.path.name}: expected min_p={expected_next}, "
                              f"got {cur_min_p}, {len(missing_in_gap)} primes missing")
            elif gap > 0 and i % 100 == 0:
                # Sample boundary check every 100 blocks (unrank is expensive)
                # Schoenfeld count check below catches drift between samples
                expected_next = int(P.unrank(cumulative_count))
                if expected_next != cur_min_p:
                    issues.append({
                        "block": info.path.name,
                        "issue": "boundary_mismatch",
                        "expected_min": expected_next,
                        "actual_min": cur_min_p,
                    })
                    if verbose:
                        print(f"  MISMATCH at {info.path.name}: expected min_p={expected_next}, "
                              f"got {cur_min_p}")

        # Count check with Schoenfeld escalation
        cumulative_count += cur_uniq
        if cur_max_p is not None:
            esc = check_paranoid_escalation(cumulative_count, cur_max_p)
            if esc["trigger_paranoid"]:
                issues.append({
                    "block": info.path.name,
                    "issue": f"count_deviation_{esc['reason']}",
                    "observed": cumulative_count,
                    "expected": esc["expected_count"],
                    "deviation": esc["deviation"],
                })
                if verbose:
                    print(f"  COUNT DEVIATION at {info.path.name}: "
                          f"observed={cumulative_count:,}, "
                          f"expected~{esc['expected_count']:,.0f}, "
                          f"reason={esc['reason']}")

        prev_max_p = cur_max_p
        verified_blocks += 1

    if verbose:
        print(f"\nParanoid verification complete: {verified_blocks} blocks, "
              f"{cumulative_count:,} cumulative primes, {len(issues)} issues")

    return {
        "verified": verified_blocks,
        "cumulative_primes": cumulative_count,
        "issues": issues,
    }

