"""
Exploration of recurrence structure in prime power decompositions.

For p = 2^m + q^n, any two solutions sharing the same q satisfy:
    p2 - p1 = q^{n1} * (q^{n2-n1} - 1)    [when m is also fixed]

The q-action is multiplication by q in the exponent: n -> n+1.  A q-chain
is the full ordered sequence of solutions for fixed q (greedy) or fixed
(q, m) (sub-chain).  The gap structure -- the distribution of delta_n
between consecutive solutions -- characterizes how the q-action interacts
with primality.

Hierarchy:
    q-chain (all m)  ⊃  (q, m)-stratum  (fixed m slice)
"""

from __future__ import annotations

from collections import Counter
from math import gcd

import polars as pl

from .utils import get_data_dir


def _block_pattern() -> str:
    return str(get_data_dir() / "blocks" / "pp_b*.parquet")


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------


def q_chain(q: int) -> pl.DataFrame:
    """Extract all (m, n, p) triples for a fixed q, sorted by n then m.

    This is the greedy q-chain: every solution where q_k == q, regardless
    of m.  Returns DataFrame with columns: p, m_k, n_k.
    """
    return (
        pl.scan_parquet(_block_pattern())
        .filter(pl.col("q_k") == q)
        .select("p", "m_k", "n_k")
        .unique(subset=["p", "m_k", "n_k"])
        .sort("n_k", "m_k", "p")
        .collect()
    )


def stratum(q: int, m: int) -> pl.DataFrame:
    """Extract all (n, p) pairs for fixed (q, m), sorted by n.

    Returns DataFrame with columns: p, n_k, delta_n, delta_p.
    The full ordered sequence IS the chain -- delta_n encodes the
    gap structure of the q-action on this stratum.
    """
    df = (
        pl.scan_parquet(_block_pattern())
        .filter((pl.col("q_k") == q) & (pl.col("m_k") == m))
        .select("p", "n_k")
        .unique(subset=["p", "n_k"])
        .sort("n_k", "p")
        .collect()
    )
    if df.height == 0:
        return df.with_columns(
            pl.lit(None).cast(pl.Int64).alias("delta_n"),
            pl.lit(None).cast(pl.Int64).alias("delta_p"),
        )

    return df.with_columns(
        pl.col("n_k").diff().alias("delta_n"),
        pl.col("p").diff().alias("delta_p"),
    )


# ---------------------------------------------------------------------------
# Gap structure analysis
# ---------------------------------------------------------------------------


def gap_histogram(df: pl.DataFrame) -> Counter:
    """Distribution of delta_n values (the q-action gap sizes).

    delta_n == 1 means the next application of the q-action hit a prime.
    delta_n == k means k-1 consecutive non-prime values of 2^m + q^n were
    skipped.  Returns Counter {delta_n: count}.
    """
    gaps = df.filter(pl.col("delta_n").is_not_null()).get_column("delta_n").to_list()
    return Counter(int(g) for g in gaps)


def gap_summary(df: pl.DataFrame) -> dict:
    """Summary statistics on the gap structure."""
    hist = gap_histogram(df)
    if not hist:
        return {"n_transitions": 0}

    total = sum(hist.values())
    consecutive = hist.get(1, 0)
    max_gap = max(hist.keys())
    mean_gap = sum(k * v for k, v in hist.items()) / total

    return {
        "n_transitions": total,
        "consecutive_hits": consecutive,
        "consecutive_frac": round(consecutive / total, 4) if total else 0,
        "max_gap": max_gap,
        "mean_gap": round(mean_gap, 2),
        "gap_histogram": dict(sorted(hist.items())),
    }


def stratum_summary(q: int, m: int, df: pl.DataFrame = None) -> dict:
    """Compute summary metrics for a (q, m) stratum.

    The chain IS the full sequence.  Metrics describe the gap structure
    of the q-action on this stratum.
    """
    if df is None:
        df = stratum(q, m)

    n_pairs = df.height
    if n_pairs == 0:
        return {"q": q, "m": m, "n_pairs": 0}

    n_min = int(df["n_k"].min())
    n_max = int(df["n_k"].max())
    p_min = int(df["p"].min())
    p_max = int(df["p"].max())

    # Gap structure
    gs = gap_summary(df)

    # Density: fraction of n in [n_min, n_max] that yield a prime
    span = n_max - n_min + 1
    density = n_pairs / span if span > 0 else 0

    # Divisibility check: for consecutive pairs, verify q^{n1} | (p2 - p1)
    # i.e., v_q(delta_p) >= n1 for each consecutive pair
    div_ok = 0
    div_total = 0
    consec = df.filter(pl.col("delta_p").is_not_null())
    if consec.height > 0:
        try:
            from .native_expr import v_ell
            checked = consec.with_columns(
                v_ell(pl.col("delta_p").abs(), ell=q).alias("v_q_delta_p"),
            )
            # n_prev is the n_k of the previous row = current n_k - delta_n
            checked = checked.with_columns(
                (pl.col("n_k") - pl.col("delta_n")).alias("n_prev"),
            )
            div_total = checked.height
            div_ok = int(
                checked.filter(pl.col("v_q_delta_p") >= pl.col("n_prev")).height
            )
        except ImportError:
            pass  # Rust plugin not built; skip divisibility check

    return {
        "q": q,
        "m": m,
        "n_pairs": n_pairs,
        "n_min": n_min,
        "n_max": n_max,
        "p_min": p_min,
        "p_max": p_max,
        "consecutive_frac": gs.get("consecutive_frac", 0),
        "max_gap": gs.get("max_gap", 0),
        "mean_gap": gs.get("mean_gap", 0),
        "density": round(density, 4),
        "div_ok": div_ok,
        "div_total": div_total,
    }


def q_chain_summary(q: int) -> dict:
    """Summary of the full q-chain (all m values combined).

    Shows how many distinct n-values have at least one prime across
    any m, and the gap structure of n-values in the combined chain.
    """
    chain = q_chain(q)
    if chain.height == 0:
        return {"q": q, "total_solutions": 0, "distinct_m": 0, "distinct_n": 0}

    distinct_n = chain.get_column("n_k").unique().sort()
    distinct_m = chain.get_column("m_k").n_unique()
    n_vals = distinct_n.to_list()

    # Gap structure over distinct n-values (regardless of m)
    if len(n_vals) > 1:
        deltas = [n_vals[i+1] - n_vals[i] for i in range(len(n_vals) - 1)]
        hist = Counter(deltas)
        consecutive = hist.get(1, 0)
        total_trans = len(deltas)
        max_gap = max(deltas)
        mean_gap = sum(deltas) / total_trans
    else:
        hist = {}
        consecutive = 0
        total_trans = 0
        max_gap = 0
        mean_gap = 0

    return {
        "q": q,
        "total_solutions": chain.height,
        "distinct_m": distinct_m,
        "distinct_n": len(n_vals),
        "n_min": int(n_vals[0]),
        "n_max": int(n_vals[-1]),
        "consecutive_frac": round(consecutive / total_trans, 4) if total_trans else 0,
        "max_gap": max_gap,
        "mean_gap": round(mean_gap, 2),
        "gap_histogram": dict(sorted(hist.items())),
    }


def sweep_strata(
    q: int = None,
    m: int = None,
    verbose: bool = True,
) -> pl.DataFrame:
    """Sweep across (q, m) strata and collect summaries.

    Single scan of block data, then partition in memory.
    If q fixed: sweep all m values for that q.
    If m fixed: sweep all q values for that m.
    If both: single stratum.
    If neither: sweep q in {3,5,7,11,13} x m in {1..15}.
    """
    if q is not None and m is not None:
        s = stratum_summary(q, m)
        return pl.DataFrame([s])

    # Build filter for single scan
    if verbose:
        print("Loading matching data (single scan)...", flush=True)

    lf = pl.scan_parquet(_block_pattern()).filter(pl.col("m_k") > 0)

    if q is not None:
        lf = lf.filter(pl.col("q_k") == q)
    elif m is not None:
        lf = lf.filter(pl.col("m_k") == m)
    else:
        lf = lf.filter(
            pl.col("q_k").is_in([3, 5, 7, 11, 13])
            & pl.col("m_k").is_in(list(range(1, 16)))
        )

    all_data = (
        lf.select("p", "m_k", "n_k", "q_k")
        .unique(subset=["p", "m_k", "n_k", "q_k"])
        .collect()
    )

    if verbose:
        print(f"Loaded {all_data.height:,} rows, partitioning...", flush=True)

    # Partition by (q_k, m_k) and compute summaries
    rows = []
    groups = all_data.group_by(["q_k", "m_k"])
    for (q_val, m_val), group_df in groups:
        q_val, m_val = int(q_val), int(m_val)
        # Build the stratum DataFrame with deltas
        sdf = (
            group_df.select("p", "n_k")
            .unique(subset=["p", "n_k"])
            .sort("n_k", "p")
        )
        if sdf.height > 0:
            sdf = sdf.with_columns(
                pl.col("n_k").diff().alias("delta_n"),
                pl.col("p").diff().alias("delta_p"),
            )
        s = stratum_summary(q_val, m_val, df=sdf)
        rows.append(s)

    if verbose:
        print(f"Computed {len(rows)} strata.", flush=True)

    return pl.DataFrame(rows)


def report(q: int = None, m: int = None, verbose: bool = True) -> None:
    """Print a formatted report of stratum summaries."""

    # If only q is given (no m), show the q-chain overview first
    if q is not None and m is None:
        qcs = q_chain_summary(q)
        print(f"\n=== q-chain overview: q={q} ===")
        print(f"Total solutions: {qcs['total_solutions']:,}")
        if qcs["total_solutions"] == 0:
            return
        print(f"Distinct m values: {qcs['distinct_m']}")
        print(f"Distinct n values: {qcs['distinct_n']} "
              f"(range [{qcs['n_min']}, {qcs['n_max']}])")
        print(f"q-action gap structure (across all m):")
        print(f"  Consecutive hits (delta_n=1): "
              f"{qcs['consecutive_frac']:.1%}")
        print(f"  Mean gap: {qcs['mean_gap']:.2f}, max gap: {qcs['max_gap']}")
        if qcs["gap_histogram"]:
            top = sorted(qcs["gap_histogram"].items(),
                         key=lambda x: -x[1])[:10]
            print(f"  Top gaps: "
                  + ", ".join(f"dn={k}:{v}" for k, v in top))
        print()

    df = sweep_strata(q=q, m=m, verbose=verbose)

    if df.height == 0:
        print("No strata found.")
        return

    # Single stratum: detailed report
    if q is not None and m is not None:
        row = df.row(0, named=True)
        print(f"\n=== Stratum q={row['q']}, m={row['m']} ===")
        print(f"Chain length: {row['n_pairs']:,} solutions")
        if row["n_pairs"] == 0:
            return
        print(f"n range: [{row['n_min']}, {row['n_max']}]")
        print(f"p range: [{row['p_min']:,}, {row['p_max']:,}]")
        print(f"Density: {row['density']:.4f} "
              f"({row['n_pairs']} / {row['n_max'] - row['n_min'] + 1})")
        print(f"\nq-action gap structure:")
        print(f"  Consecutive hits (delta_n=1): "
              f"{row['consecutive_frac']:.1%}")
        print(f"  Mean gap: {row['mean_gap']:.2f}, max gap: {row['max_gap']}")
        if row["div_total"] > 0:
            pct = 100 * row["div_ok"] / row["div_total"]
            print(f"  Divisibility v_q(delta_p) >= n_prev: "
                  f"{row['div_ok']}/{row['div_total']} ({pct:.1f}%)")

        # Full gap histogram for single stratum
        s = stratum(q, m)
        gh = gap_histogram(s)
        if gh:
            print(f"  Gap histogram: "
                  + ", ".join(f"{k}:{v}" for k, v in sorted(gh.items())))

        # Show the actual stratum data if small enough
        if s.height <= 50:
            print(f"\nAll {s.height} solutions:")
            print(s)
        else:
            print(f"\nFirst 20 solutions:")
            print(s.head(20))
            print(f"\nLast 10 solutions:")
            print(s.tail(10))
        return

    # Sweep report: table format
    label = []
    if q is not None:
        label.append(f"q={q}")
    if m is not None:
        label.append(f"m={m}")
    title = f"Strata sweep ({', '.join(label)})" if label else "Strata sweep"
    print(f"=== {title}: {df.height} strata ===\n")

    # Filter to non-empty strata for the table
    nonempty = df.filter(pl.col("n_pairs") > 0)
    if nonempty.height == 0:
        print("All strata empty.")
        return

    # Print table header
    print(f"{'q':>5} {'m':>3} {'pairs':>8} {'n_range':>12} "
          f"{'cons%':>6} {'mean_g':>6} {'max_g':>6} "
          f"{'density':>7} {'div%':>5}")
    print("-" * 72)

    for row in nonempty.sort(["q", "m"]).iter_rows(named=True):
        n_range = f"[{row['n_min']},{row['n_max']}]"
        div_pct = (
            f"{100 * row['div_ok'] / row['div_total']:.0f}"
            if row["div_total"] > 0
            else "-"
        )
        cons_pct = f"{100 * row['consecutive_frac']:.0f}"
        print(
            f"{row['q']:>5} {row['m']:>3} {row['n_pairs']:>8,} "
            f"{n_range:>12} {cons_pct:>6} "
            f"{row['mean_gap']:>6.1f} {row['max_gap']:>6} "
            f"{row['density']:>7.4f} {div_pct:>5}"
        )

    # Highlight notable strata
    most_pairs = nonempty.sort("n_pairs", descending=True).row(0, named=True)
    densest = nonempty.sort("density", descending=True).row(0, named=True)
    print(f"\nMost solutions: q={most_pairs['q']}, m={most_pairs['m']} "
          f"({most_pairs['n_pairs']:,} solutions)")
    print(f"Highest density: q={densest['q']}, m={densest['m']} "
          f"({densest['density']:.4f})")


def n_fiber(q: int) -> pl.DataFrame:
    """For each n, show the set of m values that yield a prime.

    Returns DataFrame sorted by n_k, with columns:
        n_k, n_solutions, m_values (list), p_min, p_max, first_m, first_p
    where first_m is the smallest m yielding a prime at this n.
    """
    chain = q_chain(q)
    if chain.height == 0:
        return pl.DataFrame()

    return (
        chain
        .group_by("n_k")
        .agg(
            pl.col("m_k").count().alias("n_solutions"),
            pl.col("m_k").sort_by("m_k").alias("m_values"),
            pl.col("p").min().alias("p_min"),
            pl.col("p").max().alias("p_max"),
            # first appearance: smallest m (and its p)
            pl.col("m_k").sort_by("m_k").first().alias("first_m"),
            pl.col("p").sort_by("m_k").first().alias("first_p"),
        )
        .sort("n_k")
    )


def n_fiber_report(q: int) -> None:
    """Print the fiber over each n-value: which m values yield primes."""
    fiber = n_fiber(q)
    if fiber.height == 0:
        print(f"No solutions for q={q}.")
        return

    print(f"\n=== n-fiber for q={q}: solutions grouped by n ===")
    print(f"{'n':>3} {'#m':>4} {'first_m':>7} {'first_p':>14} "
          f"{'p_max':>14}  m values")
    print("-" * 75)

    for row in fiber.iter_rows(named=True):
        m_list = row["m_values"]
        m_str = ",".join(str(v) for v in m_list[:20])
        if len(m_list) > 20:
            m_str += f"...({len(m_list)} total)"
        print(
            f"{row['n_k']:>3} {row['n_solutions']:>4} "
            f"{row['first_m']:>7} {row['first_p']:>14,} "
            f"{row['p_max']:>14,}  [{m_str}]"
        )

    # Tree-style view: show branching structure
    print(f"\n=== Tree view: q={q}, n -> m branches ===\n")
    for row in fiber.iter_rows(named=True):
        n = row["n_k"]
        m_list = row["m_values"]
        q_power = q ** n
        branch_count = len(m_list)
        print(f"n={n:>2}  {q}^{n} = {q_power:>14,}  "
              f"({branch_count:>2} branches)")
        for mv in m_list[:15]:
            p = (1 << mv) + q_power
            print(f"       m={mv:>2} -> p = 2^{mv} + {q}^{n} = {p:,}")
        if len(m_list) > 15:
            print(f"       ... ({len(m_list) - 15} more)")

    # Branching statistics
    widths = fiber.get_column("n_solutions").to_list()
    print(f"\nBranching: min={min(widths)}, max={max(widths)}, "
          f"mean={sum(widths)/len(widths):.1f}")

    # Which n-values share m values? (horizontal connections in the tree)
    all_n = fiber.get_column("n_k").to_list()
    all_m = fiber.get_column("m_values").to_list()
    print(f"\n=== Shared m-values between consecutive n levels ===")
    for i in range(len(all_n) - 1):
        n1, n2 = all_n[i], all_n[i + 1]
        s1 = set(all_m[i])
        s2 = set(all_m[i + 1])
        shared = sorted(s1 & s2)
        if shared:
            print(f"  n={n1} & n={n2}: m in {{{','.join(str(x) for x in shared)}}}")
    if not any(
        set(all_m[i]) & set(all_m[i + 1]) for i in range(len(all_n) - 1)
    ):
        print("  (no shared m values between consecutive n levels)")


# ---------------------------------------------------------------------------
# Bipartite adjacency: n-values x m-values
# ---------------------------------------------------------------------------


def adjacency(q: int) -> dict:
    """Build the bipartite adjacency between n and m for fixed q.

    Returns dict with:
        edges: list of (n, m, p) triples
        n_to_m: dict {n: set of m}
        m_to_n: dict {m: set of n}
        all_n: sorted list of n values
        all_m: sorted list of m values
    """
    chain = q_chain(q)
    if chain.height == 0:
        return {
            "edges": [], "n_to_m": {}, "m_to_n": {},
            "all_n": [], "all_m": [],
        }

    edges = [
        (int(r[2]), int(r[1]), int(r[0]))  # (n, m, p)
        for r in chain.iter_rows()
    ]

    n_to_m: dict[int, set[int]] = {}
    m_to_n: dict[int, set[int]] = {}
    for n, m, _p in edges:
        n_to_m.setdefault(n, set()).add(m)
        m_to_n.setdefault(m, set()).add(n)

    return {
        "edges": edges,
        "n_to_m": n_to_m,
        "m_to_n": m_to_n,
        "all_n": sorted(n_to_m),
        "all_m": sorted(m_to_n),
    }


def connected_components(q: int) -> list[dict]:
    """Find connected components of the (n, m) bipartite graph.

    Two solutions are connected if they share an n-value or an m-value.
    A connected component is a maximal set of (n, m) pairs reachable
    by alternating n- and m-steps.

    Returns list of components, each a dict with:
        n_values, m_values, n_edges (number of solution triples),
        diameter (longest shortest path in the component graph).
    """
    adj = adjacency(q)
    if not adj["edges"]:
        return []

    n_to_m = adj["n_to_m"]
    m_to_n = adj["m_to_n"]

    # BFS on the bipartite graph.  Nodes are ("n", val) or ("m", val).
    visited: set[tuple[str, int]] = set()
    components = []

    for n in adj["all_n"]:
        node = ("n", n)
        if node in visited:
            continue

        # BFS
        comp_n: set[int] = set()
        comp_m: set[int] = set()
        queue = [node]
        visited.add(node)

        while queue:
            next_queue = []
            for kind, val in queue:
                if kind == "n":
                    comp_n.add(val)
                    for m in n_to_m.get(val, ()):
                        nb = ("m", m)
                        if nb not in visited:
                            visited.add(nb)
                            next_queue.append(nb)
                else:
                    comp_m.add(val)
                    for n2 in m_to_n.get(val, ()):
                        nb = ("n", n2)
                        if nb not in visited:
                            visited.add(nb)
                            next_queue.append(nb)
            queue = next_queue

        # Count edges in this component
        n_edges = sum(
            1 for en, em, _ep in adj["edges"]
            if en in comp_n and em in comp_m
        )

        components.append({
            "n_values": sorted(comp_n),
            "m_values": sorted(comp_m),
            "n_edges": n_edges,
        })

    components.sort(key=lambda c: -len(c["n_values"]))
    return components


def adjacency_report(q: int) -> None:
    """Print full adjacency analysis for fixed q."""
    adj = adjacency(q)
    if not adj["edges"]:
        print(f"No solutions for q={q}.")
        return

    n_vals = adj["all_n"]
    m_vals = adj["all_m"]

    print(f"\n=== Bipartite adjacency for q={q} ===")
    print(f"|N| = {len(n_vals)}, |M| = {len(m_vals)}, "
          f"|E| = {len(adj['edges'])} solutions")
    print(f"n range: [{n_vals[0]}, {n_vals[-1]}]")
    print(f"m range: [{m_vals[0]}, {m_vals[-1]}]")

    # Degree distributions
    n_degrees = {n: len(adj["n_to_m"][n]) for n in n_vals}
    m_degrees = {m: len(adj["m_to_n"][m]) for m in m_vals}

    print(f"\n--- n-degree (how many m values per n) ---")
    for n in n_vals:
        d = n_degrees[n]
        bar = "#" * d
        m_list = sorted(adj["n_to_m"][n])
        m_str = ",".join(str(x) for x in m_list)
        print(f"  n={n:>2} [{d:>2}] {bar}  m={{{m_str}}}")

    print(f"\n--- m-degree (how many n values per m) ---")
    for m in m_vals:
        d = m_degrees[m]
        bar = "#" * d
        n_list = sorted(adj["m_to_n"][m])
        n_str = ",".join(str(x) for x in n_list)
        print(f"  m={m:>2} [{d:>2}] {bar}  n={{{n_str}}}")

    # Connected components
    comps = connected_components(q)
    print(f"\n--- Connected components: {len(comps)} ---")
    for i, c in enumerate(comps):
        n_str = ",".join(str(x) for x in c["n_values"])
        m_str = ",".join(str(x) for x in c["m_values"])
        print(f"  C{i}: |N|={len(c['n_values']):>2}, |M|={len(c['m_values']):>2}, "
              f"|E|={c['n_edges']:>3}")
        print(f"       n={{{n_str}}}")
        print(f"       m={{{m_str}}}")

    # Adjacency matrix (n x m) as a compact grid
    if len(n_vals) <= 30 and len(m_vals) <= 40:
        edge_set = {(n, m) for n, m, _p in adj["edges"]}
        print(f"\n--- Adjacency grid (n rows x m cols, '.' = solution) ---")
        # Header: m values
        hdr = "    " + "".join(f"{m:>3}" for m in m_vals)
        print(hdr)
        print("    " + "---" * len(m_vals))
        for n in n_vals:
            row = f"n={n:>2}|"
            for m in m_vals:
                row += "  ." if (n, m) in edge_set else "   "
            print(row)

    # Full pairwise n-n sharing: for all pairs (n1, n2), how many m are shared?
    print(f"\n--- Shared m-values between all n pairs ---")
    sharing_pairs = []
    for i, n1 in enumerate(n_vals):
        for n2 in n_vals[i+1:]:
            shared = adj["n_to_m"][n1] & adj["n_to_m"][n2]
            if shared:
                sharing_pairs.append((n1, n2, sorted(shared)))

    if sharing_pairs:
        for n1, n2, shared in sharing_pairs:
            m_str = ",".join(str(x) for x in shared)
            print(f"  n={n1:>2} & n={n2:>2}: {len(shared):>2} shared  "
                  f"m={{{m_str}}}")
    else:
        print("  (no shared m values between any n pair)")

    # Same for m-m: which m values share n values?
    print(f"\n--- Shared n-values between m pairs (top 20) ---")
    m_sharing = []
    for i, m1 in enumerate(m_vals):
        for m2 in m_vals[i+1:]:
            shared = adj["m_to_n"][m1] & adj["m_to_n"][m2]
            if len(shared) >= 2:
                m_sharing.append((m1, m2, sorted(shared)))

    m_sharing.sort(key=lambda x: -len(x[2]))
    for m1, m2, shared in m_sharing[:20]:
        n_str = ",".join(str(x) for x in shared)
        print(f"  m={m1:>2} & m={m2:>2}: {len(shared):>2} shared  "
              f"n={{{n_str}}}")
    if len(m_sharing) > 20:
        print(f"  ... ({len(m_sharing) - 20} more pairs)")
    if not m_sharing:
        print("  (no m pair shares 2+ n values)")


# ---------------------------------------------------------------------------
# M-pair arithmetic progressions
# ---------------------------------------------------------------------------


def m_pair_progressions(q: int) -> None:
    """Analyze arithmetic structure in m-pair sharing.

    For all (m1, m2) pairs that share n-values, compute:
      - delta_m = m2 - m1 (arithmetic progression step)
      - ratio m2/m1 (geometric progression step, when integer)
    Report frequency of each delta and ratio.
    """
    adj = adjacency(q)
    if not adj["edges"]:
        return

    m_vals = adj["all_m"]
    m_to_n = adj["m_to_n"]

    # Collect all sharing pairs with delta and sharing count
    pairs = []
    for i, m1 in enumerate(m_vals):
        for m2 in m_vals[i+1:]:
            shared = m_to_n[m1] & m_to_n[m2]
            if shared:
                pairs.append({
                    "m1": m1, "m2": m2,
                    "delta": m2 - m1,
                    "ratio": m2 / m1,
                    "shared": len(shared),
                    "n_vals": sorted(shared),
                })

    if not pairs:
        print("No m-pairs share n-values.")
        return

    print(f"\n=== M-pair arithmetic progressions for q={q} ===")
    print(f"Total sharing pairs: {len(pairs)}")

    # Delta frequency, weighted by sharing count
    delta_freq: Counter = Counter()
    delta_weight: Counter = Counter()
    for p in pairs:
        delta_freq[p["delta"]] += 1
        delta_weight[p["delta"]] += p["shared"]

    print(f"\n--- Delta_m frequency (m2 - m1) ---")
    print(f"{'delta':>6} {'pairs':>6} {'total_shared':>12} {'avg_shared':>10}")
    print("-" * 40)
    for d in sorted(delta_freq.keys()):
        avg = delta_weight[d] / delta_freq[d]
        print(f"{d:>6} {delta_freq[d]:>6} {delta_weight[d]:>12} {avg:>10.2f}")

    # Ratio frequency (integer ratios only)
    ratio_freq: Counter = Counter()
    ratio_weight: Counter = Counter()
    for p in pairs:
        r = p["ratio"]
        if r == int(r):
            ratio_freq[int(r)] += 1
            ratio_weight[int(r)] += p["shared"]

    if ratio_freq:
        print(f"\n--- Integer ratio frequency (m2 / m1) ---")
        print(f"{'ratio':>6} {'pairs':>6} {'total_shared':>12} {'avg_shared':>10}")
        print("-" * 40)
        for r in sorted(ratio_freq.keys()):
            avg = ratio_weight[r] / ratio_freq[r]
            print(f"{r:>6} {ratio_freq[r]:>6} {ratio_weight[r]:>12} {avg:>10.2f}")

    # Look for arithmetic progressions: sequences m, m+d, m+2d, ...
    # that all share n-values pairwise
    print(f"\n--- Arithmetic progressions in m (length >= 3) ---")
    # For each delta, find maximal APs
    found_aps = []
    m_set = set(m_vals)
    for d in sorted(delta_freq.keys()):
        if d == 0:
            continue
        visited = set()
        for m_start in m_vals:
            if m_start in visited:
                continue
            # Extend the AP as far as possible
            ap = [m_start]
            m_cur = m_start + d
            while m_cur in m_set:
                # Check that consecutive pair shares n-values
                if m_to_n.get(ap[-1], set()) & m_to_n.get(m_cur, set()):
                    ap.append(m_cur)
                    m_cur += d
                else:
                    break
            if len(ap) >= 3:
                # Find n-values shared across ALL members of the AP
                common_n = m_to_n.get(ap[0], set())
                for mv in ap[1:]:
                    common_n = common_n & m_to_n.get(mv, set())
                found_aps.append({
                    "delta": d, "seq": ap,
                    "common_n": sorted(common_n),
                    "pairwise_sharing": [
                        sorted(m_to_n[ap[i]] & m_to_n[ap[i+1]])
                        for i in range(len(ap) - 1)
                    ],
                })
            for mv in ap:
                visited.add(mv)

    if found_aps:
        for ap in found_aps:
            seq_str = ",".join(str(x) for x in ap["seq"])
            print(f"  d={ap['delta']}: m=[{seq_str}] (len {len(ap['seq'])})")
            if ap["common_n"]:
                print(f"    n shared by ALL: {{{','.join(str(x) for x in ap['common_n'])}}}")
            for i, pw in enumerate(ap["pairwise_sharing"]):
                m1, m2 = ap["seq"][i], ap["seq"][i+1]
                print(f"    m={m1}&{m2}: n={{{','.join(str(x) for x in pw)}}}")
    else:
        print("  (none found)")


# ---------------------------------------------------------------------------
# Gap anatomy: what's at the missing n-positions?
# ---------------------------------------------------------------------------


def gap_anatomy(q: int, m: int, max_n: int = None) -> list[dict]:
    """For a (q, m) stratum, examine the composite values at gap positions.

    For n-values in [1, max_n] that are NOT in the stratum (i.e., 2^m + q^n
    is composite), factor the composite to see which q-lines it sits on.

    Returns list of dicts: {n, value, factors, q_valuation, cofactor}
    where value = 2^m + q^n, q_valuation = v_q(value), cofactor = value / q^v_q.
    """
    from sympy import factorint

    s = stratum(q, m)
    present_n = set(s.get_column("n_k").to_list()) if s.height > 0 else set()

    if max_n is None:
        max_n = max(present_n) if present_n else 21

    results = []
    for n in range(1, max_n + 1):
        if n in present_n:
            continue
        val = (1 << m) + q ** n
        if val < 2:
            continue
        factors = factorint(val)
        # q-adic valuation of val itself (not of val - something)
        # More useful: which small primes divide val?
        # And: v_q(val) for our base q
        v_q = factors.get(q, 0)
        cofactor = val
        for _ in range(v_q):
            cofactor //= q

        results.append({
            "n": n,
            "value": val,
            "factors": factors,
            "v_q": v_q,
            "cofactor": cofactor,
            "smallest_factor": min(factors.keys()),
        })

    return results


def gap_anatomy_report(q: int, m: int, max_n: int = None) -> None:
    """Report on composite values at gap positions in a stratum."""
    gaps = gap_anatomy(q, m, max_n=max_n)

    s = stratum(q, m)
    present_n = sorted(s.get_column("n_k").to_list()) if s.height > 0 else []

    print(f"\n=== Gap anatomy: q={q}, m={m} ===")
    print(f"Solutions at n = {{{','.join(str(x) for x in present_n)}}}")
    print(f"Gaps ({len(gaps)} composites):\n")

    if not gaps:
        print("  No gaps in range.")
        return

    print(f"{'n':>3} {'2^m + q^n':>16} {'smallest_pf':>11} "
          f"{'v_q':>3} {'cofactor':>14} {'factorization'}")
    print("-" * 80)

    # Track which small primes obstruct
    obstruction_primes: Counter = Counter()

    for g in gaps:
        fstr = " * ".join(
            f"{p}^{e}" if e > 1 else str(p)
            for p, e in sorted(g["factors"].items())
        )
        print(f"{g['n']:>3} {g['value']:>16,} {g['smallest_factor']:>11} "
              f"{g['v_q']:>3} {g['cofactor']:>14,} {fstr}")
        obstruction_primes[g["smallest_factor"]] += 1

    print(f"\n--- Obstruction by smallest prime factor ---")
    for p, cnt in obstruction_primes.most_common():
        print(f"  {p}: {cnt} gaps")


def obstruction_q_lines(q: int, max_n: int = 21) -> None:
    """For each gap position across all (q, m) strata, analyze which
    q'-lines the composite values sit on.

    The key question: when 2^m + q^n is composite, is it divisible by
    some other small prime q'?  If so, p = 2^m + q^n sits on the q'-line
    (meaning q' | (p - 2^m)), but it's a non-prime-power point on that line.

    This connects obstructions to the recurrence framework: the composite
    c = 2^m + q^n = q' * k means the q-action at this (m, n) produces a
    value on the q'-line instead.
    """
    from sympy import factorint

    adj = adjacency(q)
    if not adj["edges"]:
        return

    print(f"\n=== Obstruction q-lines for q={q} ===")
    print(f"For each m, examining composite values at gap n-positions.\n")

    # Collect all obstruction data across strata
    all_obstructions: list[dict] = []
    cross_line: Counter = Counter()  # (q, q') -> count

    for m in adj["all_m"]:
        s = stratum(q, m)
        present_n = set(s.get_column("n_k").to_list()) if s.height > 0 else set()

        for n in range(1, max_n + 1):
            if n in present_n:
                continue
            val = (1 << m) + q ** n
            if val < 4:
                continue
            factors = factorint(val)
            spf = min(factors.keys())
            all_obstructions.append({
                "m": m, "n": n, "value": val,
                "smallest_factor": spf,
                "factors": factors,
            })
            cross_line[(q, spf)] += 1

    if not all_obstructions:
        print("No obstructions found.")
        return

    print(f"Total composite values examined: {len(all_obstructions)}")

    # Cross-line summary: when the q-action misses, which q' catches it?
    print(f"\n--- Cross-line obstructions: q={q} gaps divisible by q' ---")
    print(f"{'q_prime':>7} {'count':>6} {'fraction':>8}")
    print("-" * 25)
    total = len(all_obstructions)
    for (_, qp), cnt in sorted(cross_line.items(), key=lambda x: -x[1]):
        print(f"{qp:>7} {cnt:>6} {cnt/total:>8.1%}")

    # For the top cross-line primes, show which (m, n) positions they hit
    top_qp = [qp for (_, qp), _ in sorted(cross_line.items(), key=lambda x: -x[1])[:5]]

    for qp in top_qp:
        hits = [(o["m"], o["n"]) for o in all_obstructions if o["smallest_factor"] == qp]
        if len(hits) <= 30:
            mn_str = ", ".join(f"({m},{n})" for m, n in sorted(hits))
        else:
            mn_str = ", ".join(f"({m},{n})" for m, n in sorted(hits)[:15])
            mn_str += f" ... ({len(hits)} total)"
        print(f"\n  q'={qp} hits (m,n): {mn_str}")

        # Check for pattern: are the n-values for q' concentrated?
        n_vals_hit = Counter(n for _, n in hits)
        if n_vals_hit:
            print(f"  n-distribution: "
                  + ", ".join(f"n={n}:{c}" for n, c in n_vals_hit.most_common(10)))

        # Check for pattern: are the m-values for q' concentrated?
        m_vals_hit = Counter(m for m, _ in hits)
        if m_vals_hit:
            top_m = m_vals_hit.most_common(5)
            print(f"  m-distribution (top 5): "
                  + ", ".join(f"m={m}:{c}" for m, c in top_m))


# ---------------------------------------------------------------------------
# Local-global analysis: sieve density from obstruction grids
# ---------------------------------------------------------------------------


def multiplicative_order(a: int, n: int) -> int:
    """Compute ord_n(a), the smallest k > 0 with a^k = 1 (mod n).

    Returns 0 when gcd(a, n) != 1.  Uses SageMath (PARI-backed).
    """
    from sage.all import Mod

    if gcd(a, n) != 1:
        return 0
    return int(Mod(a, n).multiplicative_order())


def local_obstruction_grid(q: int, ell: int) -> dict:
    """Compute the (m mod, n mod) obstruction grid at prime ell for fixed q.

    For cell (i, j) in Z/ord_ell(2) x Z/ord_ell(q), determines whether
    ell | (2^m + q^n) when m = i (mod ord_ell(2)), n = j (mod ord_ell(q)).
    Such (m, n) pairs cannot yield primes > ell.

    Also computes the sumset S = {2^m + q^n mod ell} and its coverage.
    """
    from .fixed_mod import power_residues_mod

    # ell = 2: 2^m + q^n is always odd (q odd, m >= 1). No obstruction.
    # ell = q: 2^m + q^n = 2^m mod q, never 0 since gcd(2, q) = 1.
    if ell == 2 or ell == q:
        return {
            "ell": ell, "ord_2": 0, "ord_q": 0,
            "killed": 0, "total": 0,
            "obstruction_rate": 0.0, "survival_rate": 1.0,
            "grid": [], "killed_cells": [],
            "sumset": set(), "sumset_coverage": 1.0,
        }

    d2 = multiplicative_order(2, ell)
    dq = multiplicative_order(q, ell)

    if d2 == 0 or dq == 0:
        return {
            "ell": ell, "ord_2": d2, "ord_q": dq,
            "killed": 0, "total": 0,
            "obstruction_rate": 0.0, "survival_rate": 1.0,
            "grid": [], "killed_cells": [],
            "sumset": set(), "sumset_coverage": 1.0,
        }

    # grid[i][j] = True when ell | (2^m + q^n) for m = i mod d2, n = j mod dq
    # pow(2, 0, ell) = 1 = 2^{d2} mod ell, representing m = 0 mod d2.
    grid = []
    killed_cells = []
    for i in range(d2):
        row = []
        r2 = pow(2, i, ell)
        for j in range(dq):
            rq = pow(q, j, ell)
            hit = (r2 + rq) % ell == 0
            row.append(hit)
            if hit:
                killed_cells.append((i, j))
        grid.append(row)

    killed = len(killed_cells)
    total = d2 * dq

    # Sumset for residue-class coverage
    R_2 = power_residues_mod(2, ell)
    R_q = power_residues_mod(q, ell)
    sumset = {(r2 + rq) % ell for r2 in R_2 for rq in R_q}

    return {
        "ell": ell,
        "ord_2": d2,
        "ord_q": dq,
        "killed": killed,
        "total": total,
        "obstruction_rate": killed / total if total > 0 else 0.0,
        "survival_rate": 1 - killed / total if total > 0 else 1.0,
        "grid": grid,
        "killed_cells": killed_cells,
        "sumset": sumset,
        "sumset_coverage": len(sumset) / ell,
    }


def sieve_density(q: int, ell_max: int = 100) -> dict:
    """Compute cumulative sieve-density prediction for fixed q.

    For each prime ell <= ell_max, computes the local obstruction rate
    (fraction of (m, n) mod-class pairs killed by ell), then takes the
    running product of survival rates.
    """
    from sage.all import prime_range

    primes = [int(p) for p in prime_range(ell_max + 1)]
    cumulative = 1.0
    per_prime = []

    for ell in primes:
        info = local_obstruction_grid(q, ell)
        cumulative *= info["survival_rate"]
        per_prime.append({
            "ell": ell,
            "ord_2": info["ord_2"],
            "ord_q": info["ord_q"],
            "killed": info["killed"],
            "total": info["total"],
            "obstruction_rate": info["obstruction_rate"],
            "survival_rate": info["survival_rate"],
            "cumulative_survival": cumulative,
        })

    return {
        "q": q,
        "ell_max": ell_max,
        "final_density": cumulative,
        "per_prime": per_prime,
    }


def obstruction_grid_report(q: int, ell_max: int = 50) -> None:
    """Print obstruction-grid analysis for each small prime ell.

    Shows multiplicative orders, the boolean grid, killed (m mod, n mod)
    classes, and obstruction rate.  Then overlays empirical data from the
    q-chain to verify no solution sits in a killed class.
    """
    from sage.all import prime_range

    print(f"\n=== Obstruction grid analysis: q={q}, ell_max={ell_max} ===\n")

    primes = [int(p) for p in prime_range(ell_max + 1)]
    grids = {}

    for ell in primes:
        info = local_obstruction_grid(q, ell)
        grids[ell] = info

        if info["total"] == 0:
            print(f"ell={ell:>3}: trivial (no obstruction)")
            continue

        d2, dq = info["ord_2"], info["ord_q"]
        print(f"ell={ell:>3}: ord(2)={d2}, ord({q})={dq}, "
              f"killed={info['killed']}/{info['total']} "
              f"({info['obstruction_rate']:.4f}), "
              f"sumset {len(info['sumset'])}/{ell} classes")

        # Compact grid for small orders
        if d2 <= 20 and dq <= 20 and info["grid"]:
            hdr = "  m\\n " + "".join(f"{j:>2}" for j in range(dq))
            print(hdr)
            for i, row in enumerate(info["grid"]):
                cells = "".join(" X" if v else " ." for v in row)
                print(f"  {i:>3} {cells}")

        if info["killed_cells"]:
            cells_str = ", ".join(
                f"({i},{j})" for i, j in info["killed_cells"]
            )
            print(f"  killed (m mod {d2}, n mod {dq}): {cells_str}")

        print()

    # Sanity check against empirical data
    print("--- Sanity: empirical solutions vs obstruction grids ---")
    chain = q_chain(q)
    if chain.height == 0:
        print("  No solutions to check.")
        return

    violations = 0
    for row in chain.iter_rows():
        p_val, m_val, n_val = int(row[0]), int(row[1]), int(row[2])
        for ell in primes:
            info = grids[ell]
            if info["total"] == 0 or not info["grid"]:
                continue
            d2, dq = info["ord_2"], info["ord_q"]
            i = m_val % d2
            j = n_val % dq
            if info["grid"][i][j] and p_val != ell:
                print(f"  VIOLATION: p={p_val}, m={m_val}, n={n_val}, "
                      f"ell={ell} (m%{d2}={i}, n%{dq}={j})")
                violations += 1

    if violations == 0:
        print(f"  OK: {chain.height} solutions, all clear of killed cells.")
    else:
        print(f"  {violations} violations found!")


def local_global_comparison(q: int, ell_max: int = 100) -> None:
    """Compare cumulative sieve prediction to empirical q-chain density.

    Builds the sieve product prime by prime and reports a table:
        ell, ord_2, ord_q, killed, local_rate, cumulative, ratio
    The ratio (empirical / cumulative) should converge as ell_max grows.
    Deviations indicate structure not captured by local obstructions.
    """
    from sage.all import prime_range

    print(f"\n=== Local-global comparison: q={q}, ell_max={ell_max} ===\n")

    # Empirical side: single scan of q-chain
    adj = adjacency(q)
    if not adj["edges"]:
        print(f"No solutions found for q={q}.")
        return

    all_m = adj["all_m"]
    all_n = adj["all_n"]
    n_solutions = len(adj["edges"])
    n_max = max(all_n)

    # Density: solutions per (m, n) slot in the observed grid
    empirical = n_solutions / (len(all_m) * n_max)
    print(f"q-chain: {n_solutions} solutions, {len(all_m)} m-values, "
          f"n in [1, {n_max}]")
    print(f"Empirical density: {n_solutions} / ({len(all_m)} * {n_max}) "
          f"= {empirical:.6f}\n")

    # Cumulative sieve
    primes_list = [int(p) for p in prime_range(ell_max + 1)]
    cumulative = 1.0

    print(f"{'ell':>5} {'ord2':>5} {'ordq':>5} {'kill':>7} "
          f"{'local':>8} {'cumul':>10} {'emp/cumul':>10}")
    print("-" * 58)

    for ell in primes_list:
        info = local_obstruction_grid(q, ell)
        cumulative *= info["survival_rate"]
        ratio = empirical / cumulative if cumulative > 0 else float('inf')

        if info["total"] == 0:
            print(f"{ell:>5} {'--':>5} {'--':>5} {'--':>7} "
                  f"{'--':>8} {cumulative:>10.6f} {ratio:>10.4f}")
        else:
            kill_str = f"{info['killed']}/{info['total']}"
            print(f"{ell:>5} {info['ord_2']:>5} {info['ord_q']:>5} "
                  f"{kill_str:>7} {info['obstruction_rate']:>8.4f} "
                  f"{cumulative:>10.6f} {ratio:>10.4f}")

    print(f"\nSieve density (ell <= {ell_max}): {cumulative:.6f}")
    print(f"Empirical density:              {empirical:.6f}")
    ratio = empirical / cumulative if cumulative > 0 else float('inf')
    print(f"Ratio (empirical / sieve):      {ratio:.4f}")

    if ratio < 1.0:
        print("\nRatio < 1: empirical is below sieve prediction (expected).")
        print("The gap reflects primality constraints beyond local obstructions,")
        print("including PNT decay and possible global structure.")
    else:
        print("\nRatio >= 1: empirical meets or exceeds sieve prediction.")
        print("Check density normalization or edge effects.")


# ---------------------------------------------------------------------------
# Partition queries: primes grouped by decomposition count k
# ---------------------------------------------------------------------------


def partition_report(
    k: int = None,
    p: int = None,
    q: int = None,
    limit: int = 50,
    max_p: int = None,
) -> None:
    """Print partition data via DuckDB.

    - max_p=P: all decompositions for primes <= P, grouped by k
    - p=P: show all decompositions for prime P
    - k=N: list primes with exactly N decompositions
    - k=N, q=Q: same, restricted to base q
    """
    from .querydb import QueryDB

    try:
        db = QueryDB(read_only=True)
    except Exception:
        print("No DuckDB database found. Run `funbuns --build-db` first.")
        return

    with db:
        if max_p is not None:
            df = db.decompositions_up_to(max_p)
            if df.height == 0:
                print(f"No primes found up to {max_p}.")
                return

            n_primes = df["p"].n_unique()
            k_values = df["k"].unique().sort().to_list()

            print(f"\nAll primes up to {max_p:,}  ({n_primes} primes)\n")

            for kv in k_values:
                group = df.filter(pl.col("k") == kv)
                primes_in_group = group["p"].unique().sort().to_list()

                if kv == 0:
                    print(f"--- k = 0  (obstructed, {len(primes_in_group)} primes) ---")
                    # Print obstructed primes in compact rows
                    line = "  "
                    for i, pv in enumerate(primes_in_group):
                        entry = f"{pv:,}"
                        if len(line) + len(entry) + 2 > 80:
                            print(line)
                            line = "  "
                        if line != "  ":
                            line += ", "
                        line += entry
                    if line != "  ":
                        print(line)
                else:
                    print(f"--- k = {kv}  ({len(primes_in_group)} primes) ---")
                    for pv in primes_in_group:
                        rows = group.filter(pl.col("p") == pv)
                        exprs = []
                        for row in rows.iter_rows(named=True):
                            if row["q"] is not None:
                                exprs.append(f"2^{row['m']}+{row['q']}^{row['n']}")
                        print(f"  {pv:>14,} = {' = '.join(exprs)}")
                print()
            return

        if p is not None:
            df = db.partitions_for_prime(p)
            if df.height == 0:
                print(f"No decompositions found for p={p}.")
                return
            print(f"\np = {p:,}  ({df.height} decompositions)")
            for row in df.iter_rows(named=True):
                print(f"  2^{row['m']} + {row['q']}^{row['n']}")
            return

        if k is not None:
            if k == 0:
                df = db.partitions_for_k(0, limit=limit)
                if df.height == 0:
                    print("No obstructed primes found.")
                    return
                print(f"\nObstructed primes (k=0), first {df.height}:")
                for row in df.iter_rows(named=True):
                    print(f"  p = {row['p']:,}")
                return

            df = db.partitions_for_k(k, q=q, limit=limit)
            if df.height == 0:
                label = f"k={k}" + (f", q={q}" if q else "")
                print(f"No primes with {label} found.")
                return

            # Format: derive m is already done by DuckDB (column "m")
            formatted = (
                df
                .sort("p", "q_k", "n_k")
                .with_columns(
                    pl.format("2^{}+{}^{}", "m", "q_k", "n_k").alias("expr")
                )
                .group_by("p")
                .agg(pl.col("expr").str.join(" = "))
                .sort("p")
            )

            label = f"k={k}" + (f", q={q}" if q else "")
            print(f"\nPrimes with {label} "
                  f"(showing {formatted.height}, limit={limit}):\n")

            for row in formatted.iter_rows(named=True):
                print(f"  p = {row['p']:>14,}  =  {row['expr']}")
            return

        print("Usage: --partitions with --partition-max-p P, --partition-k K, or --partition-p P")


def run_exploration(
    q: int = None,
    m: int = None,
    verbose: bool = True,
    tree: bool = False,
    local_global: bool = False,
    partitions: bool = False,
    partition_k: int = None,
    partition_p: int = None,
    partition_limit: int = 50,
    partition_max_p: int = None,
) -> None:
    """Entry point for CLI."""
    if (partitions or partition_k is not None
            or partition_p is not None or partition_max_p is not None):
        partition_report(k=partition_k, p=partition_p, q=q,
                         limit=partition_limit, max_p=partition_max_p)
        return
    if local_global:
        lq = q if q is not None else 3
        obstruction_grid_report(lq)
        local_global_comparison(lq)
        return
    if tree and q is not None:
        n_fiber_report(q)
        adjacency_report(q)
        m_pair_progressions(q)
        obstruction_q_lines(q)
    elif tree:
        n_fiber_report(3)
        adjacency_report(3)
        m_pair_progressions(3)
        obstruction_q_lines(3)
    else:
        report(q=q, m=m, verbose=verbose)
