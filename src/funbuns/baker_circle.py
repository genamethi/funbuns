"""
Baker circle visualization for archimedean H^1 obstruction analysis.

For prime p and small primes q, projects theta(n) = frac(log2(p - q^n))
onto the unit circle. A solution p = 2^m + q^n exists iff theta = 0.

Baker's theorem constrains how close theta can approach 0, providing
the archimedean obstruction invisible to finite-prime analysis.

Mathematical setup:
  For fixed p and prime q:
    n ranges from 1 to floor(log_q(p))
    r(n) = p - q^n
    theta(n) = frac(log2(r(n))), projected onto [0, 1)
    Solution at n* iff theta(n*) = 0, i.e. r(n*) is a power of 2

Usage: funbuns --baker-circle <P>
"""

import math
import numpy as np
import altair as alt
import polars as pl
from pathlib import Path

from .utils import get_data_dir

DEFAULT_Q_PRIMES = [3, 5, 7, 11, 13, 17, 19, 23]


def _is_prime_power_odd(n: int) -> tuple[int, int] | None:
    """If n is a prime power of an odd prime (q^k), return (q, k). Else None."""
    if n <= 1 or n % 2 == 0:
        return None
    d = 3
    while d * d <= n:
        if n % d == 0:
            k = 0
            temp = n
            while temp % d == 0:
                temp //= d
                k += 1
            return (d, k) if temp == 1 else None
        d += 2
    return (n, 1)


def all_solutions(p: int) -> list[tuple[int, int, int]]:
    """Find ALL solutions to p = 2^m + q^n (checking every q, not just small ones).

    Returns list of (m, q, n) tuples.
    """
    solutions = []
    m = 1
    while (1 << m) < p:
        r = p - (1 << m)
        if r >= 3:
            pp = _is_prime_power_odd(r)
            if pp is not None:
                q, n = pp
                solutions.append((m, q, n))
        m += 1
    return solutions


def _factor_trial(n: int) -> str:
    """Trial division factorization, returns string like '2^3 * 7'."""
    if n <= 1:
        return str(n)
    factors = {}
    d = 2
    temp = abs(n)
    while d * d <= temp:
        while temp % d == 0:
            factors[d] = factors.get(d, 0) + 1
            temp //= d
        d += 1
    if temp > 1:
        factors[temp] = factors.get(temp, 0) + 1
    parts = []
    for pf, e in sorted(factors.items()):
        parts.append(f"{pf}^{e}" if e > 1 else str(pf))
    return " * ".join(parts) if parts else str(n)


def circle_data(p: int, q_primes: list[int] | None = None) -> pl.DataFrame:
    """Compute circle points for each small prime q.

    For each q, computes n_max = floor(log_q(p)), then for n = 1..n_max:
      r = p - q^n, theta = frac(log2(r))
      Solution iff r is a power of 2 (r > 0)

    Returns DataFrame with columns:
      q, n, r, theta, angular_dist, is_solution, m, factorization,
      x, y, temporal_dist, q_has_solution, equation
    """
    if q_primes is None:
        q_primes = DEFAULT_Q_PRIMES

    rows = []
    solutions_by_q: dict[int, list[int]] = {}

    for q in q_primes:
        if q >= p:
            continue
        n_max = int(math.log(p) / math.log(q))
        q_sols = []

        for n in range(1, n_max + 1):
            r = p - q ** n
            if r <= 0:
                break

            theta = math.log2(r) % 1.0
            is_sol = (r & (r - 1)) == 0 and r > 0
            m = r.bit_length() - 1 if is_sol else None

            if is_sol:
                q_sols.append(n)

            angular_dist = min(theta, 1.0 - theta)

            rows.append({
                "q": q,
                "n": n,
                "r": r,
                "theta": round(theta, 8),
                "angular_dist": round(angular_dist, 8),
                "is_solution": is_sol,
                "m": m,
                "factorization": _factor_trial(r) if r < 10 ** 15 else f"~{r:.4e}",
                "x": round(math.cos(2 * math.pi * theta), 8),
                "y": round(math.sin(2 * math.pi * theta), 8),
            })

        solutions_by_q[q] = q_sols

    if not rows:
        return pl.DataFrame()

    # Add computed fields before constructing DataFrame
    for row in rows:
        sols = solutions_by_q[row["q"]]
        row["q_has_solution"] = bool(sols)
        row["temporal_dist"] = min(abs(row["n"] - s) for s in sols) if sols else None
        row["equation"] = (
            f"{p} = 2^{row['m']} + {row['q']}^{row['n']}" if row["is_solution"]
            else f"r = {p} - {row['q']}^{row['n']} = {row['r']}"
        )

    return pl.DataFrame(rows)


def baker_clearance(p: int, q: int) -> float:
    """Lower bound on |theta(n)| from Baker's theorem (Matveev 2000).

    For the linear form Lambda = m*log(2) - log(p - q^n):
      |Lambda| > exp(-C * log(B) * h(2) * h(q))
    where B = max(m, n), h(alpha) = log(alpha) (logarithmic height).

    Returns theta_min = |Lambda| / (2*pi), the minimum angular distance
    from theta = 0 that a non-solution can achieve. Typically astronomically
    small -- the point is that it's nonzero, not that it's large.
    """
    log_p = math.log(p)
    n_max = int(log_p / math.log(q))
    m_max = int(log_p / math.log(2))

    if n_max < 1 or m_max < 1:
        return 0.0

    # Matveev's constant for linear forms in 2 logarithms over Q
    C = 1.4 * 30 ** 7
    B = max(m_max, n_max)
    h1 = math.log(2)
    h2 = math.log(q)

    exponent = -C * math.log(max(B, 2)) * h1 * h2
    if exponent < -700:
        return 0.0

    return math.exp(exponent) / (2 * math.pi)


def generate_baker_circle(p: int, q_primes: list[int] | None = None,
                          output: Path | None = None) -> Path | None:
    """Generate Baker circle HTML visualization for prime p."""
    if q_primes is None:
        q_primes = [q for q in DEFAULT_Q_PRIMES if q < p]
    else:
        q_primes = [q for q in q_primes if q < p]

    df = circle_data(p, q_primes)
    if df.is_empty():
        print(f"No data for p = {p}")
        return None

    max_n = df["n"].max()

    # Slider for stepping through n
    n_slider = alt.param(
        name="n_step",
        value=max_n,
        bind=alt.binding_range(min=1, max=max_n, step=1, name="n <= "),
    )

    # Circle outline (unit circle, 201 points to close)
    t = np.linspace(0, 2 * np.pi, 201)
    outline_df = pl.DataFrame({
        "ox": np.round(np.cos(t), 6).tolist(),
        "oy": np.round(np.sin(t), 6).tolist(),
        "order": list(range(201)),
    })

    # Theta tick labels at cardinal positions
    tick_df = pl.DataFrame({
        "tx": [1.2, 0.0, -1.2, 0.0],
        "ty": [0.0, 1.2, 0.0, -1.2],
        "label": ["0", "1/4", "1/2", "3/4"],
    })

    # Target crosshair at theta=0
    target_df = pl.DataFrame({"tx": [1.0], "ty": [0.0]})

    # Shared scales
    x_scale = alt.Scale(domain=[-1.6, 1.6])
    y_scale = alt.Scale(domain=[-1.6, 1.6])

    # Max temporal distance for color domain
    td_vals = df.filter(pl.col("temporal_dist").is_not_null())["temporal_dist"]
    max_td = max(int(td_vals.max()), 1) if len(td_vals) > 0 else 1

    # Comprehensive solution check (all q, not just visualization set)
    all_sols = all_solutions(p)
    has_any_solution = len(all_sols) > 0

    q_charts = []

    for i, q in enumerate(q_primes):
        q_data = df.filter(pl.col("q") == q)
        if q_data.is_empty():
            continue

        q_sols = q_data.filter(pl.col("is_solution"))
        q_has_sol = q_sols.height > 0

        # Layer 0: Circle outline
        outline = alt.Chart(outline_df).mark_line(
            color="#cccccc", strokeWidth=1.5
        ).encode(
            x=alt.X("ox:Q", scale=x_scale, axis=None),
            y=alt.Y("oy:Q", scale=y_scale, axis=None),
            order="order:Q",
        )

        # Layer 1: Theta tick labels
        ticks = alt.Chart(tick_df).mark_text(
            fontSize=9, color="#999999"
        ).encode(
            x=alt.X("tx:Q", scale=x_scale),
            y=alt.Y("ty:Q", scale=y_scale),
            text="label:N",
        )

        # Layer 2: Target crosshair at theta=0
        target = alt.Chart(target_df).mark_point(
            shape="cross", size=100, color="#cc0000", strokeWidth=2
        ).encode(
            x=alt.X("tx:Q", scale=x_scale),
            y=alt.Y("ty:Q", scale=y_scale),
        )

        # Common tooltip
        tip = [
            alt.Tooltip("n:Q", title="n"),
            alt.Tooltip("r:Q", title="r = p - q^n"),
            alt.Tooltip("theta:Q", title="theta", format=".6f"),
            alt.Tooltip("angular_dist:Q", title="|theta|", format=".6f"),
            alt.Tooltip("equation:N", title="equation"),
            alt.Tooltip("factorization:N", title="r factors"),
        ]
        if q_has_sol:
            tip.append(alt.Tooltip("temporal_dist:Q", title="|n - n*|"))

        layers = [outline, ticks, target]

        if q_has_sol:
            # Non-solution points: color by temporal distance (green -> red)
            nonsol = q_data.filter(~pl.col("is_solution"))
            if nonsol.height > 0:
                nonsol_layer = alt.Chart(nonsol).mark_circle(
                    size=80, opacity=0.85
                ).encode(
                    x=alt.X("x:Q", scale=x_scale),
                    y=alt.Y("y:Q", scale=y_scale),
                    color=alt.Color("temporal_dist:Q",
                                    scale=alt.Scale(scheme="redyellowgreen",
                                                    reverse=True,
                                                    domain=[0, max_td]),
                                    legend=None),
                    tooltip=tip,
                ).transform_filter(alt.datum.n <= n_slider)
                layers.append(nonsol_layer)

            # Solution points: green diamonds
            sol_layer = alt.Chart(q_sols).mark_point(
                shape="diamond", size=250, filled=True,
                color="#00aa00", stroke="#006600", strokeWidth=1.5
            ).encode(
                x=alt.X("x:Q", scale=x_scale),
                y=alt.Y("y:Q", scale=y_scale),
                tooltip=tip,
            ).transform_filter(alt.datum.n <= n_slider)
            layers.append(sol_layer)
        else:
            # No solutions: color by angular distance (blue gradient)
            pts_layer = alt.Chart(q_data).mark_circle(
                size=80, opacity=0.85
            ).encode(
                x=alt.X("x:Q", scale=x_scale),
                y=alt.Y("y:Q", scale=y_scale),
                color=alt.Color("angular_dist:Q",
                                scale=alt.Scale(scheme="blues",
                                                domain=[0, 0.5]),
                                legend=None),
                tooltip=tip,
            ).transform_filter(alt.datum.n <= n_slider)
            layers.append(pts_layer)

        # n-labels on each point
        n_labels = alt.Chart(q_data).mark_text(
            fontSize=9, dy=-12, color="#555555"
        ).encode(
            x=alt.X("x:Q", scale=x_scale),
            y=alt.Y("y:Q", scale=y_scale),
            text="n:Q",
        ).transform_filter(alt.datum.n <= n_slider)
        layers.append(n_labels)

        # Subtitle: solution info for this q
        if q_has_sol:
            sol_parts = [f"n={row['n']},m={row['m']}"
                         for row in q_sols.iter_rows(named=True)]
            subtitle = f"Solutions: {', '.join(sol_parts)}"
        else:
            best = q_data.sort("angular_dist").row(0, named=True)
            subtitle = (f"No solution (best |theta| = {best['angular_dist']:.4f}"
                        f" at n={best['n']})")

        q_chart = alt.layer(*layers).properties(
            width=200, height=200,
            title=alt.TitleParams(
                text=f"q = {q}",
                subtitle=subtitle,
                fontSize=14, subtitleFontSize=10,
            ),
        )

        if i == 0:
            q_chart = q_chart.add_params(n_slider)

        q_charts.append(q_chart)

    if not q_charts:
        return None

    # Arrange in grid (4 columns)
    ncols = min(4, len(q_charts))
    chart_rows = []
    for j in range(0, len(q_charts), ncols):
        chunk = q_charts[j:j + ncols]
        chart_rows.append(
            alt.hconcat(*chunk).resolve_scale(color="independent")
        )

    chart = alt.vconcat(*chart_rows) if len(chart_rows) > 1 else chart_rows[0]

    # Title (uses comprehensive all_solutions, not just visualization q set)
    status = "NON-OBSTRUCTED" if has_any_solution else "OBSTRUCTED"
    if has_any_solution:
        sol_eqs = [f"2^{m}+{q}^{n}" for m, q, n in all_sols]
        subtitle_text = f"{p} = " + " | ".join(sol_eqs)
    else:
        subtitle_text = "No decomposition p = 2^m + q^n exists"

    # Baker bound annotation
    baker_parts = []
    for q in q_primes:
        bc = baker_clearance(p, q)
        if bc > 0:
            baker_parts.append(f"q={q}: {bc:.1e}")
    baker_text = ("Baker bounds (Matveev): " + ", ".join(baker_parts)
                  if baker_parts else "Baker bounds: below float precision")

    subtitles = [subtitle_text, baker_text]

    chart = chart.properties(
        title=alt.TitleParams(
            text=f"Baker Circle: p = {p} ({status})",
            subtitle=subtitles,
            fontSize=18, subtitleFontSize=11,
        ),
    ).configure_view(strokeWidth=0)

    if output is None:
        output = get_data_dir() / f"baker_circle_{p}.html"
    output.parent.mkdir(parents=True, exist_ok=True)
    chart.save(str(output))
    return output


def run_baker_circle(p: int, verbose: bool = False) -> None:
    """CLI entry point for --baker-circle."""
    print(f"Baker Circle Analysis: p = {p}")
    print("=" * 50)

    q_primes = [q for q in DEFAULT_Q_PRIMES if q < p]
    if not q_primes:
        print(f"p = {p} too small (need p > 3)")
        return

    # Comprehensive solution check (all q, not just visualization set)
    all_sols = all_solutions(p)
    if not all_sols:
        print("Status: OBSTRUCTED")
    else:
        print(f"Status: NON-OBSTRUCTED ({len(all_sols)} solution(s))")
        for m, q, n in all_sols:
            print(f"  {p} = 2^{m} + {q}^{n}")

    df = circle_data(p, q_primes)
    if df.is_empty():
        print("No valid data.")
        return

    print(f"\nCircles: q in {q_primes}")
    print(f"Max n: {df['n'].max()}")

    # Baker bounds
    if verbose:
        print("\nBaker clearance (Matveev 2000):")
        for q in q_primes:
            bc = baker_clearance(p, q)
            q_data = df.filter(pl.col("q") == q)
            min_ad = q_data["angular_dist"].min()
            print(f"  q={q:>2}: theory = {bc:.2e}, actual = {min_ad:.6f}")

    # Nearest approaches per q
    print("\nNearest misses (smallest |theta| per q):")
    for q in q_primes:
        qd = (df.filter((pl.col("q") == q) & (~pl.col("is_solution")))
              .sort("angular_dist"))
        if qd.height > 0:
            b = qd.row(0, named=True)
            has_sol = df.filter(
                (pl.col("q") == q) & pl.col("is_solution")
            ).height > 0
            sol_mark = " [has solution]" if has_sol else ""
            print(f"  q={q:>2}: n={b['n']}, |theta|={b['angular_dist']:.6f}, "
                  f"r={b['r']} = {b['factorization']}{sol_mark}")

    output = generate_baker_circle(p, q_primes)
    if output:
        print(f"\nVisualization saved: {output}")
