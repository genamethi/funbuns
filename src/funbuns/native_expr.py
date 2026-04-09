"""
Python bindings for funbuns_native Rust Polars plugin.

Usage:
    import polars as pl
    from funbuns.native_expr import v_ell, omega, big_omega, dominant_share, mobius

    df = pl.DataFrame({"r": [12, 30, 49, 1024]})
    df.with_columns(
        omega(pl.col("r")).alias("omega"),
        big_omega(pl.col("r")).alias("Omega"),
        v_ell(pl.col("r"), ell=2).alias("v_2"),
        v_ell(pl.col("r"), ell=3).alias("v_3"),
        dominant_share(pl.col("r")).alias("dom_share"),
        mobius(pl.col("r")).alias("mu"),
    )
"""

from pathlib import Path
import polars as pl

# Resolve the shared library path. After `maturin develop` or `pip install`,
# the .so will be importable. We also check adjacent to this file for a
# development build.
def _lib_path() -> Path:
    """Find the compiled native extension."""
    # 1. Try importlib (installed via maturin develop / pip)
    try:
        import funbuns_native as _mod
        if _mod.__file__ is not None:
            return Path(_mod.__file__)
    except ImportError:
        pass

    # 2. Development fallback: look in target/release or target/debug
    project_root = Path(__file__).resolve().parents[2]
    for profile in ("release", "debug"):
        candidate = project_root / "funbuns_native" / "target" / profile / "libfunbuns_native.so"
        if candidate.exists():
            return candidate

    raise ImportError(
        "funbuns_native not found. Build it with:\n"
        "  cd funbuns_native && maturin develop --release\n"
        "or: pixi run build-native"
    )


def _plugin_path() -> str:
    return str(_lib_path())


def v_ell(expr: pl.Expr, ell: int) -> pl.Expr:
    """ℓ-adic valuation v_ℓ(n) for each element. Returns Int32."""
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="v_ell",
        args=[expr],
        is_elementwise=True,
        kwargs={"ell": ell},
    )


def omega(expr: pl.Expr) -> pl.Expr:
    """Number of distinct prime factors ω(n). Returns UInt8."""
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="omega",
        args=[expr],
        is_elementwise=True,
    )


def big_omega(expr: pl.Expr) -> pl.Expr:
    """Total prime factors with multiplicity Ω(n). Returns UInt8."""
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="big_omega",
        args=[expr],
        is_elementwise=True,
    )


def dominant_share(expr: pl.Expr) -> pl.Expr:
    """Dominant share: max(v_q·ln q)/ln n. Returns Float64."""
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="dominant_share",
        args=[expr],
        is_elementwise=True,
    )


def mobius(expr: pl.Expr) -> pl.Expr:
    """Möbius function μ(n). Returns Int8."""
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="mobius",
        args=[expr],
        is_elementwise=True,
    )


def is_prime_power(expr: pl.Expr) -> pl.Expr:
    """Check if n is a prime power. Returns Boolean."""
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="is_prime_power",
        args=[expr],
        is_elementwise=True,
    )


def largest_prime_factor(expr: pl.Expr) -> pl.Expr:
    """Largest prime factor of n. Returns Int64."""
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="largest_prime_factor",
        args=[expr],
        is_elementwise=True,
    )


def dominant_q(expr: pl.Expr) -> pl.Expr:
    """Dominant prime: the prime whose v_q·ln(q)/ln(n) is maximal. Returns Int64."""
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="dominant_q",
        args=[expr],
        is_elementwise=True,
    )


def dominant_exp(expr: pl.Expr) -> pl.Expr:
    """Exponent of the dominant prime factor. Returns UInt8."""
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="dominant_exp",
        args=[expr],
        is_elementwise=True,
    )


def power_residue_symbol(expr: pl.Expr, ell: int, n: int) -> pl.Expr:
    """Step 2: n-th power residue symbol u^((l-1)/gcd(n,l-1)) mod l.

    Returns Int64 — the symbol value in μ_{gcd(n,l-1)} ⊂ (Z/lZ)*.
    Extracts the unit part internally (divides out l from r).

    Compose with v_ell (Step 1) to get the full local test:
        Step 1: v_ell(r, ell) % n == 0  (valuation compatible)
        Step 2: power_residue_symbol(r, ell, n) == 1  (unit is n-th power)
    """
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="power_residue_symbol",
        args=[expr],
        is_elementwise=True,
        kwargs={"ell": ell, "n": n},
    )


def power_residue(expr: pl.Expr, ell: int, n: int) -> pl.Expr:
    """Full power residue struct (both steps combined).

    Returns a Struct with fields: v_ell, v_mod_n, unit_mod_ell, symbol, is_nth_power.
    Use power_residue_symbol() + v_ell() for the lean composable approach.
    """
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="power_residue",
        args=[expr],
        is_elementwise=True,
        kwargs={"ell": ell, "n": n},
    )


def full_profile(expr: pl.Expr, filtration_primes: list[int] | None = None) -> pl.Expr:
    """Full arithmetic profile from a single factorization.

    Returns a Struct column with fields:
        omega, big_omega, dominant_q, dominant_exp, dominant_share,
        mu, is_prime_power, v_2, v_3, ... (for each filtration prime)

    Use .struct.unnest() to expand into individual columns.
    """
    if filtration_primes is None:
        filtration_primes = [2, 3, 5, 7, 11, 13]
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="full_profile",
        args=[expr],
        is_elementwise=True,
        kwargs={"filtration_primes": filtration_primes},
    )


# ---------------------------------------------------------------------------
# Graph queries: successors, predecessors, ancestry chain
#
# Each returns a Struct{primes: List(Int64), ms: List(UInt8)}.
# Set FUNBUNS_GRAPH_DIR to point to the graph directory, or it defaults
# to /media/extssd/research/dioph.pp/data/graph.
# ---------------------------------------------------------------------------

def graph_successors(expr: pl.Expr) -> pl.Expr:
    """Successor primes in the partition DAG (q → p edges from this node).

    Returns Struct{primes: List(Int64), ms: List(UInt8)}.
    """
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="graph_successors",
        args=[expr],
        is_elementwise=True,
    )


def graph_predecessors(expr: pl.Expr) -> pl.Expr:
    """Predecessor (ancestor) primes in the partition DAG.

    Returns Struct{primes: List(Int64), ms: List(UInt8)}.
    """
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="graph_predecessors",
        args=[expr],
        is_elementwise=True,
    )


def graph_chain(expr: pl.Expr) -> pl.Expr:
    """Follow the ancestry chain back to a k=0 source.

    Returns Struct{primes: List(Int64), ms: List(UInt8)} tracing the path
    from the input prime to its terminal ancestor.
    """
    return pl.plugins.register_plugin_function(
        plugin_path=_plugin_path(),
        function_name="graph_chain",
        args=[expr],
        is_elementwise=True,
    )
