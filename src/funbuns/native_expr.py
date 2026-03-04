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
    return expr.register_plugin(
        lib=_plugin_path(),
        symbol="v_ell",
        is_elementwise=True,
        kwargs={"ell": ell},
    )


def omega(expr: pl.Expr) -> pl.Expr:
    """Number of distinct prime factors ω(n). Returns UInt8."""
    return expr.register_plugin(
        lib=_plugin_path(),
        symbol="omega",
        is_elementwise=True,
    )


def big_omega(expr: pl.Expr) -> pl.Expr:
    """Total prime factors with multiplicity Ω(n). Returns UInt8."""
    return expr.register_plugin(
        lib=_plugin_path(),
        symbol="big_omega",
        is_elementwise=True,
    )


def dominant_share(expr: pl.Expr) -> pl.Expr:
    """Dominant share: max(v_q·ln q)/ln n. Returns Float64."""
    return expr.register_plugin(
        lib=_plugin_path(),
        symbol="dominant_share",
        is_elementwise=True,
    )


def mobius(expr: pl.Expr) -> pl.Expr:
    """Möbius function μ(n). Returns Int8."""
    return expr.register_plugin(
        lib=_plugin_path(),
        symbol="mobius",
        is_elementwise=True,
    )


def is_prime_power(expr: pl.Expr) -> pl.Expr:
    """Check if n is a prime power. Returns Boolean."""
    return expr.register_plugin(
        lib=_plugin_path(),
        symbol="is_prime_power",
        is_elementwise=True,
    )


def largest_prime_factor(expr: pl.Expr) -> pl.Expr:
    """Largest prime factor of n. Returns Int64."""
    return expr.register_plugin(
        lib=_plugin_path(),
        symbol="largest_prime_factor",
        is_elementwise=True,
    )
