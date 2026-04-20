"""
FastAPI web server for browsing prime power partition data.

Launch via: funbuns-admin serve [--port 8081]
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

import polars as pl
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from pyiceberg.catalog import Catalog

from .iceberg_schema import open_catalog, scan_decompositions, scan_primes
from .query_filters import filter_min, parse_filter

# Persistent catalog, opened at startup.
_catalog: Catalog | None = None

# Server-side count cache for filtered queries.
# Keyed on (p_min, p_max, k_min, k_max, q, m, n) -> total count.
_count_cache: dict[tuple, int] = {}

# k-distribution cached at startup; valid for the server lifetime
# (server must restart after new commits land to refresh).
_k_dist: list[dict] | None = None

_TEMPLATES_DIR = Path(__file__).parent / "templates"


def _primes_lf() -> pl.LazyFrame:
    return scan_primes(_catalog)


def _decomp_lf() -> pl.LazyFrame:
    return scan_decompositions(_catalog)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _catalog, _k_dist
    _catalog = open_catalog()
    print("Iceberg catalog opened", flush=True)
    _k_dist = _compute_k_distribution()
    print(f"k-distribution cached ({len(_k_dist)} buckets)", flush=True)
    yield
    _catalog = None


def _compute_k_distribution() -> list[dict]:
    df = (
        _primes_lf()
        .group_by("k")
        .agg(pl.len().alias("count"))
        .sort("k")
        .collect(engine="streaming")
    )
    return [{"k": int(r["k"]), "count": int(r["count"])} for r in df.iter_rows(named=True)]


app = FastAPI(title="funbuns partition browser", lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse(_TEMPLATES_DIR / "index.html", media_type="text/html")


@app.get("/api/stats")
def stats():
    return _k_dist


@app.get("/api/prime/{p}")
def prime_detail(p: int):
    df = (
        _decomp_lf()
        .filter((pl.col("p") == p) & (pl.col("q_k") > 0))
        .select([
            pl.col("m_k").alias("m"),
            pl.col("q_k").alias("q"),
            pl.col("n_k").alias("n"),
        ])
        .sort(["q", "n"])
        .collect(engine="streaming")
    )
    rows = [
        {"m": int(r["m"]), "q": int(r["q"]), "n": int(r["n"])}
        for r in df.iter_rows(named=True)
    ]
    return {"p": p, "k": len(rows), "decompositions": rows}


@app.get("/api/partitions")
def partitions(
    p_min: int = Query(2, ge=2),
    p_max: int | None = Query(None, ge=2),
    k_min: int = Query(0, ge=0),
    k_max: int | None = Query(None, ge=0),
    q: str | None = Query(None),
    m: str | None = Query(None),
    n: str | None = Query(None),
    page: int = Query(0, ge=0),
    page_size: int = Query(50, ge=1, le=500),
    sort_by: str = Query("p", pattern="^(p|k)$"),
    sort_dir: str = Query("asc", pattern="^(asc|desc)$"),
):
    # Validate decomposition-side expressions up front.
    try:
        decomp_exprs: list[pl.Expr] = []
        for label, expr, col in [("q", q, "q_k"), ("m", m, "m_k"), ("n", n, "n_k")]:
            if expr:
                decomp_exprs.append(parse_filter(expr, col))
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

    # Derive tighter p_min from filter expressions.
    # p = 2^m + q^n, so:
    #   m_lo  =>  p > 2^m_lo
    #   q_lo and n_lo (conjunct)  =>  p > q_lo^n_lo
    effective_p_min = p_min
    if m:
        m_lo = filter_min(m)
        if m_lo is not None:
            effective_p_min = max(effective_p_min, 2 ** m_lo)
    if q and n:
        q_lo = filter_min(q)
        n_lo = filter_min(n)
        if q_lo is not None and n_lo is not None:
            effective_p_min = max(effective_p_min, q_lo ** n_lo)

    # Primes-side filter
    primes_lf = _primes_lf().filter(pl.col("p") >= effective_p_min)
    if p_max is not None:
        primes_lf = primes_lf.filter(pl.col("p") <= p_max)
    primes_lf = primes_lf.filter(pl.col("k") >= k_min)
    if k_max is not None:
        primes_lf = primes_lf.filter(pl.col("k") <= k_max)

    has_decomp_filter = bool(decomp_exprs)

    if has_decomp_filter:
        # Restrict primes to those with at least one matching decomposition.
        decomp_filtered = (
            _decomp_lf()
            .filter(pl.col("q_k") > 0)
            .filter(pl.col("p") >= effective_p_min)
        )
        if p_max is not None:
            decomp_filtered = decomp_filtered.filter(pl.col("p") <= p_max)
        for e in decomp_exprs:
            decomp_filtered = decomp_filtered.filter(e)
        matching_p = decomp_filtered.select("p").unique()
        primes_lf = primes_lf.join(matching_p, on="p", how="semi")

    cache_key = (effective_p_min, p_max, k_min, k_max, q, m, n)
    total = _count_cache.get(cache_key)
    if total is None:
        total = int(primes_lf.select(pl.len()).collect(engine="streaming")[0, 0])
        _count_cache[cache_key] = total

    if total == 0:
        return {"total": 0, "page": page, "page_size": page_size, "primes": []}

    descending = sort_dir == "desc"
    offset = page * page_size
    page_df = (
        primes_lf
        .sort(sort_by, descending=descending)
        .slice(offset, page_size)
        .collect(engine="streaming")
    )

    if page_df.height == 0:
        return {"total": total, "page": page, "page_size": page_size, "primes": []}

    page_p_list = page_df["p"].to_list()
    k_by_p = {int(row["p"]): int(row["k"]) for row in page_df.iter_rows(named=True)}

    # Fetch decompositions for just this page's primes.
    decomp_rows = (
        _decomp_lf()
        .filter(pl.col("p").is_in(page_p_list))
        .filter(pl.col("q_k") > 0)
        .select(["p", "m_k", "q_k", "n_k"])
        .sort(["p", "q_k", "n_k"])
        .collect(engine="streaming")
    )

    decomps: dict[int, list] = {int(p): [] for p in page_p_list}
    for row in decomp_rows.iter_rows(named=True):
        decomps[int(row["p"])].append(
            {"m": int(row["m_k"]), "q": int(row["q_k"]), "n": int(row["n_k"])}
        )

    primes = [
        {"p": int(p), "k": k_by_p[int(p)], "decompositions": decomps[int(p)]}
        for p in page_p_list
    ]
    return {"total": total, "page": page, "page_size": page_size, "primes": primes}


def serve(port: int = 8080):
    """Launch the web server."""
    import uvicorn

    print(f"Starting partition browser at http://127.0.0.1:{port}")
    uvicorn.run(app, host="127.0.0.1", port=port)
