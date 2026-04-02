"""
FastAPI web server for browsing prime power partition data.

Launch via: funbuns --serve [--port 8080]
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse

from .querydb import QueryDB, filter_min

# Persistent read-only connection, opened at startup
_db: QueryDB | None = None

# Server-side count cache for filtered queries.
# Keyed on (p_min, p_max, k_min, k_max, q, m, n) -> total count.
_count_cache: dict[tuple, int] = {}

# k-distribution cached at startup; valid for the server lifetime
# (server must restart after sync/rebuild, which recomputes this).
_k_dist: list[dict] | None = None

_TEMPLATES_DIR = Path(__file__).parent / "templates"


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _db, _k_dist
    _db = QueryDB(read_only=True)
    _db.__enter__()
    print(f"DuckDB opened: {_db.db_path}", flush=True)
    _k_dist = _db.k_distribution()
    print(f"k-distribution cached ({len(_k_dist)} buckets)", flush=True)
    yield
    if _db is not None:
        _db.__exit__(None, None, None)
        _db = None


app = FastAPI(title="funbuns partition browser", lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse(_TEMPLATES_DIR / "index.html", media_type="text/html")


@app.get("/api/stats")
def stats():
    return _k_dist


@app.get("/api/prime/{p}")
def prime_detail(p: int):
    df = _db.partitions_for_prime(p)
    rows = [
        {"m": row["m"], "q": row["q"], "n": row["n"]}
        for row in df.iter_rows(named=True)
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
):
    from .querydb import parse_filter
    # Validate expressions before hitting the DB
    try:
        for label, expr, col in [("q", q, "q_k"), ("m", m, "m_k"), ("n", n, "n_k")]:
            if expr:
                parse_filter(expr, col)
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

    return _db.partitions_query(
        p_min=effective_p_min, p_max=p_max,
        k_min=k_min, k_max=k_max,
        q=q, m=m, n=n,
        page=page, page_size=page_size,
        _count_cache=_count_cache,
    )


def serve(port: int = 8080):
    """Launch the web server."""
    import uvicorn

    print(f"Starting partition browser at http://127.0.0.1:{port}")
    uvicorn.run(app, host="127.0.0.1", port=port)
