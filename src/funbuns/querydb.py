"""
DuckDB query backend for prime partition data.

Hybrid storage: partition_counts TABLE on ext4 (indexed),
decompositions VIEW over parquet on exFAT (zero copy).
"""

from __future__ import annotations

import re
from pathlib import Path
from datetime import datetime

import duckdb
import polars as pl

from .utils import get_data_dir


def filter_min(expr: str) -> int | None:
    """Return the minimum value implied by a filter expression, or None.

    Works for exact values, ranges, and lists. Returns None for modular
    patterns (e.g. 2*i+1) where a useful minimum can't be determined.
    """
    expr = expr.strip()
    expr = re.sub(
        r'(\d+)\s*\^\s*(\d+)',
        lambda m: str(int(m.group(1)) ** int(m.group(2))),
        expr,
    )
    if re.fullmatch(r'\d+', expr):
        return int(expr)
    m = re.fullmatch(r'(\d+)\s*\.\.\s*(\d+)', expr)
    if m:
        return int(m.group(1))
    if re.fullmatch(r'\d+(\s*,\s*\d+)+', expr):
        return min(int(x.strip()) for x in expr.split(','))
    return None


def parse_filter(expr: str, column: str) -> str:
    """Parse a filter expression into a SQL WHERE clause fragment.

    Patterns:
        5        -> col = 5           (exact)
        2*i      -> col % 2 = 0      (multiples)
        2*i+1    -> col % 2 = 1      (modular)
        3*i-1    -> col % 3 = 2      (modular, negative offset)
        3..10    -> col BETWEEN 3 AND 10  (range)
        3,5,7    -> col IN (3, 5, 7)     (list)
    """
    expr = expr.strip()

    # Expand power notation: 10^9 -> 1000000000, 2^5 -> 32
    expr = re.sub(
        r'(\d+)\s*\^\s*(\d+)',
        lambda m: str(int(m.group(1)) ** int(m.group(2))),
        expr,
    )

    # Exact integer
    if re.fullmatch(r'\d+', expr):
        return f"{column} = {int(expr)}"

    # k*i  or  k*i+c  or  k*i-c
    m = re.fullmatch(r'(\d+)\s*\*\s*i\s*(?:([+-])\s*(\d+))?', expr)
    if m:
        k = int(m.group(1))
        if k == 0:
            raise ValueError("Multiplier cannot be 0")
        if m.group(2) and m.group(3):
            c = int(m.group(3))
            if m.group(2) == '-':
                c = -c
            return f"{column} % {k} = {c % k}"
        return f"{column} % {k} = 0"

    # Range: a..b
    m = re.fullmatch(r'(\d+)\s*\.\.\s*(\d+)', expr)
    if m:
        return f"{column} BETWEEN {int(m.group(1))} AND {int(m.group(2))}"

    # List: a,b,c
    if re.fullmatch(r'\d+(\s*,\s*\d+)+', expr):
        values = [int(x.strip()) for x in expr.split(',')]
        return f"{column} IN ({', '.join(str(v) for v in values)})"

    raise ValueError(f"Cannot parse filter: {expr!r}")


def get_db_path() -> Path:
    """Resolve database path from config hierarchy.

    1. FUNBUNS_DB_PATH env var
    2. pixi.toml [tool.funbuns.directories] db_path
    3. Fallback: data/funbuns.duckdb (relative to project)
    """
    import os

    if db_path := os.getenv("FUNBUNS_DB_PATH"):
        return Path(db_path)

    try:
        from .utils import get_config
        config = get_config()
        if db_path := config.get("db_path"):
            return Path(db_path)
    except Exception:
        pass

    return Path("data") / "funbuns.duckdb"


def _parquet_pattern() -> str:
    return str(get_data_dir() / "blocks" / "pp_b*.parquet")


class QueryDB:
    """DuckDB query interface for partition data."""

    def __init__(self, db_path: Path = None, read_only: bool = True):
        self.db_path = db_path or get_db_path()
        self._conn = None
        self._read_only = read_only

    def __enter__(self):
        if self.db_path.exists():
            self._conn = duckdb.connect(str(self.db_path), read_only=self._read_only)
        else:
            if self._read_only:
                raise FileNotFoundError(
                    f"No database at {self.db_path}. "
                    f"Run `funbuns --build-db` first."
                )
            self._conn = duckdb.connect(str(self.db_path))

        # Point temp storage to the SSD (630GB free) — the default lands on
        # /home which can fill up during large parquet scans, causing silent
        # partial results.
        temp_dir = get_data_dir() / "duckdb_tmp"
        temp_dir.mkdir(exist_ok=True)
        self._conn.execute(f"SET temp_directory = '{temp_dir}'")

        return self

    def __exit__(self, *exc):
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("QueryDB not open. Use as context manager.")
        return self._conn

    # ------------------------------------------------------------------
    # Build / sync
    # ------------------------------------------------------------------

    def build(self, parquet_pattern: str = None):
        """One-time import: create partition_counts table + decompositions view."""
        pattern = parquet_pattern or _parquet_pattern()

        print(f"Building database at {self.db_path}")
        print(f"Parquet source: {pattern}")

        # Force zstd compression for space efficiency
        self.conn.execute("SET force_compression = 'zstd'")

        # Point temp storage to exFAT SSD (614GB free) — regular I/O,
        # no flock needed for temp files.
        temp_dir = get_data_dir() / "duckdb_tmp"
        temp_dir.mkdir(exist_ok=True)
        self.conn.execute(f"SET temp_directory = '{temp_dir}'")

        # Decompositions view: zero-copy pointer to parquet
        print("Creating decompositions view...", flush=True)
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW decompositions AS
            SELECT * FROM read_parquet('{pattern}')
        """)

        # Partition counts: group by p, count decomposition rows.
        # Blocks are clean (no cross-block duplicates after 2026-03-19 cleanup).
        # Obstructed primes (q_k=0) get k=0.
        print("Building partition_counts (this takes a while)...", flush=True)
        self.conn.execute("""
            CREATE OR REPLACE TABLE partition_counts AS
            SELECT p, COUNT(*) FILTER (WHERE q_k > 0) AS k
            FROM decompositions
            GROUP BY p
        """)

        row_count = self.conn.execute(
            "SELECT COUNT(*) FROM partition_counts"
        ).fetchone()[0]
        print(f"  {row_count:,} primes indexed.", flush=True)

        # Skip ART index — DuckDB columnar scan on k (tiny cardinality,
        # 0-14) is already fast via zone maps.  ART index on 1.17B rows
        # exceeds 16GB RAM.

        # Metadata
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS db_meta (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)
        self._set_meta("build_timestamp", datetime.now().isoformat())
        self._set_meta("parquet_pattern", pattern)
        self._set_meta("prime_count", str(row_count))

        max_p = self.conn.execute(
            "SELECT MAX(p) FROM partition_counts"
        ).fetchone()[0]
        self._set_meta("max_prime", str(max_p))

        print(f"Done. {row_count:,} primes, max {max_p:,}", flush=True)

        return {"n_primes": row_count, "max_prime": max_p}

    def sync(self, parquet_pattern: str = None):
        """Incremental: append primes beyond the current max."""
        pattern = parquet_pattern or _parquet_pattern()
        max_p = int(self._get_meta("max_prime") or "0")

        print(f"Syncing from p > {max_p:,}...", flush=True)

        # Update the view to pick up new parquet files
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW decompositions AS
            SELECT * FROM read_parquet('{pattern}')
        """)

        # Insert new primes
        self.conn.execute(f"""
            INSERT INTO partition_counts
            SELECT p, COUNT(*) FILTER (WHERE q_k > 0) AS k
            FROM decompositions
            WHERE p > {max_p}
            GROUP BY p
        """)

        new_count = self.conn.execute(
            "SELECT COUNT(*) FROM partition_counts"
        ).fetchone()[0]
        new_max = self.conn.execute(
            "SELECT MAX(p) FROM partition_counts"
        ).fetchone()[0]

        self._set_meta("prime_count", str(new_count))
        self._set_meta("max_prime", str(new_max))
        self._set_meta("last_sync", datetime.now().isoformat())

        print(f"Done. {new_count:,} primes, max {new_max:,}", flush=True)

        return {"n_primes": new_count, "max_prime": new_max}

    def sync_blocks(self, block_nums: list[int]):
        """Sync specific blocks by number into partition_counts.

        For every prime in the specified blocks, recompute k from the
        full decompositions view (across all parquet files) and upsert
        into partition_counts.
        """
        blocks_dir = get_data_dir() / "blocks"
        # Build index: block number -> path (parse once)
        block_index = {}
        for p in blocks_dir.glob("pp_b*_p*.parquet"):
            try:
                bnum = int(p.name.split("_")[1][1:])  # pp_b{N}_p... -> N
                block_index[bnum] = p
            except (IndexError, ValueError):
                continue

        paths = []
        for num in block_nums:
            if num not in block_index:
                print(f"  WARNING: no block file found for b{num}", flush=True)
                continue
            paths.append(block_index[num])

        if not paths:
            print("No block files found. Nothing to sync.", flush=True)
            return

        file_list = [str(p) for p in paths]
        print(f"Syncing {len(file_list)} block(s): {', '.join(p.name for p in paths)}", flush=True)

        # Refresh the decompositions view
        pattern = _parquet_pattern()
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW decompositions AS
            SELECT * FROM read_parquet('{pattern}')
        """)

        prime_count = self.conn.execute(f"""
            SELECT COUNT(DISTINCT p) FROM read_parquet({file_list})
        """).fetchone()[0]
        print(f"  {prime_count:,} primes in selected blocks", flush=True)

        # Remove stale rows, then reinsert with correct k from all blocks
        deleted = self.conn.execute(f"""
            DELETE FROM partition_counts
            WHERE p IN (SELECT DISTINCT p FROM read_parquet({file_list}))
        """).fetchone()[0]
        if deleted:
            print(f"  Replaced {deleted:,} existing rows", flush=True)

        self.conn.execute(f"""
            INSERT INTO partition_counts
            SELECT p, COUNT(*) FILTER (WHERE q_k > 0) AS k
            FROM decompositions
            WHERE p IN (SELECT DISTINCT p FROM read_parquet({file_list}))
            GROUP BY p
        """)

        # Update metadata
        new_count = self.conn.execute(
            "SELECT COUNT(*) FROM partition_counts"
        ).fetchone()[0]
        new_max = self.conn.execute(
            "SELECT MAX(p) FROM partition_counts"
        ).fetchone()[0]
        self._set_meta("prime_count", str(new_count))
        self._set_meta("max_prime", str(new_max))
        self._set_meta("last_sync", datetime.now().isoformat())

        print(f"Done. {new_count:,} total primes, max {new_max:,}", flush=True)

    def status(self):
        """Print database status."""
        print(f"\nDatabase: {self.db_path}")
        print(f"Size: {self.db_path.stat().st_size / (1024**3):.2f} GB")

        # Try metadata table (may not exist if build was interrupted)
        try:
            for key in ["build_timestamp", "last_sync", "prime_count",
                         "max_prime", "parquet_pattern"]:
                val = self._get_meta(key)
                if val:
                    print(f"  {key}: {val}")
        except Exception:
            pass

        # k distribution (fast columnar scan)
        row_count = self.conn.execute(
            "SELECT COUNT(*) FROM partition_counts"
        ).fetchone()[0]
        print(f"\nTotal primes: {row_count:,}")

        dist = self.conn.execute("""
            SELECT k, COUNT(*) AS n FROM partition_counts
            GROUP BY k ORDER BY k
        """).fetchall()
        print(f"\n  {'k':>4} {'primes':>14}")
        print(f"  {'-'*20}")
        for k, n in dist:
            print(f"  {k:>4} {n:>14,}")

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def _fetch_pl(self, sql: str, columns: list[str]) -> pl.DataFrame:
        """Execute SQL and return Polars DataFrame without pyarrow."""
        rows = self.conn.execute(sql).fetchall()
        if not rows:
            return pl.DataFrame(schema={c: pl.Int64 for c in columns})
        return pl.DataFrame(rows, schema=columns, orient="row")

    def partitions_for_k(
        self, k: int, q: int = None, limit: int = 50
    ) -> pl.DataFrame:
        """Primes with exactly k decompositions, with their (q, n, m) triples."""
        if k == 0:
            return self._fetch_pl(f"""
                SELECT p FROM partition_counts
                WHERE k = 0
                ORDER BY p
                LIMIT {limit}
            """, ["p"])

        # Get matching primes from the partition_counts table
        prime_list = self.conn.execute(f"""
            SELECT p FROM partition_counts
            WHERE k = {k}
            ORDER BY p
            LIMIT {limit}
        """).fetchall()

        if not prime_list:
            return pl.DataFrame()

        primes = [r[0] for r in prime_list]

        q_filter = f"AND q_k = {q}" if q is not None else ""
        return self._fetch_pl(f"""
            SELECT p, q_k, n_k, m_k AS m
            FROM decompositions
            WHERE p IN (SELECT * FROM unnest({primes}))
              AND q_k > 0
              {q_filter}
            ORDER BY p, q_k, n_k
        """, ["p", "q_k", "n_k", "m"])

    def partitions_for_prime(self, p: int) -> pl.DataFrame:
        """All decompositions for a specific prime."""
        return self._fetch_pl(f"""
            SELECT p, m_k AS m, q_k AS q, n_k AS n
            FROM decompositions
            WHERE p = {p} AND q_k > 0
            ORDER BY q_k, n_k
        """, ["p", "m", "q", "n"])

    # ------------------------------------------------------------------
    # Web query interface
    # ------------------------------------------------------------------

    def k_distribution(self) -> list[dict]:
        """Count of primes per decomposition count k."""
        rows = self.conn.execute("""
            SELECT k, COUNT(*) AS count
            FROM partition_counts
            GROUP BY k ORDER BY k
        """).fetchall()
        return [{"k": int(k), "count": int(c)} for k, c in rows]

    def partitions_query(
        self,
        p_min: int = 2,
        p_max: int | None = None,
        k_min: int = 0,
        k_max: int | None = None,
        q: str | None = None,
        m: str | None = None,
        n: str | None = None,
        page: int = 0,
        page_size: int = 50,
        sort_by: str = "p",
        sort_dir: str = "asc",
        _count_cache: dict | None = None,
    ) -> dict:
        """Flexible partition query with filters and pagination.

        q, m, n accept expression strings parsed by parse_filter():
            "5"  ->  exact match
            "2*i"  ->  multiples of 2
            "2*i+1"  ->  odd values
            "3..10"  ->  range
            "3,5,7"  ->  set membership

        Two paths: fast (partition_counts only when no m/n/q filters)
        and filtered (single decompositions scan with COUNT(*) OVER()).

        Returns {"total": int, "page": int, "page_size": int,
                 "primes": [{"p": int, "k": int,
                             "decompositions": [{"m": int, "q": int, "n": int}]}]}
        """
        p_max_val = p_max if p_max is not None else 999_999_999_999
        if k_max is not None:
            k_clause = f"k BETWEEN {k_min} AND {k_max}"
        else:
            k_clause = f"k >= {k_min}"
        offset = page * page_size

        q_clause = f"AND {parse_filter(q, 'd.q_k')}" if q else ""
        m_clause = f"AND {parse_filter(m, 'd.m_k')}" if m else ""
        n_clause = f"AND {parse_filter(n, 'd.n_k')}" if n else ""
        has_decomp_filter = any([q_clause, m_clause, n_clause])

        order = f"{sort_by} {sort_dir.upper()}"

        if not has_decomp_filter:
            return self._query_fast(p_min, p_max_val, k_clause,
                                    page, page_size, offset, order)
        return self._query_filtered(p_min, p_max_val, k_clause,
                                    q, m, n,
                                    q_clause, m_clause, n_clause,
                                    page, page_size, offset, order,
                                    _count_cache)

    def _query_fast(self, p_min, p_max, k_clause,
                    page, page_size, offset, order) -> dict:
        """Fast path: no decomposition filters, use partition_counts only."""
        total = self.conn.execute(f"""
            SELECT COUNT(*) FROM partition_counts
            WHERE {k_clause}
              AND p BETWEEN {p_min} AND {p_max}
        """).fetchone()[0]

        page_rows = self.conn.execute(f"""
            SELECT p, k FROM partition_counts
            WHERE {k_clause}
              AND p BETWEEN {p_min} AND {p_max}
            ORDER BY {order}
            LIMIT {page_size} OFFSET {offset}
        """).fetchall()

        if not page_rows:
            return {"total": int(total), "page": page,
                    "page_size": page_size, "primes": []}

        return self._hydrate_page(page_rows, int(total), page, page_size)

    def _query_filtered(self, p_min, p_max, k_clause,
                        q, m, n,
                        q_clause, m_clause, n_clause,
                        page, page_size, offset, order,
                        _count_cache) -> dict:
        """Filtered path: scan decompositions once with COUNT(*) OVER()."""
        cache_key = (p_min, p_max, k_clause, q, m, n)

        cached_total = (_count_cache or {}).get(cache_key)

        if cached_total is not None:
            # Cache hit: skip the window function, just paginate
            page_rows_raw = self.conn.execute(f"""
                WITH matched AS (
                    SELECT DISTINCT d.p
                    FROM decompositions d
                    JOIN partition_counts pc ON d.p = pc.p
                    WHERE d.q_k > 0
                      AND d.p BETWEEN {p_min} AND {p_max}
                      AND pc.{k_clause}
                      {q_clause} {m_clause} {n_clause}
                )
                SELECT m.p, pc.k
                FROM matched m
                JOIN partition_counts pc ON m.p = pc.p
                ORDER BY {order}
                LIMIT {page_size} OFFSET {offset}
            """).fetchall()
            total = cached_total
        else:
            # First hit: use COUNT(*) OVER() to get total in one scan
            rows = self.conn.execute(f"""
                WITH matched AS (
                    SELECT DISTINCT d.p
                    FROM decompositions d
                    JOIN partition_counts pc ON d.p = pc.p
                    WHERE d.q_k > 0
                      AND d.p BETWEEN {p_min} AND {p_max}
                      AND pc.{k_clause}
                      {q_clause} {m_clause} {n_clause}
                )
                SELECT m.p, pc.k, COUNT(*) OVER() AS total
                FROM matched m
                JOIN partition_counts pc ON m.p = pc.p
                ORDER BY {order}
                LIMIT {page_size} OFFSET {offset}
            """).fetchall()

            if not rows:
                if _count_cache is not None:
                    _count_cache[cache_key] = 0
                return {"total": 0, "page": page,
                        "page_size": page_size, "primes": []}

            total = int(rows[0][2])
            if _count_cache is not None:
                _count_cache[cache_key] = total
            page_rows_raw = [(r[0], r[1]) for r in rows]

        if not page_rows_raw:
            return {"total": int(total), "page": page,
                    "page_size": page_size, "primes": []}

        return self._hydrate_page(page_rows_raw, int(total), page, page_size)

    def _hydrate_page(self, page_rows, total, page, page_size) -> dict:
        """Fetch decomposition details for a page of (p, k) tuples."""
        page_primes = [r[0] for r in page_rows]
        k_by_p = {r[0]: int(r[1]) for r in page_rows}

        decomp_rows = self.conn.execute(f"""
            SELECT p, m_k, q_k, n_k FROM decompositions
            WHERE p IN (SELECT * FROM unnest({page_primes})) AND q_k > 0
            ORDER BY p, q_k, n_k
        """).fetchall()

        decomps: dict[int, list] = {p: [] for p in page_primes}
        for dp, dm, dq, dn in decomp_rows:
            decomps[dp].append({"m": int(dm), "q": int(dq), "n": int(dn)})

        primes = [
            {"p": int(p), "k": k_by_p[p], "decompositions": decomps[p]}
            for p in page_primes
        ]
        return {"total": total, "page": page,
                "page_size": page_size, "primes": primes}

    def decompositions_up_to(self, max_p: int) -> pl.DataFrame:
        """All decompositions for primes up to max_p.

        Returns DataFrame with columns: p, k, m, q, n
        where k is the decomposition count for that prime.
        Obstructed primes (k=0) included with null m/q/n.
        """
        # Get all primes up to max_p with their k values
        rows = self.conn.execute(f"""
            SELECT pc.p, pc.k, d.m_k, d.q_k, d.n_k
            FROM partition_counts pc
            LEFT JOIN decompositions d
              ON pc.p = d.p AND d.q_k > 0
            WHERE pc.p <= {max_p}
            ORDER BY pc.k, pc.p, d.q_k, d.n_k
        """).fetchall()

        if not rows:
            return pl.DataFrame(schema={"p": pl.Int64, "k": pl.Int64,
                                        "m": pl.Int64, "q": pl.Int64,
                                        "n": pl.Int64})

        return pl.DataFrame(rows,
                            schema=["p", "k", "m", "q", "n"],
                            orient="row")

    # ------------------------------------------------------------------
    # Poset / planarity
    # ------------------------------------------------------------------

    def k33_search(self, p_bound: int = 1_000_000):
        """Search for K_{3,3} among k=3 primes below p_bound.

        For primes with exactly 3 decompositions, their parent set has
        exactly 3 elements. If 3+ primes share the same parent triple,
        that's a K_{3,3} subgraph (non-planarity witness).

        Returns list of (frozenset({q1,q2,q3}), [p1,p2,...]) for groups
        of 3+ primes sharing the same ancestor triple.
        """
        from collections import defaultdict

        print(f"K_{{3,3}} search: k=3 primes with p < {p_bound:,}", flush=True)

        rows = self.conn.execute(f"""
            SELECT d.p, d.q_k AS q
            FROM decompositions d
            JOIN partition_counts pc ON d.p = pc.p
            WHERE pc.k = 3 AND d.q_k > 0 AND d.p < {p_bound}
            ORDER BY d.p, d.q_k
        """).fetchall()

        # Build parent sets: p -> {q1, q2, q3}
        parent_sets: dict[int, set[int]] = defaultdict(set)
        for p, q in rows:
            parent_sets[p].add(int(q))

        print(f"  {len(parent_sets)} primes with k=3 below {p_bound:,}", flush=True)

        # Group by parent triple
        triple_groups: dict[frozenset, list[int]] = defaultdict(list)
        for p, parents in parent_sets.items():
            triple_groups[frozenset(parents)].append(int(p))

        # Report all triples and flag K_{3,3} witnesses
        print(f"  {len(triple_groups)} distinct parent triples", flush=True)

        # Show distribution of group sizes
        from collections import Counter
        size_dist = Counter(len(ps) for ps in triple_groups.values())
        for size in sorted(size_dist):
            print(f"    size {size}: {size_dist[size]} triples", flush=True)

        witnesses = []
        for triple, primes in sorted(triple_groups.items(),
                                      key=lambda x: -len(x[1])):
            if len(primes) >= 3:
                witnesses.append((triple, sorted(primes)))
                ancestors = sorted(triple)
                print(f"\n  K_{{3,3}} FOUND: ancestors {ancestors}", flush=True)
                print(f"    descendants ({len(primes)}): {sorted(primes)[:20]}"
                      f"{'...' if len(primes) > 20 else ''}", flush=True)

        if not witnesses:
            print("\n  No K_{3,3} found among k=3 primes.", flush=True)

            # Show largest groups as near-misses
            top = sorted(triple_groups.items(), key=lambda x: -len(x[1]))[:5]
            if top:
                print("  Largest groups:", flush=True)
                for triple, primes in top:
                    print(f"    {sorted(triple)} -> {sorted(primes)}", flush=True)

        return witnesses

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _set_meta(self, key: str, value: str):
        self.conn.execute("""
            INSERT OR REPLACE INTO db_meta (key, value) VALUES (?, ?)
        """, [key, value])

    def _get_meta(self, key: str) -> str | None:
        result = self.conn.execute(
            "SELECT value FROM db_meta WHERE key = ?", [key]
        ).fetchone()
        return result[0] if result else None
