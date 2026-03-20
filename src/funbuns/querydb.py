"""
DuckDB query backend for prime partition data.

Hybrid storage: partition_counts TABLE on ext4 (indexed),
decompositions VIEW over parquet on exFAT (zero copy).
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime

import duckdb
import polars as pl

from .utils import get_data_dir


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

        print(f"Done. Max prime: {max_p:,}", flush=True)

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
            SELECT p, q_k, n_k,
                   CAST(LOG2(p - POWER(q_k, n_k)) AS INTEGER) AS m
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
