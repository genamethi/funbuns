"""
Iceberg table definitions and write primitive for the prime-power decomposition
dataset.

Two tables:

    funbuns.primes          — one row per p (including k=0), universe of primes
    funbuns.decompositions  — fact table, one row per (p, m_k, n_k, q_k) with q_k>0

Both are partitioned by identity(commit_seq) and sorted by (p) / (p, m_k).
The sort declaration is advisory in PyIceberg 0.11.1 — we enforce it on the
writer side via pre-sort and validation.

``commit_seq`` is a **write-side flush counter**, not a position along the
p-axis. Under multiprocessing ingest (``imap_unordered``) workers finish in
non-deterministic order, so a batch with lower commit_seq may cover a higher
p-range than a later one. Every (p_lo, p_hi) range is still disjoint and
contiguous, but ``ORDER BY commit_seq`` does not walk primes in p-order.
Queries that want p-ordering must sort on ``p`` (or read the manifest's
``p.lower_bound`` per file). Resume state is derived from ``max(p)``, not
from ``max(commit_seq)``, so reorder is safe for the write path.

Public surface:

    open_catalog(warehouse_root=None) -> Catalog
    ensure_tables(cat) -> (primes_tbl, decomp_tbl)
    validate_primes_batch(df, *, commit_seq)
    validate_decomp_batch(df, *, commit_seq)
    validate_cross(primes_df, decomp_df)
    build_file_kv(pa_table, *, table_name, commit_seq, ...) -> dict[bytes, bytes]
    write_batch(cat, *, commit_seq, primes_df, decomp_df, staging_dir=None) -> WriteBatchResult

Write path does not go through polars.DataFrame.write_iceberg. PyIceberg 0.11.1
ignores row-group sizing, does not sort on write, and does not pass through
parquet key-value metadata. Instead we:

    sort → chunk at p-boundaries → pa.Table with custom KV → pq.write_table →
    tbl.add_files(...) → catalog commit → pointer file

All of those are stable, fully-configurable APIs.

Schema evolution:

    To add a column to either table:

        1. Bump SCHEMA_VERSION here
        2. Add the NestedField to PRIMES_SCHEMA or DECOMP_SCHEMA with the next
           available field_id (never reuse)
        3. Run a one-off migration:
               tbl = cat.load_table(PRIMES_IDENT)
               with tbl.update_schema() as us:
                   us.add_column("new_col", IntegerType(), required=False)
        4. Old files remain readable; Iceberg fills missing columns with NULL
        5. Update build_file_kv / validators as needed

    Never change field_id assignments or types of existing fields. Renames go
    through update_schema().rename_column(); drops through .delete_column().
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import SortDirection, SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, LongType, NestedField

from . import __version__ as _PACKAGE_VERSION  # noqa: F401  # re-exported via build_file_kv
from .utils import get_config

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 1
NAMESPACE = "funbuns"
PRIMES_IDENT = f"{NAMESPACE}.primes"
DECOMP_IDENT = f"{NAMESPACE}.decompositions"

PRIMES_SCHEMA = Schema(
    NestedField(1, "p", LongType(), required=False),
    NestedField(2, "k", IntegerType(), required=False),
    NestedField(3, "commit_seq", IntegerType(), required=False),
)

DECOMP_SCHEMA = Schema(
    NestedField(1, "p", LongType(), required=False),
    NestedField(2, "m_k", IntegerType(), required=False),
    NestedField(3, "n_k", IntegerType(), required=False),
    NestedField(4, "q_k", LongType(), required=False),
    NestedField(5, "commit_seq", IntegerType(), required=False),
)

PRIMES_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="commit_seq")
)
DECOMP_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=5, field_id=1000, transform=IdentityTransform(), name="commit_seq")
)

PRIMES_SORT_ORDER = SortOrder(
    SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC)
)
DECOMP_SORT_ORDER = SortOrder(
    SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC),
    SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC),
)

TARGET_FILE_SIZE_BYTES = 1 << 30        # 1 GiB
ROW_GROUP_SIZE_BYTES = 128 << 20        # 128 MiB
ROW_GROUP_SIZE_ROWS = 1_048_576         # pyarrow takes rows, not bytes

# Observed in step-1 prototype; conservative rounding
BYTES_PER_ROW_PRIMES = 3
BYTES_PER_ROW_DECOMP = 6

TABLE_PROPERTIES: dict[str, str] = {
    "write.parquet.compression-codec": "zstd",
    "write.parquet.compression-level": "3",
    "write.target-file-size-bytes": str(TARGET_FILE_SIZE_BYTES),
    "funbuns.schema_version": str(SCHEMA_VERSION),
    "funbuns.algorithm_version": _PACKAGE_VERSION if isinstance(_PACKAGE_VERSION, str) else "unknown",
}

PARQUET_WRITER_KWARGS: dict[str, Any] = {
    "compression": "zstd",
    "compression_level": 3,
    "row_group_size": ROW_GROUP_SIZE_ROWS,
    "write_statistics": True,
    "use_dictionary": True,
    "data_page_size": 1 << 20,
}

# ---------------------------------------------------------------------------
# Catalog / filesystem layout
# ---------------------------------------------------------------------------

def get_iceberg_dir() -> Path:
    if env := os.getenv("FUNBUNS_ICEBERG_DIR"):
        return Path(env)
    cfg = get_config()
    if d := cfg.get("iceberg_dir"):
        return Path(d)
    raise RuntimeError("iceberg_dir not configured in pixi.toml [tool.funbuns.directories]")


def get_warehouse_dir() -> Path:
    return get_iceberg_dir() / "warehouse"


HMS_URI_DEFAULT = "thrift://localhost:9083"


def open_catalog(warehouse_root: Path | None = None) -> Catalog:
    """
    Return an Iceberg catalog handle.

    Production (no args): SqlCatalog backed by ``<iceberg_dir>/catalog.db``.
    No dependency on Hive/HMS/k8s — core.py ingest and polars reads work
    directly against the filesystem + sqlite. Run ``scripts/sync_hms.py``
    after a flush to publish the new snapshot to Hive Metastore.

    Set ``FUNBUNS_CATALOG_BACKEND=hive`` to instead open a HiveCatalog at
    ``FUNBUNS_HMS_URI`` (default ``thrift://localhost:9083``) — useful for
    scripts that need to round-trip through HMS itself.

    Temp/test (warehouse_root given): isolated SqlCatalog backed by SQLite
    in the same directory — no HMS dependency for throwaway runs.
    """
    if warehouse_root is not None:
        catalog_db = warehouse_root.parent / "catalog.db"
        warehouse_root.mkdir(parents=True, exist_ok=True)
        return SqlCatalog(
            "funbuns",
            uri=f"sqlite:///{catalog_db}",
            warehouse=f"file://{warehouse_root}",
        )
    warehouse = get_warehouse_dir()
    warehouse.mkdir(parents=True, exist_ok=True)
    backend = os.getenv("FUNBUNS_CATALOG_BACKEND", "sqlite").lower()
    if backend == "hive":
        uri = os.getenv("FUNBUNS_HMS_URI", HMS_URI_DEFAULT)
        return HiveCatalog(
            "funbuns",
            uri=uri,
            warehouse=f"file://{warehouse}",
        )
    catalog_db = get_iceberg_dir() / "catalog.db"
    return SqlCatalog(
        "funbuns",
        uri=f"sqlite:///{catalog_db}",
        warehouse=f"file://{warehouse}",
    )


def _partition_dir(tbl: Table, commit_seq: int) -> Path:
    location = tbl.location().replace("file://", "")
    return Path(location) / "data" / f"commit_seq={commit_seq}"


def ensure_tables(cat: Catalog) -> tuple[Table, Table]:
    cat.create_namespace_if_not_exists(NAMESPACE)
    primes_tbl = cat.create_table_if_not_exists(
        identifier=PRIMES_IDENT,
        schema=PRIMES_SCHEMA,
        partition_spec=PRIMES_PARTITION_SPEC,
        sort_order=PRIMES_SORT_ORDER,
        properties=TABLE_PROPERTIES,
    )
    decomp_tbl = cat.create_table_if_not_exists(
        identifier=DECOMP_IDENT,
        schema=DECOMP_SCHEMA,
        partition_spec=DECOMP_PARTITION_SPEC,
        sort_order=DECOMP_SORT_ORDER,
        properties=TABLE_PROPERTIES,
    )
    return primes_tbl, decomp_tbl


def scan_primes(cat: Catalog | None = None) -> pl.LazyFrame:
    """Lazy scan over ``funbuns.primes`` with predicate pushdown via manifests."""
    cat = cat or open_catalog()
    return pl.scan_iceberg(cat.load_table(PRIMES_IDENT))


def scan_decompositions(cat: Catalog | None = None) -> pl.LazyFrame:
    """Lazy scan over ``funbuns.decompositions`` with predicate pushdown via manifests."""
    cat = cat or open_catalog()
    return pl.scan_iceberg(cat.load_table(DECOMP_IDENT))


def migrate_rename_batch_id_to_commit_seq(cat: Catalog) -> None:
    """
    One-shot schema migration for catalogs created before the rename.

    Renames ``batch_id`` → ``commit_seq`` on both ``funbuns.primes`` and
    ``funbuns.decompositions`` via iceberg's ``update_schema().rename_column``.
    Field-ids are preserved, so historical data files remain readable under the
    new name without rewrite. Safe to run multiple times: if the old name is
    already gone, pyiceberg raises and this function swallows the no-op.

    Call once on the production catalog after upgrading funbuns past the
    rename commit; subsequent opens read the new name transparently.
    """
    for ident in (PRIMES_IDENT, DECOMP_IDENT):
        tbl = cat.load_table(ident)
        if "commit_seq" in tbl.schema().column_names:
            continue
        with tbl.update_schema() as us:
            us.rename_column("batch_id", "commit_seq")


# ---------------------------------------------------------------------------
# KV metadata
# ---------------------------------------------------------------------------

_GIT_SHA_CACHE: str | None = None


def _git_sha() -> str:
    global _GIT_SHA_CACHE
    if _GIT_SHA_CACHE is not None:
        return _GIT_SHA_CACHE
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            cwd=Path(__file__).resolve().parent,
        ).decode().strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        sha = "unknown"
    _GIT_SHA_CACHE = sha
    return sha


def _content_sha256(pa_table: pa.Table) -> str:
    h = hashlib.sha256()
    for col in pa_table.column_names:
        for chunk in pa_table.column(col).chunks:
            h.update(chunk.buffers()[-1] or b"")
    return h.hexdigest()


def build_file_kv(
    pa_table: pa.Table,
    *,
    table_name: str,
    commit_seq: int,
    generator: str = "funbuns-core",
) -> dict[bytes, bytes]:
    """
    Build per-file parquet footer KV metadata for a prepared pa.Table.

    The table must already be in canonical sort order — content_sha256 is
    computed over the in-memory buffers and is reproducible only for identical
    row ordering.
    """
    if table_name not in ("primes", "decompositions"):
        raise ValueError(f"unknown table_name: {table_name}")

    p_col = pa_table.column("p")
    p_min = int(pa.compute.min(p_col).as_py())
    p_max = int(pa.compute.max(p_col).as_py())
    n_rows = pa_table.num_rows
    n_primes = (
        n_rows
        if table_name == "primes"
        else int(pa.compute.count_distinct(p_col).as_py())
    )

    kv: dict[str, str] = {
        "funbuns.schema_version": str(SCHEMA_VERSION),
        "funbuns.algorithm_version": _PACKAGE_VERSION if isinstance(_PACKAGE_VERSION, str) else "unknown",
        "funbuns.algorithm_git_sha": _git_sha(),
        "funbuns.table": table_name,
        "funbuns.commit_seq": str(commit_seq),
        "funbuns.p_min": str(p_min),
        "funbuns.p_max": str(p_max),
        "funbuns.n_rows": str(n_rows),
        "funbuns.n_primes": str(n_primes),
        "funbuns.content_sha256": _content_sha256(pa_table),
        "funbuns.generated_at": datetime.now(timezone.utc).isoformat(),
        "funbuns.generator": generator,
    }

    if table_name == "primes":
        k_hist = (
            pl.from_arrow(pa_table)
            .group_by("k")
            .len()
            .sort("k")
            .to_dict(as_series=False)
        )
        kv["funbuns.k_histogram"] = json.dumps(
            dict(zip(k_hist["k"], k_hist["len"])),
            separators=(",", ":"),
        )

    return {k.encode(): v.encode() for k, v in kv.items()}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class ValidationError(AssertionError):
    pass


def validate_primes_batch(df: pl.DataFrame, *, commit_seq: int) -> None:
    if df.schema != {"p": pl.Int64, "k": pl.Int32, "commit_seq": pl.Int32}:
        raise ValidationError(f"primes schema mismatch: {df.schema}")
    if df.null_count().sum_horizontal().item() != 0:
        raise ValidationError("primes contains nulls")
    if df.height == 0:
        raise ValidationError("primes batch is empty")
    p = df["p"]
    if not p.is_sorted(descending=False):
        raise ValidationError("primes.p not ascending")
    if p.n_unique() != df.height:
        raise ValidationError("primes.p has duplicates")
    if not (df["commit_seq"] == commit_seq).all():
        raise ValidationError(f"primes.commit_seq != {commit_seq}")
    if (df["k"] < 0).any():
        raise ValidationError("primes.k has negative values")


def validate_decomp_batch(df: pl.DataFrame, *, commit_seq: int) -> None:
    expected = {
        "p": pl.Int64,
        "m_k": pl.Int32,
        "n_k": pl.Int32,
        "q_k": pl.Int64,
        "commit_seq": pl.Int32,
    }
    if df.schema != expected:
        raise ValidationError(f"decomp schema mismatch: {df.schema}")
    if df.null_count().sum_horizontal().item() != 0:
        raise ValidationError("decomp contains nulls")
    if df.height == 0:
        raise ValidationError("decomp batch is empty")
    if (df["q_k"] <= 0).any():
        raise ValidationError("decomp.q_k <= 0 (sentinel rows must be filtered)")
    if (df["m_k"] < 1).any() or (df["n_k"] < 1).any():
        raise ValidationError("decomp.m_k or n_k < 1")
    sort_check = df.select(
        (pl.col("p").shift(1).fill_null(-1) < pl.col("p")).alias("p_up"),
        (
            (pl.col("p").shift(1).fill_null(-1) < pl.col("p"))
            | (
                (pl.col("p").shift(1) == pl.col("p"))
                & (pl.col("m_k").shift(1).fill_null(-1) <= pl.col("m_k"))
            )
        ).alias("ok"),
    )
    if not sort_check["ok"].all():
        raise ValidationError("decomp not sorted by (p, m_k)")
    if not (df["commit_seq"] == commit_seq).all():
        raise ValidationError(f"decomp.commit_seq != {commit_seq}")


def validate_cross(primes_df: pl.DataFrame, decomp_df: pl.DataFrame) -> None:
    k_sum = int(primes_df["k"].sum())
    if k_sum != decomp_df.height:
        raise ValidationError(
            f"sum(primes.k)={k_sum} != len(decomp)={decomp_df.height}"
        )
    k_zero_ps = primes_df.filter(pl.col("k") == 0)["p"]
    if k_zero_ps.len() > 0:
        overlap = decomp_df.filter(pl.col("p").is_in(k_zero_ps.implode())).height
        if overlap != 0:
            raise ValidationError(
                f"{overlap} decomp rows reference k=0 primes"
            )
    missing = decomp_df.join(primes_df.select("p"), on="p", how="anti")
    if missing.height > 0:
        raise ValidationError(
            f"{missing.height} decomp rows reference primes absent from primes batch"
        )


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------

@dataclass
class WriteBatchResult:
    commit_seq: int
    primes_files: list[Path]
    decomp_files: list[Path]
    primes_rows: int
    decomp_rows: int
    primes_snapshot_id: int | None
    decomp_snapshot_id: int | None
    pointer_files: list[Path] = field(default_factory=list)


def _to_iceberg_arrow(df: pl.DataFrame, schema: Schema) -> pa.Table:
    target = schema_to_pyarrow(schema)
    tbl = df.to_arrow()
    return tbl.cast(target)


def _chunk_row_boundaries(
    n_rows: int,
    target_rows: int,
    *,
    p_col: pa.ChunkedArray | None = None,
) -> list[tuple[int, int]]:
    """
    Return [(start, end), ...] row-index pairs splitting a table into chunks of
    approximately target_rows each. If p_col is provided, chunk boundaries are
    snapped forward to the next p-change so no p value straddles two chunks.
    """
    if n_rows <= target_rows:
        return [(0, n_rows)]

    bounds: list[tuple[int, int]] = []
    start = 0
    if p_col is not None:
        p_list = p_col.to_pylist()
        while start < n_rows:
            end = min(start + target_rows, n_rows)
            while end < n_rows and p_list[end] == p_list[end - 1]:
                end += 1
            bounds.append((start, end))
            start = end
    else:
        while start < n_rows:
            end = min(start + target_rows, n_rows)
            bounds.append((start, end))
            start = end
    return bounds


def _write_chunks(
    pa_table: pa.Table,
    *,
    table_name: str,
    commit_seq: int,
    partition_dir: Path,
    target_rows: int,
    snap_to_p_boundary: bool,
) -> list[Path]:
    partition_dir.mkdir(parents=True, exist_ok=True)
    p_col = pa_table.column("p") if snap_to_p_boundary else None
    bounds = _chunk_row_boundaries(pa_table.num_rows, target_rows, p_col=p_col)
    tmp_paths: list[Path] = []
    final_paths: list[Path] = []
    try:
        for i, (start, end) in enumerate(bounds):
            chunk = pa_table.slice(start, end - start)
            kv = build_file_kv(chunk, table_name=table_name, commit_seq=commit_seq)
            merged = dict(chunk.schema.metadata or {})
            merged.update(kv)
            chunk = chunk.replace_schema_metadata(merged)
            name = f"{table_name}_b{commit_seq:06d}_{i:03d}.parquet"
            final = partition_dir / name
            tmp = partition_dir / f".{name}.tmp"
            pq.write_table(chunk, tmp, **PARQUET_WRITER_KWARGS)
            tmp_paths.append(tmp)
            final_paths.append(final)
    except Exception:
        for t in tmp_paths:
            if t.exists():
                t.unlink()
        raise
    for tmp, final in zip(tmp_paths, final_paths):
        os.replace(tmp, final)
    return final_paths


def _write_pointer(tbl: Table) -> Path:
    location = tbl.location().replace("file://", "")
    meta_dir = Path(location) / "metadata"
    latest = sorted(meta_dir.glob("*.metadata.json"))[-1]
    pointer = meta_dir / "current.metadata.json.txt"
    pointer.write_text(latest.name + "\n")
    return pointer


def shape_for_write(
    raw: pl.DataFrame,
    *,
    commit_seq: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Transform a raw partition frame {p, m_k, n_k, q_k} into the (primes, decomp)
    pair that write_batch expects. Deduplicates on the full row key, filters
    sentinel rows (q_k == 0) out of decomp, and derives per-prime k counts for
    the primes table. The raw frame's primes that are entirely obstructed still
    land in primes with k=0.
    """
    src = raw.unique(subset=["p", "m_k", "n_k", "q_k"])
    decomp = (
        src.filter(pl.col("q_k") > 0)
        .with_columns(
            pl.col("p").cast(pl.Int64),
            pl.col("m_k").cast(pl.Int32),
            pl.col("n_k").cast(pl.Int32),
            pl.col("q_k").cast(pl.Int64),
            pl.lit(commit_seq, dtype=pl.Int32).alias("commit_seq"),
        )
        .select(["p", "m_k", "n_k", "q_k", "commit_seq"])
    )
    primes = (
        src.group_by("p")
        .agg((pl.col("q_k") > 0).sum().cast(pl.Int32).alias("k"))
        .with_columns(
            pl.col("p").cast(pl.Int64),
            pl.lit(commit_seq, dtype=pl.Int32).alias("commit_seq"),
        )
        .select(["p", "k", "commit_seq"])
    )
    return primes, decomp


class IcebergWriter:
    """
    Live-path writer: owns a catalog handle and a monotonic commit_seq counter,
    initialized from existing iceberg manifest state. Each flush() call shapes
    a raw partition frame, commits a new batch, and advances the counter.

    Intended to be constructed once per process and passed as a bound-method
    callback (writer.flush) into PPConsumer.

    Attributes:
        cat: active Catalog handle.
        next_commit_seq: next commit_seq to be assigned on flush().
        resume_p: max prime already committed, or 0 if the tables are empty.
    """

    def __init__(self, cat: Catalog | None = None):
        self.cat = cat if cat is not None else open_catalog()
        ensure_tables(self.cat)
        self.next_commit_seq, self.resume_p = self._init_state()

    def _init_state(self) -> tuple[int, int]:
        try:
            tbl = self.cat.load_table(PRIMES_IDENT)
        except Exception:
            return 0, 0
        if tbl.current_snapshot() is None:
            return 0, 0
        files = pl.from_arrow(tbl.inspect.files().select(["readable_metrics"]))
        if files.height == 0:
            return 0, 0
        bounds = files.select(
            pl.col("readable_metrics")
            .struct.field("commit_seq")
            .struct.field("upper_bound")
            .max()
            .alias("max_seq"),
            pl.col("readable_metrics")
            .struct.field("p")
            .struct.field("upper_bound")
            .max()
            .alias("max_p"),
        )
        max_seq = bounds["max_seq"].item()
        max_p = bounds["max_p"].item()
        if max_seq is None or max_p is None:
            return 0, 0
        return int(max_seq) + 1, int(max_p)

    def flush(self, raw_df: pl.DataFrame) -> WriteBatchResult:
        commit_seq = self.next_commit_seq
        primes_df, decomp_df = shape_for_write(raw_df, commit_seq=commit_seq)
        result = write_batch(
            self.cat,
            commit_seq=commit_seq,
            primes_df=primes_df,
            decomp_df=decomp_df,
        )
        self.next_commit_seq += 1
        return result


def write_batch(
    cat: Catalog,
    *,
    commit_seq: int,
    primes_df: pl.DataFrame,
    decomp_df: pl.DataFrame,
) -> WriteBatchResult:
    """
    Validate, sort, chunk, write, and commit a single (primes, decompositions)
    batch directly into the canonical warehouse partition paths.

    primes_df and decomp_df must have the exact dtypes the validators expect
    (see validate_primes_batch / validate_decomp_batch). write_batch sorts
    them but does not reshape or filter.

    Files are written as `.<name>.parquet.tmp` in the canonical partition dir,
    then atomically renamed into place before add_files is called. If any
    chunk write fails, tmp files for that table are cleaned up. Orphaned
    post-rename files from a failure between rename and commit can be
    identified later by comparing warehouse contents to manifest entries.
    """
    primes_df = primes_df.sort("p")
    decomp_df = decomp_df.sort(["p", "m_k"])
    validate_primes_batch(primes_df, commit_seq=commit_seq)
    validate_decomp_batch(decomp_df, commit_seq=commit_seq)
    validate_cross(primes_df, decomp_df)

    primes_tbl = cat.load_table(PRIMES_IDENT)
    decomp_tbl = cat.load_table(DECOMP_IDENT)

    primes_at = _to_iceberg_arrow(primes_df, PRIMES_SCHEMA)
    decomp_at = _to_iceberg_arrow(decomp_df, DECOMP_SCHEMA)

    primes_target = TARGET_FILE_SIZE_BYTES // BYTES_PER_ROW_PRIMES
    decomp_target = TARGET_FILE_SIZE_BYTES // BYTES_PER_ROW_DECOMP

    primes_files = _write_chunks(
        primes_at,
        table_name="primes",
        commit_seq=commit_seq,
        partition_dir=_partition_dir(primes_tbl, commit_seq),
        target_rows=primes_target,
        snap_to_p_boundary=False,
    )
    decomp_files = _write_chunks(
        decomp_at,
        table_name="decompositions",
        commit_seq=commit_seq,
        partition_dir=_partition_dir(decomp_tbl, commit_seq),
        target_rows=decomp_target,
        snap_to_p_boundary=True,
    )

    primes_tbl.add_files([str(p) for p in primes_files])
    primes_tbl = cat.load_table(PRIMES_IDENT)

    decomp_tbl.add_files([str(p) for p in decomp_files])
    decomp_tbl = cat.load_table(DECOMP_IDENT)

    pointer_files = [_write_pointer(primes_tbl), _write_pointer(decomp_tbl)]

    return WriteBatchResult(
        commit_seq=commit_seq,
        primes_files=primes_files,
        decomp_files=decomp_files,
        primes_rows=primes_df.height,
        decomp_rows=decomp_df.height,
        primes_snapshot_id=primes_tbl.current_snapshot().snapshot_id if primes_tbl.current_snapshot() else None,
        decomp_snapshot_id=decomp_tbl.current_snapshot().snapshot_id if decomp_tbl.current_snapshot() else None,
        pointer_files=pointer_files,
    )
