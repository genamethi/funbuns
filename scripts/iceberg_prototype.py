"""
Step-1 Iceberg prototype.

Writes a single legacy block file into a throwaway Iceberg table pair
(funbuns.primes, funbuns.decompositions) and captures answers to the
plumbing questions Q1-Q8 from the architect plan.

Nothing here is load-bearing; the entire scratch directory is disposable.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortDirection, SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, LongType, NestedField

SCRATCH_ROOT = Path("/media/extssd/research/dioph.pp/data/iceberg_scratch")
WAREHOUSE = SCRATCH_ROOT / "warehouse"
CATALOG_DB = SCRATCH_ROOT / "catalog.db"
SOURCE_BLOCK = Path(
    "/media/extssd/research/dioph.pp/data/blocks/pp_b001_p7368791.parquet"
)

PRIMES_SCHEMA = Schema(
    NestedField(1, "p", LongType(), required=False),
    NestedField(2, "k", IntegerType(), required=False),
    NestedField(3, "batch_id", IntegerType(), required=False),
)

DECOMP_SCHEMA = Schema(
    NestedField(1, "p", LongType(), required=False),
    NestedField(2, "m_k", IntegerType(), required=False),
    NestedField(3, "n_k", IntegerType(), required=False),
    NestedField(4, "q_k", LongType(), required=False),
    NestedField(5, "batch_id", IntegerType(), required=False),
)

PARTITION_SPEC_PRIMES = PartitionSpec(
    PartitionField(
        source_id=3, field_id=1000, transform=IdentityTransform(), name="batch_id"
    )
)
PARTITION_SPEC_DECOMP = PartitionSpec(
    PartitionField(
        source_id=5, field_id=1000, transform=IdentityTransform(), name="batch_id"
    )
)

SORT_ORDER_PRIMES = SortOrder(
    SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC)
)
SORT_ORDER_DECOMP = SortOrder(
    SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC),
    SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC),
)

TABLE_PROPERTIES = {
    "write.target-file-size-bytes": str(1024 * 1024 * 1024),
    "write.parquet.row-group-size-bytes": str(128 * 1024 * 1024),
    "write.parquet.compression-codec": "zstd",
    "write.parquet.compression-level": "3",
    "funbuns.schema_version": "1",
}


def banner(msg: str) -> None:
    print(f"\n{'=' * 8} {msg} {'=' * 8}")


def reset_scratch() -> None:
    banner("reset scratch")
    for p in (WAREHOUSE, CATALOG_DB):
        if p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()
    WAREHOUSE.mkdir(parents=True, exist_ok=True)


def make_catalog() -> SqlCatalog:
    banner("init SqlCatalog")
    cat = SqlCatalog(
        "funbuns_scratch",
        **{
            "uri": f"sqlite:///{CATALOG_DB}",
            "warehouse": f"file://{WAREHOUSE}",
        },
    )
    cat.create_namespace_if_not_exists("funbuns")
    return cat


def create_tables(cat: SqlCatalog):
    banner("create tables")
    primes_tbl = cat.create_table_if_not_exists(
        identifier="funbuns.primes",
        schema=PRIMES_SCHEMA,
        partition_spec=PARTITION_SPEC_PRIMES,
        sort_order=SORT_ORDER_PRIMES,
        properties=TABLE_PROPERTIES,
    )
    decomp_tbl = cat.create_table_if_not_exists(
        identifier="funbuns.decompositions",
        schema=DECOMP_SCHEMA,
        partition_spec=PARTITION_SPEC_DECOMP,
        sort_order=SORT_ORDER_DECOMP,
        properties=TABLE_PROPERTIES,
    )
    print("primes location:", primes_tbl.location())
    print("decomp location:", decomp_tbl.location())
    return primes_tbl, decomp_tbl


def build_frames(batch_id: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    banner(f"load source block: {SOURCE_BLOCK.name}")
    src = pl.read_parquet(SOURCE_BLOCK)
    print("source schema:", src.schema)
    print("source rows:", src.height)
    print(
        "source p range:",
        src["p"].min(),
        "..",
        src["p"].max(),
    )

    decomp = (
        src.filter(pl.col("q_k") > 0)
        .with_columns(
            pl.col("p").cast(pl.Int64),
            pl.col("m_k").cast(pl.Int32),
            pl.col("n_k").cast(pl.Int32),
            pl.col("q_k").cast(pl.Int64),
            pl.lit(batch_id, dtype=pl.Int32).alias("batch_id"),
        )
        .sort(["p", "m_k"])
        .select(["p", "m_k", "n_k", "q_k", "batch_id"])
    )

    k_per_p = src.group_by("p").agg(
        (pl.col("q_k") > 0).sum().cast(pl.Int32).alias("k")
    )
    primes = (
        k_per_p.with_columns(
            pl.col("p").cast(pl.Int64),
            pl.lit(batch_id, dtype=pl.Int32).alias("batch_id"),
        )
        .sort("p")
        .select(["p", "k", "batch_id"])
    )

    print("primes rows:", primes.height)
    print("decomp rows:", decomp.height)
    print("k histogram:", primes.group_by("k").len().sort("k").to_dicts())
    return primes, decomp


def write_tables(primes_tbl, decomp_tbl, primes_df, decomp_df) -> None:
    banner("Polars write_iceberg (primes)")
    primes_df.write_iceberg(primes_tbl, mode="append")
    banner("Polars write_iceberg (decompositions)")
    decomp_df.write_iceberg(decomp_tbl, mode="append")


def roundtrip_validate(primes_tbl, decomp_tbl, primes_df, decomp_df) -> None:
    banner("roundtrip validate via scan_iceberg")
    pr = pl.scan_iceberg(primes_tbl).collect()
    dc = pl.scan_iceberg(decomp_tbl).collect()
    print("primes roundtrip rows:", pr.height, "src:", primes_df.height)
    print("decomp roundtrip rows:", dc.height, "src:", decomp_df.height)
    assert pr.height == primes_df.height
    assert dc.height == decomp_df.height
    src_sum = int(primes_df["k"].sum())
    rt_sum = int(pr["k"].sum())
    print("k sum src:", src_sum, "roundtrip:", rt_sum)
    assert src_sum == rt_sum
    print("OK: row counts and k sums match")


def inspect_layout() -> None:
    banner("on-disk layout")
    for f in sorted(WAREHOUSE.rglob("*")):
        if f.is_file():
            rel = f.relative_to(WAREHOUSE)
            size = f.stat().st_size
            print(f"  {size:>12}  {rel}")


def inspect_parquet_footers() -> None:
    banner("parquet footer probe")
    for f in sorted(WAREHOUSE.rglob("*.parquet")):
        rel = f.relative_to(WAREHOUSE)
        pqf = pq.ParquetFile(f)
        md = pqf.metadata
        print(f"\n-- {rel}")
        print(f"   num_rows={md.num_rows}  num_row_groups={md.num_row_groups}")
        print(f"   created_by={md.created_by}")
        rg0 = md.row_group(0)
        print(f"   rg0 total_byte_size={rg0.total_byte_size}")
        for c in range(rg0.num_columns):
            col = rg0.column(c)
            print(
                f"     col[{c}] {col.path_in_schema}: "
                f"encodings={col.encodings} "
                f"codec={col.compression} "
                f"size={col.total_compressed_size}"
            )
        arrow_schema = pqf.schema_arrow
        print(f"   arrow schema field names: {arrow_schema.names}")
        kv = md.metadata or {}
        kv_decoded = {
            k.decode(): (v.decode() if len(v) < 200 else f"<{len(v)}B>")
            for k, v in kv.items()
        }
        print(f"   kv_metadata keys: {list(kv_decoded.keys())}")
        if "iceberg.schema" in kv_decoded:
            print(f"   iceberg.schema present ({len(kv['iceberg.schema'.encode()])}B)")
        if any(k.startswith("funbuns.") for k in kv_decoded):
            print("   *** funbuns.* KV present ***")
        else:
            print("   (no funbuns.* KV — Q5 negative)")
        print(
            f"   sorting_columns: "
            f"{md.row_group(0).sorting_columns if hasattr(md.row_group(0), 'sorting_columns') else 'n/a'}"
        )


def inspect_metadata_json() -> None:
    banner("iceberg metadata.json")
    for meta_dir in sorted(WAREHOUSE.rglob("metadata")):
        print(f"\n-- {meta_dir.relative_to(WAREHOUSE)}")
        for f in sorted(meta_dir.iterdir()):
            print(f"   {f.stat().st_size:>10}  {f.name}")
        latest = sorted(meta_dir.glob("*.metadata.json"))
        if latest:
            with open(latest[-1]) as fh:
                meta = json.load(fh)
            print(f"   latest: {latest[-1].name}")
            print(f"   format-version: {meta.get('format-version')}")
            print(f"   current-snapshot-id: {meta.get('current-snapshot-id')}")
            print(f"   properties: {meta.get('properties')}")
            print(
                f"   sort-orders: {json.dumps(meta.get('sort-orders'), default=str)[:200]}"
            )
            print(
                f"   partition-specs: {json.dumps(meta.get('partition-specs'), default=str)[:200]}"
            )


def main() -> int:
    if not SOURCE_BLOCK.exists():
        print(f"source block missing: {SOURCE_BLOCK}", file=sys.stderr)
        return 1
    reset_scratch()
    cat = make_catalog()
    primes_tbl, decomp_tbl = create_tables(cat)
    primes_df, decomp_df = build_frames(batch_id=0)
    write_tables(primes_tbl, decomp_tbl, primes_df, decomp_df)
    # Reload tables from catalog to pick up the new snapshot before scanning.
    primes_tbl = cat.load_table("funbuns.primes")
    decomp_tbl = cat.load_table("funbuns.decompositions")
    roundtrip_validate(primes_tbl, decomp_tbl, primes_df, decomp_df)
    inspect_layout()
    inspect_parquet_footers()
    inspect_metadata_json()
    return 0


if __name__ == "__main__":
    sys.exit(main())
