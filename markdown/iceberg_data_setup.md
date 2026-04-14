# Iceberg data setup

This document describes the canonical data layout for the funbuns dataset
after the legacy `blocks/` glob was retired. It is narrowly focused on the
**write-side** schema and filesystem layout — things that are stable and
will not change as downstream consumers are retargeted off DuckDB.

Ground truth lives in `src/funbuns/iceberg_schema.py`. This doc summarizes;
the module is authoritative.

---

## Tables

The dataset is expressed as two Apache Iceberg tables under the namespace
`funbuns`:

| Table                    | Rows                           | Purpose                                    |
|--------------------------|--------------------------------|--------------------------------------------|
| `funbuns.primes`         | one per prime `p`, incl. `k=0` | universe of primes ≤ current max           |
| `funbuns.decompositions` | one per `(p, m_k, n_k, q_k)`   | fact table; non-obstructed partitions only |

Every prime `p` that has been processed appears in `primes` exactly once.
Obstructed primes (no decomposition exists) are stored with `k=0` and do
**not** appear in `decompositions`. The row in `primes` is the marker.

Current state (2026-04-14 post-migration):

- 504 batches (batch_ids 0–503)
- 10,098,850,000 rows in `primes`
- 19,009,703,663 rows in `decompositions`
- `k_max = 16`
- `sum(primes.k) == rows(decompositions)` — the key cross-table invariant

## Schemas

**`funbuns.primes`** (field_ids fixed; never reuse):

| field_id | name       | type    | notes                       |
|----------|------------|---------|-----------------------------|
| 1        | `p`        | Long    | prime                       |
| 2        | `k`        | Integer | # decompositions; 0 = obstructed |
| 3        | `batch_id` | Integer | partition key               |

**`funbuns.decompositions`**:

| field_id | name       | type    | notes                       |
|----------|------------|---------|-----------------------------|
| 1        | `p`        | Long    | prime                       |
| 2        | `m_k`      | Integer | exponent on 2, ≥ 1          |
| 3        | `n_k`      | Integer | exponent on q, ≥ 1          |
| 4        | `q_k`      | Long    | odd prime base, > 0         |
| 5        | `batch_id` | Integer | partition key               |

All fields are non-required at the iceberg schema level but the writer
enforces non-null at validation time. `q_k == 0` sentinel rows (legacy
obstruction marker) are filtered out of `decompositions` — their
information is captured by `primes.k == 0` instead.

## Sort order

- `primes`: `p ASC`
- `decompositions`: `(p ASC, m_k ASC)`

Sort order is **declared** on the table but PyIceberg 0.11.1 does not
enforce it on append. The writer owns the invariant: `write_batch()`
sorts before the parquet write and validators reject unsorted batches.

## Partitioning

Both tables use identity partitioning on `batch_id`:

```
funbuns/primes/data/batch_id=0/primes_b000000_000.parquet
funbuns/primes/data/batch_id=1/primes_b000001_000.parquet
...
funbuns/decompositions/data/batch_id=0/decompositions_b000000_000.parquet
...
```

A batch is the atomic unit of ingest. The compactor currently targets
~20M primes per batch (≈40M decomposition rows); this is a runtime flag,
not a schema property.

## Filesystem layout

```
/media/extssd/research/dioph.pp/data/iceberg/
├── catalog.db                          # SqlCatalog SQLite state
└── warehouse/
    └── funbuns/
        ├── primes/
        │   ├── data/batch_id=N/*.parquet
        │   └── metadata/
        │       ├── NNNNN-<uuid>.metadata.json
        │       ├── <uuid>-m0.avro      # manifests
        │       ├── snap-<snap-id>-*.avro
        │       └── current.metadata.json.txt
        └── decompositions/
            ├── data/batch_id=N/*.parquet
            └── metadata/
                └── ... (same shape)
```

**Catalog**: `SqlCatalog` backed by SQLite at `catalog.db`. The catalog
is the source of truth for the current metadata.json path.

**Pointer file**: `current.metadata.json.txt` contains the filename
(relative to `metadata/`) of the live metadata.json. Written after every
`write_batch()` commit. Exists so that non-Python readers (e.g. a future
Rust consumer via `iceberg-rust::StaticTable`) can resolve the current
snapshot without running PyIceberg.

## Table properties

Declared in `TABLE_PROPERTIES` and propagated into every `metadata.json`:

```
write.parquet.compression-codec    = zstd
write.parquet.compression-level    = 3
write.target-file-size-bytes       = 1073741824   (1 GiB)
funbuns.schema_version             = 1
funbuns.algorithm_version          = <package version>
```

Note: PyIceberg 0.11.1 **silently ignores** `write.parquet.row-group-size-bytes`
and bloom filter properties. Row-group sizing is controlled directly by
`PARQUET_WRITER_KWARGS` in `iceberg_schema.py`, not through iceberg
properties.

## Parquet writer configuration

Not routed through PyIceberg's writer. `write_batch()` uses
`pyarrow.parquet.write_table` directly with:

```
compression      = zstd
compression_level= 3
row_group_size   = 1_048_576 rows
data_page_size   = 1 MiB
write_statistics = True
use_dictionary   = True
```

Files are written as `.<name>.parquet.tmp` in the canonical partition dir,
then atomically renamed before `add_files` is called.

## Per-file KV metadata

Every parquet file carries a `funbuns.*` key-value block in its footer,
built by `build_file_kv()` and preserved through the `add_files` path:

| Key                          | Meaning                                         |
|------------------------------|-------------------------------------------------|
| `funbuns.schema_version`     | matches `SCHEMA_VERSION` constant               |
| `funbuns.algorithm_version`  | funbuns package version at write time          |
| `funbuns.algorithm_git_sha`  | short git SHA of the writer                     |
| `funbuns.table`              | `primes` or `decompositions`                    |
| `funbuns.batch_id`           | matches the partition directory                 |
| `funbuns.p_min`, `funbuns.p_max` | p range in this file                        |
| `funbuns.n_rows`             | row count                                       |
| `funbuns.n_primes`           | distinct primes (== n_rows for `primes`)        |
| `funbuns.content_sha256`     | sha256 over column buffers, order-sensitive     |
| `funbuns.generated_at`       | ISO-8601 UTC timestamp                          |
| `funbuns.generator`          | writer name (default `funbuns-core`)            |
| `funbuns.k_histogram`        | JSON `{k: count}` — **primes only**             |

This is a third tier of metadata on top of iceberg's own per-file
manifest statistics (min/max/null counts). Manifest stats cover
predicate pushdown; the KV block carries provenance and domain-specific
summaries.

## Write path

`write_batch(cat, batch_id, primes_df, decomp_df)` is the single entry
point for producers. Flow:

```
sort  → validate → to_arrow → chunk at p-boundaries
      → build KV → pq.write_table (tmp)
      → atomic rename
      → tbl.add_files([...])   # commits an iceberg snapshot
      → reload table
      → write current.metadata.json.txt pointer
```

Chunking within a batch is driven by `TARGET_FILE_SIZE_BYTES / bytes_per_row`
estimates. For `decompositions`, chunk boundaries are snapped forward so
that no single `p` value straddles two files.

## Validation invariants

Enforced before commit. A failure raises `ValidationError` and aborts
without writing anything committable:

**`primes` batch:**
- Schema `{p: Int64, k: Int32, batch_id: Int32}`
- No nulls
- Non-empty
- `p` strictly ascending, no duplicates
- `k ≥ 0`
- `batch_id` column matches the batch_id parameter

**`decompositions` batch:**
- Schema `{p: Int64, m_k: Int32, n_k: Int32, q_k: Int64, batch_id: Int32}`
- No nulls
- Non-empty
- `q_k > 0` (no sentinel rows)
- `m_k ≥ 1`, `n_k ≥ 1`
- Sorted by `(p, m_k)` ascending
- `batch_id` column matches the batch_id parameter

**Cross-table:**
- `sum(primes.k) == rows(decompositions)`
- No `decompositions` rows reference a `primes.p` with `k == 0`
- Every `decompositions.p` appears in `primes.p` (anti-join is empty)

## Reading

### Python (polars)

```python
import polars as pl

primes = pl.scan_iceberg(
    "/media/extssd/research/dioph.pp/data/iceberg/warehouse/funbuns/primes"
)
decomp = pl.scan_iceberg(
    "/media/extssd/research/dioph.pp/data/iceberg/warehouse/funbuns/decompositions"
)

# Example: count obstructed primes below 10^9
(
    primes
    .filter(pl.col("p") < 1_000_000_000)
    .filter(pl.col("k") == 0)
    .select(pl.len())
    .collect()
)
```

Predicate pushdown via manifest stats happens transparently; `p` range
filters only open the files whose min/max overlap the range.

### Python (PyIceberg, for metadata inspection)

```python
from pyiceberg.catalog.sql import SqlCatalog

cat = SqlCatalog(
    "funbuns",
    uri="sqlite:////media/extssd/research/dioph.pp/data/iceberg/catalog.db",
    warehouse="file:///media/extssd/research/dioph.pp/data/iceberg/warehouse",
)
tbl = cat.load_table("funbuns.primes")
tbl.inspect.files()          # per-file stats, metadata-only, no data reads
tbl.inspect.snapshots()
tbl.current_snapshot()
```

### Rust / other

Consumers that can't run PyIceberg should:

1. Read `metadata/current.metadata.json.txt` (single filename line).
2. Open the indicated `*.metadata.json`.
3. Walk its current snapshot's manifest list for the data file paths.

The `iceberg-rust` crate's `StaticTable::from_metadata_file` does exactly
this. Filesystem globbing `data/batch_id=N/*.parquet` in batch_id order
also works in the current write-once-no-compaction regime but is not
safe against future manifest rewrites.

## Compactor

`scripts/iceberg_compactor.py` is a one-shot tool that read legacy
`blocks/*.parquet` in true p-order, deduplicated them
(`unique(subset=["p", "m_k", "n_k", "q_k"])`), and ran them through
`write_batch` in ~20M-prime batches. The legacy blocks had overlapping
adjacent pairs (workflow artifact, not corruption), so dedup is a
correctness requirement, not an optimization.

The compactor is no longer needed for ongoing ingest once step 7 of the
migration plan (direct iceberg writes from `core.py`) is complete. It is
kept as a reference for bulk rebuild scenarios.

## Schema evolution

To add a column without breaking existing files:

1. Bump `SCHEMA_VERSION` in `iceberg_schema.py`.
2. Add the field to the relevant `Schema` with the next available
   `field_id`. **Never reuse or reassign field_ids.**
3. Run a one-off migration:
   ```python
   with tbl.update_schema() as us:
       us.add_column("new_col", IntegerType(), required=False)
   ```
4. Old files remain readable — Iceberg fills missing columns with NULL.
5. Update `build_file_kv` and validators as needed.

Renames go through `update_schema().rename_column()`; drops through
`delete_column()`. Never change field types of existing fields.

## Environment

- `FUNBUNS_ICEBERG_DIR` — overrides `[tool.funbuns.directories].iceberg_dir`
  for scratch/test runs.
- Warehouse location otherwise read from `pixi.toml
  [tool.funbuns.directories] iceberg_dir = "/media/extssd/.../iceberg"`.
