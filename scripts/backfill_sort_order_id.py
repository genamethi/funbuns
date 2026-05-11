"""
One-shot backfill: stamp ``default_sort_order_id`` on every DataFile
manifest entry for tables whose existing files were registered under the
buggy pyiceberg 0.11.1 ``parquet_file_to_data_file`` helper (which
hardcodes ``sort_order_id=None``).

Produces a single new REPLACE snapshot per table. Data parquet files are
not touched; only manifests are rewritten (new avro paths, preserving
immutability of the originals).

Usage:
    pixi run python scripts/backfill_sort_order_id.py --uri sqlite:///... --warehouse file://... [--table funbuns.primes] [--dry-run]
"""
from __future__ import annotations

import argparse
import uuid

from funbuns import iceberg_schema  # noqa: F401 — applies _patches via iceberg_schema

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import (
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    write_manifest,
)
from pyiceberg.table.update.snapshot import _SnapshotProducer
from pyiceberg.table.snapshots import Operation


class _SortOrderBackfill(_SnapshotProducer["_SortOrderBackfill"]):
    """Rewrite every live manifest so that each DataFile carries the
    table's ``default_sort_order_id``. Commits as a REPLACE snapshot.

    No data files are added or deleted; no parquet data is rewritten.
    """

    def _deleted_entries(self) -> list[ManifestEntry]:
        return []

    def _existing_manifests(self) -> list[ManifestFile]:
        sort_order_id = self._transaction.table_metadata.default_sort_order_id
        snapshot = self._transaction.table_metadata.snapshot_by_name(name=self._target_branch)
        if snapshot is None:
            return []

        new_manifests: list[ManifestFile] = []
        for manifest_file in snapshot.manifests(io=self._io):
            spec = self._transaction.table_metadata.specs()[manifest_file.partition_spec_id]
            with write_manifest(
                format_version=self._transaction.table_metadata.format_version,
                spec=spec,
                schema=self._transaction.table_metadata.schema(),
                output_file=self.new_manifest_output(),
                snapshot_id=self._snapshot_id,
                avro_compression=self._compression,
            ) as writer:
                for entry in manifest_file.fetch_manifest_entry(io=self._io, discard_deleted=True):
                    entry.data_file._data[15] = sort_order_id
                    writer.add_entry(
                        ManifestEntry.from_args(
                            status=ManifestEntryStatus.EXISTING,
                            snapshot_id=entry.snapshot_id,
                            sequence_number=entry.sequence_number,
                            file_sequence_number=entry.file_sequence_number,
                            data_file=entry.data_file,
                        )
                    )
            new_manifests.append(writer.to_manifest_file())
        return new_manifests


def backfill_table(cat: SqlCatalog, name: str, dry_run: bool) -> None:
    tbl = cat.load_table(name)
    sort_order_id = tbl.metadata.default_sort_order_id
    files_before = tbl.inspect.files().select(["file_path", "sort_order_id"])
    null_before = sum(1 for v in files_before["sort_order_id"].to_pylist() if v is None)
    total_before = files_before.num_rows
    print(f"[{name}] default_sort_order_id={sort_order_id}  "
          f"files={total_before}  null_sort_order_id={null_before}")

    if null_before == 0:
        print(f"[{name}] nothing to do — all entries already stamped")
        return

    if dry_run:
        print(f"[{name}] dry-run: would commit REPLACE snapshot stamping "
              f"{null_before} entries")
        return

    # pyiceberg 0.11 only implements summary updates for APPEND/OVERWRITE/DELETE
    # so we report the manifest rewrite under OVERWRITE with 0 added/0 removed.
    with tbl.transaction() as tx:
        producer = _SortOrderBackfill(
            operation=Operation.OVERWRITE,
            transaction=tx,
            io=tbl.io,
            commit_uuid=uuid.uuid4(),
        )
        updates, requirements = producer._commit()
        tx._updates += updates
        tx._requirements += requirements

    # reload and verify
    tbl = cat.load_table(name)
    files_after = tbl.inspect.files().select(["file_path", "sort_order_id"])
    null_after = sum(1 for v in files_after["sort_order_id"].to_pylist() if v is None)
    total_after = files_after.num_rows
    print(f"[{name}] after commit: files={total_after}  "
          f"null_sort_order_id={null_after}")
    if null_after != 0:
        raise RuntimeError(f"[{name}] backfill failed: {null_after} nulls remain")
    if total_after != total_before:
        raise RuntimeError(f"[{name}] file count changed: {total_before} -> {total_after}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True, help="SQLite URI, e.g. sqlite:////path/catalog.db")
    ap.add_argument("--warehouse", required=True, help="file:// warehouse root")
    ap.add_argument("--catalog-name", default="funbuns")
    ap.add_argument("--table", action="append", default=None,
                    help="Fully-qualified table (repeatable). Default: both funbuns.primes and funbuns.decompositions")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cat = SqlCatalog(args.catalog_name, uri=args.uri, warehouse=args.warehouse)
    tables = args.table or ["funbuns.primes", "funbuns.decompositions"]
    for name in tables:
        backfill_table(cat, name, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
