"""
One-shot: migrate the funbuns namespace from SqlCatalog (SQLite) to
HiveCatalog (HMS). No data is moved — each table's current metadata.json
location is read from the SQLite catalog and registered as-is in HMS.

Preconditions:
    - HMS is running and reachable at FUNBUNS_HMS_URI (or the default
      thrift://localhost:9083). Start with `pixi run hive`.
    - catalog.db still exists at <iceberg_dir>/catalog.db.

After success, the HiveCatalog contains funbuns.primes and
funbuns.decompositions pointing at the same warehouse files the SQLite
catalog was tracking. The SQLite catalog.db can then be archived.

Usage:
    pixi run python scripts/register_in_hms.py
"""

from __future__ import annotations

import os
from pathlib import Path

from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import TableAlreadyExistsError

from funbuns.iceberg_schema import (
    DECOMP_IDENT,
    HMS_URI_DEFAULT,
    NAMESPACE,
    PRIMES_IDENT,
    get_iceberg_dir,
    get_warehouse_dir,
)


def _open_sqlite_source() -> SqlCatalog:
    iceberg_root = get_iceberg_dir()
    catalog_db = iceberg_root / "catalog.db"
    if not catalog_db.exists():
        raise FileNotFoundError(f"no SQLite catalog at {catalog_db}")
    return SqlCatalog(
        "funbuns",
        uri=f"sqlite:///{catalog_db}",
        warehouse=f"file://{iceberg_root / 'warehouse'}",
    )


def _open_hive_target() -> HiveCatalog:
    uri = os.getenv("FUNBUNS_HMS_URI", HMS_URI_DEFAULT)
    return HiveCatalog(
        "funbuns",
        uri=uri,
        warehouse=f"file://{get_warehouse_dir()}",
    )


def main() -> None:
    src = _open_sqlite_source()
    dst = _open_hive_target()

    dst.create_namespace_if_not_exists(NAMESPACE)

    for ident in (PRIMES_IDENT, DECOMP_IDENT):
        src_tbl = src.load_table(ident)
        meta_loc = src_tbl.metadata_location
        print(f"{ident}")
        print(f"  metadata: {meta_loc}")
        try:
            dst.register_table(ident, meta_loc)
            print(f"  registered in HMS")
        except TableAlreadyExistsError:
            hms_tbl = dst.load_table(ident)
            if hms_tbl.metadata_location == meta_loc:
                print(f"  already registered with same metadata — no-op")
            else:
                raise RuntimeError(
                    f"{ident} exists in HMS with different metadata "
                    f"({hms_tbl.metadata_location}); refusing to overwrite"
                )

    print("\nHMS catalog now contains:")
    for ns in dst.list_namespaces():
        for tbl in dst.list_tables(ns):
            t = dst.load_table(tbl)
            print(f"  {'.'.join(tbl)}  @  {t.metadata_location}")


if __name__ == "__main__":
    main()
