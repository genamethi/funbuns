"""
One-shot: apply Iceberg table properties to funbuns.primes and
funbuns.decompositions.

Property changes only affect future commits; existing parquet files are
untouched. The delete-after-commit cleanup runs on the next commit, not
retroactively.

Usage:
    pixi run python scripts/set_table_properties.py
"""

from funbuns.iceberg_schema import DECOMP_IDENT, PRIMES_IDENT, open_catalog

PROPS: dict[str, str] = {
    "write.metadata.metrics.default": "full",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
    "write.parquet.row-group-size-bytes": str(128 << 20),
}


def main() -> None:
    cat = open_catalog()
    for ident in (PRIMES_IDENT, DECOMP_IDENT):
        tbl = cat.load_table(ident)
        with tbl.transaction() as tx:
            tx.set_properties(PROPS)
        after = cat.load_table(ident).properties
        print(f"\n{ident}")
        for k in sorted(after):
            print(f"  {k} = {after[k]}")


if __name__ == "__main__":
    main()
