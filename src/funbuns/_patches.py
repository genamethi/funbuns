"""
Runtime patches for third-party libraries.

Imported for side effects from ``funbuns.__init__``. Each patch is idempotent
and documents the upstream issue it addresses so the patch can be removed
when the library is fixed.
"""

from __future__ import annotations


def _patch_pyiceberg_sort_order_id() -> None:
    """
    pyiceberg 0.11.1 hardcodes ``sort_order_id=None`` in
    ``parquet_file_to_data_file``. Every file registered via
    ``Table.add_files(...)`` lands in the manifest stamped as unsorted even
    when the writer pre-sorts rows. Downstream SQL engines then plan
    redundant sort steps on scans and MV rebuilds.

    We stamp each new DataFile with the table's ``default_sort_order_id``
    after construction. Safe for funbuns because ``write_batch`` enforces
    the declared sort order before calling ``add_files``. If pyiceberg ever
    adds the field to its constructor or changes the ``_data`` layout, this
    patch should be revisited (slot 15 is the sort_order_id position in
    pyiceberg 0.11.1's DataFile Record).
    """
    import pyiceberg.io.pyarrow as _m

    if getattr(_m.parquet_file_to_data_file, "_funbuns_patched", False):
        return

    _orig = _m.parquet_file_to_data_file

    def _patched(io, table_metadata, file_path):
        df = _orig(io, table_metadata, file_path)
        df._data[15] = table_metadata.default_sort_order_id
        return df

    _patched._funbuns_patched = True  # type: ignore[attr-defined]
    _m.parquet_file_to_data_file = _patched


_patch_pyiceberg_sort_order_id()
