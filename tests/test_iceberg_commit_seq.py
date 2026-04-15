"""
Pins the contract that ``commit_seq`` is a flush-order counter, **not** a
p-ordered index. Under imap_unordered ingest, workers can commit a later
p-range before an earlier one. This test reproduces that ordering inversion
against a fresh throwaway iceberg catalog and asserts:

  (a) the write path succeeds for out-of-order commits,
  (b) ``commit_seq`` 0 can cover a higher p-range than ``commit_seq`` 1,
  (c) ``IcebergWriter._init_state`` still returns the correct
      ``(next_commit_seq, resume_p)`` — i.e. resume is driven by max(p),
      not by max(commit_seq), so the reorder does not break resume.

If a future change adds a reorder buffer to PPConsumer to force batches to
commit in p-order, assertion (b) becomes impossible and this test fails,
flagging the behavioral change explicitly.
"""

from __future__ import annotations

import polars as pl
import pytest

from funbuns import iceberg_schema as isch
from funbuns.iceberg_schema import IcebergWriter, shape_for_write


def _raw_batch(p_lo: int, p_hi: int) -> pl.DataFrame:
    """Build a minimal raw {p, m_k, n_k, q_k} frame covering p in [p_lo, p_hi]."""
    ps = list(range(p_lo, p_hi + 1, 2))
    return pl.DataFrame(
        {
            "p": ps,
            "m_k": [1] * len(ps),
            "n_k": [1] * len(ps),
            "q_k": [3] * len(ps),
        },
        schema={"p": pl.Int64, "m_k": pl.Int32, "n_k": pl.Int32, "q_k": pl.Int64},
    )


def test_commit_seq_reorder_is_safe(tmp_path):
    cat = isch.open_catalog(warehouse_root=tmp_path / "warehouse")

    writer = IcebergWriter(cat=cat)
    assert writer.next_commit_seq == 0
    assert writer.resume_p == 0

    # Flush HIGH p-range first (simulates faster worker finishing after the
    # slower worker that owns the lower range). This is the precise inversion
    # the test exists to pin.
    high = _raw_batch(1_000_003, 1_000_099)
    low = _raw_batch(1_000_001, 1_000_001)

    r0 = writer.flush(high)
    r1 = writer.flush(low)

    assert r0.commit_seq == 0
    assert r1.commit_seq == 1

    primes = cat.load_table(isch.PRIMES_IDENT).scan().to_polars()
    by_seq = (
        primes.group_by("commit_seq")
        .agg(pl.col("p").min().alias("p_min"), pl.col("p").max().alias("p_max"))
        .sort("commit_seq")
    )

    seq0 = by_seq.filter(pl.col("commit_seq") == 0).row(0, named=True)
    seq1 = by_seq.filter(pl.col("commit_seq") == 1).row(0, named=True)

    # (b): commit_seq is NOT monotonic in p. seq 0 owns the higher range.
    assert seq0["p_min"] > seq1["p_max"], (
        "reorder invariant broken: commit_seq 0 no longer above commit_seq 1 "
        "in p-space. If a reorder buffer was added to PPConsumer, update this "
        "test and the iceberg_schema module docstring."
    )

    # (c): fresh writer constructed against the reordered catalog reports the
    # correct resume state. next_commit_seq is max(commit_seq)+1 = 2; resume_p
    # is the true max(p), which came from commit_seq 0 — not commit_seq 1.
    writer2 = IcebergWriter(cat=cat)
    assert writer2.next_commit_seq == 2
    assert writer2.resume_p == int(primes["p"].max())
    assert writer2.resume_p == seq0["p_max"]


def test_shape_for_write_stamps_commit_seq():
    raw = _raw_batch(3, 11)
    primes, decomp = shape_for_write(raw, commit_seq=42)
    assert (primes["commit_seq"] == 42).all()
    assert (decomp["commit_seq"] == 42).all()
    assert primes.schema == {"p": pl.Int64, "k": pl.Int32, "commit_seq": pl.Int32}
    assert decomp.schema == {
        "p": pl.Int64,
        "m_k": pl.Int32,
        "n_k": pl.Int32,
        "q_k": pl.Int64,
        "commit_seq": pl.Int32,
    }
