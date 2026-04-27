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

If a future change adds a reorder buffer to the ingest path to force
batches to commit in p-order, assertion (b) becomes impossible and this
test fails, flagging the behavioral change explicitly.
"""

from __future__ import annotations

import polars as pl
import pytest

from pathlib import Path

from funbuns import iceberg_schema as isch
from funbuns.iceberg_schema import IcebergWriter, PendingFlush, shape_for_write


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

    # flush() is parquet-only; catalog sees nothing until commit_pending.
    writer.commit_pending()

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


def test_flush_shaped_accepts_empty_decompositions(tmp_path):
    cat = isch.open_catalog(warehouse_root=tmp_path / "warehouse")
    writer = IcebergWriter(cat=cat)

    primes = pl.DataFrame(
        {"p": [149, 331], "k": [0, 0]},
        schema={"p": pl.Int64, "k": pl.Int32},
    )
    decomp = pl.DataFrame(
        {"p": [], "m_k": [], "n_k": [], "q_k": []},
        schema={"p": pl.Int64, "m_k": pl.Int32, "n_k": pl.Int32, "q_k": pl.Int64},
    )

    result = writer.flush_shaped(primes, decomp)
    assert result.primes_rows == 2
    assert result.decomp_rows == 0
    assert len(writer.pending) == 1
    assert len(writer.pending[0].primes_files) == 1
    assert writer.pending[0].decomp_files == []

    writer.commit_pending()
    stored = cat.load_table(isch.PRIMES_IDENT).scan().to_polars().sort("p")
    assert stored.select(["p", "k"]).to_dict(as_series=False) == {
        "p": [149, 331],
        "k": [0, 0],
    }


def _flush(start_idx: int, processed_count: int, *, commit_seq: int) -> PendingFlush:
    """Build a PendingFlush with empty path lists — _contiguous_prefix
    only inspects rank fields, so paths are irrelevant for unit tests."""
    return PendingFlush(
        primes_files=[],
        decomp_files=[],
        p_max=start_idx + processed_count,  # arbitrary monotone proxy
        commit_seq=commit_seq,
        start_idx=start_idx,
        processed_count=processed_count,
    )


class TestContiguousPrefix:
    """Unit tests for IcebergWriter._contiguous_prefix — the rank-ordering
    filter applied at commit_pending time when an anchor is provided."""

    def test_empty_pending(self):
        kept, dropped = IcebergWriter._contiguous_prefix([], anchor_start_idx=10)
        assert kept == [] and dropped == []

    def test_all_in_order_arrival_independent(self):
        # Arrival order shuffled (mirrors imap_unordered): the helper sorts
        # by start_idx internally, so kept reflects rank order, not arrival.
        a, b, c = _flush(10, 2, commit_seq=1), _flush(12, 2, commit_seq=2), _flush(14, 2, commit_seq=0)
        kept, dropped = IcebergWriter._contiguous_prefix(
            [c, a, b], anchor_start_idx=10,
        )
        assert [f.start_idx for f in kept] == [10, 12, 14]
        assert dropped == []

    def test_gap_in_middle_drops_tail(self):
        a = _flush(10, 2, commit_seq=0)
        b = _flush(12, 2, commit_seq=1)
        d = _flush(16, 2, commit_seq=2)  # 14 missing
        kept, dropped = IcebergWriter._contiguous_prefix(
            [a, b, d], anchor_start_idx=10,
        )
        assert [f.start_idx for f in kept] == [10, 12]
        assert [f.start_idx for f in dropped] == [16]

    def test_gap_at_start_drops_everything(self):
        b = _flush(12, 2, commit_seq=0)
        c = _flush(14, 2, commit_seq=1)
        kept, dropped = IcebergWriter._contiguous_prefix(
            [b, c], anchor_start_idx=10,
        )
        assert kept == []
        assert [f.start_idx for f in dropped] == [12, 14]

    def test_partial_batch_terminates_prefix(self):
        # Worker B finished only 1 of its 2 primes (interrupted mid-batch).
        # The prefix ends after B because no successor's start_idx can
        # equal 12 + 1 = 13 (subsequent batches sit on the batch_size grid).
        a = _flush(10, 2, commit_seq=0)
        b = _flush(12, 1, commit_seq=1)  # partial
        c = _flush(14, 2, commit_seq=2)
        kept, dropped = IcebergWriter._contiguous_prefix(
            [a, b, c], anchor_start_idx=10,
        )
        assert [f.start_idx for f in kept] == [10, 12]
        assert [f.start_idx for f in dropped] == [14]

    def test_missing_rank_info_raises(self):
        a = _flush(10, 2, commit_seq=0)
        legacy = PendingFlush(
            primes_files=[], decomp_files=[], p_max=99, commit_seq=1,
            start_idx=None, processed_count=None,
        )
        with pytest.raises(ValueError, match="start_idx/processed_count"):
            IcebergWriter._contiguous_prefix([a, legacy], anchor_start_idx=10)


def test_commit_pending_with_anchor_drops_post_gap_files(tmp_path):
    """End-to-end: when anchor_start_idx is set and pending has a gap, only
    the contiguous prefix lands in the catalog and orphan parquet files are
    deleted from disk."""
    cat = isch.open_catalog(warehouse_root=tmp_path / "warehouse")
    writer = IcebergWriter(cat=cat)

    def primes_df(values):
        return pl.DataFrame(
            {"p": values, "k": [0] * len(values)},
            schema={"p": pl.Int64, "k": pl.Int32},
        )

    empty_decomp = pl.DataFrame(
        {"p": [], "m_k": [], "n_k": [], "q_k": []},
        schema={"p": pl.Int64, "m_k": pl.Int32, "n_k": pl.Int32, "q_k": pl.Int64},
    )

    # Three batches arriving out of rank order. Middle batch (start=4) is
    # absent — simulates "worker for ranks 4-5 was still mid-batch when
    # SIGINT fired and never returned".
    writer.flush_shaped(primes_df([7, 11]), empty_decomp, start_idx=6, processed_count=2)
    writer.flush_shaped(primes_df([2, 3]), empty_decomp, start_idx=2, processed_count=2)

    assert len(writer.pending) == 2
    orphan_paths = [Path(p) for p in writer.pending[0].primes_files]
    assert all(p.exists() for p in orphan_paths)

    writer.commit_pending(anchor_start_idx=2)

    # Prefix [start=2] committed; tail [start=6] dropped because the rank
    # 4-5 batch is missing. Orphan parquet deleted from disk.
    stored = cat.load_table(isch.PRIMES_IDENT).scan().to_polars().sort("p")
    assert stored["p"].to_list() == [2, 3]
    assert all(not p.exists() for p in orphan_paths)
    assert writer.pending == []

    # Snapshot summary reflects only kept batches.
    writer2 = IcebergWriter(cat=cat)
    assert writer2.resume_p == 3
