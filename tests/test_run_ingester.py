"""Tests for run_ingester.py: integration, deduplication, partial block absorption."""

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from funbuns import VERSION

from funbuns.run_ingester import integrate_runs_into_blocks


def _write_run_parquet(path: Path, data: list[tuple[int, int, int, int]]):
    """Write a run parquet from (p, m_k, n_k, q_k) tuples."""
    df = pl.DataFrame(
        data, schema={"p": pl.Int64, "m_k": pl.Int64, "n_k": pl.Int64, "q_k": pl.Int64},
        orient="row",
    )
    df.write_parquet(path)
    return df


def _read_all_blocks(blocks_dir: Path) -> pl.DataFrame:
    """Read and concat all block parquets, sorted by p."""
    files = sorted(blocks_dir.glob("pp_b*.parquet"))
    if not files:
        return pl.DataFrame(schema={"p": pl.Int64, "m_k": pl.Int64, "n_k": pl.Int64, "q_k": pl.Int64})
    return pl.concat([pl.read_parquet(f) for f in files]).sort("p")


class TestIntegrateRunsIntoBlocks:
    """T18: Run file integration into blocks."""

    def test_basic_integration(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        runs = tmp_path / "runs"
        runs.mkdir()
        blocks = tmp_path / "blocks"
        blocks.mkdir()

        # 3 run files with known data
        _write_run_parquet(runs / "run_001.parquet", [
            (7, 1, 1, 5), (11, 1, 1, 9), (13, 1, 1, 11),
        ])
        _write_run_parquet(runs / "run_002.parquet", [
            (17, 1, 1, 15), (19, 1, 1, 17), (23, 1, 1, 21),
        ])
        _write_run_parquet(runs / "run_003.parquet", [
            (29, 1, 1, 27), (31, 1, 1, 29),
        ])

        result = integrate_runs_into_blocks(
            target_prime_count=100, delete_run_files=False, verbose=False
        )
        assert result is True

        # Blocks created
        block_files = list(blocks.glob("pp_b*.parquet"))
        assert len(block_files) >= 1

        # All data present and sorted
        all_data = _read_all_blocks(blocks)
        assert all_data["p"].to_list() == [7, 11, 13, 17, 19, 23, 29, 31]

        # Run files still exist (delete_run_files=False)
        assert len(list(runs.glob("*.parquet"))) == 3

    def test_deduplication(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        runs = tmp_path / "runs"
        runs.mkdir()
        blocks = tmp_path / "blocks"
        blocks.mkdir()

        # Duplicate rows across files
        _write_run_parquet(runs / "run_001.parquet", [
            (7, 1, 1, 5), (11, 1, 1, 9),
        ])
        _write_run_parquet(runs / "run_002.parquet", [
            (7, 1, 1, 5), (13, 1, 1, 11),  # (7,1,1,5) is a duplicate
        ])

        integrate_runs_into_blocks(
            target_prime_count=100, delete_run_files=False, verbose=False
        )

        all_data = _read_all_blocks(blocks)
        # (7,1,1,5) should appear only once
        p7_rows = all_data.filter(pl.col("p") == 7)
        assert p7_rows.height == 1

    def test_delete_run_files(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        runs = tmp_path / "runs"
        runs.mkdir()
        blocks = tmp_path / "blocks"
        blocks.mkdir()

        _write_run_parquet(runs / "run_001.parquet", [(7, 1, 1, 5)])

        integrate_runs_into_blocks(
            target_prime_count=100, delete_run_files=True, verbose=False
        )

        assert len(list(runs.glob("*.parquet"))) == 0
        assert len(list(blocks.glob("pp_b*.parquet"))) >= 1

    def test_no_runs_no_work(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        runs = tmp_path / "runs"
        runs.mkdir()
        blocks = tmp_path / "blocks"
        blocks.mkdir()

        result = integrate_runs_into_blocks(
            target_prime_count=100, verbose=False
        )
        assert result is False


class TestPartialBlockAbsorption:
    """T19: Undersized last block is absorbed with new run data."""

    def test_absorbs_partial_block(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        runs = tmp_path / "runs"
        runs.mkdir()
        blocks = tmp_path / "blocks"
        blocks.mkdir()

        # Create an undersized block (3 primes, target is 10)
        old_block = pl.DataFrame({
            "p": [7, 11, 13],
            "m_k": [1, 1, 1],
            "n_k": [1, 1, 1],
            "q_k": [5, 9, 11],
        }).cast({"p": pl.Int64, "m_k": pl.Int64, "n_k": pl.Int64, "q_k": pl.Int64})
        old_block.write_parquet(blocks / "pp_b001_p13.parquet")

        # New run data
        _write_run_parquet(runs / "run_001.parquet", [
            (17, 1, 1, 15), (19, 1, 1, 17), (23, 1, 1, 21),
        ])

        integrate_runs_into_blocks(
            target_prime_count=10, delete_run_files=False, verbose=False
        )

        # Old block should be replaced
        assert not (blocks / "pp_b001_p13.parquet").exists()

        # New block(s) should contain all data
        all_data = _read_all_blocks(blocks)
        primes = all_data["p"].unique().sort().to_list()
        assert primes == [7, 11, 13, 17, 19, 23]


PARTITION_DTYPES = {"p": pl.Int64, "m_k": pl.Int64, "n_k": pl.Int64, "q_k": pl.Int64}


@pytest.mark.xfail(
    condition=VERSION < (1, 1, 0),
    reason="Run files deleted before integrity verification — crash between "
           "delete and write loses data",
    strict=True,
)
class TestRunFileDeletionSafety:
    """X9: Run files should not be deleted until new blocks are verified.

    Two issues in run_ingester.py:
    1. delete_run_files=True deletes immediately after block writes,
       with no verification that new blocks contain expected data.
    2. Partial block absorption (line 103) calls .unlink() on the old
       block BEFORE writing replacements — a crash between delete and
       write loses both old block data and unwritten run data.

    Desired: delete run files only after verifying new blocks contain
    all expected data. If an integrity check is pending/scheduled,
    defer deletion entirely.
    """

    def test_integrity_check_before_deletion(self, tmp_path, monkeypatch):
        """An integrity check must occur between block write and run file delete."""
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        runs = tmp_path / "runs"
        runs.mkdir()
        blocks = tmp_path / "blocks"
        blocks.mkdir()

        df = pl.DataFrame({
            "p": [7, 11, 13],
            "m_k": [1, 1, 1],
            "n_k": [1, 1, 1],
            "q_k": [5, 9, 11],
        }).cast(PARTITION_DTYPES)
        df.write_parquet(runs / "run_001.parquet")

        call_log = []
        original_write = pl.DataFrame.write_parquet
        original_unlink = Path.unlink

        def tracking_write(self_df, path, *args, **kwargs):
            call_log.append(("write_block", str(path)))
            return original_write(self_df, path, *args, **kwargs)

        def tracking_unlink(self_path, *args, **kwargs):
            call_log.append(("delete_run", str(self_path)))
            return original_unlink(self_path, *args, **kwargs)

        with (
            patch.object(pl.DataFrame, "write_parquet", tracking_write),
            patch.object(Path, "unlink", tracking_unlink),
        ):
            integrate_runs_into_blocks(
                target_prime_count=100,
                delete_run_files=True,
                verbose=False,
            )

        events = [e[0] for e in call_log]
        assert "integrity_check" in events, (
            f"No integrity check between block write and run file deletion. "
            f"Event sequence: {events}"
        )

    def test_old_block_survives_failed_absorption(self, tmp_path, monkeypatch):
        """Old undersized block must not be deleted until replacement is confirmed."""
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        runs = tmp_path / "runs"
        runs.mkdir()
        blocks = tmp_path / "blocks"
        blocks.mkdir()

        old_block = pl.DataFrame({
            "p": [7, 11, 13],
            "m_k": [1, 1, 1],
            "n_k": [1, 1, 1],
            "q_k": [5, 9, 11],
        }).cast(PARTITION_DTYPES)
        old_block.write_parquet(blocks / "pp_b001_p13.parquet")

        run_df = pl.DataFrame({
            "p": [17, 19, 23],
            "m_k": [1, 1, 1],
            "n_k": [1, 1, 1],
            "q_k": [15, 17, 21],
        }).cast(PARTITION_DTYPES)
        run_df.write_parquet(runs / "run_001.parquet")

        # Fail all new block writes — old block should survive
        original_write = pl.DataFrame.write_parquet

        def failing_block_write(self_df, path, *args, **kwargs):
            if "pp_b" in str(path):
                raise IOError("Simulated write failure")
            return original_write(self_df, path, *args, **kwargs)

        with patch.object(pl.DataFrame, "write_parquet", failing_block_write):
            try:
                integrate_runs_into_blocks(
                    target_prime_count=10,
                    delete_run_files=False,
                    verbose=False,
                )
            except IOError:
                pass

        assert (blocks / "pp_b001_p13.parquet").exists(), (
            "Old block deleted before replacement written — data loss!"
        )
