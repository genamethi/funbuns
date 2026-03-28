"""Tests for run_ingester.py: integration, deduplication, partial block absorption."""

from pathlib import Path

import polars as pl
import pytest

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
