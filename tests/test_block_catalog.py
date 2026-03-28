"""Tests for block_catalog.py: filename parsing, block bounds, sorting."""

from pathlib import Path

import polars as pl
import pytest

from funbuns.block_catalog import (
    BlockInfo,
    _fast_block_bounds,
    _parse_block_filename,
    sorted_blocks_by_data,
)


def _make_block_parquet(path: Path, primes: list[int], m_k=None, n_k=None, q_k=None):
    """Write a minimal partition parquet to path."""
    n = len(primes)
    df = pl.DataFrame({
        "p": primes,
        "m_k": m_k or [1] * n,
        "n_k": n_k or [1] * n,
        "q_k": q_k or [3] * n,
    }).cast({"p": pl.Int64, "m_k": pl.Int64, "n_k": pl.Int64, "q_k": pl.Int64})
    df.write_parquet(path)
    return df


class TestParseBlockFilename:
    """T11: Parse block filenames into (block_num, max_prime)."""

    def test_valid_standard(self):
        p = Path("/some/dir/pp_b001_p7249729.parquet")
        assert _parse_block_filename(p) == (1, 7249729)

    def test_valid_zero_index(self):
        p = Path("pp_b000_p2.parquet")
        assert _parse_block_filename(p) == (0, 2)

    def test_valid_large_numbers(self):
        p = Path("pp_b2675_p25165843009.parquet")
        assert _parse_block_filename(p) == (2675, 25165843009)

    def test_invalid_random_name(self):
        p = Path("random.parquet")
        assert _parse_block_filename(p) == (None, None)

    def test_invalid_no_parquet_ext(self):
        p = Path("pp_b001_p7249729.csv")
        assert _parse_block_filename(p) == (None, None)

    def test_invalid_missing_prefix(self):
        p = Path("b001_p7249729.parquet")
        assert _parse_block_filename(p) == (None, None)

    def test_directory_component_ignored(self):
        """path.name strips the directory."""
        p = Path("/very/deep/path/pp_b042_p999983.parquet")
        assert _parse_block_filename(p) == (42, 999983)


class TestFastBlockBounds:
    """T12: Compute (min_p, max_p, rows, unique) from parquet content."""

    def test_basic_bounds(self, tmp_path):
        path = tmp_path / "pp_b001_p29.parquet"
        # Include duplicate p to verify rows != unique
        _make_block_parquet(path, [7, 7, 11, 13, 29], m_k=[1, 2, 1, 1, 1])
        min_p, max_p, rows, uniq = _fast_block_bounds(path)
        assert min_p == 7
        assert max_p == 29
        assert rows == 5
        assert uniq == 4  # 7 appears twice

    def test_single_row(self, tmp_path):
        path = tmp_path / "pp_b000_p2.parquet"
        _make_block_parquet(path, [2])
        min_p, max_p, rows, uniq = _fast_block_bounds(path)
        assert min_p == 2
        assert max_p == 2
        assert rows == 1
        assert uniq == 1

    def test_corrupt_file(self, tmp_path):
        path = tmp_path / "pp_b001_p100.parquet"
        path.write_bytes(b"not a parquet file at all")
        assert _fast_block_bounds(path) == (None, None, None, None)

    def test_missing_p_column(self, tmp_path):
        path = tmp_path / "pp_b001_p100.parquet"
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(path)
        assert _fast_block_bounds(path) == (None, None, None, None)


class TestSortedBlocksByData:
    """T13: Sort blocks by content-derived min_prime."""

    def test_out_of_order_filenames(self, tmp_path):
        # b003 has the smallest primes, b001 has the largest
        _make_block_parquet(tmp_path / "pp_b001_p97.parquet", [89, 97])
        _make_block_parquet(tmp_path / "pp_b002_p53.parquet", [41, 43, 47, 53])
        _make_block_parquet(tmp_path / "pp_b003_p7.parquet", [2, 3, 5, 7])

        files = list(tmp_path.glob("pp_b*.parquet"))
        result = sorted_blocks_by_data(files=files)

        assert len(result) == 3
        assert result[0].min_prime == 2
        assert result[1].min_prime == 41
        assert result[2].min_prime == 89

    def test_corrupt_file_sorts_last(self, tmp_path):
        _make_block_parquet(tmp_path / "pp_b001_p29.parquet", [23, 29])
        corrupt = tmp_path / "pp_b002_p100.parquet"
        corrupt.write_bytes(b"garbage")

        files = list(tmp_path.glob("pp_b*.parquet"))
        result = sorted_blocks_by_data(files=files)

        assert len(result) == 2
        assert result[0].min_prime == 23
        # Corrupt block sorts to end (min_prime=None -> inf)
        assert result[1].min_prime is None

    def test_single_block(self, tmp_path):
        _make_block_parquet(tmp_path / "pp_b001_p11.parquet", [7, 11])
        files = list(tmp_path.glob("pp_b*.parquet"))
        result = sorted_blocks_by_data(files=files)
        assert len(result) == 1
        assert result[0].min_prime == 7
        assert result[0].max_prime == 11
