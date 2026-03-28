"""Tests for dataprep.py: prime power table generation with overflow handling."""

from pathlib import Path

import polars as pl
import pytest

from funbuns.dataprep import prepare_prime_powers


@pytest.mark.sage
class TestPreparePrimePowers:
    """T22: Prime power table generation."""

    def test_output_parquet_exists(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        result = prepare_prime_powers(n=100, max_power=10)
        assert result.exists()
        assert result.suffix == ".parquet"

    def test_schema_columns(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        result = prepare_prime_powers(n=100, max_power=10)
        df = pl.read_parquet(result)
        # Column "1" = primes, columns "2" through "10" = powers
        expected_cols = [str(k) for k in range(1, 11)]
        assert df.columns == expected_cols

    def test_known_values(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        result = prepare_prime_powers(n=100, max_power=10)
        df = pl.read_parquet(result)

        # Filter to prime 7
        row7 = df.filter(pl.col("1") == 7)
        assert row7.height == 1
        assert row7["2"].item() == 49      # 7^2
        assert row7["3"].item() == 343     # 7^3
        assert row7["4"].item() == 2401    # 7^4

        # Filter to prime 2
        row2 = df.filter(pl.col("1") == 2)
        assert row2.height == 1
        assert row2["10"].item() == 1024   # 2^10

    def test_prime_count(self, tmp_path, monkeypatch):
        """prime_range(100) yields 25 primes (2..97)."""
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        result = prepare_prime_powers(n=100, max_power=4)
        df = pl.read_parquet(result)
        assert df.height == 25

    @pytest.mark.xfail(
        reason="Polars .pow() silently wraps on Int64 overflow, so the "
               "<= int64_max guard sees the wrapped (positive) value as safe. "
               "Only overflows that wrap to negative are caught.",
        strict=True,
    )
    def test_overflow_bounded(self, tmp_path, monkeypatch):
        """Large prime^high_power exceeding Int64 should be 0 in bounded mode."""
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        result = prepare_prime_powers(n=100, max_power=64, use_bounded=True)
        df = pl.read_parquet(result)

        # 97^64 vastly exceeds 2^63-1; column "64" for prime 97 should be 0
        row97 = df.filter(pl.col("1") == 97)
        assert row97["64"].item() == 0

    def test_unbounded_no_zeros(self, tmp_path, monkeypatch):
        """Small primes with low powers should have no zeros in unbounded mode."""
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        result = prepare_prime_powers(n=10, max_power=4, use_bounded=False)
        df = pl.read_parquet(result)

        # Primes < 10: [2, 3, 5, 7]. All p^k for k<=4 fit in Int64.
        for col in ["2", "3", "4"]:
            assert (df[col] > 0).all()
