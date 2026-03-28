"""Tests for data_integrity.py: duplicates, gaps, completeness, Dusart bounds."""

import math
from pathlib import Path

import polars as pl
import pytest

from funbuns.block_catalog import BlockInfo
from funbuns.data_integrity import (
    _dusart_pi_bounds,
    _natural_gap_bound,
    detect_duplicates_in_block,
    detect_gaps,
    validate_completeness_fast,
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


def _block_info(path=None, block_num=0, min_prime=None, max_prime=None,
                num_rows=None, num_unique_primes=None):
    """Construct a synthetic BlockInfo for testing."""
    return BlockInfo(
        path=path or Path(f"pp_b{block_num:03d}_p{max_prime or 0}.parquet"),
        block_num=block_num,
        min_prime=min_prime,
        max_prime=max_prime,
        num_rows=num_rows,
        num_unique_primes=num_unique_primes,
    )


class TestDetectDuplicatesInBlock:
    """T6: Duplicate detection by full (p, m_k, n_k, q_k) row."""

    def test_detects_exact_duplicates(self, tmp_path):
        path = tmp_path / "dupes.parquet"
        # Two identical rows: (7, 1, 1, 5)
        _make_block_parquet(path, [7, 7, 11], m_k=[1, 1, 1], n_k=[1, 1, 1], q_k=[5, 5, 9])
        assert detect_duplicates_in_block(path) == 1

    def test_same_p_different_partitions(self, tmp_path):
        """Same prime with different (m_k, n_k, q_k) is NOT a duplicate."""
        path = tmp_path / "no_dupes.parquet"
        _make_block_parquet(path, [7, 7, 11], m_k=[1, 2, 1], n_k=[1, 1, 1], q_k=[5, 3, 9])
        assert detect_duplicates_in_block(path) == 0

    def test_no_duplicates(self, tmp_path):
        path = tmp_path / "clean.parquet"
        _make_block_parquet(path, [7, 11, 13])
        assert detect_duplicates_in_block(path) == 0

    def test_multiple_duplicates(self, tmp_path):
        path = tmp_path / "multi.parquet"
        _make_block_parquet(
            path,
            [7, 7, 7, 11, 11],
            m_k=[1, 1, 1, 2, 2],
            n_k=[1, 1, 1, 1, 1],
            q_k=[5, 5, 5, 9, 9],
        )
        # 3 copies of (7,1,1,5) -> 2 duplicates; 2 copies of (11,2,1,9) -> 1 duplicate
        assert detect_duplicates_in_block(path) == 3

    def test_corrupt_file_raises(self, tmp_path):
        path = tmp_path / "corrupt.parquet"
        path.write_bytes(b"not parquet data")
        with pytest.raises(IOError, match="Corrupt parquet"):
            detect_duplicates_in_block(path)


class TestDetectGaps:
    """T8: Coverage gap detection via interval merging."""

    def test_detects_large_gap(self):
        """Gap exceeding natural prime gap bound is detected."""
        infos = [
            _block_info(block_num=0, min_prime=2, max_prime=1000,
                        num_rows=168, num_unique_primes=168),
            # Gap from 1000 to 1_000_000 -- way beyond 5*(ln 1000)^2 ~ 238
            _block_info(block_num=1, min_prime=1_000_000, max_prime=2_000_000,
                        num_rows=50000, num_unique_primes=50000),
        ]
        gaps, merged = detect_gaps(infos)
        assert len(gaps) == 1
        assert gaps[0]["gap_start"] == 1000
        assert gaps[0]["gap_end"] == 1_000_000

    def test_contiguous_no_gap(self):
        """Adjacent blocks with overlapping or tight ranges -> no gap."""
        infos = [
            _block_info(block_num=0, min_prime=2, max_prime=1000,
                        num_rows=168, num_unique_primes=168),
            _block_info(block_num=1, min_prime=997, max_prime=2000,
                        num_rows=135, num_unique_primes=135),
        ]
        gaps, merged = detect_gaps(infos)
        assert len(gaps) == 0

    def test_single_block(self):
        """Single block -> no gaps possible (needs >= 2 merged intervals)."""
        infos = [
            _block_info(block_num=0, min_prime=2, max_prime=1000,
                        num_rows=168, num_unique_primes=168),
        ]
        gaps, merged = detect_gaps(infos)
        assert len(gaps) == 0

    def test_sub_threshold_gap_not_detected(self):
        """Gap below the natural gap bound is invisible at this layer.
        Documents intended coarseness, not a bug."""
        # At p ~ 10^6, bound is ~5*(13.8)^2 = 953
        infos = [
            _block_info(block_num=0, min_prime=999000, max_prime=1_000_000,
                        num_rows=100, num_unique_primes=100),
            # Gap of 500 -- below the ~953 bound
            _block_info(block_num=1, min_prime=1_000_500, max_prime=1_001_000,
                        num_rows=100, num_unique_primes=100),
        ]
        gaps, merged = detect_gaps(infos)
        assert len(gaps) == 0  # Expected: gap is below threshold

    def test_empty_infos(self):
        gaps, merged = detect_gaps([])
        assert len(gaps) == 0
        assert len(merged) == 0


class TestValidateCompletenessFast:
    """T9: Layered completeness validation."""

    def test_layer2_overlapping_complete(self):
        """No gaps + overlapping blocks + starts at p<=3 -> COMPLETE."""
        infos = [
            _block_info(block_num=0, min_prime=2, max_prime=1000,
                        num_rows=168, num_unique_primes=168),
            _block_info(block_num=1, min_prime=997, max_prime=2000,
                        num_rows=135, num_unique_primes=135),
        ]
        result = validate_completeness_fast(infos)
        assert result["complete"] is True
        assert result["n_overlapping_pairs"] == 1
        assert not result["needs_exact"]

    def test_layer1_gap_incomplete(self):
        """Gap between blocks -> INCOMPLETE."""
        infos = [
            _block_info(block_num=0, min_prime=2, max_prime=1000,
                        num_rows=168, num_unique_primes=168),
            _block_info(block_num=1, min_prime=1_000_000, max_prime=2_000_000,
                        num_rows=50000, num_unique_primes=50000),
        ]
        result = validate_completeness_fast(infos)
        assert result["complete"] is False
        assert len(result["gaps"]) == 1

    def test_layer3_below_dusart_threshold(self):
        """Non-overlapping blocks below Dusart upper threshold -> needs_exact."""
        infos = [
            _block_info(block_num=0, min_prime=2, max_prime=1000,
                        num_rows=168, num_unique_primes=168),
            # No overlap (1001 > 1000), below 2,953,652,287
            _block_info(block_num=1, min_prime=1009, max_prime=2000,
                        num_rows=135, num_unique_primes=135),
        ]
        result = validate_completeness_fast(infos)
        assert result["complete"] is False
        assert result["needs_exact"] is True

    def test_all_corrupt(self):
        """All blocks with None stats -> empty valid list, incomplete."""
        infos = [
            _block_info(block_num=0, min_prime=None, max_prime=None,
                        num_rows=None, num_unique_primes=None),
        ]
        result = validate_completeness_fast(infos)
        assert result["complete"] is False
        assert result["per_block_sum"] == 0

    def test_layer3_above_dusart_within_bounds(self):
        """Above Dusart threshold, per_block_sum within bounds -> COMPLETE.

        Uses approximate pi(5*10^9) ~ 234,954,223. Dusart bounds at this
        scale have error < 0.1%, so we set per_block_sum to a value
        safely within the bracket.
        """
        x = 5_000_000_000
        pi_lo, pi_hi = _dusart_pi_bounds(x)
        # Set sum to midpoint of bounds
        approx_sum = int((pi_lo + pi_hi) / 2)

        infos = [
            _block_info(block_num=0, min_prime=2, max_prime=x,
                        num_rows=approx_sum, num_unique_primes=approx_sum),
        ]
        result = validate_completeness_fast(infos)
        assert result["complete"] is True
        assert not result["needs_exact"]

    def test_layer3_above_dusart_outside_bounds(self):
        """Above Dusart threshold, per_block_sum outside bounds -> needs_exact."""
        x = 5_000_000_000
        pi_lo, pi_hi = _dusart_pi_bounds(x)

        infos = [
            _block_info(block_num=0, min_prime=2, max_prime=x,
                        num_rows=100, num_unique_primes=100),  # Way too low
        ]
        result = validate_completeness_fast(infos)
        assert result["complete"] is False
        assert result["needs_exact"] is True


class TestDusartPiBounds:
    """T10: Dusart (2010) unconditional bounds on pi(x)."""

    @pytest.mark.sage
    def test_lower_bound_valid_above_88783(self):
        """Lower bound valid for x >= 88,783."""
        from sage.all import prime_pi
        for x in [100_000, 1_000_000, 10**8]:
            lo, _ = _dusart_pi_bounds(x)
            actual = int(prime_pi(x))
            assert lo <= actual, (
                f"Lower bound {lo:.0f} > pi({x}) = {actual}"
            )

    @pytest.mark.sage
    def test_upper_bound_valid_above_2953652287(self):
        """Upper bound valid for x >= 2,953,652,287."""
        from sage.all import prime_pi
        for x in [3_000_000_000, 10**10, 10**11]:
            _, hi = _dusart_pi_bounds(x)
            actual = int(prime_pi(x))
            assert actual <= hi, (
                f"pi({x}) = {actual} > upper bound {hi:.0f}"
            )

    @pytest.mark.sage
    def test_both_bounds_bracket_above_threshold(self):
        """Both bounds bracket pi(x) when x is above both thresholds."""
        from sage.all import prime_pi
        for x in [5_000_000_000, 10**10, 10**11]:
            lo, hi = _dusart_pi_bounds(x)
            actual = int(prime_pi(x))
            assert lo <= actual <= hi, (
                f"Dusart [{lo:.0f}, {hi:.0f}] doesn't bracket pi({x}) = {actual}"
            )

    @pytest.mark.sage
    def test_upper_bound_invalid_below_threshold(self):
        """Below the upper threshold, the upper bound may NOT hold.
        This documents the validity boundary, not a bug."""
        from sage.all import prime_pi
        x = 10**8  # Below 2,953,652,287
        _, hi = _dusart_pi_bounds(x)
        actual = int(prime_pi(x))
        # Upper bound is NOT guaranteed to hold here
        # (and empirically it doesn't: pi(10^8) = 5761455 > 5760728)
        assert actual > hi, (
            f"Upper bound unexpectedly holds at x={x} "
            f"(below validity threshold)"
        )

    def test_lower_less_than_upper(self):
        """Basic sanity: lower bound < upper bound for all valid x."""
        for x in [10**5, 10**8, 10**10, 10**12]:
            lo, hi = _dusart_pi_bounds(x)
            assert lo < hi, f"Lower bound >= upper bound at x={x}"

    def test_monotonically_increasing(self):
        """Bounds should increase with x."""
        prev_lo, prev_hi = _dusart_pi_bounds(10**6)
        for exp in [7, 8, 9, 10]:
            lo, hi = _dusart_pi_bounds(10**exp)
            assert lo > prev_lo
            assert hi > prev_hi
            prev_lo, prev_hi = lo, hi
