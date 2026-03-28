"""Tests for core.py: PPBatchProcessor, worker_batch, PPConsumer, PPBatchFeeder."""

import polars as pl
import pytest

from funbuns.utils import PARTITION_SCHEMA


@pytest.mark.sage
class TestTripleIdentity:
    """T1: For every (p, m_k, n_k, q_k) with q_k > 0: 2^m_k + q_k^n_k == p."""

    def test_random_primes(self):
        from sage.all import Integer, random_prime

        from funbuns.core import PPBatchProcessor

        processor = PPBatchProcessor()

        # 5 primes per magnitude range, 25 total
        test_primes = []
        for k in [21, 25, 31, 35, 40]:
            for _ in range(5):
                p = random_prime(2**k, lbound=2 ** (k - 1), proof=True)
                test_primes.append(Integer(p))

        for p in test_primes:
            result = processor.process_batch([p])
            assert result.shape[0] >= 1, f"No result for p={p}"

            for row_idx in range(result.shape[0]):
                row = result[row_idx]
                p_val, m_k, n_k, q_k = int(row[0]), int(row[1]), int(row[2]), int(row[3])
                assert p_val == int(p)

                if q_k > 0:
                    assert 2**m_k + q_k**n_k == p_val, (
                        f"Identity failed: 2^{m_k} + {q_k}^{n_k} != {p_val}"
                    )
                    assert Integer(q_k).is_prime(proof=True), (
                        f"q_k={q_k} is not prime"
                    )
                else:
                    # Obstructed: m_k=0, n_k=0, q_k=0
                    assert m_k == 0 and n_k == 0


@pytest.mark.sage
class TestObstructedPrimes:
    """T2: Known obstructed primes produce single (p, 0, 0, 0) row."""

    # Verified obstructed primes < 1000 (computed with proof=True)
    OBSTRUCTED = [
        2, 3, 149, 331, 373, 509, 701, 757, 809, 877, 907, 997,
    ]

    def test_obstructed_output(self):
        from sage.all import Integer

        from funbuns.core import PPBatchProcessor

        processor = PPBatchProcessor()

        for p in self.OBSTRUCTED:
            result = processor.process_batch([Integer(p)])
            assert result.shape[0] == 1, (
                f"Expected 1 row for obstructed p={p}, got {result.shape[0]}"
            )
            row = result[0]
            assert int(row[0]) == p
            assert int(row[1]) == 0  # m_k
            assert int(row[2]) == 0  # n_k
            assert int(row[3]) == 0  # q_k

    def test_paranoid_verification(self):
        """Verify obstructed list: for every m, p - 2^m is NOT a prime power."""
        from sage.all import Integer

        for p in self.OBSTRUCTED:
            sp = Integer(p)
            max_m = sp.exact_log(2)
            for m in range(1, max_m + 1):
                r = sp - Integer(2) ** m
                if r > 0:
                    assert not r.is_prime_power(proof=True), (
                        f"p={p}: remainder {r} = p - 2^{m} IS a prime power"
                    )


@pytest.mark.sage
class TestWorkerBatchDispatch:
    """T3: worker_batch resolves index range to correct primes."""

    def test_index_to_prime_mapping(self):
        from sage.all import Primes, prime_range

        from funbuns.core import worker_batch

        start_idx = 10
        count = 20
        df = worker_batch(start_idx, count)

        P = Primes()
        expected_primes = list(prime_range(int(P.unrank(start_idx)), int(P.unrank(start_idx + count))))

        result_primes = sorted(df["p"].unique().to_list())
        # Every prime in the range should appear in output
        for ep in expected_primes:
            assert ep in result_primes or any(
                row[0] == ep for row in df.iter_rows()
            ), f"Prime {ep} missing from worker_batch output"


class TestPPConsumerFlush:
    """T4: PPConsumer flush behavior — fires at threshold, finalize drains."""

    def test_flush_at_threshold(self, mocker):
        from funbuns.core import PPConsumer

        mock_save = mocker.Mock()
        consumer = PPConsumer(buffer_size=100, save_callback=mock_save)

        # Add 50 rows — should NOT trigger flush
        df_small = pl.DataFrame(
            {"p": list(range(50)), "m_k": [1] * 50, "n_k": [1] * 50, "q_k": [3] * 50},
            schema=PARTITION_SCHEMA,
        )
        consumer.add_results(df_small)
        assert mock_save.call_count == 0

        # Add 60 more — pushes past buffer_size=100, should flush
        df_trigger = pl.DataFrame(
            {"p": list(range(50, 110)), "m_k": [1] * 60, "n_k": [1] * 60, "q_k": [3] * 60},
            schema=PARTITION_SCHEMA,
        )
        consumer.add_results(df_trigger)
        assert mock_save.call_count >= 1

    def test_finalize_drains(self, mocker):
        from funbuns.core import PPConsumer

        mock_save = mocker.Mock()
        consumer = PPConsumer(buffer_size=10000, save_callback=mock_save)

        df = pl.DataFrame(
            {"p": [7, 11], "m_k": [1, 1], "n_k": [1, 1], "q_k": [5, 9]},
            schema=PARTITION_SCHEMA,
        )
        consumer.add_results(df)
        assert mock_save.call_count == 0  # Below threshold

        consumer.finalize()
        assert mock_save.call_count == 1  # Drained

    def test_empty_finalize(self, mocker):
        from funbuns.core import PPConsumer

        mock_save = mocker.Mock()
        consumer = PPConsumer(buffer_size=100, save_callback=mock_save)
        consumer.finalize()
        assert mock_save.call_count == 0  # Nothing to flush

    def test_none_input_ignored(self, mocker):
        from funbuns.core import PPConsumer

        mock_save = mocker.Mock()
        consumer = PPConsumer(buffer_size=100, save_callback=mock_save)
        consumer.add_results(None)
        assert consumer.result_count == 0

    def test_empty_df_ignored(self, mocker):
        from funbuns.core import PPConsumer

        mock_save = mocker.Mock()
        consumer = PPConsumer(buffer_size=100, save_callback=mock_save)
        consumer.add_results(pl.DataFrame(schema=PARTITION_SCHEMA))
        assert consumer.result_count == 0


@pytest.mark.sage
class TestPPBatchFeeder:
    """T5: PPBatchFeeder generates contiguous index-based batch boundaries."""

    def test_contiguous_batches(self):
        from funbuns.core import PPBatchFeeder

        feeder = PPBatchFeeder(init_p=29, num_primes=100, batch_size=10)
        batches = list(feeder.generate_batches())

        assert len(batches) == 10

        # All batches should have count=10
        for start, count in batches:
            assert count == 10

        # Batches should be contiguous
        for i in range(1, len(batches)):
            prev_start, prev_count = batches[i - 1]
            curr_start, _ = batches[i]
            assert curr_start == prev_start + prev_count

    def test_start_index_from_prime_pi(self):
        from sage.all import prime_pi

        from funbuns.core import PPBatchFeeder

        feeder = PPBatchFeeder(init_p=29, num_primes=20, batch_size=10)
        batches = list(feeder.generate_batches())
        expected_start = int(prime_pi(29))
        assert batches[0][0] == expected_start

    def test_indivisible_raises(self):
        from funbuns.core import PPBatchFeeder

        with pytest.raises(ValueError, match="divisible"):
            PPBatchFeeder(init_p=2, num_primes=100, batch_size=7)
