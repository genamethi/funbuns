"""Tests for core.py: PPBatchProcessor, worker_batch, PPBatchFeeder."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import polars as pl
import pytest

from funbuns.utils import PARTITION_SCHEMA
from funbuns import VERSION


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
            result = processor.process_batch([p], expected_count=1)
            assert result.processed_count == 1, f"No result for p={p}"
            assert int(result.prime_p[0]) == int(p)
            assert int(result.prime_k[0]) == result.decomp_count

            for row_idx in range(result.decomp_count):
                p_val = int(result.decomp_p[row_idx])
                m_k = int(result.decomp_m[row_idx])
                n_k = int(result.decomp_n[row_idx])
                q_k = int(result.decomp_q[row_idx])
                assert p_val == int(p)
                assert 2**m_k + q_k**n_k == p_val, (
                    f"Identity failed: 2^{m_k} + {q_k}^{n_k} != {p_val}"
                )
                assert Integer(q_k).is_prime(proof=True), (
                    f"q_k={q_k} is not prime"
                )


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
            result = processor.process_batch([Integer(p)], expected_count=1)
            assert result.processed_count == 1, (
                f"Expected 1 prime row for obstructed p={p}, got {result.processed_count}"
            )
            assert int(result.prime_p[0]) == p
            assert int(result.prime_k[0]) == 0
            assert result.decomp_count == 0

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
        from sage.all import nth_prime, prime_range

        from funbuns.core import worker_batch

        start_idx = 10
        count = 20
        result = worker_batch(start_idx, count)

        # worker_batch returns one prime-table row per processed prime.
        assert result is not None
        result_primes = sorted(set(result.prime_p.tolist()))

        # nth_prime is 1-indexed, matching worker_batch's start_idx semantics.
        expected_primes = list(prime_range(int(nth_prime(start_idx)), int(nth_prime(start_idx + count))))

        # Every prime in the range should appear in output
        for ep in expected_primes:
            assert ep in result_primes, f"Prime {ep} missing from worker_batch output"


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
        # nth_prime is 1-indexed, so feeder yields prime_pi(init_p) + 1
        # as the rank of the first prime to process.
        expected_start = int(prime_pi(29)) + 1
        assert batches[0][0] == expected_start

    def test_indivisible_raises(self):
        from funbuns.core import PPBatchFeeder

        with pytest.raises(ValueError, match="divisible"):
            PPBatchFeeder(init_p=2, num_primes=100, batch_size=7)


@pytest.mark.sage
class TestPPManagerShapedAppend:
    """PPManager should hand shaped prime/decomposition frames to append_data."""

    def test_manager_appends_shaped_frames(self):
        from funbuns.core import PPManager

        frames = []

        def append_data(primes_df, decomp_df, **kwargs):
            frames.append((primes_df, decomp_df))

        manager = PPManager(
            init_p=2,
            num_primes=20,
            batch_size=10,
            cores=1,
            append_data=append_data,
        )
        status = manager.run_gen()

        assert status["primes_processed"] == 20
        assert len(frames) == 2
        assert sum(primes.height for primes, _ in frames) == 20
        assert all(
            primes.schema == {"p": pl.Int64, "k": pl.Int32}
            for primes, _ in frames
        )
        assert all(
            decomp.schema == {
                "p": pl.Int64,
                "m_k": pl.Int32,
                "n_k": pl.Int32,
                "q_k": pl.Int64,
            }
            for _, decomp in frames
        )
        assert sum(int(primes["k"].sum()) for primes, _ in frames) == sum(
            decomp.height for _, decomp in frames
        )


@pytest.mark.slow
@pytest.mark.xfail(
    condition=VERSION < (1, 1, 0),
    reason="No signal handling in PPManager — SIGINT kills workers abruptly, "
           "loses in-flight results, no cleanup",
    strict=True,
)
class TestGracefulSIGINT:
    """X1: ctrl+c should cleanly terminate workers and flush completed results.

    Desired behavior:
    - Workers receive SIG_IGN (via pool initializer), so they don't crash
    - Main process catches SIGINT, calls pool.terminate(), flushes consumer
    - Any batches already returned by imap_unordered are flushed to disk
    - Process exits 0 (not KeyboardInterrupt traceback)
    - No zombie worker processes remain
    """

    @pytest.mark.skip(
        reason="Tests legacy append_data/runs-dir flush semantics removed in "
               "the iceberg ingest cutover. Also unsafely writes to the "
               "production iceberg catalog because iceberg_dir is not "
               "sandboxed. Needs rewrite against a --temp warehouse polling "
               "tbl.current_snapshot(); see task #6."
    )
    def test_sigint_flushes_completed_results(self, tmp_path):
        """After SIGINT, completed batch results must be on disk."""
        runs_dir = tmp_path / "runs"
        runs_dir.mkdir()
        (tmp_path / "blocks").mkdir()

        # 10M primes / 1M batch = 10 batches. Each batch takes several
        # seconds, so SIGINT reliably lands mid-generation. Buffer is 1
        # so every completed batch flushes a run file immediately.
        script = f"""\
import os, sys
os.environ["FUNBUNS_DATA_DIR"] = "{tmp_path}"
sys.argv = ["funbuns", "-n", "10000000", "-b", "1000000"]
from funbuns.__main__ import main
main()
"""
        proc = subprocess.Popen(
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Wait for at least one run file (= one completed batch flush).
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if list(runs_dir.glob("*.parquet")):
                break
            if proc.poll() is not None:
                break
            time.sleep(0.5)

        # Send SIGINT while batches are still in-flight
        time.sleep(1)
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)

        stdout, stderr = proc.communicate(timeout=60)

        # 1. Clean exit (0), not a KeyboardInterrupt crash (1) or signal death (-2)
        assert proc.returncode == 0, (
            f"Expected clean exit (rc=0), got {proc.returncode}.\n"
            f"stderr: {stderr.decode()[-500:]}"
        )

        # 2. No KeyboardInterrupt traceback in output
        combined = stdout.decode() + stderr.decode()
        assert "KeyboardInterrupt" not in combined, (
            "KeyboardInterrupt traceback leaked to output — handler didn't catch it"
        )

        # 3. Completed results actually flushed to disk
        run_files = list(runs_dir.glob("*.parquet"))
        assert len(run_files) >= 1, (
            "No run files on disk after SIGINT — completed results were lost"
        )

        # 4. Flushed data is valid and non-empty
        total_rows = sum(pl.read_parquet(f).height for f in run_files)
        assert total_rows > 0, "Run files exist but contain no data"


@pytest.mark.xfail(
    condition=VERSION < (1, 1, 0),
    reason="JournalWriter not wired into generation pipeline",
    strict=True,
)
class TestGenerationJournalLogging:
    """X2: Generation pipeline should produce structured journal entries.

    Expected events: run_start (args, init_p, timestamp),
    batch_complete (count, primes_processed), flush (rows, file),
    run_end (total_primes, duration).
    """

    def test_generation_creates_journal(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        (tmp_path / "blocks").mkdir()
        (tmp_path / "runs").mkdir()
        (tmp_path / "logs").mkdir()

        with (
            patch("sys.argv", ["funbuns", "-n", "1000", "-b", "1000"]),
            patch("funbuns.__main__.PPManager") as MockManager,
        ):
            instance = MagicMock()
            MockManager.return_value = instance
            instance.run_gen.return_value = None

            from funbuns.__main__ import main
            main()

        journal = tmp_path / "logs" / "funbuns.jsonl"
        assert journal.exists(), "No journal file created"

        entries = [json.loads(line) for line in journal.read_text().strip().split("\n")]
        events = [e["event"] for e in entries]
        assert "run_start" in events
        assert "run_end" in events
