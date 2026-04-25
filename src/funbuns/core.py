"""
Core implementation of prime power partition algorithm: p = 2^m + q^n

For each prime p:
  - Compute max_m = floor(log2(p))
  - For each m in [1, max_m], check if r = p - 2^m is a prime power
  - Record (p, m, exponent, base) for each hit; (p, 0, 0, 0) if none

Workers receive (start_idx, count) as 0-indexed prime indices. Each worker
resolves its range via P.unrank and generates primes locally. The feeder
does no prime generation — just one prime_pi call then arithmetic.

Concurrency:
  - `pool.imap_unordered` provides the sliding window of at-most-``cores``
    tasks in flight; no manual drain loop.
  - A background writer thread pulls DataFrames off a bounded
    ``queue.Queue`` and calls ``append_data`` (typically
    ``IcebergWriter.flush``). Backpressure falls out of the queue's
    maxsize — if writes lag, the main thread blocks on ``put``.
  - Catalog commits are deferred: ``append_data`` writes parquet only;
    the caller runs ``IcebergWriter.commit_pending`` once after
    ``run_gen`` returns.

Interrupt handling:
  - Workers ignore SIGINT so they finish cleanly.
  - First Ctrl-C: stop dispatching new batches; the input generator
    returns, imap_unordered drains in-flight results, writer thread
    drains the queue.
  - Second Ctrl-C: break out of the result loop; pool.terminate();
    writer thread still drains whatever is already queued.
"""

from sage.all import prime_range, Primes, prime_pi
import multiprocessing as mp
import polars as pl
import numpy as np
import queue
import signal
import threading
import time as _time
from .utils import PARTITION_SCHEMA, PARTITION_DISTRIBUTION  # noqa: F401

try:
    mp.set_start_method('spawn')
except RuntimeError:
    pass  # already set (e.g. by test runner or prior import)

P = Primes()
# ---------------------------------------------------------------------------
# Signal handling helpers (module-level for pickling)
# ---------------------------------------------------------------------------

_interrupt_count = 0


def _sigint_handler(signum, frame):
    """Count interrupts without raising KeyboardInterrupt."""
    global _interrupt_count
    _interrupt_count += 1
    if _interrupt_count == 1:
        print("\nInterrupt received — finishing in-flight batches. "
              "Press Ctrl-C again to abandon them.", flush=True)
    elif _interrupt_count >= 2:
        print("\nSecond interrupt — abandoning in-flight batches.", flush=True)


def _worker_ignore_sigint():
    """Pool initializer: make workers immune to SIGINT."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class PPBatchProcessor:
    """Worker class for processing prime batches using pre-allocated arrays."""

    def _process_prime_to_array(self, p):
        """Process single prime, writing results directly to pre-allocated array."""
        max_m = p.exact_log(2)
        found_partition = False
        two_i = 1
        q_hits = {}  # q_base -> count; at most 2 per (p, q) pair
        exhausted = []

        for m_i in range(1, max_m + 1):
            two_i <<= 1
            q_cand_i = p - two_i

            if exhausted and any(q_cand_i % q == 0 for q in exhausted):
                continue

            (pbase, pexp) = q_cand_i.is_prime_power(proof=False, get_data=True)

            if pexp != 0:
                if self.current_row >= len(self.results_array):
                    self._grow_array()
                self.results_array[self.current_row] = [p, m_i, pexp, pbase]
                self.current_row += 1
                found_partition = True
                pb = pbase
                q_hits[pb] = q_hits.get(pb, 0) + 1
                if q_hits[pb] >= 2:
                    exhausted.append(pb)

        if not found_partition:
            if self.current_row >= len(self.results_array):
                self._grow_array()
            self.results_array[self.current_row] = [p, 0, 0, 0]
            self.current_row += 1

    def _grow_array(self):
        """Double the array size when needed."""
        new_array = np.zeros((len(self.results_array) * 2, 4), dtype=np.int64)
        new_array[:self.current_row] = self.results_array[:self.current_row]
        self.results_array = new_array

    def process_batch(self, prime_list):
        """Process a list of primes, return results as NumPy array."""
        estimated_rows = int(len(prime_list) * PARTITION_DISTRIBUTION['avg_rows_per_prime'] * 1.2)
        self.results_array = np.zeros((estimated_rows, 4), dtype=np.int64)
        self.current_row = 0

        for prime in prime_list:
            self._process_prime_to_array(prime)

        return self.results_array[:self.current_row]


def worker_batch(start_idx: int, count: int) -> np.ndarray | None:
    """Module-level worker: generate primes locally, process, return raw array.

    Receives (start_idx, count) — 0-indexed prime indices. Only two
    integers cross the process boundary instead of a pickled list.
    Each worker uses P.unrank to resolve its index range to primes.

    Returns the raw NumPy array (int64, shape (N, 4)) to avoid Arrow IPC
    serialization overhead on the pickle boundary. The caller constructs
    the DataFrame on the main side.
    """
    first_prime = P.unrank(start_idx)
    end_prime = P.unrank(start_idx + count)  # exclusive bound
    primes = prime_range(first_prime, end_prime)

    processor = PPBatchProcessor()
    result_array = processor.process_batch(primes)

    if result_array.size > 0:
        return result_array
    return None


def _worker_star(args):
    return worker_batch(*args)


class PPBatchFeeder:
    """Compute batch boundaries as index slices — no prime generation."""

    def __init__(self, init_p: int, num_primes: int, batch_size: int, verbose: bool = False):
        if num_primes % batch_size != 0:
            raise ValueError(f"num_primes ({num_primes}) must be divisible by batch_size ({batch_size})")

        self.batch_size = batch_size
        self.num_batches = num_primes // batch_size
        # prime_pi is 1-based, P.unrank is 0-based.  prime_pi(p) fed to
        # unrank yields next_prime(p), which is correct for resume (where
        # init_p is the last *processed* prime).  For explicit -i starts,
        # the caller must pass the prime *before* the desired start.
        self.start_idx = prime_pi(init_p)

        if verbose:
            print(f"Start index: {self.start_idx} (prime_pi({init_p}))")

    def generate_batches(self):
        """Yield (start_idx, count) tuples — pure arithmetic, no prime generation."""
        for i in range(self.num_batches):
            yield (self.start_idx + i * self.batch_size, self.batch_size)


def _writer_loop(q: "queue.Queue", save_callback) -> None:
    while True:
        df = q.get()
        if df is None:
            return
        save_callback(df)


class PPManager:
    """Coordinates parallel prime power partition computation.

    Returns a status dict from run_gen() with ``interrupted`` (bool)
    and ``primes_not_processed`` so the caller can print a resume command.
    """

    def __init__(self, init_p, num_primes, batch_size, cores, append_data, verbose=False):
        self.init_p = init_p or 2
        self.num_primes = num_primes
        self.batch_size = batch_size
        self.cores = cores
        self.append_data = append_data
        self.verbose = verbose
        self.batches_processed = 0
        self.primes_processed = 0

    def run_gen(self):
        global _interrupt_count
        from tqdm import tqdm

        feeder = PPBatchFeeder(self.init_p, self.num_primes, self.batch_size, self.verbose)

        print(f"Processing {self.num_primes} primes starting from {self.init_p}")
        print(f"Batch size: {self.batch_size}, workers: {self.cores}")

        batches = feeder.generate_batches
        total_batches = self.num_primes // self.batch_size 

        _interrupt_count = 0
        old_handler = signal.signal(signal.SIGINT, _sigint_handler)

        pool = mp.Pool(self.cores, initializer=_worker_ignore_sigint)
        interrupted = False
        abandoned = False

        writer_q: "queue.Queue" = queue.Queue(maxsize=2 * self.cores)
        writer_thread = threading.Thread(
            target=_writer_loop,
            args=(writer_q, self.append_data),
            daemon=True,
        )
        writer_thread.start()

        cols = list(PARTITION_SCHEMA.keys())
        pbar = tqdm(total=self.num_primes, desc="Prime partition",
                    unit="prime", smoothing=0)
        _rate_window_start = _time.monotonic()
        _rate_window_primes = 0

        def _batch_iter():
            # Stop dispatching after first Ctrl-C; in-flight results still
            # drain through imap_unordered.
            for b in batches():
                if _interrupt_count >= 1:
                    return
                yield b

        try:
            self.batches_processed = 0
            self.primes_processed = 0

            for arr in pool.imap_unordered(_worker_star, _batch_iter()):
                if _interrupt_count >= 2:
                    abandoned = True
                    interrupted = True
                    break

                if arr is not None and arr.size > 0:
                    df = pl.DataFrame(
                        {cols[i]: arr[:, i] for i in range(4)},
                        schema=PARTITION_SCHEMA,
                    )
                    writer_q.put(df)

                self.primes_processed += self.batch_size
                self.batches_processed += 1
                _rate_window_primes += self.batch_size
                pbar.update(self.batch_size)

                now = _time.monotonic()
                window_elapsed = now - _rate_window_start
                if window_elapsed >= 60.0:
                    _rate_window_start = now
                    _rate_window_primes = 0
                    window_elapsed = 0.001
                pbar.set_postfix_str(
                    f"{self.batches_processed}/{total_batches} batches | "
                    f"{_rate_window_primes / max(window_elapsed, 0.001):.0f} prime/s (60s)"
                )

                if _interrupt_count >= 1:
                    interrupted = True

            pbar.close()
    
        finally:
            if abandoned:
                pool.terminate()
            else:
                pool.close()
            pool.join()

            # Drain the writer thread regardless of abandon — parquet that
            # already landed on disk is durable and worth committing.
            writer_q.put(None)
            writer_thread.join()

            signal.signal(signal.SIGINT, old_handler)

        remaining = self.num_primes - self.primes_processed

        if interrupted:
            print(f"\nShutdown: processed {self.primes_processed:,} of "
                  f"{self.num_primes:,} primes "
                  f"({self.batches_processed}/{total_batches} batches)"
                  + (" [abandoned in-flight]" if abandoned else " [drained in-flight]"))
        else:
            print(f"\nCompleted {self.primes_processed:,} primes "
                  f"in {self.batches_processed} batches")

        return {
            'interrupted': interrupted,
            'abandoned': abandoned,
            'primes_processed': self.primes_processed,
            'primes_not_processed': remaining,
            'batches_processed': self.batches_processed,
            'total_batches': total_batches,
            'init_p': self.init_p,
            'batch_size': self.batch_size,
        }

    def get_status(self):
        return {
            'init_p': self.init_p,
            'num_primes': self.num_primes,
            'batch_size': self.batch_size,
            'cores': self.cores,
            'batches_processed': self.batches_processed,
            'primes_processed': self.primes_processed,
            'progress_pct': (self.primes_processed / self.num_primes * 100) if self.num_primes > 0 else 0,
        }

    def reset(self):
        self.batches_processed = 0
        self.primes_processed = 0
