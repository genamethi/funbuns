"""
Core implementation of prime power partition algorithm: p = 2^m + q^n

For each prime p:
  - Compute max_m = floor(log2(p))
  - For each m in [1, max_m], check if r = p - 2^m is a prime power
  - Record (p, m, exponent, base) for each hit; (p, 0, 0, 0) if none

Workers receive (start_idx, count) as 0-indexed prime indices. Each worker
resolves its range via P.unrank and generates primes locally. The feeder
does no prime generation — just one prime_pi call then arithmetic.

Interrupt handling:
  - Workers ignore SIGINT so they finish cleanly.
  - First Ctrl-C: stop dispatching, drain in-flight workers, flush, exit.
  - Second Ctrl-C: abandon in-flight, flush collected results, exit.
  Both paths emit a shutdown event and print a resume command.
"""

from sage.all import prime_range, Primes, prime_pi
import multiprocessing as mp
import polars as pl
import numpy as np
import signal
import time as _time
from .utils import PARTITION_SCHEMA, PARTITION_DISTRIBUTION  # noqa: F401

try:
    mp.set_start_method('spawn')
except RuntimeError:
    pass  # already set (e.g. by test runner or prior import)


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


# TODO: Covering system pre-screening (research in progress)
#
# The Erdős-type covering system structure (see project_covering_systems.md)
# implies that for many m values, q_cand = p - 2^m is divisible by small
# primes determined by (p mod M, m mod ord(2, ℓ)) for a modulus M.
# The {3, 5, 7} backbone alone covers all of Z/12Z, and the mod-255255
# classifier identifies 22 unconditional obstruction classes.
#
# This structure could allow skipping is_prime_power() calls for m values
# where the covering system already determines the outcome. The exact
# mechanism (CRT-based residue lookup, precomputed bitmasks, or direct
# modular arithmetic) is TBD pending further mathematical analysis.
# See: sage.arith.misc (CRT, multiplicative_order, etc.)


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
                pb = int(pbase)
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
    P = Primes()
    first_prime = P.unrank(start_idx)
    end_prime = P.unrank(start_idx + count)  # exclusive bound
    primes = prime_range(int(first_prime), int(end_prime))

    processor = PPBatchProcessor()
    result_array = processor.process_batch(primes)

    if result_array.size > 0:
        return result_array
    return None


class PPBatchFeeder:
    """Compute batch boundaries as index slices — no prime generation."""

    def __init__(self, init_p: int, num_primes: int, batch_size: int, verbose: bool = False):
        if num_primes % batch_size != 0:
            raise ValueError(f"num_primes ({num_primes}) must be divisible by batch_size ({batch_size})")

        self.batch_size = batch_size
        self.num_batches = num_primes // batch_size
        # Single call: 0-indexed position of the first prime to process
        self.start_idx = int(prime_pi(init_p))

        if verbose:
            print(f"Start index: {self.start_idx} (prime_pi({init_p}))")

    def generate_batches(self):
        """Yield (start_idx, count) tuples — pure arithmetic, no prime generation."""
        for i in range(self.num_batches):
            yield (self.start_idx + i * self.batch_size, self.batch_size)


class PPConsumer:
    """Collects DataFrames and flushes to disk incrementally.

    Each buffered DataFrame is written as its own parquet file on flush,
    avoiding a concat that would temporarily double memory usage.
    """

    def __init__(self, buffer_size: int, save_callback, memory_pct_limit: float = 0.50):
        self.buffer_size = buffer_size
        self.save_callback = save_callback
        self.memory_pct_limit = memory_pct_limit
        self.df_buffer: list[pl.DataFrame] = []
        self.result_count = 0
        self._last_pressure_check = 0.0

    def _memory_pressure(self) -> bool:
        now = _time.monotonic()
        if now - self._last_pressure_check < 1.0:
            return False
        self._last_pressure_check = now
        try:
            import psutil
            return psutil.virtual_memory().percent / 100.0 > self.memory_pct_limit
        except Exception:
            return False

    def add_results(self, results_df: pl.DataFrame):
        if results_df is not None and results_df.height > 0:
            self.df_buffer.append(results_df)
            self.result_count += results_df.height

            if self.result_count >= self.buffer_size or self._memory_pressure():
                self._flush_results()

    def _flush_results(self):
        """Write each buffered DataFrame individually (no concat)."""
        if not self.df_buffer:
            return
        for df in self.df_buffer:
            self.save_callback(df)
        self.df_buffer = []
        self.result_count = 0

    def finalize(self):
        self._flush_results()


class PPManager:
    """Coordinates parallel prime power partition computation.

    Returns a status dict from run_gen() with ``interrupted`` (bool)
    and ``primes_not_processed`` so the caller can print a resume command.
    """

    def __init__(self, init_p, num_primes, batch_size, cores, buffer_size, append_data, verbose=False):
        self.init_p = init_p or 2
        self.num_primes = num_primes
        self.batch_size = batch_size
        self.cores = cores
        self.buffer_size = buffer_size
        self.append_data = append_data
        self.verbose = verbose
        self.batches_processed = 0
        self.primes_processed = 0

    def run_gen(self):
        global _interrupt_count
        from tqdm import tqdm

        feeder = PPBatchFeeder(self.init_p, self.num_primes, self.batch_size, self.verbose)
        consumer = PPConsumer(self.buffer_size, self.append_data)

        print(f"Processing {self.num_primes} primes starting from {self.init_p}")
        print(f"Batch size: {self.batch_size}, workers: {self.cores}, "
              f"flush buffer: {self.buffer_size}")

        batches = list(feeder.generate_batches())
        total_batches = len(batches)

        # Install signal handler; workers will ignore SIGINT via initializer
        _interrupt_count = 0
        old_handler = signal.signal(signal.SIGINT, _sigint_handler)

        pool = mp.Pool(self.cores, initializer=_worker_ignore_sigint)
        interrupted = False
        abandoned = False

        try:
            self.batches_processed = 0
            self.primes_processed = 0

            # Sliding window: keep at most self.cores tasks in-flight
            pending = []  # list of AsyncResult
            next_idx = 0

            # Seed the pipeline
            seed_count = min(self.cores, total_batches)
            for i in range(seed_count):
                pending.append(pool.apply_async(worker_batch, batches[i]))
            next_idx = seed_count

            cols = list(PARTITION_SCHEMA.keys())
            pbar = tqdm(total=self.num_primes, desc="Prime partition",
                        unit="prime", smoothing=0)

            # Rolling 60s throughput window (complements tqdm's cumulative avg)
            _rate_window_start = _time.monotonic()
            _rate_window_primes = 0

            while pending:
                if _interrupt_count >= 2:
                    abandoned = True
                    interrupted = True
                    break

                # Drain ALL ready results in one pass
                ready_indices = [i for i in range(len(pending)) if pending[i].ready()]

                if not ready_indices:
                    _time.sleep(0.05)  # 20 Hz poll — no busy-wait
                    continue

                # Pop from end first to keep indices stable
                ready_results = [pending.pop(i) for i in reversed(ready_indices)]

                for ar in ready_results:
                    result_array = ar.get()

                    if result_array is not None:
                        results_df = pl.DataFrame(
                            {cols[i]: result_array[:, i] for i in range(4)},
                            schema=PARTITION_SCHEMA,
                        )
                        consumer.add_results(results_df)
                        n_results = result_array.shape[0]
                    else:
                        n_results = 0

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

                # Refill the pipeline with as many new tasks as we just drained
                if _interrupt_count == 0:
                    for _ in range(len(ready_results)):
                        if next_idx < total_batches:
                            pending.append(
                                pool.apply_async(worker_batch, batches[next_idx])
                            )
                            next_idx += 1
                elif not interrupted:
                    interrupted = True

            pbar.close()

        finally:
            if abandoned:
                pool.terminate()
            else:
                pool.close()
            pool.join()

            # Restore original handler before any further work
            signal.signal(signal.SIGINT, old_handler)

            # Flush whatever we collected
            consumer.finalize()

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
