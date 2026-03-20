"""
Core implementation of prime power partition algorithm: p = 2^m + q^n

For each prime p:
  - Compute max_m = floor(log2(p))
  - For each m in [1, max_m], check if r = p - 2^m is a prime power
  - Record (p, m, exponent, base) for each hit; (p, 0, 0, 0) if none

Workers receive (start_idx, count) as 0-indexed prime indices. Each worker
resolves its range via P.unrank and generates primes locally. The feeder
does no prime generation — just one prime_pi call then arithmetic.
"""

from sage.all import prime_range, Primes, prime_pi
import polars as pl
import numpy as np
from .utils import PARTITION_SCHEMA, PARTITION_DISTRIBUTION  # noqa: F401


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


def worker_batch(start_idx: int, count: int) -> pl.DataFrame:
    """Module-level worker: generate primes locally, process, return DataFrame.

    Receives (start_idx, count) — 0-indexed prime indices. Only two
    integers cross the process boundary instead of a pickled list.
    Each worker uses P.unrank to resolve its index range to primes.
    """
    P = Primes()
    first_prime = P.unrank(start_idx)
    end_prime = P.unrank(start_idx + count)  # exclusive bound
    primes = prime_range(int(first_prime), int(end_prime))

    processor = PPBatchProcessor()
    result_array = processor.process_batch(primes)

    if result_array.size > 0:
        return pl.DataFrame(result_array, schema=PARTITION_SCHEMA, orient='row')
    return pl.DataFrame(schema=PARTITION_SCHEMA)


def _worker_star(args):
    """Unpack (start_idx, count) tuple for imap_unordered."""
    return worker_batch(*args)


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

    def _memory_pressure(self) -> bool:
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
    """Coordinates parallel prime power partition computation."""

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
        import multiprocessing as mp
        from tqdm import tqdm

        feeder = PPBatchFeeder(self.init_p, self.num_primes, self.batch_size, self.verbose)
        consumer = PPConsumer(self.buffer_size, self.append_data)

        print(f"Processing {self.num_primes} primes starting from {self.init_p}")
        print(f"Batch size: {self.batch_size}, workers: {self.cores}")

        mp.set_start_method('spawn', force=True)

        with mp.Pool(self.cores) as pool:
            self.batches_processed = 0
            self.primes_processed = 0

            # imap_unordered: all workers busy, results yielded as they complete
            batches = list(feeder.generate_batches())
            with tqdm(total=self.num_primes, desc="Prime partition", unit="prime") as pbar:
                for results_df in pool.imap_unordered(
                    _worker_star, batches
                ):
                    consumer.add_results(results_df)

                    self.primes_processed += self.batch_size
                    self.batches_processed += 1
                    pbar.update(self.batch_size)
                    pbar.set_postfix({
                        "batches": self.batches_processed,
                        "results": results_df.height,
                    })

        consumer.finalize()

        print(f"\nCompleted {self.primes_processed} primes in {self.batches_processed} batches")

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
