#!/usr/bin/env python
"""Benchmark core.py prime processing: 10M primes, batch_size=100k.

Writes to a unique temp dir. Reports wall time and throughput.
"""
import time
import sys
import shutil
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def main():
    from funbuns.core import PPManager
    from funbuns.utils import get_data_dir
    import psutil
    import polars as pl

    NUM_PRIMES = 10_000_000
    BATCH_SIZE = 100_000

    BENCH_DIR = Path(__file__).resolve().parents[1] / "data" / f"bench_{uuid.uuid4().hex[:8]}"
    BENCH_DIR.mkdir(parents=True, exist_ok=True)

    file_counter = [0]

    def bench_append(df):
        file_counter[0] += 1
        path = BENCH_DIR / f"b_{file_counter[0]:04d}.parquet"
        df.write_parquet(str(path))

    cores = psutil.cpu_count(logical=False)

    print(f"=== Core.py Baseline Benchmark ===")
    print(f"Primes:     {NUM_PRIMES:>12,}")
    print(f"Batch size: {BATCH_SIZE:>12,}")
    print(f"Workers:    {cores:>12}")
    print(f"Output:     {BENCH_DIR}")
    print()

    manager = PPManager(
        init_p=2,
        num_primes=NUM_PRIMES,
        batch_size=BATCH_SIZE,
        cores=cores,
        buffer_size=BATCH_SIZE * 2,
        append_data=bench_append,
        verbose=True,
    )

    t0 = time.perf_counter()
    manager.run_gen()
    t1 = time.perf_counter()

    elapsed = t1 - t0
    throughput = NUM_PRIMES / elapsed

    print(f"\n=== Results ===")
    print(f"Wall time:   {elapsed:>10.2f} s")
    print(f"Throughput:  {throughput:>10.0f} primes/s")
    print(f"Files:       {file_counter[0]}")

    total_rows = 0
    for f in sorted(BENCH_DIR.glob("b_*.parquet")):
        total_rows += pl.scan_parquet(str(f)).select(pl.len()).collect().item()
    print(f"Total rows:  {total_rows:>10,}")

    print(f"\nCleaning up {BENCH_DIR}...")
    shutil.rmtree(BENCH_DIR)
    print("Done.")


if __name__ == "__main__":
    main()
