#!/usr/bin/env python3
import polars as pl

def check_last_block():
    """Check the last block's max prime."""
    last_block = "data/blocks/pp_b062_p733302529.parquet"
    
    print(f"🔍 Checking last block: {last_block}")
    
    df = pl.read_parquet(last_block)
    max_prime = df.select(pl.col("p").max()).item()
    min_prime = df.select(pl.col("p").min()).item()
    unique_primes = df.select(pl.col("p").n_unique()).item()
    
    print(f"📊 Last block stats:")
    print(f"  Min prime: {min_prime:,}")
    print(f"  Max prime: {max_prime:,}")
    print(f"  Unique primes: {unique_primes:,}")
    print(f"  Total rows: {df.height:,}")

if __name__ == '__main__':
    check_last_block()
