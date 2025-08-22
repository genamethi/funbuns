#!/usr/bin/env python3
import polars as pl
import glob

def check_specific_prime():
    """Check the specific prime 530,720,627 for duplicates."""
    target_prime = 530720627
    
    print(f"🔍 Checking prime {target_prime:,} for duplicates...")
    
    # Load all data
    pattern = 'data/blocks/*.parquet'
    all_data = pl.scan_parquet(pattern).collect()
    
    # Get all entries for this prime
    prime_entries = all_data.filter(pl.col('p') == target_prime)
    print(f"Total entries for {target_prime:,}: {prime_entries.height}")
    
    # Show all entries
    print("\n📋 All entries:")
    for row in prime_entries.iter_rows(named=True):
        print(f"  p={row['p']}, m_k={row['m_k']}, n_k={row['n_k']}, q_k={row['q_k']}")
    
    # Check for duplicates
    actual_partitions = prime_entries.filter(pl.col('m_k') > 0)
    print(f"\nActual partitions: {actual_partitions.height}")
    
    # Group by partition values
    partition_groups = actual_partitions.group_by(['m_k', 'n_k', 'q_k']).agg([
        pl.len().alias('count')
    ]).sort(['m_k', 'n_k', 'q_k'])
    
    print("\n📊 Partition groups:")
    for row in partition_groups.iter_rows(named=True):
        m, n, q, count = row['m_k'], row['n_k'], row['q_k'], row['count']
        print(f"  2^{m} + {q}^{n}: {count} times")
    
    # Check if this is a widespread issue by sampling a few other primes
    print("\n🔍 Sampling other primes for duplicates...")
    sample_primes = [1000003, 10000019, 100000007, 1000000007]
    
    for sample_p in sample_primes:
        sample_entries = all_data.filter(pl.col('p') == sample_p)
        if sample_entries.height > 0:
            sample_partitions = sample_entries.filter(pl.col('m_k') > 0)
            unique_partitions = sample_partitions.unique(['m_k', 'n_k', 'q_k'])
            print(f"  {sample_p:,}: {sample_partitions.height} partitions, {unique_partitions.height} unique")
        else:
            print(f"  {sample_p:,}: Not found in data")

if __name__ == '__main__':
    check_specific_prime()
