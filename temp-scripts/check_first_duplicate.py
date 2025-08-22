#!/usr/bin/env python3
import polars as pl

def check_first_duplicate():
    """Check if the first prime that had duplicates is now fixed."""
    target_prime = 530709637
    
    print(f"🔍 Checking if prime {target_prime:,} is now fixed...")
    
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
    
    if partition_groups.filter(pl.col('count') > 1).height == 0:
        print("\n✅ No duplicates found - prime is fixed!")
    else:
        print("\n❌ Duplicates still found!")

if __name__ == '__main__':
    check_first_duplicate()
