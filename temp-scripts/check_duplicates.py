#!/usr/bin/env python3
import polars as pl
import glob

def check_for_duplicate_partitions():
    """Check for duplicate partitions in the data."""
    pattern = 'data/blocks/*.parquet'
    
    print("🔍 Checking for duplicate partitions...")
    
    # Load all data
    all_data = pl.scan_parquet(pattern).collect()
    print(f"Total rows: {all_data.height:,}")
    
    # Get actual partitions (where m_k > 0)
    actual_partitions = all_data.filter(pl.col('m_k') > 0)
    print(f"Actual partitions: {actual_partitions.height:,}")
    
    # Check for exact duplicates
    duplicates = actual_partitions.group_by(['p', 'm_k', 'n_k', 'q_k']).agg([
        pl.len().alias('count')
    ]).filter(pl.col('count') > 1)
    
    print(f"Primes with duplicate partitions: {duplicates.height:,}")
    
    if duplicates.height > 0:
        total_duplicates = duplicates.select(pl.col('count').sum()).item() - duplicates.height
        print(f"Total duplicate rows: {total_duplicates:,}")
        
        # Show some examples
        print("\n📋 Examples of primes with duplicates:")
        for row in duplicates.head(5).iter_rows(named=True):
            p, m, n, q, count = row['p'], row['m_k'], row['n_k'], row['q_k'], row['count']
            print(f"  {p:,} = 2^{m} + {q}^{n} (appears {count} times)")
        
        # Check if this affects the partition counts
        print("\n🔢 Impact on partition counts:")
        unique_partitions = actual_partitions.unique(['p', 'm_k', 'n_k', 'q_k'])
        print(f"Unique partitions: {unique_partitions.height:,}")
        
        # Recalculate partition counts without duplicates
        correct_counts = unique_partitions.group_by('p').agg([
            pl.len().alias('partition_count')
        ]).group_by('partition_count').agg([
            pl.len().alias('prime_count')
        ]).sort('partition_count')
        
        print("\n📊 Corrected partition distribution (without duplicates):")
        for row in correct_counts.iter_rows(named=True):
            pc = row['partition_count']
            count = row['prime_count']
            print(f"  {pc:2d} partitions: {count:,} primes")
    
    else:
        print("✅ No duplicate partitions found!")

if __name__ == '__main__':
    check_for_duplicate_partitions()
