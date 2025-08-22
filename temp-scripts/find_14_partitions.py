#!/usr/bin/env python3
import polars as pl
import glob

def find_first_prime_with_14_partitions():
    """Find the first prime with exactly 14 partitions."""
    # Load all block data
    pattern = 'data/blocks/*.parquet'
    files = glob.glob(pattern)
    print(f'Loading {len(files)} block files...')
    
    # Find primes with exactly 14 partitions
    all_data = pl.scan_parquet(pattern).collect()
    print(f'Total data loaded: {all_data.height:,} rows')
    
    # Get actual partitions (where m_k > 0)
    actual_partitions = all_data.filter(pl.col('m_k') > 0)
    
    # Count partitions per prime
    partition_counts = actual_partitions.group_by('p').agg([
        pl.len().alias('partition_count')
    ]).filter(pl.col('partition_count') == 14).sort('p')
    
    print(f'Primes with exactly 14 partitions: {partition_counts.height}')
    if partition_counts.height > 0:
        first_prime_14 = partition_counts.head(1).to_dicts()[0]
        prime_value = first_prime_14['p']
        print(f'First prime with 14 partitions: {prime_value:,}')
        
        # Show the actual partitions for this prime
        prime_partitions = actual_partitions.filter(pl.col('p') == prime_value).sort(['m_k', 'n_k', 'q_k'])
        print(f'\nPartitions for prime {prime_value:,}:')
        for row in prime_partitions.iter_rows(named=True):
            print(f'  {row["p"]} = 2^{row["m_k"]} + {row["q_k"]}^{row["n_k"]}')
        
        # Also show which block file contains this prime
        for file in sorted(files):
            block_data = pl.read_parquet(file)
            if block_data.filter(pl.col('p') == prime_value).height > 0:
                print(f'\nFound in block file: {file}')
                break
    else:
        print('No primes found with exactly 14 partitions')

if __name__ == '__main__':
    find_first_prime_with_14_partitions()
