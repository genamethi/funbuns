#!/usr/bin/env python3
import polars as pl
import glob

def find_first_duplicate():
    """Find the first prime that has duplicate partitions."""
    pattern = 'data/blocks/*.parquet'
    
    print("🔍 Finding the first prime with duplicate partitions...")
    
    # Process blocks in order to find the earliest duplicate
    block_files = sorted(glob.glob(pattern))
    
    for i, block_file in enumerate(block_files):
        print(f"Checking block {i+1}/{len(block_files)}: {block_file.split('/')[-1]}")
        
        block_data = pl.read_parquet(block_file)
        actual_partitions = block_data.filter(pl.col('m_k') > 0)
        
        if actual_partitions.height == 0:
            continue
            
        # Check for duplicates in this block
        duplicates = actual_partitions.group_by(['p', 'm_k', 'n_k', 'q_k']).agg([
            pl.len().alias('count')
        ]).filter(pl.col('count') > 1).sort('p')
        
        if duplicates.height > 0:
            first_duplicate = duplicates.head(1).to_dicts()[0]
            prime_with_duplicate = first_duplicate['p']
            
            print(f"\n🎯 Found first prime with duplicates: {prime_with_duplicate:,}")
            print(f"   In block: {block_file}")
            
            # Show the duplicate partitions for this prime
            prime_partitions = actual_partitions.filter(pl.col('p') == prime_with_duplicate)
            print(f"\n📋 All partitions for {prime_with_duplicate:,}:")
            for row in prime_partitions.iter_rows(named=True):
                print(f"  {row['p']} = 2^{row['m_k']} + {row['q_k']}^{row['n_k']}")
            
            # Show which partitions are duplicated
            partition_groups = prime_partitions.group_by(['m_k', 'n_k', 'q_k']).agg([
                pl.len().alias('count')
            ]).filter(pl.col('count') > 1).sort(['m_k', 'n_k', 'q_k'])
            
            print(f"\n📊 Duplicated partitions:")
            for row in partition_groups.iter_rows(named=True):
                m, n, q, count = row['m_k'], row['n_k'], row['q_k'], row['count']
                print(f"  2^{m} + {q}^{n}: {count} times")
            
            return prime_with_duplicate
    
    print("✅ No duplicates found in any block!")
    return None

if __name__ == '__main__':
    find_first_duplicate()
