#!/usr/bin/env python3
import polars as pl
import glob

def investigate_duplication():
    """Investigate the duplication issue in block 56."""
    
    print("🔍 Investigating duplication in block 56...")
    
    # Check block 56 specifically
    block_56 = "data/blocks/pp_b056_p567393859.parquet"
    
    if not pl.scan_parquet(block_56).collect().height:
        print(f"❌ Block 56 not found: {block_56}")
        return
    
    # Load block 56 data
    block_data = pl.read_parquet(block_56)
    print(f"📦 Block 56: {block_data.height:,} rows")
    
    # Find the first prime with duplicates in this block
    actual_partitions = block_data.filter(pl.col('m_k') > 0)
    
    # Check for duplicates
    duplicates = actual_partitions.group_by(['p', 'm_k', 'n_k', 'q_k']).agg([
        pl.len().alias('count')
    ]).filter(pl.col('count') > 1).sort('p')
    
    if duplicates.height == 0:
        print("✅ No duplicates found in block 56")
        return
    
    print(f"🎯 Found {duplicates.height} primes with duplicates in block 56")
    
    # Show the first few duplicates
    print("\n📋 First few primes with duplicates:")
    for row in duplicates.head(5).iter_rows(named=True):
        p, m, n, q, count = row['p'], row['m_k'], row['n_k'], row['q_k'], row['count']
        print(f"  {p:,} = 2^{m} + {q}^{n} (appears {count} times)")
    
    # Check if the issue is related to the append operation
    # Look for primes that appear in both the original block and appended data
    print("\n🔍 Checking for append-related duplication...")
    
    # Get the range of primes in this block
    min_prime = block_data.select(pl.col('p').min()).item()
    max_prime = block_data.select(pl.col('p').max()).item()
    print(f"  Prime range: {min_prime:,} to {max_prime:,}")
    
    # Check if there are any run files that might have been appended
    run_files = glob.glob("data/runs/*.parquet")
    if run_files:
        print(f"  Found {len(run_files)} run files")
        
        # Check if any run files contain primes in this range
        for run_file in run_files:
            try:
                run_data = pl.read_parquet(run_file)
                run_min = run_data.select(pl.col('p').min()).item()
                run_max = run_data.select(pl.col('p').max()).item()
                
                # Check for overlap
                if run_min <= max_prime and run_max >= min_prime:
                    print(f"    📁 {run_file}: primes {run_min:,} to {run_max:,} (OVERLAP!)")
                    
                    # Check for specific duplicates
                    overlap_data = run_data.filter(
                        (pl.col('p') >= min_prime) & (pl.col('p') <= max_prime)
                    )
                    
                    if overlap_data.height > 0:
                        print(f"      Contains {overlap_data.height:,} rows in block 56 range")
                        
                        # Check for exact duplicates
                        overlap_partitions = overlap_data.filter(pl.col('m_k') > 0)
                        overlap_duplicates = overlap_partitions.group_by(['p', 'm_k', 'n_k', 'q_k']).agg([
                            pl.len().alias('count')
                        ]).filter(pl.col('count') > 1)
                        
                        if overlap_duplicates.height > 0:
                            print(f"      Contains {overlap_duplicates.height} primes with duplicates")
                else:
                    print(f"    📁 {run_file}: primes {run_min:,} to {run_max:,}")
            except Exception as e:
                print(f"    📁 {run_file}: Error reading ({e})")
    
    # Check the specific prime 530,709,637 more closely
    target_prime = 530709637
    prime_entries = block_data.filter(pl.col('p') == target_prime)
    
    print(f"\n🔍 Detailed analysis of prime {target_prime:,}:")
    print(f"  Total entries: {prime_entries.height}")
    
    # Show all entries for this prime
    for i, row in enumerate(prime_entries.iter_rows(named=True)):
        print(f"    Entry {i+1}: p={row['p']}, m_k={row['m_k']}, n_k={row['n_k']}, q_k={row['q_k']}")
    
    # Check if this prime appears in any run files
    print(f"\n🔍 Checking if {target_prime:,} appears in run files:")
    for run_file in run_files:
        try:
            run_data = pl.read_parquet(run_file)
            if run_data.filter(pl.col('p') == target_prime).height > 0:
                print(f"  📁 Found in {run_file}")
                run_entries = run_data.filter(pl.col('p') == target_prime)
                for row in run_entries.iter_rows(named=True):
                    print(f"    Run entry: p={row['p']}, m_k={row['m_k']}, n_k={row['n_k']}, q_k={row['q_k']}")
        except Exception as e:
            print(f"  📁 {run_file}: Error reading ({e})")

if __name__ == '__main__':
    investigate_duplication()
