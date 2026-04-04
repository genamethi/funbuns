#!/usr/bin/env python3
"""
Analyze the distribution of m and n values in the prime power partition data.
Enhanced with data verification and block file support.
"""

import polars as pl
from pathlib import Path
import argparse

def verify_partition_equation(df: pl.DataFrame) -> pl.DataFrame:
    """Verify that p = 2^m + q^n holds for all partitions."""
    print("🔍 Verifying partition equation: p = 2^m + q^n")
    
    # Separate 0-partition entries (m_k=0, n_k=0, q_k=0) from actual partitions
    zero_partitions = df.filter((pl.col("m_k") == 0) & (pl.col("n_k") == 0) & (pl.col("q_k") == 0))
    actual_partitions = df.filter(~((pl.col("m_k") == 0) & (pl.col("n_k") == 0) & (pl.col("q_k") == 0)))
    
    print(f"  📊 0-partition entries: {zero_partitions.height:,}")
    print(f"  📊 Actual partitions to verify: {actual_partitions.height:,}")
    
    if actual_partitions.height == 0:
        print("  ✅ No actual partitions to verify")
        return df
    
    # Calculate the right side of the equation: 2^m + q^n for actual partitions only
    verified_partitions = actual_partitions.with_columns([
        (2**pl.col("m_k") + pl.col("q_k")**pl.col("n_k")).alias("calculated_p")
    ]).with_columns([
        (pl.col("p") == pl.col("calculated_p")).alias("equation_valid")
    ])
    
    # Count valid and invalid partitions
    valid_count = verified_partitions.filter(pl.col("equation_valid")).height
    invalid_count = verified_partitions.filter(~pl.col("equation_valid")).height
    total_actual = actual_partitions.height
    
    print(f"  ✅ Valid partitions: {valid_count:,} ({valid_count/total_actual*100:.1f}%)")
    print(f"  ❌ Invalid partitions: {invalid_count:,} ({invalid_count/total_actual*100:.1f}%)")
    
    if invalid_count > 0:
        print(f"  ⚠️  Found {invalid_count} partitions where p ≠ 2^m + q^n!")
        
        # Show some examples of invalid partitions
        invalid_examples = verified_partitions.filter(~pl.col("equation_valid")).head(5)
        print(f"  📋 Examples of invalid partitions:")
        for row in invalid_examples.iter_rows(named=True):
            p, m, n, q, calculated = row["p"], row["m_k"], row["n_k"], row["q_k"], row["calculated_p"]
            print(f"    p={p}, m={m}, n={n}, q={q} → 2^{m} + {q}^{n} = {calculated} ≠ {p}")
    
    # Combine back with zero partitions
    if zero_partitions.height > 0:
        # Add calculated_p and equation_valid columns to zero partitions (with default values)
        zero_partitions = zero_partitions.with_columns([
            pl.lit(0, dtype=pl.Int64).alias("calculated_p"),
            pl.lit(True).alias("equation_valid")  # 0-partition entries are considered "valid"
        ])
        return pl.concat([verified_partitions, zero_partitions])
    else:
        return verified_partitions

def analyze_partition_data(use_blocks: bool = True, verify_data: bool = True):
    """Analyze the current partition data to show distribution of m and n values."""
    
    data_dir = Path('data')
    
    if use_blocks:
        # Use block files with glob pattern
        block_pattern = str(data_dir / "blocks" / "*.parquet")
        files = list(data_dir.glob("blocks/*.parquet"))
        
        if not files:
            print(f"❌ No block files found in {data_dir}/blocks/")
            return
        
        print(f"📊 Analyzing partition data from {len(files)} block files")
        print(f"  Pattern: {block_pattern}")
        
        # Load all data using glob pattern
        df = pl.scan_parquet(block_pattern).collect()
        
    else:
        # Use monolithic file
        pparts_file = data_dir / 'pparts.parquet'
        
        if not pparts_file.exists():
            print(f"❌ No partition data found at {pparts_file}")
            return
        
        print(f"📊 Analyzing partition data from {pparts_file}")
        df = pl.read_parquet(pparts_file)
    
    print(f"\n📈 Overall Statistics:")
    print(f"  Total rows: {len(df):,}")
    print(f"  Unique primes: {df['p'].n_unique():,}")
    print(f"  Prime range: {df['p'].min():,} to {df['p'].max():,}")
    
    # Verify data integrity if requested
    if verify_data:
        df = verify_partition_equation(df)
        print()  # Add spacing
    
    # Separate actual partitions from 0-partition entries
    actual_partitions = df.filter(pl.col('m_k') > 0)
    zero_partition_entries = df.filter((pl.col('m_k') == 0) & (pl.col('n_k') == 0) & (pl.col('q_k') == 0))
    
    # Count unique primes with partitions vs without partitions
    primes_with_partitions = actual_partitions.select('p').n_unique()
    primes_without_partitions = zero_partition_entries.select('p').n_unique()
    
    print(f"🔍 Partition Breakdown:")
    print(f"  Primes with partitions: {primes_with_partitions:,}")
    print(f"  Primes with no partitions: {primes_without_partitions:,}")
    print(f"  Partition rate: {primes_with_partitions / df.select('p').n_unique() * 100:.1f}%")
    
    if actual_partitions.height == 0:
        print("❌ No valid partitions found in the data!")
        return
    
    # Analyze m values (powers of 2)
    print(f"\n📊 Distribution of m values (powers of 2):")
    m_counts = actual_partitions.group_by('m_k').agg(pl.len().alias('count')).sort('m_k')
    for row in m_counts.iter_rows(named=True):
        m = row['m_k']
        count = row['count']
        print(f"  m = {m:2d}: {count:6,} partitions  (2^{m} = {2**m:,})")
    
    # Analyze n values (powers of q)
    print(f"\n📊 Distribution of n values (powers of q):")
    n_counts = actual_partitions.group_by('n_k').agg(pl.len().alias('count')).sort('n_k')
    for row in n_counts.iter_rows(named=True):
        n = row['n_k']
        count = row['count']
        print(f"  n = {n:2d}: {count:6,} partitions")
    
    # Check for interesting cases
    print(f"\n🔍 Special Cases:")
    
    # n > 1 cases (higher powers)
    n_gt_1 = actual_partitions.filter(pl.col('n_k') > 1)
    print(f"  n > 1 cases: {n_gt_1.height:,}")
    if n_gt_1.height > 0:
        print(f"    Max n value: {n_gt_1['n_k'].max()}")
        example = n_gt_1.head(1).to_dicts()[0]
        print(f"    Example: p = {example['p']}, m = {example['m_k']}, n = {example['n_k']}, q = {example['q_k']}")
    
    # Large m cases
    large_m = actual_partitions.filter(pl.col('m_k') >= 10)
    print(f"  m >= 10 cases: {large_m.height:,}")
    if large_m.height > 0:
        print(f"    Max m value: {large_m['m_k'].max()}")
    
    # Most common q values
    print(f"\n📊 Most common q values (prime bases):")
    q_counts = (actual_partitions
                .group_by('q_k')
                .agg(pl.len().alias('count'))
                .sort('count', descending=True)
                .head(10))
    
    for row in q_counts.iter_rows(named=True):
        q = row['q_k']
        count = row['count']
        print(f"  q = {q:6,}: {count:6,} partitions")

def main():
    parser = argparse.ArgumentParser(description="Analyze prime partition data")
    parser.add_argument("--monolithic", action="store_true", help="Use monolithic file instead of blocks")
    parser.add_argument("--no-verify", action="store_true", help="Skip data verification")
    
    args = parser.parse_args()
    
    analyze_partition_data(
        use_blocks=not args.monolithic,
        verify_data=not args.no_verify
    )

if __name__ == "__main__":
    main()
