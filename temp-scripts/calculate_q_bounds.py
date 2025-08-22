#!/usr/bin/env python3
"""
Calculate the bounds for q values where q^n doesn't overflow.
"""

import math
import polars as pl
from pathlib import Path

def calculate_q_bounds():
    """Calculate the maximum q values for different powers before 64-bit overflow."""
    
    max_64bit = 2**63 - 1  # Signed 64-bit max
    max_64bit_unsigned = 2**64 - 1  # Unsigned 64-bit max
    
    print("🔢 64-bit Integer Limits:")
    print(f"  Signed 64-bit max:   {max_64bit:,}")
    print(f"  Unsigned 64-bit max: {max_64bit_unsigned:,}")
    
    print("\n📊 Maximum q values for q^n < 2^64:")
    
    bounds = {}
    for n in range(1, 21):
        max_q_signed = int(max_64bit ** (1/n))
        max_q_unsigned = int(max_64bit_unsigned ** (1/n))
        bounds[n] = (max_q_signed, max_q_unsigned)
        
        if n <= 10:  # Show first 10 powers
            print(f"  q^{n:2} < 2^64: q ≤ {max_q_unsigned:,} (signed: {max_q_signed:,})")
    
    print(f"\n🎯 Key Bounds:")
    print(f"  q^2 safe for q ≤ {bounds[2][1]:,}")
    print(f"  q^3 safe for q ≤ {bounds[3][1]:,}")
    print(f"  q^4 safe for q ≤ {bounds[4][1]:,}")
    print(f"  q^5 safe for q ≤ {bounds[5][1]:,}")
    
    return bounds

def analyze_current_q_values():
    """Analyze the q values in your current partition data."""
    
    data_dir = Path('data')
    pparts_file = data_dir / 'pparts.parquet'
    
    if not pparts_file.exists():
        print("❌ No partition data found")
        return None
    
    print(f"\n📊 Analyzing your current q values...")
    
    # Load partition data
    df = pl.read_parquet(pparts_file)
    partitions = df.filter(pl.col('m_k') > 0)
    
    print(f"  Total partitions: {len(partitions):,}")
    print(f"  Prime range: {df['p'].min():,} to {df['p'].max():,}")
    
    # Analyze q value distribution
    q_stats = partitions.select([
        pl.col('q_k').min().alias('min_q'),
        pl.col('q_k').max().alias('max_q'),
        pl.col('q_k').n_unique().alias('unique_q'),
        pl.col('q_k').mean().alias('mean_q'),
        pl.col('q_k').median().alias('median_q')
    ])
    
    min_q = q_stats['min_q'].item()
    max_q = q_stats['max_q'].item()
    unique_q = q_stats['unique_q'].item()
    mean_q = q_stats['mean_q'].item()
    median_q = q_stats['median_q'].item()
    
    print(f"\n🔍 Your q value statistics:")
    print(f"  Min q: {min_q:,}")
    print(f"  Max q: {max_q:,}")
    print(f"  Unique q values: {unique_q:,}")
    print(f"  Mean q: {mean_q:,.1f}")
    print(f"  Median q: {median_q:,.1f}")
    
    # Show q value percentiles
    percentiles = [50, 90, 95, 99, 99.9]
    print(f"\n📊 Q value percentiles:")
    for p in percentiles:
        q_p = partitions.select(pl.col('q_k').quantile(p/100)).item()
        print(f"  {p:4.1f}%: q ≤ {q_p:,}")
    
    return {
        'min_q': min_q,
        'max_q': max_q,
        'unique_q': unique_q,
        'mean_q': mean_q,
        'median_q': median_q
    }

def analyze_overflow_impact(bounds, q_stats):
    """Analyze what percentage of your q values would have overflow issues."""
    
    if not q_stats:
        return
    
    max_q = q_stats['max_q']
    
    print(f"\n🎯 Overflow Impact Analysis:")
    print(f"  Your max q value: {max_q:,}")
    
    for n in [2, 3, 4, 5]:
        max_safe_q = bounds[n][1]  # Unsigned bound
        
        if max_q <= max_safe_q:
            status = "✅ All your q values are safe"
        else:
            status = f"⚠️  Some q > {max_safe_q:,} would overflow"
        
        print(f"  q^{n}: {status}")
    
    # Load data to check specific cases
    data_dir = Path('data')
    pparts_file = data_dir / 'pparts.parquet'
    df = pl.read_parquet(pparts_file)
    partitions = df.filter(pl.col('m_k') > 0)
    
    print(f"\n🔍 Specific overflow analysis:")
    for n in [2, 3, 4, 5]:
        max_safe_q = bounds[n][1]
        
        # Count how many partitions use this n value
        n_cases = partitions.filter(pl.col('n_k') == n)
        if len(n_cases) == 0:
            print(f"  n={n}: No cases in your data")
            continue
        
        # Count how many would overflow
        overflow_cases = n_cases.filter(pl.col('q_k') > max_safe_q)
        
        safe_percent = (len(n_cases) - len(overflow_cases)) / len(n_cases) * 100
        
        print(f"  n={n}: {len(n_cases):,} cases, {safe_percent:.1f}% safe (max q in data: {n_cases['q_k'].max():,})")

if __name__ == "__main__":
    # Calculate theoretical bounds
    bounds = calculate_q_bounds()
    
    # Analyze your current data
    q_stats = analyze_current_q_values()
    
    # Analyze overflow impact
    analyze_overflow_impact(bounds, q_stats)
