"""
Data preparation module for prime power computations.
"""

import polars as pl
from pathlib import Path
from .utils import get_data_dir


def prepare_prime_powers(n, max_power=100):
    """
    Prepare prime powers data using polars lazy expressions.
    
    Args:
        n: Generate prime powers for first n primes
        max_power: Maximum power to compute (default: 100)
        
    Returns:
        Path to saved parquet file
    """
    from sage.all import prime_range
    
    print(f"Generating prime powers for first {n} primes (powers 1-{max_power})")
    
    # Get the first n primes
    primes = list(prime_range(0, n))
    actual_count = len(primes)
    
    if actual_count == 0:
        raise ValueError(f"No primes found up to {n}")
    
    print(f"Found {actual_count} primes up to {n}, largest: {primes[-1]}")
    
    # Create LazyFrame with primes in column "1" 
    df = pl.LazyFrame({"1": primes})
    
    # Generate expressions for p^k where k = 2 to max_power
    expressions = []
    for k in range(2, max_power + 1):
        expressions.append(pl.col("1").pow(k).alias(str(k)))
    
    # Add all power columns using lazy evaluation
    df = df.with_columns(expressions)
    
    # Save to data directory
    data_dir = get_data_dir()
    data_dir.mkdir(exist_ok=True)
    
    output_file = data_dir / f"prime_powers_{actual_count}p_{max_power}pow.parquet"
    
    print(f"Computing and saving to {output_file}...")
    
    # Collect and save (this is where computation actually happens)
    df.collect().write_parquet(output_file)
    
    print(f"Prime powers data saved: {output_file}")
    print(f"Columns: {actual_count} primes × {max_power} powers = {actual_count * max_power} total values")
    
    return output_file
