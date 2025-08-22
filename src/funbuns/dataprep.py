"""
Data preparation module for prime power computations.
"""

import polars as pl
from pathlib import Path
from .utils import get_data_dir, get_config


def prepare_prime_powers(n=None, max_power=None, use_bounded=True):
    """
    Prepare prime powers data with intelligent overflow handling.
    Uses configuration from pixi.toml [tool.funbuns] section.
    
    Args:
        n: Generate prime powers for first n primes (default: from config largest_small_prime)
        max_power: Maximum power to compute (default: from config max_power)
        use_bounded: If True, use bounded approach (zeros for overflow)
        
    Returns:
        Path to saved parquet file
    """
    # Load configuration from pixi.toml
    config = get_config()
    
    # Use config defaults if not provided
    if n is None:
        n = config.get("lim_sm_p", 100000)
    if max_power is None:
        max_power = config.get("max_power", 64)
    
    filename = config.get("small_primes_filename", "small_primes.parquet")
    from sage.all import prime_range, Integer
    import math
    
    print(f"Generating prime powers for primes less than {n} (powers 1-{max_power})")
    if use_bounded:
        print("Using bounded approach - zeros for overflow values")
    
    # Get all primes less than the limit
    primes = list(prime_range(n))
    actual_count = len(primes)
    
    #When would this ever happen? It won't I'm commenting it out.
    #if actual_count == 0:
    #    raise ValueError(f"No primes found up to {n}")
    
    print(f"Found {actual_count} primes up to {n}, largest: {primes[-1]}")
    
    # Create LazyFrame with just the prime values (column "1")
    df = pl.LazyFrame({"1": primes}, schema={"1": pl.Int64})
    
    # Use single lazy expression to compute all powers at once
    int64_max = 2**63 - 1
    
    # Build all power expressions
    if use_bounded:
        # Create expressions for all powers with overflow protection
        power_expressions = [
            pl.when(pl.col("1").pow(k) <= int64_max)
            .then(pl.col("1").pow(k))
            .otherwise(0)
            .alias(str(k))
            for k in range(2, max_power + 1)
        ]
    else:
        # Create expressions for all powers without bounds checking
        power_expressions = [
            pl.col("1").pow(k).alias(str(k))
            for k in range(2, max_power + 1)
        ]
    
    # Apply all power expressions in a single lazy operation
    df = df.with_columns(power_expressions)
    
    # Calculate overflow statistics if using bounded approach (requires collection)
    if use_bounded:
        total_cells = len(primes) * (max_power - 1)  # Exclude column "1"
        overflow_count = df.select([
            pl.sum_horizontal([pl.col(str(k)).eq(0).sum() for k in range(2, max_power + 1)])
        ]).collect().item()
        safe_count = total_cells - overflow_count
        
        overflow_stats = {
            "total_cells": total_cells,
            "overflow_cells": overflow_count, 
            "safe_cells": safe_count
        }
    else:
        overflow_stats = {"total_cells": 0, "overflow_cells": 0, "safe_cells": 0}
    
    # Save to data directory with configured filename
    data_dir = get_data_dir()
    data_dir.mkdir(exist_ok=True)
    
    output_file = data_dir / filename
    
    print(f"Saving to {output_file}...")
    
    # Collect LazyFrame and save to parquet
<<<<<<< HEAD
    df.collect().write_parquet(output_file)
=======
    df.collect().write_parquet(output_file, statistics={"max": True})
>>>>>>> 53189d8 (This again)
    
    if use_bounded:
        print(f"\n📊 Overflow Statistics:")
        print(f"  Total cells: {overflow_stats['total_cells']:,}")
        print(f"  Safe cells: {overflow_stats['safe_cells']:,} ({overflow_stats['safe_cells']/overflow_stats['total_cells']*100:.1f}%)")
        print(f"  Overflow cells: {overflow_stats['overflow_cells']:,} ({overflow_stats['overflow_cells']/overflow_stats['total_cells']*100:.1f}%)")
    
    print(f"Prime powers data saved: {output_file}")
    print(f"Columns: {actual_count} primes × {max_power} powers = {actual_count * max_power} total values")
    
    return output_file
