#!/usr/bin/env python3
import polars as pl

# Check main data file
df = pl.read_parquet('data/pparts.parquet')
print(f"Main data: {len(df):,} rows")
print(f"Last prime: {df.select(pl.col('p').max()).item():,}")
print(f"Unique primes: {df.select(pl.col('p').n_unique()).item():,}")
