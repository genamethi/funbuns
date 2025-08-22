#!/usr/bin/env python3
"""
Safe data truncation script for funbuns parquet data.

This script:
1. Creates a backup of the current data
2. Truncates data to p <= 527177771 (last known good prime)
3. Provides detailed reporting of the operation

Run with: pixi run python truncate_data.py
"""

import polars as pl
from pathlib import Path

def main():
    # File paths
    data_dir = Path('data')
    main_file = data_dir / 'pparts.parquet'
    backup_file = data_dir / 'pparts.backup.parquet'
    
    print("=== Funbuns Data Truncation Script ===")
    print(f"Main file: {main_file}")
    print(f"Backup file: {backup_file}")
    print()
    
    if not main_file.exists():
        print("❌ No parquet file found at data/pparts.parquet")
        return
    
    try:
        # Read current data
        print("📖 Reading current data...")
        df = pl.read_parquet(main_file)
        
        print(f"📊 Current data:")
        print(f"   Shape: {df.shape}")
        print(f"   Prime range: {df['p'].min():,} to {df['p'].max():,}")
        print(f"   File size: {main_file.stat().st_size / (1024*1024):.1f} MB")
        print()
        
        # Filter to truncation point
        truncation_point = 527177771
        print(f"🔪 Truncating to p <= {truncation_point:,}...")
        
        truncated = df.filter(pl.col('p') <= truncation_point)
        removed_count = len(df) - len(truncated)
        
        print(f"📊 After truncation:")
        print(f"   Shape: {truncated.shape}")
        print(f"   Prime range: {truncated['p'].min():,} to {truncated['p'].max():,}")
        print(f"   Removed: {removed_count:,} rows")
        print()
        
        # Create backup
        print("💾 Creating backup...")
        data_dir.mkdir(exist_ok=True)
        df.write_parquet(backup_file)
        backup_size = backup_file.stat().st_size / (1024*1024)
        print(f"✅ Backup created: {backup_file} ({backup_size:.1f} MB)")
        
        # Write truncated data
        print("✂️  Writing truncated data...")
        truncated.write_parquet(main_file)
        new_size = main_file.stat().st_size / (1024*1024)
        print(f"✅ Main file updated: {main_file} ({new_size:.1f} MB)")
        
        print()
        print("🎉 Truncation completed successfully!")
        print(f"   Backup saved as: {backup_file.name}")
        print(f"   Data now ends at prime: {truncated['p'].max():,}")
        
    except Exception as e:
        print(f"❌ Error during truncation: {e}")
        print("   No changes made to data files.")
        raise

if __name__ == "__main__":
    main()
