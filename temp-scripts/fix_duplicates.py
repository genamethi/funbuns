#!/usr/bin/env python3
"""
Fix duplicate data by regenerating blocks from block 56 onwards.
This script will:
1. Backup existing blocks 56+
2. Remove the corrupted blocks
3. Regenerate them from run files with proper deduplication
"""

import polars as pl
import glob
import shutil
from pathlib import Path
from datetime import datetime

def fix_duplicate_blocks():
    """Fix duplicate data by regenerating blocks from block 56 onwards."""
    
    print("🔧 Fixing duplicate data in blocks...")
    
    # Configuration
    blocks_dir = Path("data/blocks")
    runs_dir = Path("data/runs")
    backup_dir = Path("data/backup")
    
    # Create backup directory
    backup_dir.mkdir(exist_ok=True)
    
    # Find all block files
    block_files = sorted(blocks_dir.glob("pp_b*.parquet"))
    print(f"📦 Found {len(block_files)} total block files")
    
    # Find blocks that need fixing (block 56 and onwards)
    corrupted_blocks = [f for f in block_files if int(f.name.split('_')[1][1:]) >= 56]
    
    if not corrupted_blocks:
        print("✅ No corrupted blocks found (all blocks < 56)")
        return
    
    print(f"🔍 Found {len(corrupted_blocks)} corrupted blocks (56+):")
    for block in corrupted_blocks:
        print(f"  📁 {block.name}")
    
    # Create backup of corrupted blocks
    backup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"corrupted_blocks_backup_{backup_timestamp}"
    backup_path.mkdir(exist_ok=True)
    
    print(f"\n💾 Backing up corrupted blocks to {backup_path}...")
    for block in corrupted_blocks:
        shutil.copy2(block, backup_path / block.name)
        print(f"  ✅ Backed up {block.name}")
    
    # Remove corrupted blocks
    print(f"\n🗑️  Removing corrupted blocks...")
    for block in corrupted_blocks:
        block.unlink()
        print(f"  ✅ Removed {block.name}")
    
    # Find run files
    run_files = list(runs_dir.glob("*.parquet"))
    if not run_files:
        print("❌ No run files found to regenerate blocks")
        return
    
    print(f"\n📁 Found {len(run_files)} run files")
    
    # Load all run data and deduplicate
    print("📊 Loading and deduplicating run data...")
    all_data = pl.scan_parquet(run_files).collect().unique(['p', 'm_k', 'n_k', 'q_k'])
    print(f"  Total unique rows: {all_data.height:,}")
    
    # Get the range of primes in run files
    min_prime = all_data.select(pl.col('p').min()).item()
    max_prime = all_data.select(pl.col('p').max()).item()
    print(f"  Prime range: {min_prime:,} to {max_prime:,}")
    
    # Find the last good block (block 55)
    good_blocks = [f for f in block_files if int(f.name.split('_')[1][1:]) < 56]
    last_good_block = None
    last_good_max_prime = 0
    
    if good_blocks:
        last_good_block = sorted(good_blocks)[-1]
        last_good_data = pl.read_parquet(last_good_block)
        last_good_max_prime = last_good_data.select(pl.col('p').max()).item()
        print(f"\n📦 Last good block: {last_good_block.name} (max prime: {last_good_max_prime:,})")
    
    # Filter run data to only include primes after the last good block
    if last_good_max_prime > 0:
        run_data = all_data.filter(pl.col('p') > last_good_max_prime)
        print(f"📊 Run data after last good block: {run_data.height:,} rows")
    else:
        run_data = all_data
        print(f"📊 Using all run data: {run_data.height:,} rows")
    
    if run_data.height == 0:
        print("✅ No new data to process")
        return
    
    # Create new blocks with proper deduplication
    target_prime_count = 500_000
    unique_primes = run_data.select("p").unique().sort("p")
    total_primes = len(unique_primes)
    
    print(f"\n📦 Creating new blocks (target: {target_prime_count:,} primes per block)...")
    
    blocks_needed = (total_primes + target_prime_count - 1) // target_prime_count
    start_block_num = 56  # Start from block 56
    
    for block_num in range(blocks_needed):
        start_idx = block_num * target_prime_count
        end_idx = min((block_num + 1) * target_prime_count, total_primes)
        
        # Get prime range for this block
        block_primes = unique_primes.slice(start_idx, end_idx - start_idx)
        min_prime = block_primes.select(pl.col("p").min()).item()
        max_prime = block_primes.select(pl.col("p").max()).item()
        
        # Get all rows for primes in this range (already deduplicated)
        block_data = run_data.filter(
            (pl.col("p") >= min_prime) & (pl.col("p") <= max_prime)
        )
        
        # Generate block filename
        block_filename = f"pp_b{start_block_num + block_num:03d}_p{max_prime}.parquet"
        block_path = blocks_dir / block_filename
        
        block_rows = len(block_data)
        block_unique_primes = block_data.select(pl.col("p").n_unique()).item()
        
        print(f"  📦 {block_filename}: {block_rows:,} rows, {block_unique_primes:,} primes ({min_prime:,} to {max_prime:,})")
        
        # Write the block
        block_data.write_parquet(block_path)
    
    print(f"\n✅ Successfully regenerated {blocks_needed} blocks!")
    print(f"💾 Backup of corrupted blocks saved in: {backup_path}")
    
    # Verify no duplicates in new blocks
    print(f"\n🔍 Verifying no duplicates in new blocks...")
    new_blocks = sorted(blocks_dir.glob("pp_b*.parquet"))
    new_blocks = [f for f in new_blocks if int(f.name.split('_')[1][1:]) >= 56]
    
    total_duplicates = 0
    for block_file in new_blocks:
        block_data = pl.read_parquet(block_file)
        actual_partitions = block_data.filter(pl.col('m_k') > 0)
        
        # Check for duplicates
        duplicates = actual_partitions.group_by(['p', 'm_k', 'n_k', 'q_k']).agg([
            pl.len().alias('count')
        ]).filter(pl.col('count') > 1)
        
        if duplicates.height > 0:
            print(f"  ❌ {block_file.name}: {duplicates.height} primes with duplicates")
            total_duplicates += duplicates.height
        else:
            print(f"  ✅ {block_file.name}: No duplicates")
    
    if total_duplicates == 0:
        print(f"\n🎉 All new blocks are duplicate-free!")
    else:
        print(f"\n⚠️  Found {total_duplicates} primes with duplicates in new blocks")

if __name__ == '__main__':
    fix_duplicate_blocks()
