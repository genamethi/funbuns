#!/usr/bin/env python3
"""
Comprehensive block management for prime partition data.
Handles block organization, naming, sizing, and analysis without aggregation.
"""

import polars as pl
from pathlib import Path
import argparse
import shutil
from typing import List, Tuple, Optional

from .utils import convert_runs_to_blocks_auto
from .data_integrity import quick_integrity_report


class BlockManager:
    """Manages prime partition data blocks with configurable organization."""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.runs_dir = self.data_dir / "runs" 
        self.blocks_dir = self.data_dir / "blocks"
        self.backup_dir = self.data_dir / "backup"
        
        # Ensure directories exist
        self.blocks_dir.mkdir(exist_ok=True)
        self.backup_dir.mkdir(exist_ok=True)
    
    def analyze_current_organization(self) -> dict:
        """Analyze current data organization."""
        print("🔍 Analyzing current data organization...")
        
        # Check what files exist
        run_files = list(self.runs_dir.glob("*.parquet")) if self.runs_dir.exists() else []
        block_files = list(self.blocks_dir.glob("*.parquet"))
        monolithic = self.data_dir / "pparts.parquet"
        
        analysis = {
            "run_files": len(run_files),
            "block_files": len(block_files), 
            "has_monolithic": monolithic.exists(),
            "total_files": len(run_files) + len(block_files) + (1 if monolithic.exists() else 0)
        }
        
        if run_files:
            print(f"  📁 Found {len(run_files)} run files in data/runs/")
            # Sample a few files to understand content
            sample_files = run_files[:3] + run_files[-2:] if len(run_files) > 5 else run_files
            
            for i, file in enumerate(sample_files):
                try:
                    df = pl.read_parquet(file)
                    rows = len(df)
                    min_p = df.select(pl.col("p").min()).item()
                    max_p = df.select(pl.col("p").max()).item() 
                    unique_primes = df.select(pl.col("p").n_unique()).item()
                    
                    print(f"    {file.name}: {rows:,} rows, {unique_primes:,} primes ({min_p:,} to {max_p:,})")
                    
                    analysis[f"sample_{i}"] = {
                        "file": file.name,
                        "rows": rows,
                        "primes": unique_primes,
                        "min_prime": min_p,
                        "max_prime": max_p
                    }
                except Exception as e:
                    print(f"    {file.name}: Error reading ({e})")
        
        if block_files:
            print(f"  📦 Found {len(block_files)} block files in data/blocks/")
            
        if monolithic.exists():
            try:
                df_info = pl.scan_parquet(monolithic).select([
                    pl.len().alias("rows"),
                    pl.col("p").min().alias("min_p"),
                    pl.col("p").max().alias("max_p"),
                    pl.col("p").n_unique().alias("unique_primes")
                ]).collect()
                
                rows = df_info["rows"].item()
                min_p = df_info["min_p"].item()
                max_p = df_info["max_p"].item()
                unique_primes = df_info["unique_primes"].item()
                
                print(f"  📄 Monolithic file: {rows:,} rows, {unique_primes:,} primes ({min_p:,} to {max_p:,})")
                analysis["monolithic"] = {
                    "rows": rows,
                    "primes": unique_primes,
                    "min_prime": min_p,
                    "max_prime": max_p
                }
            except Exception as e:
                print(f"  📄 Monolithic file: Error reading ({e})")
        
        return analysis
    
    def convert_runs_to_blocks(self, target_prime_count: int = 500_000, dry_run: bool = False):
        """Convert run files to properly named block files."""
        print(f"🔄 Converting runs to blocks (target: {target_prime_count:,} primes per block)")
        
        if dry_run:
            print("  🧪 DRY RUN - no files will be modified")
        
        run_files = list(self.runs_dir.glob("*.parquet"))
        if not run_files:
            print("  ❌ No run files found to convert")
            return
        
        print(f"  📁 Found {len(run_files)} run files to process")
        
        # Read all data and sort by prime
        print("  📊 Loading and sorting all data...")
        all_data = pl.scan_parquet(run_files).collect().sort("p")
        total_rows = len(all_data)
        total_primes = all_data.select(pl.col("p").n_unique()).item()
        
        print(f"  📈 Total: {total_rows:,} rows, {total_primes:,} unique primes")
        
        # Check if we can append to the last existing block
        existing_blocks = sorted(self.blocks_dir.glob("pp_b*.parquet"))
        last_block_path = None
        last_block_primes = 0
        
        if existing_blocks:
            last_block_path = existing_blocks[-1]
            last_block_data = pl.read_parquet(last_block_path)
            last_block_primes = last_block_data.select(pl.col("p").n_unique()).item()
            print(f"  📦 Found existing last block: {last_block_path.name} ({last_block_primes:,} primes)")
            
            if last_block_primes < target_prime_count:
                space_remaining = target_prime_count - last_block_primes
                print(f"  ➕ Can append up to {space_remaining:,} more primes to last block")
            else:
                print(f"  ✅ Last block is full ({last_block_primes:,} primes)")
                last_block_path = None
        
        if not dry_run:
            # Create backup
            backup_timestamp = __import__('datetime').datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_runs_dir = self.backup_dir / f"runs_backup_{backup_timestamp}"
            backup_runs_dir.mkdir(exist_ok=True)
            
            for run_file in run_files:
                shutil.copy2(run_file, backup_runs_dir / run_file.name)
            print(f"  💾 Backed up {len(run_files)} run files to {backup_runs_dir}")
        
        # Split by prime ranges
        unique_primes = all_data.select("p").unique().sort("p")
        
        # Handle appending to last block if possible
        if last_block_path and last_block_primes < target_prime_count:
            space_remaining = target_prime_count - last_block_primes
            primes_to_append = min(space_remaining, len(unique_primes))
            
            if primes_to_append > 0:
                # Get primes to append
                append_primes = unique_primes.slice(0, primes_to_append)
                min_append_prime = append_primes.select(pl.col("p").min()).item()
                max_append_prime = append_primes.select(pl.col("p").max()).item()
                
                # Get all rows for these primes and deduplicate
                append_data = all_data.filter(
                    (pl.col("p") >= min_append_prime) & (pl.col("p") <= max_append_prime)
                ).unique(['p', 'm_k', 'n_k', 'q_k'])
                
                print(f"    ➕ Appending {len(append_data):,} rows ({primes_to_append:,} primes) to {last_block_path.name}")
                
                if not dry_run:
                    # Read existing data, append new data, write back
                    existing_data = pl.read_parquet(last_block_path)
                    combined_data = pl.concat([existing_data, append_data]).sort("p")
                    combined_data.write_parquet(last_block_path)
                    
                    # Update block filename to reflect new max prime
                    new_filename = f"pp_b{len(existing_blocks):03d}_p{max_append_prime}.parquet"
                    new_path = self.blocks_dir / new_filename
                    if new_path != last_block_path:
                        last_block_path.rename(new_path)
                        last_block_path = new_path
                
                # Remove appended primes from the list
                unique_primes = unique_primes.slice(primes_to_append)
        
        # Calculate remaining blocks needed
        remaining_primes = len(unique_primes)
        if remaining_primes > 0:
            blocks_needed = (remaining_primes + target_prime_count - 1) // target_prime_count
            print(f"  📦 Creating {blocks_needed} additional blocks (~{target_prime_count:,} primes each)")
            
            # Create new blocks for remaining data
            start_block_num = len(existing_blocks)
            for block_num in range(blocks_needed):
                start_idx = block_num * target_prime_count
                end_idx = min((block_num + 1) * target_prime_count, remaining_primes)
                
                # Get prime range for this block
                block_primes = unique_primes.slice(start_idx, end_idx - start_idx)
                min_prime = block_primes.select(pl.col("p").min()).item()
                max_prime = block_primes.select(pl.col("p").max()).item()
                
                # Get all rows for primes in this range and deduplicate
                block_data = all_data.filter(
                    (pl.col("p") >= min_prime) & (pl.col("p") <= max_prime)
                ).unique(['p', 'm_k', 'n_k', 'q_k'])
                
                # Generate block filename: pp_b001_p7249729.parquet
                block_filename = f"pp_b{start_block_num + block_num + 1:03d}_p{max_prime}.parquet"
                block_path = self.blocks_dir / block_filename
                
                block_rows = len(block_data)
                block_unique_primes = block_data.select(pl.col("p").n_unique()).item()
                
                print(f"    📦 {block_filename}: {block_rows:,} rows, {block_unique_primes:,} primes ({min_prime:,} to {max_prime:,})")
                
                if not dry_run:
                    block_data.write_parquet(block_path)
        else:
            print("  ✅ All data appended to existing blocks")
    
    def show_block_summary(self, use_blocks: bool = True):
        """Show partition summary using glob patterns (no aggregation needed)."""
        print("📊 Block Summary (using glob patterns)")
        
        if use_blocks:
            pattern = str(self.blocks_dir / "*.parquet")
            files = list(self.blocks_dir.glob("*.parquet"))
        else:
            pattern = str(self.runs_dir / "*.parquet") 
            files = list(self.runs_dir.glob("*.parquet"))
        
        if not files:
            print(f"  ❌ No files found matching {pattern}")
            return
        
        print(f"  📁 Found {len(files)} files matching {pattern}")
        
        # Use polars lazy scanning with glob pattern
        try:
            # Get overall stats without full aggregation
            stats = pl.scan_parquet(pattern).select([
                pl.len().alias("total_rows"),
                pl.col("p").min().alias("min_prime"),
                pl.col("p").max().alias("max_prime"),
                pl.col("p").n_unique().alias("unique_primes")
            ]).collect()
            
            total_rows = stats["total_rows"].item()
            min_prime = stats["min_prime"].item()
            max_prime = stats["max_prime"].item()
            unique_primes = stats["unique_primes"].item()
            
            print(f"  📈 Total: {total_rows:,} rows, {unique_primes:,} unique primes")
            print(f"  📏 Range: {min_prime:,} to {max_prime:,}")
            
            # Partition frequency analysis using glob pattern
            print("  🔢 Partition frequency distribution:")
            
            prime_partition_counts = pl.scan_parquet(pattern).group_by("p").agg([
                pl.len().alias("total_entries"),
                pl.col("m_k").filter(pl.col("m_k") > 0).len().alias("actual_partitions")
            ]).collect()
            
            partition_counts = prime_partition_counts.group_by("actual_partitions").agg([
                pl.len().alias("prime_count")
            ]).sort("actual_partitions")
            
            for row in partition_counts.iter_rows(named=True):
                pc = row["actual_partitions"] 
                prime_count = row["prime_count"]
                percentage = (prime_count / unique_primes) * 100
                print(f"    {pc:3d} partitions: {prime_count:,} primes ({percentage:.1f}%)")
                
        except Exception as e:
            print(f"  ❌ Error analyzing files: {e}")
        
        # Show individual block info
        print(f"\n  📦 Individual block details:")
        for file in sorted(files)[:10]:  # Show first 10
            try:
                file_stats = pl.scan_parquet(file).select([
                    pl.len().alias("rows"),
                    pl.col("p").min().alias("min_p"),
                    pl.col("p").max().alias("max_p"),
                    pl.col("p").n_unique().alias("primes")
                ]).collect()
                
                rows = file_stats["rows"].item()
                min_p = file_stats["min_p"].item()
                max_p = file_stats["max_p"].item()
                primes = file_stats["primes"].item()
                
                print(f"    {file.name}: {rows:,} rows, {primes:,} primes ({min_p:,} to {max_p:,})")
                
            except Exception as e:
                print(f"    {file.name}: Error reading ({e})")
        
        if len(files) > 10:
            print(f"    ... and {len(files) - 10} more files")
    
    def reconfigure_block_size(self, new_prime_count: int, dry_run: bool = False):
        """Reconfigure existing blocks to a new target prime count."""
        print(f"🔧 Reconfiguring blocks to {new_prime_count:,} primes per block")
        
        if dry_run:
            print("  🧪 DRY RUN - no files will be modified")
        
        # Check if we have blocks or runs
        block_files = list(self.blocks_dir.glob("*.parquet"))
        run_files = list(self.runs_dir.glob("*.parquet"))
        
        if block_files:
            source_files = block_files
            source_pattern = str(self.blocks_dir / "*.parquet")
            print(f"  📦 Using {len(block_files)} existing block files")
        elif run_files:
            source_files = run_files
            source_pattern = str(self.runs_dir / "*.parquet")
            print(f"  📁 Using {len(run_files)} existing run files")
        else:
            print("  ❌ No block or run files found to reconfigure")
            return
        
        # Read all data using glob pattern
        print("  📊 Loading all data...")
        all_data = pl.scan_parquet(source_pattern).collect().sort("p")
        total_primes = all_data.select(pl.col("p").n_unique()).item()
        
        new_blocks_needed = (total_primes + new_prime_count - 1) // new_prime_count
        print(f"  📦 Will create {new_blocks_needed} new blocks")
        
        if not dry_run:
            # Create backup of existing blocks
            backup_timestamp = __import__('datetime').datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_blocks_dir = self.backup_dir / f"blocks_backup_{backup_timestamp}"
            backup_blocks_dir.mkdir(exist_ok=True)
            
            for file in source_files:
                shutil.copy2(file, backup_blocks_dir / file.name)
            print(f"  💾 Backed up {len(source_files)} files to {backup_blocks_dir}")
            
            # Clear existing blocks
            for file in block_files:
                file.unlink()
        
        # Create new blocks with the new organization
        self.convert_runs_to_blocks(target_prime_count=new_prime_count, dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(description="Prime partition block manager")
    parser.add_argument("--analyze", action="store_true", help="Analyze current organization")
    parser.add_argument("--convert", action="store_true", help="Convert runs to blocks")
    parser.add_argument("--summary", action="store_true", help="Show block summary")
    parser.add_argument("--reconfigure", type=int, metavar="PRIMES", help="Reconfigure to N primes per block")
    parser.add_argument("--block-size", type=int, default=500_000, help="Target primes per block (default: 500,000)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without making changes")
    parser.add_argument("--data-dir", default="data", help="Data directory (default: data)")
    parser.add_argument("--integrate-check", action="store_true", help="Integrate runs into blocks and run integrity checks (deletes runs on success)")
    parser.add_argument("--integrity", action="store_true", help="Print quick data integrity report")
    
    args = parser.parse_args()
    
    manager = BlockManager(args.data_dir)
    
    if args.analyze:
        analysis = manager.analyze_current_organization()
        print(f"\n📋 Analysis complete - found {analysis['total_files']} data files")
    
    if args.convert:
        manager.convert_runs_to_blocks(target_prime_count=args.block_size, dry_run=args.dry_run)
    
    if args.summary:
        # Try blocks first, fall back to runs
        block_files = list(manager.blocks_dir.glob("*.parquet"))
        use_blocks = len(block_files) > 0
        manager.show_block_summary(use_blocks=use_blocks)
    
    if args.reconfigure:
        manager.reconfigure_block_size(args.reconfigure, dry_run=args.dry_run)
    
    if args.integrate_check:
        # Use library flow that integrates, runs checks, and conditionally deletes runs
        convert_runs_to_blocks_auto(target_prime_count=args.block_size)
    
    if args.integrity:
        print(quick_integrity_report())
    
    if not any([args.analyze, args.convert, args.summary, args.reconfigure, args.integrate_check, args.integrity]):
        # Default behavior - show current organization
        manager.analyze_current_organization()


if __name__ == "__main__":
    main()


