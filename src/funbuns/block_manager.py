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

from .utils import convert_runs_to_blocks_auto, get_default_data_file
from .data_integrity import quick_integrity_report
from .block_catalog import sorted_blocks_by_data
from sage.all import Primes, next_prime


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

    def audit_boundaries(self) -> pl.DataFrame:
        """Audit block boundaries using next_prime(max_prev) continuity.

        Returns a DataFrame with columns:
          idx, file, block_num, min_p, max_p, expected_prev_next, status
        where status is 'ok' or 'gap'.
        """
        infos = sorted_blocks_by_data()
        rows = []
        prev_max = None
        for idx, info in enumerate(infos):
            exp = None
            status = 'ok'
            if prev_max is not None and info.min_prime is not None:
                exp = int(next_prime(int(prev_max)))
                if info.min_prime != exp:
                    status = 'gap'
            rows.append({
                'idx': idx + 1,
                'file': info.path.name,
                'block_num': info.block_num,
                'min_p': info.min_prime,
                'max_p': info.max_prime,
                'expected_prev_next': exp,
                'status': status,
            })
            if info.max_prime is not None:
                prev_max = info.max_prime
        return pl.DataFrame(rows)

    def _lazy_unique_sorted_primes(self) -> pl.LazyFrame:
        bdir = self.blocks_dir
        pattern = str(bdir / 'pp_b*.parquet')
        lf = pl.scan_parquet(pattern).select('p').unique().sort('p')
        return lf

    def _prime_at_index(self, lf: pl.LazyFrame, idx: int) -> int:
        row = lf.slice(idx, 1).collect()
        return int(row['p'].item())

    def audit_prefix_first_mismatch(self) -> dict:
        """Find the first index i where data_prime[i] != Primes.unrank(i).

        Returns dict with keys: index, data_prime, expected_prime.
        If no mismatch, returns empty dict.
        """
        lf = self._lazy_unique_sorted_primes()
        total = int(lf.select(pl.col('p').n_unique()).collect().item())
        if total == 0:
            return {}
        P = Primes(proof=False)
        # Quick check for prefix start
        first_data = self._prime_at_index(lf, 0)
        if first_data != int(P.unrank(0)):
            return {'index': 0, 'data_prime': first_data, 'expected_prime': int(P.unrank(0))}
        # Binary search the first mismatch
        lo, hi = 0, total - 1
        mismatch_found = False
        while lo < hi:
            mid = (lo + hi) // 2
            dmid = self._prime_at_index(lf, mid)
            emid = int(P.unrank(mid))
            if dmid == emid:
                lo = mid + 1
            else:
                hi = mid
                mismatch_found = True
        # Verify candidate
        d = self._prime_at_index(lf, lo)
        e = int(P.unrank(lo))
        if d != e:
            return {'index': lo, 'data_prime': d, 'expected_prime': e}
        # No mismatch in range
        return {}

    def truncate_from_block(self, start_block_num: int, yes: bool = False, dry_run: bool = False) -> int:
        """Delete blocks with block_num >= start_block_num. Returns count deleted.

        If dry_run is True and yes is False, only prints what would be deleted.
        """
        files = sorted(self.blocks_dir.glob('pp_b*.parquet'))
        to_delete = []
        for p in files:
            try:
                base = p.name.replace('.parquet','')
                parts = base.split('_')
                b_part = next((x for x in parts if x.startswith('b')), None)
                bnum = int(b_part[1:]) if b_part and b_part[1:].isdigit() else None
            except Exception:
                bnum = None
            if bnum is not None and bnum >= start_block_num:
                to_delete.append(p)
        if not to_delete:
            print(f"  No blocks at or beyond b{start_block_num:03d} found.")
            return 0
        print(f"  Will remove {len(to_delete)} blocks starting at b{start_block_num:03d}:")
        for p in to_delete[:10]:
            print(f"    {p.name}")
        if len(to_delete) > 10:
            print(f"    ... and {len(to_delete)-10} more")
        if dry_run or not yes:
            print("  (dry-run or missing --yes; no files deleted)")
            return 0
        # Backup then delete
        backup_timestamp = __import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_blocks_dir = self.backup_dir / f"truncate_backup_{backup_timestamp}"
        backup_blocks_dir.mkdir(exist_ok=True)
        for p in to_delete:
            shutil.copy2(p, backup_blocks_dir / p.name)
            p.unlink()
        print(f"  ✅ Deleted {len(to_delete)} blocks; backup at {backup_blocks_dir}")
        return len(to_delete)

    def truncate_from_prime(self, start_prime: int, yes: bool = False, dry_run: bool = False) -> int:
        """Delete blocks whose min_p >= start_prime. Returns count deleted."""
        infos = sorted_blocks_by_data()
        to_delete = [i.path for i in infos if i.min_prime is not None and i.min_prime >= start_prime]
        if not to_delete:
            print(f"  No blocks with min_p >= {start_prime} found.")
            return 0
        print(f"  Will remove {len(to_delete)} blocks with min_p >= {start_prime}:")
        for p in to_delete[:10]:
            print(f"    {p.name}")
        if len(to_delete) > 10:
            print(f"    ... and {len(to_delete)-10} more")
        if dry_run or not yes:
            print("  (dry-run or missing --yes; no files deleted)")
            return 0
        backup_timestamp = __import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_blocks_dir = self.backup_dir / f"truncate_backup_{backup_timestamp}"
        backup_blocks_dir.mkdir(exist_ok=True)
        for p in to_delete:
            shutil.copy2(p, backup_blocks_dir / p.name)
            p.unlink()
        print(f"  ✅ Deleted {len(to_delete)} blocks; backup at {backup_blocks_dir}")
        return len(to_delete)

    def rebuild_blocks_prefix(self, target_prime_count: int = 500_000, include_runs: bool = True, include_monolithic: bool = True):
        """Rebuild all blocks from the union of existing blocks (and runs) to enforce prefix property.

        Steps:
        - Load union of data lazily from blocks (+ runs if requested)
        - Deduplicate by (p,m_k,n_k,q_k) and sort by p
        - Backup existing blocks, clear them
        - Emit new blocks of target_prime_count unique primes each, naming by content max p
        - Run a prefix check at the end
        """
        print(f"🔄 Rebuilding blocks from union (target {target_prime_count:,} primes per block)", flush=True)
        block_files = list(self.blocks_dir.glob('*.parquet'))
        run_files = list(self.runs_dir.glob('*.parquet')) if include_runs and self.runs_dir.exists() else []
        mono_file = get_default_data_file() if include_monolithic else None
        if not block_files and not run_files and not (mono_file and mono_file.exists()):
            print("  ❌ No input data found in blocks/ or runs/ to rebuild from", flush=True)
            return

        # Build file list
        print(f"  📁 Sources: {len(block_files)} blocks, {len(run_files)} runs, monolithic: {mono_file.exists() if mono_file else False}", flush=True)

        # Show current prefix summary up front (always visible)
        infos0 = sorted_blocks_by_data()
        cum0 = 0
        P0 = Primes(proof=False)
        rows0 = []
        prev_max0 = None
        gaps0 = []
        for info in infos0:
            uniq = info.num_unique_primes or 0
            cum0 += uniq
            exp = int(P0.unrank(cum0 - 1)) if cum0 > 0 else None
            rows0.append({'file': info.path.name, 'min_p': info.min_prime, 'max_p': info.max_prime, 'unique_p': uniq, 'cum_unique_p': cum0, 'expected_max_p': exp})
            if prev_max0 is not None and info.min_prime is not None and info.min_prime > prev_max0:
                gaps0.append((prev_max0, info.min_prime))
            if info.max_prime is not None:
                prev_max0 = info.max_prime
        df0 = pl.DataFrame(rows0) if rows0 else pl.DataFrame({})
        print("\n  === CURRENT PREFIX SUMMARY (last 10) ===", flush=True)
        if df0.height > 0:
            print(df0.tail(10))
        else:
            print("  (no blocks)")
        if gaps0:
            print(f"\n  Current gaps: {len(gaps0)} (showing up to 10)", flush=True)
            for a,b in gaps0[:10]:
                print(f"    {a} -> {b}")
        else:
            print("\n  No gaps detected in current blocks.", flush=True)

        # Lazy union -> dedup -> sort
        print("  📊 Loading union, deduplicating and sorting by p...", flush=True)

        def lf_blocks():
            if not block_files:
                return None
            lf = pl.scan_parquet([str(p) for p in block_files])
            return lf

        def lf_runs():
            if not run_files:
                return None
            lf = pl.scan_parquet([str(p) for p in run_files])
            return lf

        def lf_mono():
            if not (mono_file and mono_file.exists()):
                return None
            lf = pl.scan_parquet(str(mono_file))
            # Normalize schema: m,n,q -> m_k,n_k,q_k if needed
            cols = lf.columns
            rename_map = {}
            for src, dst in [("m","m_k"),("n","n_k"),("q","q_k")]:
                if src in cols and dst not in cols:
                    rename_map[src] = dst
            if rename_map:
                lf = lf.rename(rename_map)
            return lf

        parts = []
        for maker in (lf_blocks, lf_runs, lf_mono):
            obj = maker()
            if obj is not None:
                parts.append(obj.select([c for c in obj.columns if c in ("p","m_k","n_k","q_k")]))
        if not parts:
            print("  ❌ No sources produced data after normalization", flush=True)
            return
        lf_union = pl.concat(parts, how="vertical_relaxed")
        lf_union = lf_union.unique(['p','m_k','n_k','q_k'])
        all_data = lf_union.sort('p').collect()
        total_rows = len(all_data)
        total_primes = int(all_data.select(pl.col('p').n_unique()).item())
        print(f"  📈 Union: {total_rows:,} rows, {total_primes:,} unique primes", flush=True)

        # Backup existing blocks
        backup_timestamp = __import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_blocks_dir = self.backup_dir / f"blocks_backup_{backup_timestamp}"
        backup_blocks_dir.mkdir(exist_ok=True)
        for file in block_files:
            shutil.copy2(file, backup_blocks_dir / file.name)
        print(f"  💾 Backed up {len(block_files)} block files to {backup_blocks_dir}", flush=True)

        # Clear existing blocks
        for file in block_files:
            file.unlink()

        # Partition into new blocks by unique primes with contiguity at boundaries
        unique_primes = all_data.select('p').unique().sort('p')
        blocks_needed = (len(unique_primes) + target_prime_count - 1) // target_prime_count
        print(f"  📦 Emitting {blocks_needed} new blocks", flush=True)
        for block_idx in range(blocks_needed):
            start_idx = block_idx * target_prime_count
            slice_size = min(target_prime_count, len(unique_primes) - start_idx)
            slice_primes = unique_primes.slice(start_idx, slice_size)
            min_p = int(slice_primes.select(pl.col('p').min()).item())
            max_p = int(slice_primes.select(pl.col('p').max()).item())
            block_df = all_data.filter((pl.col('p') >= min_p) & (pl.col('p') <= max_p)).unique(['p','m_k','n_k','q_k'])
            out_path = self.blocks_dir / f"pp_b{block_idx+1:03d}_p{max_p}.parquet"
            block_df.write_parquet(out_path)
            print(f"    📦 {out_path.name}: {len(block_df):,} rows, {int(block_df.select(pl.col('p').n_unique()).item()):,} primes ({min_p:,}..{max_p:,})", flush=True)

        # Final prefix check
        print("  ✅ Rebuild complete. Running prefix check...", flush=True)
        # Reuse CLI path by calling main behaviors lightly
        infos = sorted_blocks_by_data()
        cumulative = 0
        P = Primes(proof=False)
        mismatches = []
        prev_max = None
        gaps = []
        rows = []
        for info in infos:
            uniq = info.num_unique_primes or 0
            cumulative += uniq
            expected_max = int(P.unrank(cumulative - 1)) if cumulative > 0 else None
            if expected_max is not None and info.max_prime is not None and expected_max != info.max_prime:
                mismatches.append((info.path.name, info.max_prime, expected_max, cumulative))
            if prev_max is not None and info.min_prime is not None and info.min_prime > prev_max:
                gaps.append((prev_max, info.min_prime))
            if info.max_prime is not None:
                prev_max = info.max_prime
            rows.append({
                'file': info.path.name,
                'min_p': info.min_prime,
                'max_p': info.max_prime,
                'unique_p': uniq,
                'cum_unique_p': cumulative,
                'expected_max_p': expected_max,
            })
        df = pl.DataFrame(rows) if rows else pl.DataFrame({})
        print("\n  === PREFIX SUMMARY (last 10) ===", flush=True)
        if df.height > 0:
            print(df.tail(10))
        else:
            print("  (no blocks)")
        if gaps:
            print(f"\n  ❌ Gaps remain after rebuild: {len(gaps)} (showing up to 10)", flush=True)
            for a,b in gaps[:10]:
                print(f"    {a} -> {b}")
        else:
            print("\n  No gaps detected between blocks.", flush=True)
        if mismatches:
            print(f"\n  ❌ Prefix mismatches remain after rebuild: {len(mismatches)} (showing up to 10)", flush=True)
            for (name, cm, em, cu) in mismatches[:10]:
                print(f"    {name}: content max {cm} vs expected {em} at cum_unique {cu}")
        else:
            print("\n  ✅ Prefix property verified across rebuilt blocks.", flush=True)


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
    parser.add_argument("--prefix-check", action="store_true", help="Verify cumulative unique primes match Primes.unrank(max_index) per block and report gaps")
    parser.add_argument("--rebuild-prefix", action="store_true", help="Rebuild blocks from union (blocks + runs) enforcing prefix property")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output for checks and operations")
    parser.add_argument("--audit-boundaries", action="store_true", help="Audit block boundary continuity and suggest first bad block index")
    parser.add_argument("--audit-prefix", action="store_true", help="Find first index where data prime differs from unrank(index)")
    parser.add_argument("--truncate-from-block", type=int, metavar="N", help="Backup and delete blocks with block_num >= N (requires --yes to execute)")
    parser.add_argument("--truncate-from-prime", type=int, metavar="P", help="Backup and delete blocks with min_p >= P (requires --yes to execute)")
    parser.add_argument("--yes", action="store_true", help="Confirm destructive actions like truncate")
    
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
        print("🔄 Integrating runs into blocks and checking integrity...", flush=True)
        convert_runs_to_blocks_auto(target_prime_count=args.block_size)
        # Always show a summary, even if there were no runs
        print("\n=== INTEGRITY SUMMARY ===", flush=True)
        print(quick_integrity_report())
        # Compact prefix summary
        infos = sorted_blocks_by_data()
        if infos:
            cumulative = 0
            P = Primes(proof=False)
            rows = []
            gaps = []
            prev_max = None
            for info in infos:
                uniq = info.num_unique_primes or 0
                cumulative += uniq
                expected_max = int(P.unrank(cumulative - 1)) if cumulative > 0 else None
                rows.append({
                    'file': info.path.name,
                    'min_p': info.min_prime,
                    'max_p': info.max_prime,
                    'unique_p': uniq,
                    'cum_unique_p': cumulative,
                    'expected_max_p': expected_max,
                })
                if prev_max is not None and info.min_prime is not None and info.min_prime > prev_max:
                    gaps.append((prev_max, info.min_prime))
                if info.max_prime is not None:
                    prev_max = info.max_prime
            df = pl.DataFrame(rows)
            print("\n=== PREFIX SUMMARY (last 5) ===", flush=True)
            print(df.tail(5))
            if gaps:
                print(f"\nGaps detected: {len(gaps)} (showing up to 5)", flush=True)
                for a,b in gaps[:5]:
                    print(f"  {a} -> {b}")
            else:
                print("\nNo gaps detected between blocks.", flush=True)
    
    if args.integrity:
        print(quick_integrity_report())
    
    if args.prefix_check:
        infos = sorted_blocks_by_data()
        if not infos:
            print("No block files found.")
            return
        # Build cumulative table (small N; loop acceptable for CLI)
        cumulative = 0
        P = Primes(proof=False)
        rows = []
        mismatches = []
        gaps = []
        filename_mismatch = []
        prev_max = None
        for info in infos:
            uniq = info.num_unique_primes or 0
            cumulative += uniq
            expected_max = int(P.unrank(cumulative - 1)) if cumulative > 0 else None
            # filename vs content max: parse pMAX
            name = info.path.name
            try:
                # expect pp_bNNN_pMAX.parquet
                base = name.replace('.parquet','')
                parts = base.split('_')
                p_part = next((p for p in parts if p.startswith('p')), None)
                name_max = int(p_part[1:]) if p_part and p_part[1:].isdigit() else None
            except Exception:
                name_max = None
            if name_max is not None and info.max_prime is not None and name_max != info.max_prime:
                filename_mismatch.append((name, name_max, info.max_prime))
            # gap check using next_prime boundary
            if prev_max is not None and info.min_prime is not None:
                boundary = int(next_prime(int(prev_max)))
                if info.min_prime != boundary:
                    gaps.append((prev_max, info.min_prime, boundary))
            prev_max = info.max_prime if info.max_prime is not None else prev_max
            # expected vs content
            if expected_max is not None and info.max_prime is not None and expected_max != info.max_prime:
                mismatches.append((name, info.max_prime, expected_max, cumulative))
            rows.append({
                'file': name,
                'min_p': info.min_prime,
                'max_p': info.max_prime,
                'unique_p': uniq,
                'cum_unique_p': cumulative,
                'expected_max_p': expected_max,
            })
        df = pl.DataFrame(rows)
        print("\n=== PREFIX CHECK (content-sorted) ===")
        print(df.tail(10))
        if gaps:
            print("\nGaps detected between blocks (max_prev, min_next, expected_next_prime):")
            for a,b,bd in gaps[:20]:
                print(f"  {a} -> {b} (expected {bd})")
            if len(gaps) > 20:
                print(f"  ... and {len(gaps)-20} more")
        else:
            print("\nNo gaps detected between blocks.")
        if filename_mismatch:
            print("\nFilename/content max(p) mismatches:")
            for name, nm, cm in filename_mismatch:
                print(f"  {name}: name p{nm} vs content max {cm}")
        if mismatches:
            print("\nCumulative unique vs unrank(max_index) mismatches:")
            for name, cm, em, cu in mismatches[:20]:
                print(f"  {name}: content max {cm} vs expected {em} at cum_unique {cu}")
            if len(mismatches) > 20:
                print(f"  ... and {len(mismatches)-20} more")
        if not gaps and not filename_mismatch and not mismatches:
            print("\n✅ Prefix property holds across all blocks.")
    
    if args.rebuild_prefix:
        manager.rebuild_blocks_prefix(target_prime_count=args.block_size, include_runs=True)

    if args.audit_boundaries:
        df = manager.audit_boundaries()
        print("\n=== BOUNDARY AUDIT (last 10) ===")
        print(df.tail(10))
        first_gap = df.filter(pl.col('status') == 'gap').head(1)
        if first_gap.height > 0:
            gap_row = first_gap.to_dicts()[0]
            # Recommend truncating from the next block (gap after idx k → truncate from idx k+1)
            next_block = (gap_row.get('block_num') or gap_row.get('idx')) + 1
            print(f"\nSuggested truncate-from-block: {next_block}")
        else:
            print("\nAll boundaries contiguous by next_prime.")

    if args.audit_prefix:
        res = manager.audit_prefix_first_mismatch()
        print("\n=== PREFIX FIRST MISMATCH ===")
        if not res:
            print("All data primes match Primes.unrank(i) across the prefix.")
        else:
            print(f"index={res['index']:,}, data_prime={res['data_prime']}, expected_prime={res['expected_prime']}")

    if args.truncate_from_block is not None:
        manager.truncate_from_block(args.truncate_from_block, yes=args.yes, dry_run=not args.yes)

    if args.truncate_from_prime is not None:
        manager.truncate_from_prime(args.truncate_from_prime, yes=args.yes, dry_run=not args.yes)
    
    if not any([args.analyze, args.convert, args.summary, args.reconfigure, args.integrate_check, args.integrity]):
        # Default behavior - show current organization
        manager.analyze_current_organization()


if __name__ == "__main__":
    main()


