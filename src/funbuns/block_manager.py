#!/usr/bin/env python3
"""
Comprehensive block management for prime partition data.
Handles block organization, naming, sizing, diagnosis, and analysis.
"""

import polars as pl
from pathlib import Path
import argparse
import shutil
import time
from typing import List, Optional

from .utils import convert_runs_to_blocks_auto, get_data_dir, JournalWriter
from .data_integrity import (
    quick_integrity_report, comprehensive_diagnosis, prefix_check_report,
    check_proof_regime,
)
from .block_catalog import sorted_blocks_by_data

from sage.all import Primes


class BlockManager:
    """Manages prime partition data blocks with configurable organization."""

    def __init__(self, data_dir: str = None):
        self.data_dir = Path(data_dir) if data_dir else get_data_dir()
        self.runs_dir = self.data_dir / "runs"
        self.blocks_dir = self.data_dir / "blocks"
        self.backup_dir = self.data_dir / "backup"

        self.blocks_dir.mkdir(exist_ok=True)
        self.backup_dir.mkdir(exist_ok=True)

    def analyze_current_organization(self) -> dict:
        """Analyze current data organization."""
        print("Analyzing current data organization...")

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
            print(f"  Found {len(run_files)} run files in data/runs/")
            sample_files = run_files[:3] + run_files[-2:] if len(run_files) > 5 else run_files

            for i, file in enumerate(sample_files):
                try:
                    stats = pl.scan_parquet(file).select([
                        pl.len().alias("rows"),
                        pl.col("p").min().alias("min_p"),
                        pl.col("p").max().alias("max_p"),
                        pl.col("p").n_unique().alias("unique_primes")
                    ]).collect()
                    rows = stats["rows"].item()
                    min_p = stats["min_p"].item()
                    max_p = stats["max_p"].item()
                    unique_primes = stats["unique_primes"].item()

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
            print(f"  Found {len(block_files)} block files in data/blocks/")

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

                print(f"  Monolithic file: {rows:,} rows, {unique_primes:,} primes ({min_p:,} to {max_p:,})")
                analysis["monolithic"] = {
                    "rows": rows,
                    "primes": unique_primes,
                    "min_prime": min_p,
                    "max_prime": max_p
                }
            except Exception as e:
                print(f"  Monolithic file: Error reading ({e})")

        return analysis

    def show_block_summary(self, use_blocks: bool = True):
        """Show partition summary. Uses DuckDB when available, else lightweight Polars."""
        print("Block Summary")

        if use_blocks:
            files = list(self.blocks_dir.glob("*.parquet"))
        else:
            files = list(self.runs_dir.glob("*.parquet"))

        if not files:
            print(f"  No parquet files found.")
            return

        print(f"  Found {len(files)} block files")

        # Try DuckDB first (fast, already indexed)
        try:
            from .querydb import QueryDB
            db = QueryDB(read_only=True)
            with db:
                db.status()
            return
        except Exception:
            pass

        # Fallback: lightweight Polars scan (no n_unique — OOMs on 1B+ rows)
        pattern = str((self.blocks_dir if use_blocks else self.runs_dir) / "*.parquet")
        try:
            stats = pl.scan_parquet(pattern).select([
                pl.len().alias("total_rows"),
                pl.col("p").min().alias("min_prime"),
                pl.col("p").max().alias("max_prime"),
            ]).collect()

            print(f"  Total rows: {stats['total_rows'].item():,}")
            print(f"  Prime range: {stats['min_prime'].item():,} to {stats['max_prime'].item():,}")
            print("\n  (Run `funbuns --build-db` for full k-distribution summary)")
        except Exception as e:
            print(f"  Error analyzing files: {e}")

        # Show a few individual blocks
        print(f"\n  First 10 blocks:")
        for file in sorted(files)[:10]:
            try:
                file_stats = pl.scan_parquet(file).select([
                    pl.len().alias("rows"),
                    pl.col("p").min().alias("min_p"),
                    pl.col("p").max().alias("max_p"),
                ]).collect()
                print(f"    {file.name}: {file_stats['rows'].item():,} rows "
                      f"({file_stats['min_p'].item():,} to {file_stats['max_p'].item():,})")
            except Exception as e:
                print(f"    {file.name}: Error ({e})")

        if len(files) > 10:
            print(f"    ... and {len(files) - 10} more files")

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
        P = Primes()
        first_data = self._prime_at_index(lf, 0)
        if first_data != int(P.unrank(0)):
            return {'index': 0, 'data_prime': first_data, 'expected_prime': int(P.unrank(0))}
        lo, hi = 0, total - 1
        while lo < hi:
            mid = (lo + hi) // 2
            dmid = self._prime_at_index(lf, mid)
            emid = int(P.unrank(mid))
            if dmid == emid:
                lo = mid + 1
            else:
                hi = mid
        d = self._prime_at_index(lf, lo)
        e = int(P.unrank(lo))
        if d != e:
            return {'index': lo, 'data_prime': d, 'expected_prime': e}
        return {}

    def truncate_from_block(self, start_block_num: int, yes: bool = False, dry_run: bool = False) -> int:
        """Delete blocks with block_num >= start_block_num. Returns count deleted."""
        files = sorted(self.blocks_dir.glob('pp_b*.parquet'))
        to_delete = []
        for p in files:
            try:
                base = p.name.replace('.parquet', '')
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
        backup_timestamp = __import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_blocks_dir = self.backup_dir / f"truncate_backup_{backup_timestamp}"
        backup_blocks_dir.mkdir(exist_ok=True)
        for p in to_delete:
            shutil.copy2(p, backup_blocks_dir / p.name)
            p.unlink()
        print(f"  Deleted {len(to_delete)} blocks; backup at {backup_blocks_dir}")
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
        print(f"  Deleted {len(to_delete)} blocks; backup at {backup_blocks_dir}")
        return len(to_delete)


def main():
    journal = JournalWriter(name="bmgr")
    t0 = time.monotonic()

    parser = argparse.ArgumentParser(description="Prime partition block manager")
    parser.add_argument("--analyze", action="store_true", help="Analyze current organization")
    parser.add_argument("--convert", action="store_true", help="Convert runs to blocks")
    parser.add_argument("--summary", action="store_true", help="Show block summary")
    parser.add_argument("--show-runs", action="store_true", help="Show summary of all run files")
    parser.add_argument("--block-size", type=int, default=500_000, help="Target primes per block (default: 500,000)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without making changes")
    parser.add_argument("--data-dir", default=None, help="Data directory (default: from pixi.toml or ./data)")
    parser.add_argument("--integrate-check", action="store_true",
                        help="Integrate runs into blocks and run integrity checks (deletes runs on success)")
    parser.add_argument("--integrity", action="store_true", help="Print quick data integrity report")
    parser.add_argument("-p", "--paranoid", action="store_true",
                        help="Enable paranoid verification (with --integrity or --integrate-check)")
    parser.add_argument("--diagnose", action="store_true",
                        help="Full diagnosis: completeness, gaps, overlaps, with fix commands")
    parser.add_argument("--prefix-check", action="store_true",
                        help="Verify cumulative unique primes match Primes.unrank(max_index) and report gaps")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--audit-prefix", action="store_true",
                        help="Find first index where data prime differs from unrank(index)")
    parser.add_argument("--truncate-from-block", type=int, metavar="N",
                        help="Backup and delete blocks with block_num >= N (requires --yes)")
    parser.add_argument("--truncate-from-prime", type=int, metavar="P",
                        help="Backup and delete blocks with min_p >= P (requires --yes)")
    parser.add_argument("--yes", action="store_true", help="Confirm destructive actions like truncate")

    args = parser.parse_args()

    journal.log("bmgr", "start", args=vars(args))

    manager = BlockManager(args.data_dir)

    # Check proof regime. Below 2^64, PARI B-PSW is deterministic —
    # all integrity reduces to gap/continuity checks.
    infos = sorted_blocks_by_data()
    if infos:
        dataset_max_p = max(i.max_prime for i in infos if i.max_prime is not None)
        check_proof_regime(dataset_max_p)

    ran_action = False

    try:
        if args.analyze:
            analysis = manager.analyze_current_organization()
            print(f"\nAnalysis complete - found {analysis['total_files']} data files")
            ran_action = True

        if args.convert:
            convert_runs_to_blocks_auto(target_prime_count=args.block_size)
            ran_action = True

        if args.summary:
            block_files = list(manager.blocks_dir.glob("*.parquet"))
            use_blocks = len(block_files) > 0
            manager.show_block_summary(use_blocks=use_blocks)
            ran_action = True

        if args.show_runs:
            from .utils import show_run_files_summary
            show_run_files_summary()
            ran_action = True

        if args.diagnose:
            comprehensive_diagnosis(verbose=args.verbose)
            ran_action = True

        if args.integrate_check:
            print("Integrating runs into blocks and checking integrity...", flush=True)
            convert_runs_to_blocks_auto(target_prime_count=args.block_size)
            print("\n=== INTEGRITY SUMMARY ===", flush=True)
            print(quick_integrity_report())
            # Compact prefix summary using rank-based check
            report = prefix_check_report()
            comp = report["completeness"]
            if comp["valid_infos"]:
                print(f"\nCompleteness: {'COMPLETE' if comp['complete'] else 'INCOMPLETE'} "
                      f"({comp['per_block_sum']:,} per-block sum)", flush=True)
                if report["gaps"]:
                    print(f"Gaps: {len(report['gaps'])}")
                    for gap in report["gaps"]:
                        print(f"  {gap['block_a']} -> {gap['block_b']}: "
                              f"~{gap['est_missing']:,} missing primes")
                if report["filename_mismatches"]:
                    print(f"Filename mismatches: {len(report['filename_mismatches'])}")
                journal.log("bmgr", "dataset_summary",
                            n_blocks=len(comp["valid_infos"]),
                            n_primes=comp["per_block_sum"],
                            min_prime=comp["actual_min"],
                            max_prime=comp["actual_max"],
                            complete=comp["complete"],
                            n_gaps=len(report["gaps"]),
                            n_overlapping_pairs=comp["n_overlapping_pairs"])
            if args.paranoid:
                from .data_integrity import paranoid_rolling_verify
                print("\n=== PARANOID VERIFICATION ===", flush=True)
                paranoid_rolling_verify(verbose=args.verbose)
            ran_action = True

        if args.integrity:
            print(quick_integrity_report())
            if args.paranoid:
                from .data_integrity import paranoid_rolling_verify
                print("\n=== PARANOID VERIFICATION ===", flush=True)
                paranoid_rolling_verify(verbose=args.verbose)
            ran_action = True

        if args.prefix_check:
            report = prefix_check_report()
            comp = report["completeness"]
            if not comp["valid_infos"]:
                print("No block files found.")
            else:
                print("\n=== PREFIX CHECK ===")
                print(f"Blocks: {len(comp['valid_infos'])}, "
                      f"Per-block unique sum: {comp['per_block_sum']:,}")
                if comp["n_overlapping_pairs"] > 0:
                    print(f"  ({comp['n_overlapping_pairs']} overlapping block pairs; "
                          f"sum overcounts)")
                print(f"Completeness: {'COMPLETE' if comp['complete'] else 'INCOMPLETE'}")
                if not comp["complete"]:
                    dusart_lo = comp.get("dusart_lower")
                    if dusart_lo is not None:
                        print(f"  Dusart bounds: [{int(dusart_lo):,}, "
                              f"{int(comp['dusart_upper']):,}]")
                    if comp.get("needs_exact"):
                        print(f"  Count outside Dusart bounds; exact verification needed")
                if report["gaps"]:
                    print(f"\nGaps ({len(report['gaps'])}):")
                    for gap in report["gaps"][:20]:
                        print(f"  {gap['block_a']} (max {gap['gap_start']:,}) -> "
                              f"{gap['block_b']} (min {gap['gap_end']:,}): "
                              f"~{gap['est_missing']:,} missing primes")
                    if len(report["gaps"]) > 20:
                        print(f"  ... and {len(report['gaps'])-20} more")
                else:
                    print("\nNo gaps detected between blocks.")
                if report["filename_mismatches"]:
                    print(f"\nFilename/content max(p) mismatches ({len(report['filename_mismatches'])}):")
                    for m in report["filename_mismatches"]:
                        print(f"  {m['file']}: name p{m['name_max']} vs content max {m['content_max']}")
                if comp["complete"] and not report["filename_mismatches"]:
                    print("\nPrefix property holds across all blocks.")
            ran_action = True

        if args.audit_prefix:
            res = manager.audit_prefix_first_mismatch()
            print("\n=== PREFIX FIRST MISMATCH ===")
            if not res:
                print("All data primes match Primes.unrank(i) across the prefix.")
            else:
                print(f"index={res['index']:,}, data_prime={res['data_prime']}, "
                      f"expected_prime={res['expected_prime']}")
            ran_action = True

        if args.truncate_from_block is not None:
            manager.truncate_from_block(args.truncate_from_block, yes=args.yes, dry_run=not args.yes)
            ran_action = True

        if args.truncate_from_prime is not None:
            manager.truncate_from_prime(args.truncate_from_prime, yes=args.yes, dry_run=not args.yes)
            ran_action = True

        if not ran_action:
            manager.analyze_current_organization()

    finally:
        journal.log("bmgr", "end", elapsed_s=round(time.monotonic() - t0, 2))


if __name__ == "__main__":
    main()
