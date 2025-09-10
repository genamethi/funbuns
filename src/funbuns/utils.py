"""
Utility functions for file handling, OS operations, and I/O.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
import polars as pl
import tomllib
import time
import shutil
from typing import Dict, List, Optional
import numpy as np



class TimingCollector:
    """Collect and store timing data for performance profiling."""
    
    def __init__(self, verbose: bool = False):
        self.timings: List[Dict] = []
        self.verbose = verbose
        self.active_timers: Dict[str, float] = {}
    
    def start_timer(self, operation: str, **metadata) -> str:
        """Start timing an operation. Returns timer_id for ending."""
        timer_id = f"{operation}_{len(self.timings)}"
        start_time = time.perf_counter()
        self.active_timers[timer_id] = start_time
        
        if self.verbose:
            logging.info(f"Started: {operation}")
        
        return timer_id
    
    def end_timer(self, timer_id: str, operation: str, **metadata):
        """End timing and record the result."""
        end_time = time.perf_counter()
        start_time = self.active_timers.pop(timer_id, end_time)
        duration = end_time - start_time
        
        timing_record = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'duration_ms': duration * 1000,
            **metadata
        }
        self.timings.append(timing_record)
        
        if self.verbose:
            logging.info(f"Completed: {operation} in {duration*1000:.2f}ms")
    
    def time_operation(self, operation: str, **metadata):
        """Context manager for timing operations."""
        return TimingContext(self, operation, **metadata)
    
    def get_stats(self) -> Dict:
        """Get timing statistics."""
        if not self.timings:
            return {}
        
        df = pl.DataFrame(self.timings)
        stats = {}
        
        for operation in df['operation'].unique():
            op_data = df.filter(pl.col('operation') == operation)['duration_ms']
            stats[operation] = {
                'count': len(op_data),
                'mean_ms': op_data.mean(),
                'median_ms': op_data.median(),
                'min_ms': op_data.min(),
                'max_ms': op_data.max(),
                'std_ms': op_data.std()
            }
        return stats
    
    def save_debug_log(self, filepath: Optional[Path] = None):
        """Save timing data to debug log."""
        if not self.timings:
            return
            
        if filepath is None:
            filepath = get_data_dir() / f"timing_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        
        df = pl.DataFrame(self.timings)
        df.write_parquet(filepath)
        
        # Also save human-readable stats
        stats_file = filepath.with_suffix('.txt')
        with open(stats_file, 'w') as f:
            f.write("=== TIMING STATISTICS ===\n\n")
            stats = self.get_stats()
            for operation, data in stats.items():
                f.write(f"{operation}:\n")
                f.write(f"  Count: {data['count']}\n")
                f.write(f"  Mean: {data['mean_ms']:.2f}ms\n")
                f.write(f"  Median: {data['median_ms']:.2f}ms\n")
                f.write(f"  Range: {data['min_ms']:.2f}ms - {data['max_ms']:.2f}ms\n")
                f.write(f"  Std Dev: {data['std_ms']:.2f}ms\n\n")
        
        logging.info(f"Timing data saved to {filepath}")
        logging.info(f"Timing stats saved to {stats_file}")
    
    def print_summary(self):
        """Print timing summary to console."""
        stats = self.get_stats()
        if not stats:
            print("No timing data collected")
            return
            
        print("\n=== TIMING SUMMARY ===")
        for operation, data in stats.items():
            print(f"{operation:25s}: {data['mean_ms']:6.2f}ms avg ({data['count']:4d} calls)")


class TimingContext:
    """Context manager for timing operations."""
    
    def __init__(self, collector: TimingCollector, operation: str, **metadata):
        self.collector = collector
        self.operation = operation
        self.metadata = metadata
        self.timer_id = None
    
    def __enter__(self):
        self.timer_id = self.collector.start_timer(self.operation, **self.metadata)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.timer_id:
            self.collector.end_timer(self.timer_id, self.operation, **self.metadata)


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('decomp_analysis.log'),
            logging.StreamHandler()
        ]
    )


# Partition schema and constants for worker optimization
PARTITION_SCHEMA = {'p': pl.Int64, 'm_k': pl.Int64, 'n_k': pl.Int64, 'q_k': pl.Int64}

# Empirical partition distribution from 459M primes analysis
# Used for accurate batch size estimation in workers
PARTITION_DISTRIBUTION = {
    'avg_rows_per_prime': 1.7,  # Including zero rows
    'avg_partitions_per_prime': 2.05,  # Excluding zero rows  
    'zero_probability': 0.173,  # ~17.3% have no partitions
    'max_observed_partitions': 14  # Theoretical max observed
}


def get_default_data_file():
    """Get the default data file path."""
    return get_data_dir() / "pparts.parquet"


def get_temp_data_file():
    """Get the path to a timestamped temporary data file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return get_temp_dir() / f"pparts_temp_{timestamp}.parquet"


def get_config():
    """Get application configuration from pixi.toml following hierarchy."""
    try:
        config_file = get_config_file()
        with open(config_file, "rb") as f:
            config = tomllib.load(f)
        
        # Get main config
        funbuns_config = config.get("tool", {}).get("funbuns", {})
        
        # Merge directory configuration
        directories = funbuns_config.get("directories", {})
        funbuns_config.update(directories)
        
        return funbuns_config
    except (FileNotFoundError, tomllib.TOMLDecodeError):
        logging.warning(f"Could not load {config_file}, using defaults")
        return {}


def resume_p(verbose: bool = False) -> int:
    """
    Get the last processed prime and start_idx from existing parquet data.
    
    Args:
        Verbose...
    
    Returns:
        start_idx - 0 if no data exists
    """
    #Really at the moment I don't have the code written to work without initial data.
    try:
        data_dir = get_data_dir()
        block_pattern = str(data_dir / "blocks" / "pp_b*.parquet")
        init_p = pl.scan_parquet(block_pattern).select(
                pl.col("p").max()
        ).collect().item()

        return init_p
        
    except Exception as e:
        source = "block files"
        logging.error(f"Error reading parquet {source}: {e}")
        print(f"\nError: Could not read existing data from {source}")
        print("The files may be corrupted or in an invalid format.")
        print("Please run a data check or delete the files to start fresh.")
        raise


def append_data(df: pl.DataFrame, buffer_size: int = None, filepath=None, verbose: bool = False):
    """
    Append data using incremental files to avoid O(n²) operations.
    
    Args:
        df: Polars DataFrame to append
        buffer_size: Optional buffer size for logging control  
        filepath: Optional custom file path (defaults to main data file)
        verbose: Whether to log incremental file writes
    """

    # Write directly to a new run file in data/runs/ directory
    runs_dir = get_data_dir() / "runs"
    runs_dir.mkdir(exist_ok=True)
    # Use microseconds and pid to avoid filename collisions within the same second
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    pid = os.getpid()
    run_file = runs_dir / f"pparts_run_{timestamp}_{pid}.parquet"
    df.write_parquet(run_file)
    
    if verbose:
        logging.info(f"Data written to run file: {run_file.name}")
        logging.info(f"Batch size: {len(df)} rows")
    return

def get_data_dir():
    """Get the application data directory path following configuration hierarchy."""
    # 1. Try environment variables (highest priority)
    if data_dir := os.getenv('FUNBUNS_DATA_DIR'):
        return Path(data_dir)
    
    # 2. Try pyproject.toml configuration
    try:
        config = get_config()
        if data_dir := config.get('data_dir'):
            return Path(data_dir)
    except Exception:
        pass
    
    # 3. Fallback to default
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    return data_dir


def get_config_file():
    """Get the application configuration file path."""
    # For development: use local pixi.toml
    # For distribution: could use package resources or user config directory
    return Path('pixi.toml')


def get_backup_dir():
    """Get the backup directory path following configuration hierarchy."""
    # 1. Try environment variables
    if backup_dir := os.getenv('FUNBUNS_BACKUP_DIR'):
        return Path(backup_dir)
    
    # 2. Try pyproject.toml configuration
    try:
        config = get_config()
        if backup_dir := config.get('backup_dir'):
            return Path(backup_dir)
    except Exception:
        pass
    
    # 3. Fallback to default
    backup_dir = Path('data/backups')
    backup_dir.mkdir(parents=True, exist_ok=True)
    return backup_dir


def get_temp_dir():
    """Get the temporary directory path following configuration hierarchy."""
    # 1. Try environment variables
    if temp_dir := os.getenv('FUNBUNS_TEMP_DIR'):
        return Path(temp_dir)
    
    # 2. Try pyproject.toml configuration
    try:
        config = get_config()
        if temp_dir := config.get('temp_dir'):
            return Path(temp_dir)
    except Exception:
        pass
    
    # 3. Fallback to default
    temp_dir = Path('data/tmp')
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


def get_small_primes_table():
    """
    Load small primes table from configured file with error handling.
    
    Returns:
        tuple: (table_lazy_frame, largest_small_prime) or (None, None) if not available
    """
    # Get configuration
    config = get_config()
    filename = config.get("small_primes_filename", "small_primes.parquet")
    data_dir = get_data_dir()
    table_path = data_dir / filename
    
    try:
        if table_path.exists():
            # Load table lazily (already created with Int64 schema)
            table = pl.scan_parquet(table_path)
            
            # Get largest small prime (LSP) from last row, first column
            lsp_result = table.select(pl.col("1").last()).collect()
            lsp = lsp_result["1"].item()
            
            return table, lsp
        else:
            logging.warning(f"Small primes table not found at {table_path}")
            logging.info("Run: funbuns --prep to generate the table")
            return None, None
            
    except Exception as e:
        logging.error(f"Error loading small primes table: {e}")
        return None, None

def show_run_files_summary():
    """
    Show summary of all run files.
    """
    data_dir = get_data_dir()
    run_files = list((data_dir / "runs").glob("*.parquet"))
    
    if not run_files:
        print("No run files found")
        return
    
    print(f"\n📁 Found {len(run_files)} run files:")
    
    total_rows = 0
    total_primes = 0
    for run_file in sorted(run_files):
        try:
            # Quick stats
            stats = pl.scan_parquet(run_file).select([
                pl.len().alias("rows"),
                pl.col("p").n_unique().alias("primes")
            ]).collect()
            
            rows = stats["rows"].item()
            primes = stats["primes"].item()
            total_rows += rows
            total_primes += primes
            
            print(f"  {run_file.name}: {rows:,} rows, {primes:,} primes")
            
        except Exception as e:
            print(f"  {run_file.name}: Error reading ({e})")
    
    print(f"\n📊 Total: {total_rows:,} rows, {total_primes:,} unique primes across all run files")


def setup_analysis_mode(args, config):
    """
    Setup analysis mode based on CLI arguments and config.
    
    Returns:
        tuple: (start_idx, append_func, data_file)
    """
    
    if args.temp:
        # Temporary mode - always monolithic
        data_file = get_temp_data_file()
        init_p = 2
        append_func = lambda df, buffer_size_arg: append_data(
            df, buffer_size_arg, data_file, verbose=args.verbose
        )
        return init_p, append_func, False, data_file        
    else:
        # Resume mode - smart resume logic
        init_p, append_func = setup_resume_mode(args.verbose)
        return init_p, append_func,  None




def setup_resume_mode(verbose):
    """
    Setup resume mode with smart fallback logic.
    
    Returns:
        tuple: (init_p, append_func)
    """
    init_p = resume_p(verbose=verbose)

    if init_p is None:
        print("No existing data found, starting from beginning with separate block files")
        init_p = 2
    else:
        print(f"Resuming from prime {init_p} using separate block files")

    
    append_func = lambda df, buffer_size_arg: append_data(
        df, buffer_size_arg, verbose=verbose
    )
    
    return init_p, append_func


import polars as pl
from pathlib import Path

def generate_partition_summary(verbose: bool = False):
    """
    Generate and display partition frequency summary using a single, efficient streaming query.
    """
    try:
        data_dir = get_data_dir()
        block_pattern = str(data_dir / "blocks" / "pp_b*.parquet")
        block_files = list((data_dir / "blocks").glob("pp_b*.parquet"))

        if not block_files:
            print("No block files found for summary.")
            return

        print(f"\n=== PARTITION SUMMARY (from {len(block_files)} blocks) ===")

        # Define the entire calculation as a single, lazy, streaming query
        # This is far more efficient than batching in Python.
        lazy_summary = (
            pl.scan_parquet(block_pattern)
            # 1. Count the number of partitions for each prime 'p'.
            # A partition exists if q_k > 0.
            .group_by("p")
            .agg(
                (pl.col("q_k") > 0).sum().alias("partition_count")
            )
            # 2. Count how many primes have each 'partition_count'.
            # pl.len() is a fast way to count items in a group.
            .group_by("partition_count")
            .agg(
                pl.len().alias("prime_count")
            )
            .sort("partition_count")
        )

        # Execute the query in streaming mode to keep memory usage low
        summary_df = lazy_summary.collect(streaming=True)

        # Display the results
        total_primes = summary_df["prime_count"].sum()
        print(f"Total unique primes processed: {total_primes:,}")

        for row in summary_df.iter_rows(named=True):
            count = row["partition_count"]
            primes = row["prime_count"]
            percentage = (primes / total_primes) * 100
            label = "partitions" if count != 1 else "partition"
            if count == 0:
                label = "partitions" # Grammatically better for zero
            print(f"  {count} {label}: {primes:,} primes ({percentage:.1f}%)")

        if verbose:
            # The verbose logic can be simplified as well
            print(f"\nBlock details:")
            for file in sorted(block_files)[:5]:
                stats = pl.scan_parquet(file).select(pl.len().alias("rows")).collect()
                print(f"  {file.name}: {stats['rows'].item():,} rows")
            
            if len(block_files) > 5:
                print(f"  ... and {len(block_files) - 5} more blocks")

    except Exception as e:
        print(f"Error generating partition summary: {e}")
            # Show examples in verbose mode


def convert_runs_to_blocks_auto(target_prime_count: int = 500_000):
    """
    Automatically integrate run files into blocks. The integration logic is trusted to produce
    correct, non-overlapping, and de-duplicated blocks.
    """
    # This function already handles the "no runs" case internally
    from .run_ingester import integrate_runs_into_blocks
    
    print("\n🔄 Integrating run files into blocks...")
    work_done = integrate_runs_into_blocks(
        target_prime_count=target_prime_count,
        verbose=True,
        delete_run_files=True # Trust the process and delete on success
    )
    
    if work_done:
        print("  ✅ Integration successful. Run files removed.")
    else:
        print("No work done.")

    # (Optional) If you are still concerned, you can run the fast check
    # overlaps = detect_overlaps_fast()
    # if not overlaps.is_empty():
    #     print("  ❌ WARNING: Overlaps detected after integration!")
    #     print(overlaps)
    # else:
    #     print("  ✅ Overlap check passed.")
