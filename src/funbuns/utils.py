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
from typing import Dict, List, Optional



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


def resume_p(use_separate_runs: bool = False) -> tuple[int, int]:
    """
    Get the last processed prime and start_idx from existing parquet data.
    
    Args:
        use_separate_runs: If True, scan all block files; if False, use monolithic file
    
    Returns:
        Tuple of (last_prime, start_idx) - (2, 0) if no data exists
    """
    try:
        if use_separate_runs:
            # Scan all block files using glob pattern
            data_dir = get_data_dir()
            block_pattern = str(data_dir / "blocks" / "pp_b*.parquet")
            block_files = list((data_dir / "blocks").glob("pp_b*.parquet"))
            if not block_files:
                logging.info("No existing block files found, starting from first prime")
                return (2, 0)
            
            # Use lazy scanning with glob pattern for efficiency
            df_scan = pl.scan_parquet(block_pattern).select("p")
            
        else:
            # Use monolithic file
            filepath = get_default_data_file()
            if not filepath.exists():
                logging.info("No existing data found, starting from first prime")
                return (2, 0)
            
            df_scan = pl.scan_parquet(filepath).select("p")
        
        # Get both last prime and unique count (start_idx) in one operation
        result = df_scan.select([
            pl.col("p").last().alias("last_prime"),
            pl.col("p").n_unique().alias("start_idx")
        ]).collect()
        
        last_prime = result["last_prime"].item()
        start_idx = result["start_idx"].item()
        
        if last_prime is None:
            logging.info("Parquet file is empty, starting from first prime")
            return (2, 0)
            
        source = "block files" if use_separate_runs else "main file"
        logging.info(f"Resuming from prime {last_prime} (start_idx: {start_idx}) from {source}")
        return (int(last_prime), int(start_idx))
        
    except Exception as e:
        source = "block files" if use_separate_runs else "main file"
        logging.error(f"Error reading parquet {source}: {e}")
        print(f"\nError: Could not read existing data from {source}")
        print("The files may be corrupted or in an invalid format.")
        print("Please run a data check or delete the files to start fresh.")
        raise


def append_data(df: pl.DataFrame, buffer_size: int = None, filepath=None, verbose: bool = False, use_separate_runs: bool = False):
    """
    Append data using incremental files to avoid O(n²) operations.
    
    Args:
        df: Polars DataFrame to append
        buffer_size: Optional buffer size for logging control  
        filepath: Optional custom file path (defaults to main data file)
        verbose: Whether to log incremental file writes
        use_separate_runs: If True, write to separate run files instead of monolithic file
    """
    if use_separate_runs:
        # Write directly to a new run file (no incremental merging needed)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_file = get_data_dir() / f"pparts_run_{timestamp}.parquet"
        df.write_parquet(run_file)
        
        if verbose:
            logging.info(f"Data written to run file: {run_file.name}")
            logging.info(f"Batch size: {len(df)} rows")
        return
    
    # Original incremental file approach
    if filepath is None:
        filepath = get_default_data_file()
    else:
        filepath = Path(filepath)
    
    data_dir = filepath.parent
    data_dir.mkdir(exist_ok=True)
    
    try:
        # Write to incremental file instead of merging with existing data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # milliseconds
        incremental_file = data_dir / f"{filepath.stem}_inc_{timestamp}.parquet"
        
        # Write new data to incremental file (O(1) operation)
        df.write_parquet(incremental_file)
        
        # Only log incremental writes in verbose mode
        if verbose:
            logging.info(f"Data written to incremental file: {incremental_file.name}")
            logging.info(f"Batch size: {len(df)} rows")
    except Exception as e:
        logging.error(f"Failed to write incremental data: {e}")
        raise





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
    Show summary of all block files.
    """
    data_dir = get_data_dir()
    block_files = list(data_dir.glob("pp_b*.parquet"))
    
    if not block_files:
        print("No block files found")
        return
    
    print(f"\n📁 Found {len(block_files)} block files:")
    
    total_rows = 0
    total_primes = 0
    for block_file in sorted(block_files):
        try:
            # Quick stats
            stats = pl.scan_parquet(block_file).select([
                pl.len().alias("rows"),
                pl.col("p").n_unique().alias("primes")
            ]).collect()
            
            rows = stats["rows"].item()
            primes = stats["primes"].item()
            total_rows += rows
            total_primes += primes
            
            print(f"  {block_file.name}: {rows:,} rows, {primes:,} primes")
            
        except Exception as e:
            print(f"  {block_file.name}: Error reading ({e})")
    
    print(f"\n📊 Total: {total_rows:,} rows, {total_primes:,} unique primes across all blocks")


def setup_analysis_mode(args, config):
    """
    Setup analysis mode based on CLI arguments and config.
    
    Returns:
        tuple: (init_p, start_idx, append_func, use_separate_runs, data_file)
    """
    use_separate_runs = config.get('use_separate_runs', True)
    if args.monolithic:
        use_separate_runs = False
    
    if args.temp:
        # Temporary mode - always monolithic
        data_file = get_temp_data_file()
        init_p, start_idx = 2, 0
        append_func = lambda df, buffer_size_arg: append_data(
            df, buffer_size_arg, data_file, verbose=args.verbose, use_separate_runs=False
        )
        return init_p, start_idx, append_func, False, data_file
        
    elif args.fresh:
        # Fresh start - clear existing data
        init_p, start_idx = 2, 0
        clear_existing_data(use_separate_runs)
        append_func = lambda df, buffer_size_arg: append_data(
            df, buffer_size_arg, verbose=args.verbose, use_separate_runs=use_separate_runs
        )
        return init_p, start_idx, append_func, use_separate_runs, None
        
    else:
        # Resume mode - smart resume logic
        init_p, start_idx, append_func = setup_resume_mode(use_separate_runs, args.verbose)
        return init_p, start_idx, append_func, use_separate_runs, None


def clear_existing_data(use_separate_runs):
    """Clear existing data based on mode."""
    if use_separate_runs:
        # Clear block files
        data_dir = get_data_dir()
        block_files = list(data_dir.glob("pp_b*.parquet"))
        if block_files:
            for block_file in block_files:
                block_file.unlink()
            print(f"Deleted {len(block_files)} existing block files for fresh start")
        else:
            print("No existing block files found")
    else:
        # Clear monolithic file
        data_file = get_default_data_file()
        data_file.unlink(missing_ok=True)
        print("Deleted existing data file for fresh start")


def setup_resume_mode(use_separate_runs, verbose):
    """
    Setup resume mode with smart fallback logic.
    
    Returns:
        tuple: (init_p, start_idx, append_func)
    """
    if use_separate_runs:
        # Try to resume from block files first
        init_p, start_idx = resume_p(use_separate_runs=True)
        
        if init_p == 2 and start_idx == 0:
            # No block files exist, check for existing monolithic data
            data_file = get_default_data_file()
            if data_file.exists():
                try:
                    init_p, start_idx = resume_p(use_separate_runs=False)
                    print(f"Found existing monolithic data - resuming from prime {init_p} (index {start_idx})")
                    print("💡 Future runs will be saved as separate files for better fault tolerance")
                except Exception as e:
                    print(f"Resume failed: {e}")
                    raise
            else:
                print("No existing data found, starting from beginning with separate block files")
        else:
            print(f"Resuming from prime {init_p} (index {start_idx}) using separate block files")
    else:
        # Resume from monolithic file
        data_file = get_default_data_file()
        if data_file.exists():
            try:
                init_p, start_idx = resume_p(use_separate_runs=False)
                print(f"Resuming from prime {init_p} (index {start_idx}) using monolithic file")
            except Exception as e:
                print(f"Resume failed: {e}")
                raise
        else:
            init_p, start_idx = 2, 0
            print("No existing data found, starting from beginning")
    
    append_func = lambda df, buffer_size_arg: append_data(
        df, buffer_size_arg, verbose=verbose, use_separate_runs=use_separate_runs
    )
    
    return init_p, start_idx, append_func


def generate_partition_summary(data_file=None, verbose=False):
    """Generate and display partition frequency summary using blocks or single file."""
    try:
        from pathlib import Path
        
        # Check for block files first
        data_dir = get_data_dir()
        block_pattern = str(data_dir / "blocks" / "pp_b*.parquet")
        block_files = list((data_dir / "blocks").glob("pp_b*.parquet"))
        
        if block_files:
            # Use block files with glob pattern (reuse block_manager logic)
            print(f"\n=== PARTITION SUMMARY (from blocks) ===")
            print(f"Found {len(block_files)} block files")
            
            # Get overall stats using lazy scanning
            stats = pl.scan_parquet(block_pattern).select([
                pl.len().alias("total_rows"),
                pl.col("p").n_unique().alias("unique_primes")
            ]).collect()
            
            total_rows = stats["total_rows"].item()
            unique_primes = stats["unique_primes"].item()
            
            print(f"Total primes processed: {unique_primes:,}")
            
            # Partition frequency analysis using glob pattern
            partition_counts = pl.scan_parquet(block_pattern).group_by("p").agg([
                pl.len().alias("partition_count")
            ]).group_by("partition_count").agg([
                pl.len().alias("prime_count")
            ]).sort("partition_count").collect()
            
            # Display the summary
            for row in partition_counts.iter_rows(named=True):
                count = row['partition_count']
                primes = row['prime_count']
                percentage = (primes / unique_primes) * 100
                if count == 0:
                    print(f"  {count} partitions: {primes:,} primes ({percentage:.1f}%)")
                else:
                    print(f"  {count} partition{'s' if count != 1 else ''}: {primes:,} primes ({percentage:.1f}%)")
            
            # Show examples in verbose mode
            if verbose:
                print(f"\nExamples of primes with multiple partitions:")
                multi_partition_primes = pl.scan_parquet(block_pattern).group_by("p").agg([
                    pl.len().alias("count")
                ]).filter(pl.col("count") > 1).sort("p").head(10).collect()
                
                if len(multi_partition_primes) > 0:
                    for row in multi_partition_primes.iter_rows(named=True):
                        p = row['p']
                        count = row['count']
                        print(f"  p={p} ({count} partitions)")
                
                # Show block details
                print(f"\nBlock details:")
                for file in sorted(block_files)[:5]:  # Show first 5
                    file_stats = pl.scan_parquet(file).select([
                        pl.len().alias("rows"),
                        pl.col("p").n_unique().alias("primes")
                    ]).collect()
                    rows = file_stats["rows"].item()
                    primes = file_stats["primes"].item()
                    print(f"  {file.name}: {rows:,} rows, {primes:,} primes")
                
                if len(block_files) > 5:
                    print(f"  ... and {len(block_files) - 5} more blocks")
        
        elif data_file and data_file.exists():
            # Fall back to single file
            print(f"\n=== PARTITION SUMMARY (from single file) ===")
            df = pl.read_parquet(data_file)
            total_primes = len(df)
            
            print(f"Total primes processed: {total_primes:,}")
            
            # Determine q column name (handle schema differences)
            q_col = 'q' if 'q' in df.columns else 'q_k'
            
            # Count partitions per prime
            partition_counts = df.group_by("p").agg([
                (pl.col(q_col) > 0).sum().alias("partition_count")
            ]).group_by("partition_count").agg([
                pl.len().alias("prime_count")
            ]).sort("partition_count")
            
            # Display the summary
            for row in partition_counts.iter_rows(named=True):
                count = row['partition_count']
                primes = row['prime_count']
                percentage = (primes / total_primes) * 100
                if count == 0:
                    print(f"  {count} partitions: {primes:,} primes ({percentage:.1f}%)")
                else:
                    print(f"  {count} partition{'s' if count != 1 else ''}: {primes:,} primes ({percentage:.1f}%)")
            
            # Show examples in verbose mode
            if verbose:
                multi_partition_primes = df.filter(pl.col(q_col) > 0).group_by("p").agg([
                    pl.len().alias("count")
                ]).filter(pl.col("count") > 1).sort("p").head(10)
                
                if len(multi_partition_primes) > 0:
                    print(f"\nExamples of primes with multiple partitions:")
                    for row in multi_partition_primes.iter_rows(named=True):
                        p = row['p']
                        count = row['count']
                        prime_partitions = df.filter((pl.col("p") == p) & (pl.col(q_col) > 0))
                        print(f"  p={p} ({count} partitions):")
                        for part_row in prime_partitions.iter_rows(named=True):
                            m, n, q = part_row['m' if 'm' in part_row else 'm_k'], part_row['n' if 'n' in part_row else 'n_k'], part_row[q_col]
                            print(f"    {p} = 2^{m} + {q}^{n} = {2**m} + {q**n} = {2**m + q**n}")
        else:
            print("No data files found for summary.")
        
    except Exception as e:
        print(f"Error generating partition summary: {e}")
