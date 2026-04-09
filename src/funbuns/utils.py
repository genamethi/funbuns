"""
Utility functions for file handling, OS operations, and I/O.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import polars as pl
import tomllib
import time
import shutil
from typing import Dict, List, Optional
def get_log_dir() -> Path:
    """Get the log directory path."""
    if log_dir := os.getenv('FUNBUNS_LOG_DIR'):
        p = Path(log_dir)
    elif data_dir := os.getenv('FUNBUNS_DATA_DIR'):
        # When data dir is overridden (e.g. tests), put logs under it
        p = Path(data_dir) / "logs"
    else:
        try:
            config = get_config()
            if log_dir := config.get('log_dir'):
                p = Path(log_dir)
            else:
                p = Path('logs')
        except Exception:
            p = Path('logs')
    p.mkdir(parents=True, exist_ok=True)
    return p


class JournalWriter:
    """Append-only JSONL event logger. One line per event, central journal.

    Every event automatically includes PID and memory stats (RSS/VMS in MB)
    via psutil, so crashes and leaks can be diagnosed after the fact.
    """

    def __init__(self, path: Optional[Path] = None, name: Optional[str] = None):
        if path is not None:
            self.path = path
        elif name is not None:
            self.path = get_log_dir() / f"{name}.jsonl"
        else:
            self.path = get_log_dir() / "journal.jsonl"
        self._pid = os.getpid()

    @staticmethod
    def _mem_stats() -> dict:
        """Return RSS and VMS in MB, or empty dict if psutil unavailable."""
        try:
            import psutil
            mem = psutil.Process().memory_info()
            return {
                "rss_mb": round(mem.rss / 1_048_576, 1),
                "vms_mb": round(mem.vms / 1_048_576, 1),
            }
        except Exception:
            return {}

    def log(self, module: str, event: str, **payload):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "pid": self._pid,
            "module": module,
            "event": event,
            **self._mem_stats(),
            **payload,
        }
        with open(self.path, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")


def setup_logging():
    """Set up logging configuration."""
    log_dir = get_log_dir()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / 'decomp_analysis.log'),
            logging.StreamHandler()
        ]
    )


# Partition schema and constants for worker optimization
PARTITION_SCHEMA = {'p': pl.Int64, 'm_k': pl.Int32, 'n_k': pl.Int32, 'q_k': pl.Int64}

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


def resume_p(verbose: bool = False) -> int | None:
    """Get the last processed prime from block data.

    Uses max_prime embedded in filenames (pp_b{idx}_p{max_prime}.parquet)
    to find the highest prime without reading any parquet data.

    Returns the max prime, or None if no block data exists.
    """
    try:
        from .block_catalog import _parse_block_filename

        data_dir = get_data_dir()
        block_dir = data_dir / "blocks"

        if not block_dir.exists():
            return None

        block_files = list(block_dir.glob("pp_b*.parquet"))
        if not block_files:
            return None

        # Extract max_prime from each filename, take the global max
        best_p = 0
        best_file = None
        for f in block_files:
            _, max_prime = _parse_block_filename(f)
            if max_prime is not None and max_prime > best_p:
                best_p = max_prime
                best_file = f

        if best_p == 0:
            return None

        if verbose:
            print(f"Resume: max prime {best_p:,} from {best_file.name} "
                  f"({len(block_files)} blocks)")

        return best_p

    except Exception as e:
        logging.error(f"Error reading block files: {e}")
        print(f"\nError: Could not read existing block data")
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

    # Ensure both runs/ and blocks/ exist (consistent auto-creation)
    data_dir = get_data_dir()
    runs_dir = data_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "blocks").mkdir(parents=True, exist_ok=True)
    # Use microseconds and pid to avoid filename collisions within the same second
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    pid = os.getpid()
    run_file = runs_dir / f"pparts_run_{timestamp}_{pid}.parquet"
    df.write_parquet(run_file, compression="zstd", compression_level=1,
                     row_group_size=min(len(df), 100_000))

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


def get_temp_dir():
    """Get the temporary directory (under data_dir)."""
    d = get_data_dir() / "tmp"
    d.mkdir(parents=True, exist_ok=True)
    return d


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
        append_func = lambda df: append_data(
            df, filepath=data_file, verbose=args.verbose
        )
        return init_p, append_func, data_file        
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

    
    append_func = lambda df: append_data(
        df, verbose=verbose
    )
    
    return init_p, append_func


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
