"""
Utility functions for file handling, OS operations, and I/O.
"""

import logging
from datetime import datetime
from pathlib import Path
import polars as pl
import tomllib
try:
    from importlib.resources import files
except ImportError:
    from importlib_resources import files


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
    return Path("data") / "pparts.parquet"


def get_config():
    """Get configuration from pixi.toml."""
    try:
        with open("pixi.toml", "rb") as f:
            config = tomllib.load(f)
        return config.get("tool", {}).get("funbuns", {})
    except (FileNotFoundError, tomllib.TOMLDecodeError):
        logging.warning("Could not load pixi.toml, using defaults")
        return {}


def resume_p() -> int:
    """
    Get the resume prime from existing parquet data.
    
    Returns:
        Integer - next prime to process, or 2 if no data exists
    """
    filepath = get_default_data_file()
    
    if not filepath.exists():
        logging.info("No existing data found, starting from first prime")
        return 2
    
    try:
        # Read only the last row to get max p
        df = pl.scan_parquet(filepath).tail(1).collect()
        
        if df.is_empty():
            logging.info("Parquet file is empty, starting from first prime")
            return 2
        
        max_p = df.select("p").item()
        # Find the next prime after max_p
        from sage.all import Primes
        P = Primes()
        try:
            rank = P.rank(max_p)
            next_prime = P.unrank(rank + 1)
        except:
            # Fallback: just add 1 and find next prime
            next_prime = max_p + 1
            while not next_prime.is_prime():
                next_prime += 1
        
        logging.info(f"Resuming from prime {next_prime} (max processed: {max_p})")
        return next_prime
        
    except Exception as e:
        logging.error(f"Error reading parquet file {filepath}: {e}")
        print(f"\nError: Could not read existing data file {filepath}")
        print("The file may be corrupted or in an invalid format.")
        print("Please run a data check or delete the file to start fresh.")
        raise


def append_data(df: pl.DataFrame, buffer_size: int = None):
    """
    Append data to parquet file.
    
    Args:
        df: Polars DataFrame to append
        buffer_size: Optional buffer size for logging control
    """
    filepath = get_default_data_file()
    data_dir = filepath.parent
    data_dir.mkdir(exist_ok=True)
    
    try:
        if filepath.exists():
            # Read existing data and concatenate
            existing_df = pl.read_parquet(filepath)
            combined_df = pl.concat([existing_df, df])
        else:
            combined_df = df
        
        # Sort by p for efficient resume operations
        sorted_df = combined_df.sort("p")
        
        # Write to parquet
        sorted_df.write_parquet(filepath)
        
        # Only log on first save or if it's a significant batch
        if buffer_size is None or len(sorted_df) <= buffer_size or len(sorted_df) % (buffer_size * 10) == 0:
            logging.info(f"Data saved to {filepath} ({len(df)} new rows, {len(sorted_df)} total rows)")
        
    except Exception as e:
        logging.error(f"Error saving data to {filepath}: {e}")
        raise


def get_data_dir():
    """Get the data directory path."""
    return Path('data')


def cleanup_temp_files():
    """Clean up temporary files."""
    data_dir = get_data_dir()
    if data_dir.exists():
        for temp_file in data_dir.glob("*.tmp"):
            temp_file.unlink()


def check_disk_space(min_gb=1):
    """Check available disk space."""
    import shutil
    total, used, free = shutil.disk_usage(".")
    free_gb = free // (1024**3)
    
    if free_gb < min_gb:
        logging.warning(f"Low disk space: {free_gb}GB available")
        return False
    return True


def get_memory_usage():
    """Get current memory usage."""
    import psutil
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss / 1024 / 1024  # MB
