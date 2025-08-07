"""
Utility functions for file handling, OS operations, and I/O.
"""

import json
import pickle
import logging
from datetime import datetime
from pathlib import Path
import polars as pl
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
    """Get the default data file path from configuration."""
    return Path("data") / "funbuns_progress.pkl"


def save_results(results, filename=None, resume_mode=False):
    """
    Save analysis results to pickle file.
    
    Args:
        results: List of analysis results
        filename: String - output filename (optional)
        resume_mode: Boolean - if True, use default resume file
    """
    # Determine filepath
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    
    if resume_mode or filename is None:
        filepath = get_default_data_file()
    else:
        # Generate unique filename if needed
        base_path = data_dir / filename
        if base_path.exists():
            stem = base_path.stem
            suffix = base_path.suffix or '.pkl'
            counter = 1
            while base_path.exists():
                base_path = data_dir / f"{stem}_{counter}{suffix}"
                counter += 1
        filepath = base_path
    
    # Load existing data for resume mode
    existing_data = {}
    if resume_mode and filepath.exists():
        try:
            with open(filepath, 'rb') as f:
                existing_data = pickle.load(f)
        except (pickle.PickleError, IOError):
            logging.warning(f"Could not load existing data from {filepath}, starting fresh")
    
    # Merge or create data
    if resume_mode and existing_data:
        existing_results = existing_data.get('results', [])
        # Create a set of existing primes to avoid duplicates
        existing_primes = {r['prime'] for r in existing_results}
        new_results = [r for r in results if r['prime'] not in existing_primes]
        all_results = existing_results + new_results
        
        data_to_save = {
            'timestamp': datetime.now().isoformat(),
            'original_timestamp': existing_data.get('timestamp', existing_data.get('original_timestamp')),
            'total_primes': len(all_results),
            'results': all_results
        }
    else:
        data_to_save = {
            'timestamp': datetime.now().isoformat(),
            'total_primes': len(results),
            'results': results
        }
    
    with open(filepath, 'wb') as f:
        pickle.dump(data_to_save, f)
    
    logging.info(f"Data saved to {filepath}")


def results_to_polars_df(results):
    """
    Convert results to Polars DataFrame in p,decomp_count,m,n,q format.
    
    Args:
        results: List of analysis results
        
    Returns:
        Polars DataFrame
    """
    rows = []
    for result in results:
        p = result['prime']
        decomp_count = result['count']
        
        if decomp_count == 0:
            # Empty decomposition: p,0,0,0,0
            rows.append({
                'p': int(p), 
                'decomp_count': 0, 
                'm': 0, 
                'n': 0, 
                'q': 0
            })
        else:
            # Each decomposition: p,decomp_count,m,n,q
            for m, n, q in result['decomp']:
                rows.append({
                    'p': int(p), 
                    'decomp_count': decomp_count, 
                    'm': int(m), 
                    'n': int(n), 
                    'q': int(q)
                })
    
    return pl.DataFrame(rows)


def load_results(filename=None):
    """
    Load analysis results from pickle file.
    
    Args:
        filename: String - input filename (if None, uses default)
        
    Returns:
        Tuple of (results_list, metadata_dict)
    """
    if filename is None:
        filepath = get_default_data_file()
    else:
        filepath = Path('data') / filename
    
    with open(filepath, 'rb') as f:
        data = pickle.load(f)
    
    return data['results'], {
        'timestamp': data.get('timestamp'),
        'original_timestamp': data.get('original_timestamp'),
        'total_primes': data.get('total_primes', len(data['results']))
    }


def get_completed_primes(filename=None):
    """
    Get set of primes already completed from existing results.
    
    Args:
        filename: String - input filename (if None, uses default)
        
    Returns:
        Set of completed prime numbers
    """
    try:
        results, _ = load_results(filename)
        return {r['prime'] for r in results}
    except (FileNotFoundError, pickle.PickleError, KeyError):
        return set()


def get_data_dir():
    """Get or create data directory."""
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    return data_dir


def cleanup_temp_files():
    """Clean up temporary files."""
    temp_files = Path('.').glob('*.tmp')
    for f in temp_files:
        f.unlink()
        logging.info(f"Removed temp file: {f}")


def check_disk_space(min_gb=1):
    """
    Check available disk space.
    
    Args:
        min_gb: Minimum required space in GB
        
    Returns:
        Boolean - True if sufficient space available
    """
    import shutil
    
    total, used, free = shutil.disk_usage('.')
    free_gb = free / (1024**3)
    
    if free_gb < min_gb:
        logging.warning(f"Low disk space: {free_gb:.2f} GB available")
        return False
    
    return True


def get_memory_usage():
    """Get current memory usage."""
    import psutil
    
    process = psutil.Process()
    memory_mb = process.memory_info().rss / (1024**2)
    
    return memory_mb
