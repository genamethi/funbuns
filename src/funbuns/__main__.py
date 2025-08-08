"""
Main entry point for prime power partition analysis.
"""

import argparse
import multiprocessing as mp
import psutil
from tqdm import tqdm
from sage.all import *
from .core import PPFeeder, PPProducer, PPConsumer
from .utils import setup_logging, resume_p, append_data, get_config
from .viewer import generate_dashboard


def worker(prime):
    """
    Worker function for multiprocessing.
    
    Args:
        prime: Integer - prime to process
        
    Returns:
        Polars DataFrame with partition results
    """
    producer = PPProducer()
    return producer.process_prime(prime)


def main():
    parser = argparse.ArgumentParser(description='Analyze prime power partitions p = 2^m + q^n')
    parser.add_argument('-n', '--number', type=int, required=True,
                       help='Number of primes to analyze')
    parser.add_argument('--workers', type=int, default=None,
                       help='Number of worker processes (default: number of physical cores)')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from previous progress')
    parser.add_argument('--data-file', type=str, default=None,
                       help='Specify custom data file (overrides default)')
    parser.add_argument('--view', action='store_true',
                       help='Generate interactive dashboard from existing data')
    
    args = parser.parse_args()
    
    # Handle view mode
    if args.view:
        generate_dashboard(args.data_file)
        return
    
    # Determine number of workers
    if args.workers is not None:
        cores = args.workers
        print(f"Using {cores} workers (user-specified)")
    else:
        cores = psutil.cpu_count(logical=False)
        print(f"Using {cores} workers (physical cores)")
    
    setup_logging()
    
    # Get configuration
    config = get_config()
    buffer_size = config.get('buffer_size', 10000)
    
    # Handle resume functionality
    if args.resume:
        try:
            init_p = resume_p()
        except Exception as e:
            print(f"Resume failed: {e}")
            return
    else:
        init_p = 2  # Start from first prime
    
    # Create feeder starting from resume point
    feeder = PPFeeder(init_p)
    
    # Create work items (primes)
    work_items = []
    for _ in range(args.number):
        prime = feeder.get_next_prime()
        if prime is None:
            break
        work_items.append(prime)
    
    if not work_items:
        print("No primes to process!")
        return
    
    print(f"Processing {len(work_items)} primes starting from {work_items[0]}")
    
    # Create consumer to handle results
    consumer = PPConsumer(buffer_size, append_data)
    
    # Process with multiprocessing and progress bar
    with mp.Pool(cores) as pool:
        with tqdm(total=len(work_items), desc="Prime partition analysis", unit="prime") as pbar:
            for result_df in pool.imap(worker, work_items):
                consumer.add_result(result_df)
                pbar.update(1)
    
    # Finalize any remaining results
    consumer.finalize()
    
    print(f"\nCompleted processing {len(work_items)} primes")
    print(f"Results saved to data/pparts.parquet")


if __name__ == "__main__":
    main()
