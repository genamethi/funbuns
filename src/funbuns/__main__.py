"""
Main entry point for prime decomposition analysis.
"""

import argparse
import multiprocessing as mp
import psutil
from tqdm import tqdm
from sage.all import *
from .core import analyze
from .utils import save_results, setup_logging, get_completed_primes, get_default_data_file
from .viewer import generate_dashboard


def worker(prime_rank):
    """
    Worker function for multiprocessing.
    
    Args:
        prime_rank: Integer - rank of prime to process (0-indexed)
        
    Returns:
        Analysis result for the prime
    """
    P = Primes()
    p = P.unrank(prime_rank)
    return analyze(p)


def main():
    parser = argparse.ArgumentParser(description='Analyze prime decompositions p = 2^m + q^n')
    parser.add_argument('-n', '--number', type=int, required=True,
                       help='Number of primes to analyze')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from previous progress using default data file')
    parser.add_argument('--data-file', type=str, default=None,
                       help='Specify custom data file (overrides default)')
    parser.add_argument('--view', action='store_true',
                       help='Generate interactive dashboard from existing data')
    
    args = parser.parse_args()
    
    # Handle view mode
    if args.view:
        generate_dashboard(args.data_file)
        return
    
    # Detect physical cores
    cores = psutil.cpu_count(logical=False)
    print(f"Using {cores} physical cores")
    
    setup_logging()
    
    # Handle resume functionality
    data_file = args.data_file if args.data_file else get_default_data_file()
    completed_primes = set()
    
    if args.resume:
        completed_primes = get_completed_primes(args.data_file)
        if completed_primes:
            print(f"Resuming: {len(completed_primes)} primes already completed")
            print(f"Resume file: {data_file}")
        else:
            print("No previous progress found, starting fresh")
    
    # Create work items (prime ranks), excluding completed ones
    P = Primes()
    all_work_items = list(range(args.number))
    
    if completed_primes:
        # Filter out already completed primes
        work_items = []
        for rank in all_work_items:
            prime = P.unrank(rank)
            if prime not in completed_primes:
                work_items.append(rank)
        
        if not work_items:
            print("All requested primes already completed!")
            return
        
        print(f"Remaining work: {len(work_items)} primes to analyze")
    else:
        work_items = all_work_items
    
    # Estimate total work using prime number theorem: π(n) ≈ n/ln(n)
    # Get the largest prime we'll process
    P = Primes()
    max_prime = P.unrank(args.number - 1)
    estimated_total_work = int(max_prime / log(max_prime))
    
    print(f"Processing up to prime {max_prime}")
    print(f"Estimated computational work units: {estimated_total_work}")
    
    # Process with multiprocessing and progress bar
    with mp.Pool(cores) as pool:
        with tqdm(total=estimated_total_work, desc="Prime decomposition analysis", unit="work") as pbar:
            results = []
            step_size = max(1, estimated_total_work // args.number)  # How much to advance per prime
            
            for i, result in enumerate(pool.imap(worker, work_items)):
                results.append(result)
                # Update progress based on which prime we just completed
                current_prime = P.unrank(i)
                expected_work_so_far = int(current_prime / log(current_prime)) if current_prime > 1 else 1
                pbar.n = min(expected_work_so_far, estimated_total_work)
                pbar.refresh()
    
    # Save results
    if args.resume:
        save_results(results, filename=args.data_file, resume_mode=True)
    else:
        save_results(results, f"decomp_results_n{args.number}.pkl")
    
    # Summary statistics
    total_partitions = sum(r['count'] for r in results)
    primes_with_partitions = sum(1 for r in results if r['count'] > 0)
    
    # Get the largest prime analyzed
    largest_prime = max(r['prime'] for r in results) if results else 0
    
    # Create partition count frequency table
    partition_counts = {}
    for r in results:
        count = r['count']
        partition_counts[count] = partition_counts.get(count, 0) + 1
    
    print(f"\nSummary:")
    print(f"  Primes analyzed: {len(results)}")
    print(f"  Largest prime: {largest_prime}")
    print(f"  Primes with partitions: {primes_with_partitions}")
    print(f"  Total partitions found: {total_partitions}")
    
    print(f"\nPartition Count Frequency Table:")
    print(f"  {'Partitions':<12} {'# Primes':<10} {'Percentage':<10}")
    print(f"  {'-'*12} {'-'*10} {'-'*10}")
    
    # Sort by partition count
    for partition_count in sorted(partition_counts.keys()):
        prime_count = partition_counts[partition_count]
        percentage = (prime_count / len(results)) * 100
        print(f"  {partition_count:<12} {prime_count:<10} {percentage:>8.2f}%")


if __name__ == "__main__":
    main()
