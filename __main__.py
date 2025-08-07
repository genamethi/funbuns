"""
Main entry point for prime decomposition analysis.
"""

import argparse
import multiprocessing as mp
import psutil
from sage.all import *
from core import analyze
from utils import save_results, setup_logging


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
    
    args = parser.parse_args()
    
    # Detect physical cores
    cores = psutil.cpu_count(logical=False)
    print(f"Using {cores} physical cores")
    
    setup_logging()
    
    # Create work items (prime ranks)
    work_items = list(range(args.number))
    
    # Process with multiprocessing
    with mp.Pool(cores) as pool:
        results = pool.map(worker, work_items)
    
    # Save results
    save_results(results, f"decomp_results_n{args.number}.json")
    
    # Summary
    total_decomp = sum(r['count'] for r in results)
    primes_with_decomp = sum(1 for r in results if r['count'] > 0)
    
    print(f"\nSummary:")
    print(f"  Primes analyzed: {len(results)}")
    print(f"  Primes with decompositions: {primes_with_decomp}")
    print(f"  Total decompositions found: {total_decomp}")


if __name__ == "__main__":
    main()
