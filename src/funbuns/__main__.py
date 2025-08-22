"""
Main entry point for prime power partition analysis.
"""

import argparse
import psutil
from .core import run_gen
from .utils import setup_logging, get_config, setup_analysis_mode, generate_partition_summary
from .dataprep import prepare_prime_powers
from .viewer import generate_dashboard
import polars as pl








def main():
    parser = argparse.ArgumentParser(description='Analyze prime power partitions p = 2^m + q^n (default: resume from last prime)')
    parser.add_argument('-n', '--number', type=int, required=False,
                       help='Number of primes to analyze')
    parser.add_argument('--workers', type=int, default=None,
                       help='Number of worker processes (default: number of physical cores)')
    parser.add_argument('--temp', action='store_true',
                       help='Run analysis in temporary file (for experiments)')
    parser.add_argument('--fresh', action='store_true',
                       help='Start fresh by deleting existing data')
    parser.add_argument('--data-file', type=str, default=None,
                       help='Specify custom data file (overrides default)')
    parser.add_argument('--view', action='store_true',
                       help='Generate interactive dashboard from existing data')
    parser.add_argument('-p', '--prep', type=int, metavar='N',
                       help='Prepare prime powers data for first N primes (p^1 through p^100)')
    parser.add_argument('-b', '--batch-size', type=int, default=1000,
                       help='Number of primes per worker batch (default: 1000)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Enable verbose output for debugging and profiling')
    parser.add_argument('--monolithic', action='store_true',
                       help='Use single monolithic file instead of separate run files (overrides config)')

    parser.add_argument('--show-runs', action='store_true',
                       help='Show summary of all block files')
    
    args = parser.parse_args()
    
    # Handle view mode
    if args.view:
        generate_dashboard(args.data_file)
        return
    
    # Handle prep mode
    if args.prep:
        prepare_prime_powers(args.prep)
        return
    
    # Handle show-runs mode
    if args.show_runs:
        from .utils import show_run_files_summary
        show_run_files_summary()
        return
    

    
    # Ensure -n is provided when not in view/prep/show-runs mode
    if args.number is None:
        parser.error("-n/--number is required when not using --view or --prep modes")
    
    # Determine number of workers
    if args.workers is not None:
        cores = args.workers
        print(f"Using {cores} workers (user-specified)")
    else:
        cores = psutil.cpu_count(logical=False)
        print(f"Using {cores} workers (physical cores)")
    
    setup_logging()
    
    # Get configuration and setup analysis mode
    config = get_config()
    buffer_size = config.get('buffer_size', 10000)
    
    # Setup analysis mode (handles temp, fresh, resume logic)
    init_p, start_idx, append_func, use_separate_runs, data_file = setup_analysis_mode(args, config)
    
    print(f"Data mode: {'Separate run files' if use_separate_runs else 'Monolithic file'}")
    if args.temp:
        print(f"Running in temporary mode: {data_file}")
    
    run_gen(init_p, args.number, args.batch_size, cores, buffer_size, append_func, args.verbose, start_idx,  use_separate_runs)
    
    # Show partition summary
    if args.temp:
        generate_partition_summary(data_file, verbose=args.verbose)
    else:
        generate_partition_summary(verbose=args.verbose)


if __name__ == "__main__":
    main()
