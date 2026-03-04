"""
Main entry point for prime power partition analysis.
"""

import argparse
import psutil
from .core import PPManager
from .utils import setup_logging, get_config, setup_analysis_mode, generate_partition_summary
from .dataprep import prepare_prime_powers
from .viewer import generate_dashboard
import polars as pl

#TODO: Rename this from funbuns lol
#TODO: Reduce stdout noise during lvl 0, implement different verbosity levels.
#TODO: Fix semaphore leak issue: Probably memory management issues during file saving.
#TODO: Fix issues with large data and Altair plots.
#TODO: Review "temp-scripts folder" and see if I can streamline the data management modules.



def main():
    #TODO: Add a command to print readme, and suggest bmgr commands as well.
    parser = argparse.ArgumentParser(description='Tools for study prime power partitions p = 2^m + q^n. Resumes from last prime saved in data/blocks.')
    parser.add_argument('-n', '--num-primes', type=int, required=False,
                       help='Number of primes to process')
    parser.add_argument('-b', '--batch-size', type=int, default=10000,
                       help='Number of primes per worker batch (default: 10000)')
    parser.add_argument('-p', '--processes', type=int, default=None,
                       help='Number of worker processes (default: number of physical cores)')
    parser.add_argument('-t', '--temp', action='store_true',
                       help='Run analysis in temporary file (for experiments)')
    parser.add_argument('--data-file', type=str, default=None,
                       help='Specify non-default data location')
    #TODO: Move this functionality into the bgmr
    parser.add_argument('--show-runs', action='store_true',
                       help='Show summary of all block files')

    #TODO: Re-implement this with VegaFusion
    parser.add_argument('--view', action='store_true',                        
                       help='Generate web-based reports using Altair')
    #TODO: Implement debug mode and keep this as level 1 verbosity (level 0 is default)
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Enable verbose output for debugging and profiling')
    #TODO: Implement this as level 2 verbosity
    parser.add_argument('-d', '-vv', '--debug', action='store_true',
                        help='More verbose with profiling of memory usage and timing data.')

    parser.add_argument('-g', '--genpp', type=int, metavar='N',
                       help='Prepare prime powers data for first N primes (p^1 through p^100)')

    # ℓ-adic analysis
    parser.add_argument('--ladic', action='store_true',
                       help='Run ℓ-adic Diophantine analysis on obstructed primes')
    parser.add_argument('--ladic-limit', type=int, default=None, metavar='N',
                       help='Limit ℓ-adic analysis to first N obstructed primes')
    parser.add_argument('--ladic-gap', type=int, default=None, metavar='P',
                       help='Deep gap-filling analysis for a single obstructed prime P')

    # Spectral analysis
    parser.add_argument('--spectral', action='store_true',
                       help='Run spectral/harmonic analysis on obstruction indicator')
    parser.add_argument('--clocks', type=int, default=None, metavar='N',
                       help='Run prime clock superposition analysis with first N primes')

    args = parser.parse_args()
    
    # Handle view mode
    if args.view:
        generate_dashboard(args.data_file)
        return
    
    # Handle prep mode
    if args.genpp:
        prepare_prime_powers(args.genpp)
        return
    
    # Handle show-runs mode
    if args.show_runs:
        from .utils import show_run_files_summary
        show_run_files_summary()
        return

    # Handle ℓ-adic analysis modes
    if args.ladic:
        from .ladic import run_ladic_analysis
        run_ladic_analysis(limit=args.ladic_limit, verbose=args.verbose)
        return

    if args.ladic_gap is not None:
        from .ladic import gap_filling_analysis
        df = gap_filling_analysis(args.ladic_gap)
        print(f"\nGap-filling analysis for p = {args.ladic_gap}")
        print(f"{'m':>4}  {'r':>14}  {'ω':>3}  {'Ω':>3}  {'share':>7}  factorization")
        print("-" * 70)
        for row in df.iter_rows(named=True):
            print(f"{row['m']:>4}  {row['r']:>14}  {row['omega']:>3}  "
                  f"{row['big_omega']:>3}  {row['dominant_share']:>7.4f}  {row['factorization']}")
        return

    # Handle spectral analysis
    if args.spectral:
        from .ladic import (obstruction_indicator, spectral_analysis_obstruction,
                            save_analysis)
        print("=== Spectral Analysis of Obstruction Indicator ===\n")
        indicator = obstruction_indicator(verbose=True)
        spectrum = spectral_analysis_obstruction(indicator, verbose=True)
        save_analysis(indicator, "obstruction_indicator")
        save_analysis(spectrum, "obstruction_spectrum")
        return

    if args.clocks is not None:
        from .ladic import clock_analysis, save_analysis
        print(f"=== Prime Clock Superposition (N={args.clocks}) ===\n")
        clocks = clock_analysis(limit=args.clocks, verbose=True)
        save_analysis(clocks, "clock_superposition")
        return

    # Ensure -n is provided when not in view/prep/show-runs mode
    if args.num_primes is None:
        parser.error("-n/--number is required when not using --view or --prep modes")
    
    # Determine number of workers
    if args.processes is not None:
        cores = args.processes
        print(f"Using {cores} workers (user-specified)")
    else:
        cores = psutil.cpu_count(logical=False)
        print(f"Using {cores} workers (physical cores)")
    
    #See utils.py
    setup_logging()
    
    # Get configuration and setup analysis mode
    #See get_config in utils.py
    config = get_config()
    buffer_size = args.batch_size * 2

    
    # Setup analysis mode (handles temp, fresh, resume logic) (in utils.py)
    init_p, append_func, data_file = setup_analysis_mode(args, config)
    
    if args.temp:
        print(f"Running in temporary mode: {data_file}")

    # Create PPManager instance and run
    manager = PPManager(init_p, args.num_primes, args.batch_size, cores, buffer_size, append_func, args.verbose)
    manager.run_gen()
    
    # Show partition summary
    #if args.temp:
        #generate_partition_summary(data_file, verbose=args.verbose)
    #else:
    #generate_partition_summary(verbose=args.verbose)


if __name__ == "__main__":
    main()
