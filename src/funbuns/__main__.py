"""
Main entry point for prime power partition analysis.

Infrastructure commands (database, web server, notebook) live in
funbuns-admin.
"""

import argparse
import sys
import time
import psutil
from .core import PPManager
from .utils import (setup_logging, get_config, setup_analysis_mode,
                    JournalWriter)
#from .dataprep import prepare_prime_powers
import polars as pl


def main():
    parser = argparse.ArgumentParser(
        description='Prime power partition analysis: p = 2^m + q^n.\n'
                    'Resumes from max(p) upper_bound in the iceberg primes\n'
                    'table manifest metadata (no data-file scan).\n\n'
                    'Infrastructure: use funbuns-admin (db, serve, notebook).',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # --- Generation ---
    gen = parser.add_argument_group('generation', 'Compute new prime partitions')
    gen.add_argument('-n', '--num-primes', type=int, required=False,
                     help='Number of primes to process')
    gen.add_argument('-b', '--batch-size', type=int, default=10000,
                     help='Number of primes per worker batch (default: 10000)')
    gen.add_argument('-p', '--processes', type=int, default=None,
                     help='Number of worker processes (default: physical cores)')
    gen.add_argument('-t', '--temp', action='store_true',
                     help='Run analysis in temporary file (for experiments)')
    gen.add_argument('--data-file', type=str, default=None,
                     help='Specify non-default data location')
    gen.add_argument('-i', '--init', type=int, default=None, metavar='P',
                     help='Override resume prime (start generation from prime P)')
#   gen.add_argument('-g', '--genpp', type=int, metavar='N',
#                 help='Prepare prime powers data for first N primes (p^1 through p^100)')

    # --- Cohomological / algebraic analysis ---
    cohom = parser.add_argument_group('cohomological analysis',
                                      'Local-global, l-adic, spectral, fixed-mod')
    cohom.add_argument('--ladic', action='store_true',
                       help='Run l-adic Diophantine analysis on obstructed primes')
    cohom.add_argument('--ladic-limit', type=int, default=None, metavar='N',
                       help='Limit l-adic analysis to first N obstructed primes (implies --ladic)')
    cohom.add_argument('--ladic-gap', type=int, default=None, metavar='P',
                       help='Deep gap-filling analysis for a single prime P')
    cohom.add_argument('--spectral', action='store_true',
                       help='Run spectral/harmonic analysis on obstruction indicator')
    cohom.add_argument('--clocks', type=int, default=None, metavar='N',
                       help='Run prime clock superposition analysis with first N primes')
    cohom.add_argument('--fixed-mod', action='store_true',
                       help='Run fixed-modulus ring analysis (local obstructions, Hensel lifting, CRT)')
    cohom.add_argument('--fixed-mod-limit', type=int, default=None, metavar='N',
                       help='Limit fixed-mod analysis to first N obstructed primes (implies --fixed-mod)')

    # --- Remainder / Zipf profiling ---
    prof = parser.add_argument_group('profiling', 'Remainder profiling, Zipf analysis')
    prof.add_argument('--remainder', action='store_true',
                      help='Run incremental remainder profiling (omega, near-misses, filtration)')
    prof.add_argument('--remainder-limit', type=int, default=None, metavar='N',
                      help='Limit remainder analysis to N new obstructed primes (implies --remainder)')
    prof.add_argument('--zipf', action='store_true',
                      help='Run bounded-memory Zipf/Mandelbrot analysis on q_k frequencies')
    prof.add_argument('--zipf-qmax', type=int, default=1_000_000, metavar='Q',
                      help='Max q value to track individually (default: 1000000)')

    # --- Visualization / exploration ---
    viz = parser.add_argument_group('exploration', 'Visualization, Baker circle, data exploration')
    viz.add_argument('--baker-circle', type=int, default=None, metavar='P',
                     help='Generate Baker circle visualization for prime P (archimedean H^1)')
    viz.add_argument('--explore', action='store_true',
                     help='Explore recurrence structure in (q, m) strata')
    viz.add_argument('--explore-q', type=int, default=None, metavar='Q',
                     help='Fix q for exploration (default: sweep {3,5,7,11,13})')
    viz.add_argument('--explore-m', type=int, default=None, metavar='M',
                     help='Fix m for exploration (default: sweep)')
    viz.add_argument('--tree', action='store_true',
                     help='Show tree view: n-fiber branching over m (requires --explore-q)')
    viz.add_argument('--local-global', action='store_true',
                     help='Run local-global sieve analysis (implies --explore, default q=3)')
    viz.add_argument('--view', action='store_true',
                     help='Generate web-based dashboard using Altair')

    # --- Partition queries ---
    pq = parser.add_argument_group('partition queries', 'Query primes by decomposition count')
    pq.add_argument('--partitions', action='store_true',
                    help='Query primes by decomposition count')
    pq.add_argument('--partition-k', type=int, default=None, metavar='K',
                    help='Show primes with exactly K decompositions')
    pq.add_argument('--partition-p', type=int, default=None, metavar='P',
                    help='Show all decompositions for prime P')
    pq.add_argument('--partition-limit', type=int, default=50, metavar='N',
                    help='Max primes to show (default: 50)')
    pq.add_argument('--partition-max-p', type=int, default=None, metavar='P',
                    help='Show all decompositions for primes up to P, grouped by k')

    # --- Verbosity ---
    #TODO: Implement debug mode and keep this as level 1 verbosity (level 0 is default)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose output for debugging and profiling')
    #TODO: Implement this as level 2 verbosity
    parser.add_argument('-d', '-vv', '--debug', action='store_true',
                        help='More verbose with profiling of memory usage and timing data.')

    args = parser.parse_args()

    # --ladic-limit implies --ladic
    if args.ladic_limit is not None:
        args.ladic = True

    # --fixed-mod-limit implies --fixed-mod
    if args.fixed_mod_limit is not None:
        args.fixed_mod = True

    # --remainder-limit implies --remainder
    if args.remainder_limit is not None:
        args.remainder = True

    # Identify which analysis modes were requested
    analysis_modes = []
    if args.view:
        analysis_modes.append('view')
#    if args.genpp:
#        analysis_modes.append('genpp')
    if args.ladic:
        analysis_modes.append('ladic')
    if args.ladic_gap is not None:
        analysis_modes.append('ladic_gap')
    if args.spectral:
        analysis_modes.append('spectral')
    if args.clocks is not None:
        analysis_modes.append('clocks')
    if args.fixed_mod:
        analysis_modes.append('fixed_mod')
    if args.remainder:
        analysis_modes.append('remainder')
    if args.zipf:
        analysis_modes.append('zipf')
    if args.baker_circle is not None:
        analysis_modes.append('baker_circle')
    if (args.explore or args.explore_q is not None or args.explore_m is not None
            or args.tree or args.local_global
            or args.partitions or args.partition_k is not None
            or args.partition_p is not None or args.partition_max_p is not None):
        analysis_modes.append('explore')

    # Warn about ignored flags when using special modes
    if analysis_modes and analysis_modes != ['view']:
        ignored = []
        if args.num_primes is not None and 'ladic' in analysis_modes:
            ignored.append('-n/--num-primes')
        if args.batch_size != 10000 and args.num_primes is None:
            ignored.append('-b/--batch-size')
        if args.processes is not None and args.num_primes is None:
            ignored.append('-p/--processes')
        if args.temp and args.num_primes is None:
            ignored.append('-t/--temp')
        if args.data_file and 'view' not in analysis_modes:
            ignored.append('--data-file')
        if ignored:
            print(f"Note: {', '.join(ignored)} ignored in this mode.\n")

    # Handle view mode
    if args.view:
        from .viewer import generate_dashboard
        generate_dashboard(args.data_file)
        return

    # Handle prep mode
    #Retiring this
    #if args.genpp:
    #    prepare_prime_powers(args.genpp)
    #    return

    # Handle gap-filling (standalone, doesn't need block data check for arbitrary primes)
    if args.ladic_gap is not None:
        from .ladic import gap_filling_analysis
        df = gap_filling_analysis(args.ladic_gap)
        print(f"\nGap-filling analysis for p = {args.ladic_gap}")
        print(f"{'m':>4}  {'r':>14}  {'\u03c9':>3}  {'\u03a9':>3}  {'share':>7}  factorization")
        print("-" * 70)
        for row in df.iter_rows(named=True):
            print(f"{row['m']:>4}  {row['r']:>14}  {row['omega']:>3}  "
                  f"{row['big_omega']:>3}  {row['dominant_share']:>7.4f}  {row['factorization']}")
        return

    # Baker circle (standalone, no block data needed)
    if args.baker_circle is not None:
        from .baker_circle import run_baker_circle
        run_baker_circle(args.baker_circle, verbose=args.verbose)
        return

    # Analysis modes that need block data
    ran_analysis = False

    if args.ladic:
        from .ladic import run_ladic_analysis
        run_ladic_analysis(limit=args.ladic_limit, verbose=args.verbose)
        ran_analysis = True

    if args.spectral:
        from .spectral import (obstruction_indicator, spectral_analysis_obstruction,
                               save_analysis)
        print("=== Spectral Analysis of Obstruction Indicator ===\n")
        indicator = obstruction_indicator(verbose=args.verbose)
        spectrum = spectral_analysis_obstruction(indicator, verbose=args.verbose)
        save_analysis(indicator, "obstruction_indicator")
        save_analysis(spectrum, "obstruction_spectrum")
        ran_analysis = True

    if args.clocks is not None:
        from .spectral import clock_analysis, save_analysis
        print(f"=== Prime Clock Superposition (N={args.clocks}) ===\n")
        clocks = clock_analysis(limit=args.clocks, verbose=args.verbose)
        save_analysis(clocks, "clock_superposition")
        ran_analysis = True

    if args.fixed_mod:
        from .fixed_mod import run_fixed_mod_analysis
        run_fixed_mod_analysis(limit=args.fixed_mod_limit, verbose=args.verbose)
        ran_analysis = True

    if args.remainder:
        from .remainder import run_remainder_analysis
        run_remainder_analysis(limit=args.remainder_limit, verbose=args.verbose)
        ran_analysis = True

    if args.zipf:
        from .zipf import run_zipf_analysis
        run_zipf_analysis(q_max=args.zipf_qmax, verbose=args.verbose)
        ran_analysis = True

    if 'explore' in analysis_modes:
        from .data_exploration import run_exploration
        run_exploration(q=args.explore_q, m=args.explore_m, verbose=args.verbose,
                        tree=args.tree, local_global=args.local_global,
                        partitions=args.partitions,
                        partition_k=args.partition_k,
                        partition_p=args.partition_p,
                        partition_limit=args.partition_limit,
                        partition_max_p=args.partition_max_p)
        ran_analysis = True

    if ran_analysis:
        return

    # Default mode: prime generation (requires -n)
    if args.num_primes is None:
        parser.print_help()
        print("\nInfrastructure commands: funbuns-admin db|serve|notebook")
        return

    # Determine number of workers
    if args.processes is not None:
        cores = args.processes
        print(f"Using {cores} workers (user-specified)")
    else:
        cores = psutil.cpu_count(logical=False)
        print(f"Using {cores} workers (physical cores)")

    setup_logging()

    config = get_config()

    # Journal for generation runs
    journal = JournalWriter(name="funbuns")
    t0 = time.monotonic()
    journal.log("main", "run_start",
                num_primes=args.num_primes, batch_size=args.batch_size,
                cores=cores)

    init_p, writer, temp_root = setup_analysis_mode(args, config)
    if temp_root is not None:
        print(f"Running in temporary mode: {temp_root}")
    if args.init is not None:
        print(f"Starting from prime {args.init} (-i override, writing to iceberg)")

    # Create PPManager instance and run
    manager = PPManager(init_p, args.num_primes, args.batch_size, cores,
                        append_data=writer.flush,
                        verbose=args.verbose)
    gen_status = manager.run_gen() or {}

    # Register every parquet file flushed during the run in a single
    # catalog commit. Runs even if interrupted — parquet on disk is
    # durable and worth registering.
    try:
        writer.commit_pending()
    except Exception as exc:
        journal.log("main", "commit_pending_failed", error=repr(exc))
        raise

    elapsed = round(time.monotonic() - t0, 2)

    # Emit shutdown/end journal event
    if gen_status.get('interrupted'):
        journal.log("main", "shutdown",
                    elapsed_s=elapsed,
                    abandoned=gen_status.get('abandoned', False),
                    primes_processed=gen_status.get('primes_processed', 0),
                    primes_not_processed=gen_status.get('primes_not_processed', 0))
    else:
        journal.log("main", "run_end",
                    elapsed_s=elapsed,
                    primes_processed=gen_status.get('primes_processed', 0))

    # Print resume command if interrupted
    remaining = gen_status.get('primes_not_processed', 0)
    if isinstance(remaining, int) and remaining > 0:
        print(f"\n{remaining:,} primes not processed.")
        print(f"Resume: funbuns -n {remaining} -b {args.batch_size}")


if __name__ == "__main__":
    main()
