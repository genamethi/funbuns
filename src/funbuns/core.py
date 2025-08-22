"""
Core implementation of prime power partition algorithm: p = 2^m + q^n

Following the algorithm outlined in sketch.md:
- For each prime p, compute max_m = floor(log2(p))
- For each m_i in [1, max_m], compute remainder = p - 2^m_i  
- For each n_i, check if nth_root(remainder) is integer and prime
- Use try/except pattern to avoid unnecessary computations
"""

from sage.all import *
import polars as pl
from .utils import TimingCollector


class PPBatchProcessor:
    """Worker class for processing prime batches in multiprocessing."""
    

    
    def __init__(self, verbose: bool = False, use_table: bool = True):
        """Initialize processor with results storage and small primes table."""
        self.results = []  # List of DataFrames to be concatenated by consumer
        self.timer = TimingCollector(verbose=verbose)
        self.use_table = use_table
        # Log remainder tolerance for integer detection (near IEEE 754 machine epsilon)
        self.EPSILON = 1e-15  # Commented out - validation happens later
        
        # Load small primes table using utils function (always load for LSP constraint)
        from .utils import get_small_primes_table
        self.small_primes_table, self.lsp = get_small_primes_table()
    
    def pp_parts(self, p):
        """
        Find all partitions of prime p as p = 2^m + q^n and append DataFrames to results.
        Workers drop DataFrames - consumer handles concatenation.
        
        Args:
            p: Integer (SageMath) - the prime to decompose  
        """
        zero_df = pl.DataFrame(
            [(int(p), 0, 0, 0)],
            schema={'p': pl.Int64, 'm': pl.Int64, 'n': pl.Int64, 'q': pl.Int64},
            orient='row'
        )
        # Handle special cases for p = 2, 3
        if p == 2 or p == 3:
            self.results.append(zero_df)
            return
        
        max_m = p.exact_log(2)  # floor(log_2(p))
        iteration_results = []
        results_before = len(self.results)  # Track if we add any DataFrames

        with self.timer.time_operation("iterative_method", prime=int(p)):

            two_i = 1

            for m_i in range(1, max_m + 1):
                two_i <<= 1
                q_cand_i = p - two_i
                
                (pbase, pexp) = q_cand_i.is_prime_power(proof=False, get_data=True)

                if pexp != 0:
                    iteration_results.append((int(p), m_i, pexp, int(pbase)))
            
            iteration_df = pl.DataFrame(
                iteration_results,
                schema={'p': pl.Int64, 'm': pl.Int64, 'n': pl.Int64, 'q': pl.Int64},
                orient='row'
            )
            if iteration_results:
                self.results.append(iteration_df)
        
        # Check if we added any DataFrames for this prime
        if len(self.results) == results_before: 
            self.results.append(zero_df)

    
    def process_batch(self, prime_batch):
        """
        Process a batch of primes to find prime power partitions.
        
        Args:
            prime_batch: List of primes to process
            
        Returns:
            Single Polars DataFrame with all partition results
        """
        with self.timer.time_operation("process_batch", batch_size=len(prime_batch)):
            self.results.clear()  # Reset for new batch (list of DataFrames)
            
            for prime in prime_batch:
                with self.timer.time_operation("single_prime", prime=int(prime)):
                    self.pp_parts(Integer(prime))
            
            # Concatenate all DataFrames from this batch
            if self.results:
                with self.timer.time_operation("batch_concat", num_dataframes=len(self.results)):
                    return pl.concat(self.results)
            else:
                # Return empty DataFrame with correct schema
                return pl.DataFrame(
                    schema={'p': pl.Int64, 'm': pl.Int64, 'n': pl.Int64, 'q': pl.Int64}
                )


def worker_batch(prime_batch, verbose=False, use_table=True):
    """
    Module-level worker function for multiprocessing spawn compatibility.
    
    Args:
        prime_batch: List of primes to process
        verbose: Whether to enable verbose timing logging
        use_table: Whether to use small primes table optimization
        
    Returns:
        Tuple of (DataFrame, timing_data)
    """
    processor = PPBatchProcessor(verbose=verbose, use_table=use_table)
    result_df = processor.process_batch(prime_batch)
    return result_df, processor.timer.timings


class PPBatchFeeder:
    """Efficient batch generator using Polars Series.reshape() for batching."""
    
    def __init__(self, init_p: int, num_primes: int, batch_size: int, verbose: bool = False, start_idx: int = 0):
        """
        Initialize batch feeder using Polars reshape for optimal batching.
        
        Args:
            init_p: Starting prime (inclusive)
            num_primes: Total number of primes to process
            batch_size: Size of each prime batch
            verbose: Enable verbose output for profiling
            start_idx: Starting index for prime generation (from utils.resume_p)
        """
        # Validate that num_primes is divisible by batch_size
        if num_primes % batch_size != 0:
            raise ValueError(f"num_primes ({num_primes}) must be divisible by batch_size ({batch_size})")
        
        self.batch_size = batch_size
        self.num_batches = num_primes // batch_size
        
        if verbose:
            print("Verbose: Computing final prime...")
        P = Primes(proof=False)
        final_prime = P.unrank(start_idx + num_primes - 1)
        
        # Get all primes in one call (already a list)
        # Start from next_prime(init_p) to avoid including the already-processed init_p
        if init_p <= 2:
            start_prime = init_p
        else:
            start_prime = next_prime(init_p)
            
        if verbose:
            print(f"Verbose: Getting {num_primes} primes from {start_prime} to {final_prime}...")
        p_list = prime_range(start_prime, final_prime + 1)
        
        # Create Series and reshape into batches
        if verbose:
            print(f"Verbose: Reshaping into {self.num_batches} batches of {batch_size} primes...")
        primes_series = pl.Series("prime", p_list)
        self.batched_series = primes_series.reshape((self.num_batches, batch_size))
        
        if verbose:
            print(f"Verbose: Created {self.num_batches} batches ready for processing")
    
    def generate_batches(self):
        """Generate batches by iterating through reshaped Series rows."""
        for batch_array in self.batched_series:
            # Convert Array to Python list for worker compatibility
            batch = batch_array.to_list()
            yield batch


class PPConsumer:
    """Shared consumer that collects DataFrames and manages batch saves."""
    
    def __init__(self, buffer_size: int, save_callback):
        """
        Initialize consumer.
        
        Args:
            buffer_size: Integer - number of results to accumulate before saving
            save_callback: Function to call for saving data
        """
        self.buffer_size = buffer_size
        self.save_callback = save_callback
        self.df_buffer = []  # DataFrame buffer
        self.result_count = 0
    
    def add_results(self, results_df):
        """
        Add DataFrame results.
        
        Args:
            results_df: Polars DataFrame with partition results
        """
        if results_df.height > 0:
            self.df_buffer.append(results_df)
            self.result_count += results_df.height
            
            if self.result_count >= self.buffer_size:
                self._flush_results()
    
    def _flush_results(self):
        """Flush accumulated DataFrames to storage."""
        if not self.df_buffer:
            return
        
        # Concatenate all DataFrames and rename columns to match expected schema
        combined_df = pl.concat(self.df_buffer).rename({
            'p': 'p',
            'm': 'm_k', 
            'n': 'n_k',
            'q': 'q_k'
        })
        
        # Call save callback with buffer_size for logging control
        self.save_callback(combined_df, self.buffer_size)
        
        # Reset accumulation
        self.df_buffer = []
        self.result_count = 0
    
    def finalize(self):
        """Flush any remaining DataFrames."""
        self._flush_results()


def run_gen(init_p, num_primes, batch_size, cores, buffer_size, append_data, verbose=False, start_idx=0, use_table=True, use_separate_runs=False):
    """
    Main analysis runner - handles all processing logic.
    
    Args:
        init_p: Starting prime
        num_primes: Number of primes to process
        batch_size: Primes per worker batch
        cores: Number of worker processes
        buffer_size: Consumer buffer size
        append_data: Save callback function
        verbose: Enable verbose output for profiling
        start_idx: Starting index for prime generation (from utils.resume_p)
    """
    import multiprocessing as mp
    from tqdm import tqdm
    
    # Create batch feeder and consumer
    batch_feeder = PPBatchFeeder(init_p, num_primes, batch_size, verbose, start_idx)
    consumer = PPConsumer(buffer_size, append_data)
    
    print(f"Processing {num_primes} primes starting from {init_p}")
    print(f"Batch size: {batch_size} primes per worker")
    
    # Set spawn method to avoid fork issues
    mp.set_start_method('spawn', force=True)
    
    # Initialize timing collection
    all_timing_data = []
    
    # Process with multiprocessing and progress bar
    with mp.Pool(cores) as pool:
        batches_processed = 0
        primes_processed = 0
        
        with tqdm(total=num_primes, desc="Prime partition", unit="prime") as pbar:
            for prime_batch in batch_feeder.generate_batches():
                if not prime_batch:  # Empty batch means we're done
                    break
                
                # Process batch - returns (DataFrame, timing_data)
                results_df, timing_data = pool.apply(worker_batch, (prime_batch, verbose, use_table))
                
                # Collect timing data
                all_timing_data.extend(timing_data)
                
                # Pass DataFrame directly to consumer
                consumer.add_results(results_df)
                
                # Update progress
                primes_in_batch = len(prime_batch)
                primes_processed += primes_in_batch
                batches_processed += 1
                pbar.update(primes_in_batch)
                pbar.set_postfix({
                    "Batches": batches_processed,
                    "Batch Size": primes_in_batch,
                    "Results": results_df.height
                })
    
    # Finalize any remaining results
    consumer.finalize()
    

    
    # Process and save timing data
    if all_timing_data and verbose:
        timing_collector = TimingCollector(verbose=verbose)
        timing_collector.timings = all_timing_data
        timing_collector.save_debug_log()
        timing_collector.print_summary()
    
    print(f"\nCompleted processing {primes_processed} primes in {batches_processed} batches")
    print(f"Results merged and saved")
    
    # Automatically convert run files to blocks if using separate runs
    if use_separate_runs:
        from .utils import convert_runs_to_blocks_auto
        convert_runs_to_blocks_auto()

    if verbose and all_timing_data:
        print(f"Timing data collected: {len(all_timing_data)} operations")



