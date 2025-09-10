"""
Core implementation of prime power partition algorithm: p = 2^m + q^n

Following the algorithm outlined in sketch.md:
- For each prime p, compute max_m = floor(log2(p))
- For each m_i in [1, max_m], compute remainder = p - 2^m_i  
- For each n_i, check if nth_root(remainder) is integer and prime
- Use try/except pattern to avoid unnecessary computations
"""

from curses import init_pair
from sage.all import prime_range, Primes, next_prime, prime_pi
import polars as pl
import numpy as np
from itertools import batched
from .utils import TimingCollector, convert_runs_to_blocks_auto, PARTITION_SCHEMA, PARTITION_DISTRIBUTION

class PPBatchProcessor:
    """Worker class for processing prime batches using pre-allocated arrays."""
    
    def __init__(self, verbose: bool = False):
        """Initialize processor with timing collector."""
        # Remove DataFrame storage - using arrays now
        #if verbose:  # Use verbose flag instead of undefined debug_mode
            #self.timer = TimingCollector(verbose=verbose)
        #Below is from small primes table implementation, currently excised.
        # Log remainder tolerance for integer detection (near IEEE 754 machine epsilon)
        #self.EPSILON = 1e-15  
        
        # Load small primes table using utils function (always load for LSP constraint)
        #from .utils import get_small_primes_table
        #self.small_primes_table, self.lsp = get_small_primes_table()
    
    def _process_prime_to_array(self, p):
        """
        Process single prime, writing results directly to pre-allocated array.
        
        Args:
            p: Integer (SageMath) - the prime to decompose  
        """
        # Handle special cases for p = 2, 3
        #if p == 2 or p == 3:
            #self._write_zero_row(int(p))
            #return
            
        max_m = p.exact_log(2)  # floor(log_2(p))
        found_partition = False
        two_i = 1
        
        for m_i in range(1, max_m + 1):
            two_i <<= 1
            q_cand_i = p - two_i
            
            (pbase, pexp) = q_cand_i.is_prime_power(proof=False, get_data=True)
            
            if pexp != 0:
                if self.current_row >= len(self.results_array):
                    self._grow_array()
                self.results_array[self.current_row] = [p, m_i, pexp, pbase]
                self.current_row += 1
                found_partition = True
        
        # Add zero row if no partitions found
        if not found_partition:
            if self.current_row >= len(self.results_array):
                self._grow_array()
            self.results_array[self.current_row] = [p, 0, 0, 0]
            self.current_row += 1
    
    
    def _grow_array(self):
        """Double the array size when needed."""
        old_size = len(self.results_array)
        new_size = old_size * 2
        new_array = np.zeros((new_size, 4), dtype=np.int64)
        new_array[:self.current_row] = self.results_array[:self.current_row]
        self.results_array = new_array

    
    def process_batch(self, prime_batch):
        """
        Process a batch of primes using pre-allocated array.
        
        Args:
            prime_batch: List of primes to process
            
        Returns:
            NumPy array with partition results [p, m, n, q]
        """
        batch_size = len(prime_batch)
        
        # Estimate initial array size using empirical distribution
        # With 10k batch size and 1.7 avg rows per prime, this is very stable
        estimated_rows = int(batch_size * PARTITION_DISTRIBUTION['avg_rows_per_prime'] * 1.2)  # 20% buffer
        
        # Initialize pre-allocated array
        self.results_array = np.zeros((estimated_rows, 4), dtype=np.int64)
        self.current_row = 0
        
        # Process each prime directly to array
        for prime in prime_batch:
            self._process_prime_to_array(prime)
        
        # Return only the used portion
        return self.results_array[:self.current_row]


def worker_batch(prime_batch, verbose=False):
    """
    Module-level worker function for multiprocessing spawn compatibility.
    
    Args:
        prime_batch: List of primes to process
        verbose: Whether to enable verbose timing logging

        
    Returns:
        Tuple of (DataFrame, timing_data)
    """
    processor = PPBatchProcessor(verbose=verbose)
    result_array = processor.process_batch(prime_batch)
    
    # Convert raw array results to DataFrame once per worker
    if result_array.size > 0:
        result_df = pl.DataFrame(
            result_array,
            schema=PARTITION_SCHEMA,
            orient='row'
        )
    else:
        # Return empty DataFrame with correct schema
        result_df = pl.DataFrame(schema=PARTITION_SCHEMA)
    
    # Return timing data if debug mode is enabled
    #timing_data = processor.timer.timings if hasattr(processor, 'timer') else []
    return result_df #, timing_data


class PPBatchFeeder:
    """Efficient batch generator using Polars Series.reshape() for batching."""
    
    def __init__(self, init_p: int, num_primes: int, batch_size: int, verbose: bool = False):
        """
        Initialize batch feeder using Polars reshape for optimal batching.
        
        Args:
            init_p: IT'S ME AGAIN.
            num_primes: Total number of primes to process
            batch_size: Size of each prime batch
            verbose: Enable verbose output for profiling

        """
        # Validate that num_primes is divisible by batch_size
        if num_primes % batch_size != 0:
            raise ValueError(f"num_primes ({num_primes}) must be divisible by batch_size ({batch_size})")
        
        self.batch_size = batch_size
        self.num_batches = num_primes // batch_size
        
        #Todo: Work backwards to add handling for initial prime == 2/no data case

        start_idx = prime_pi(init_p)

        P = Primes(proof=False)
        
        start_prime = next_prime(init_p)
        final_prime = P.unrank(start_idx + num_primes - 1)
                 
        if verbose:
            print(f"Verbose: Getting {num_primes} primes from {start_prime} to {final_prime}...")
        self.p_list = prime_range(start_prime, final_prime + 1)

    
    def generate_batches(self):
        """Generate batches by iterating through batched tuples."""
        for batch_tuple in batched(self.p_list, self.batch_size):
            # Convert Array to Python list for worker compatibility
            batch = list(batch_tuple)
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
        if results_df is not None and results_df.height > 0:
            self.df_buffer.append(results_df)
            self.result_count += results_df.height
            
            # By using 'while', we handle cases where a single large
            # batch might be much larger than the buffer_size.
            while self.result_count >= self.buffer_size:
                self._flush_results()
    
    def _flush_results(self):
        """Flush accumulated DataFrames to storage."""
        if not self.df_buffer:
            return
        
        # Concatenate all DataFrames
        combined_df = pl.concat(self.df_buffer)
        
        # Call save callback with buffer_size for logging control
        self.save_callback(combined_df, self.buffer_size)
        
        # Reset accumulation
        self.df_buffer = []
        self.result_count = 0
    
    def finalize(self):
        """Flush any remaining DataFrames."""
        self._flush_results()


def run_gen(init_p, num_primes, batch_size, cores, buffer_size, append_data, verbose=False):
    """
    Main analysis runner - handles all processing logic.
    
    Args:
        init_p: Last prime from data or first prime, who knows!
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
    batch_feeder = PPBatchFeeder(init_p, num_primes, batch_size, verbose)
    consumer = PPConsumer(buffer_size, append_data) 

    
    print(f"Processing {num_primes} primes starting from {init_p}")
    print(f"Batch size: {batch_size} primes per worker")
    
    # Set spawn method to avoid fork issues
    mp.set_start_method('spawn', force=True)
    
    ## Initialize timing collection
    #all_timing_data = []
    
    # Process with multiprocessing and progress bar
    with mp.Pool(cores) as pool:
        batches_processed = 0
        primes_processed = 0
        
        with tqdm(total=num_primes, desc="Prime partition", unit="prime") as pbar:
            for prime_batch in batch_feeder.generate_batches():
                if not prime_batch:  # Empty batch means we're done
                    break
                
                # Process  batch - returns (DataFrame, timing_data)
                #Reimplement with more robust profiling: results_df , timing_data = pool.apply(worker_batch, (prime_batch, verbose))
                results_df= pool.apply(worker_batch, (prime_batch, verbose))
                
                
                # Collect timing data
                #all_timing_data.extend(timing_data)
                
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
    #if all_timing_data and verbose:
    #    timing_collector = TimingCollector(verbose=verbose)
    #    timing_collector.timings = all_timing_data
    #    timing_collector.save_debug_log()
    #    timing_collector.print_summary()
    
    print(f"\nCompleted processing {primes_processed} primes in {batches_processed} batches")
    print(f"Results merged and saved")
    
    # Automatically convert run files to blocks if using separate runs
    convert_runs_to_blocks_auto()

    #if verbose and all_timing_data:
    #    print(f"Timing data collected: {len(all_timing_data)} operations")



