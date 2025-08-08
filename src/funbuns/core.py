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
from typing import Optional


def decomp(p):
    """
    Find all partitions of prime p as p = 2^m + q^n where q is prime.
    
    Args:
        p: Integer (SageMath) - the prime to decompose
        
    Returns:
        List of tuples (m, n, q) representing valid partitions
    """
    # Handle special cases for p = 2, 3
    if p == 2 or p == 3:
        return []
    
    results = []
    max_m = p.exact_log(2)  # floor(log_2(p))
    
    for m_i in range(1, max_m + 1):
        q_cand_i = p - 2**m_i
        
        # Compute max_n using smallest possible prime base (3)
        max_n = q_cand_i.exact_log(3)
        
        for n_ij in range(1, max_n + 1):
            try:
                q_cand_ij = q_cand_i.nth_root(n_ij)
                if q_cand_ij.is_prime():
                    results.append((m_i, n_ij, q_cand_ij))
            except ValueError:
                # No perfect nth root exists, continue to next n_ij
                continue
                
    return results


class PPFeeder:
    """Feeds primes to workers starting from a given prime."""
    
    def __init__(self, init_p: int):
        """
        Initialize feeder starting from prime init_p.
        
        Args:
            init_p: Integer - starting prime (inclusive)
        """
        self.P = Primes()
        # Find the first prime >= init_p
        if init_p <= 2:
            self.current_p = 2
        else:
            # Find the rank of init_p, then get the next prime
            try:
                rank = self.P.rank(init_p)
                self.current_p = self.P.unrank(rank)
            except:
                # If init_p is not prime, find the next prime
                self.current_p = init_p
                while not self.current_p.is_prime():
                    self.current_p += 1
    
    def get_next_prime(self) -> Optional[int]:
        """
        Get the next prime to process.
        
        Returns:
            Integer - next prime, or None if no more primes
        """
        if self.current_p is None:
            return None
        
        result = self.current_p
        try:
            self.current_p = self.P.next(self.current_p)
        except:
            self.current_p = None  # No more primes
        
        return result


class PPProducer:
    """Processes a single prime and returns results as Polars DataFrame."""
    
    def __init__(self):
        """Initialize producer."""
        pass
    
    def process_prime(self, p: int) -> pl.DataFrame:
        """
        Process a single prime and return partitions as Polars DataFrame.
        
        Args:
            p: Integer - prime to process
            
        Returns:
            Polars DataFrame with columns p, m_k, n_k, q_k
        """
        # Convert to SageMath Integer for exact_log method
        p_sage = Integer(p)
        partitions = decomp(p_sage)
        
        if not partitions:
            # No partitions found, return single row with zeros
            return pl.DataFrame({
                'p': [int(p)],
                'm_k': [0],
                'n_k': [0], 
                'q_k': [0]
            })
        
        # Convert partitions to DataFrame rows
        rows = []
        for m, n, q in partitions:
            rows.append({
                'p': int(p),
                'm_k': int(m),
                'n_k': int(n),
                'q_k': int(q)
            })
        
        return pl.DataFrame(rows)


class PPConsumer:
    """Shared consumer that collects results and manages batch saves."""
    
    def __init__(self, buffer_size: int, save_callback):
        """
        Initialize consumer.
        
        Args:
            buffer_size: Integer - number of primes to accumulate before saving
            save_callback: Function to call for saving data
        """
        self.buffer_size = buffer_size
        self.save_callback = save_callback
        self.accumulated_dfs = []
        self.prime_count = 0
    
    def add_result(self, df: pl.DataFrame):
        """
        Add a result DataFrame from a worker.
        
        Args:
            df: Polars DataFrame with partition results
        """
        self.accumulated_dfs.append(df)
        self.prime_count += 1
        
        if self.prime_count >= self.buffer_size:
            self._flush_results()
    
    def _flush_results(self):
        """Flush accumulated results to storage."""
        if not self.accumulated_dfs:
            return
        
        # Concatenate all DataFrames
        combined_df = pl.concat(self.accumulated_dfs)
        
        # Call save callback with buffer_size for logging control
        self.save_callback(combined_df, self.buffer_size)
        
        # Reset accumulation
        self.accumulated_dfs = []
        self.prime_count = 0
    
    def finalize(self):
        """Flush any remaining results."""
        self._flush_results()



