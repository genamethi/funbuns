"""
Core implementation of prime decomposition algorithm: p = 2^m + q^n

Following the algorithm outlined in sketch.md:
- For each prime p, compute max_m = floor(log2(p))
- For each m_i in [1, max_m], compute remainder = p - 2^m_i  
- For each n_i, check if nth_root(remainder) is integer and prime
- Use try/except pattern to avoid unnecessary computations
"""

from sage.all import *


def decomp(p):
    """
    Find all decompositions of prime p as p = 2^m + q^n where q is prime.
    
    Args:
        p: Integer (SageMath) - the prime to decompose
        
    Returns:
        List of tuples (m, n, q) representing valid decompositions
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


def analyze(p):
    """
    Analyze a single prime and return decomposition information.
    
    Args:
        p: Integer (SageMath) - the prime to analyze
        
    Returns:
        Dict with prime and its decompositions
    """
    d = decomp(p)
    
    return {
        'prime': p,
        'count': len(d),
        'decomp': d
    }


def batch(start, end):
    """
    Process a range of primes and find their decompositions.
    
    Args:
        start: Integer - starting prime (inclusive)
        end: Integer - ending prime (exclusive)
        
    Returns:
        List of analysis results
    """
    P = Primes()
    results = []
    
    p = P.next_prime(start - 1)  # Get first prime >= start
    
    while p < end:
        result = analyze(p)
        results.append(result)
        p = P.next(p)
        
    return results



