#!/usr/bin/env python3
from sage.all import *

def test_prime_range():
    """Test the prime_range function with the actual values."""
    print("🔍 Testing prime_range function...")
    
    # The values from the error
    start_prime = 733302617
    final_prime = 587606959
    
    print(f"📊 Values from error:")
    print(f"  start_prime: {start_prime:,}")
    print(f"  final_prime: {final_prime:,}")
    
    if start_prime > final_prime:
        print("  ❌ start_prime > final_prime - this will create empty range")
    else:
        print("  ✅ start_prime <= final_prime - this should work")
    
    # Test what prime_range returns
    try:
        p_list = prime_range(start_prime, final_prime + 1)
        print(f"  prime_range result: {len(p_list)} primes")
        if len(p_list) > 0:
            print(f"    First: {p_list[0]:,}")
            print(f"    Last: {p_list[-1]:,}")
        else:
            print("    Empty list")
    except Exception as e:
        print(f"  ❌ Error in prime_range: {e}")
    
    # Test what next_prime returns for 733302529
    init_p = 733302529
    next_p = next_prime(init_p)
    print(f"\n📊 Testing next_prime:")
    print(f"  init_p: {init_p:,}")
    print(f"  next_prime(init_p): {next_p:,}")
    
    # Test what P.unrank returns for the final index
    P = Primes(proof=False)
    start_idx = 30611000
    num_primes = 100000
    final_idx = start_idx + num_primes - 1
    
    print(f"\n📊 Testing P.unrank:")
    print(f"  start_idx: {start_idx:,}")
    print(f"  num_primes: {num_primes:,}")
    print(f"  final_idx: {final_idx:,}")
    
    try:
        final_prime_calc = P.unrank(final_idx)
        print(f"  P.unrank(final_idx): {final_prime_calc:,}")
    except Exception as e:
        print(f"  ❌ Error in P.unrank: {e}")

if __name__ == '__main__':
    test_prime_range()
