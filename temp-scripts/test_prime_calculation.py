#!/usr/bin/env python3
from sage.all import Primes

def test_prime_calculation():
    """Test the prime calculation logic."""
    print("🔍 Testing prime calculation logic...")
    
    # Current values
    start_idx = 30611000
    num_primes = 100000
    init_p = 733302529
    
    print(f"📊 Current values:")
    print(f"  start_idx: {start_idx:,}")
    print(f"  num_primes: {num_primes:,}")
    print(f"  init_p: {init_p:,}")
    
    # Calculate final index
    final_idx = start_idx + num_primes - 1
    print(f"  final_idx: {final_idx:,}")
    
    # Get the primes
    P = Primes(proof=False)
    
    try:
        final_prime = P.unrank(final_idx)
        print(f"  final_prime: {final_prime:,}")
        
        # Check if this makes sense
        if final_prime > init_p:
            print("  ✅ final_prime > init_p - this should work")
        else:
            print("  ❌ final_prime <= init_p - this will cause empty range")
            
        # Check what the next prime after init_p should be
        next_p = P.next_prime(init_p)
        print(f"  next_prime after {init_p:,}: {next_p:,}")
        
        # Check if we can get 100,000 primes starting from next_p
        start_range_idx = P.rank(next_p)
        end_range_idx = start_range_idx + num_primes - 1
        
        print(f"  start_range_idx: {start_range_idx:,}")
        print(f"  end_range_idx: {end_range_idx:,}")
        
        if end_range_idx <= final_idx:
            print("  ✅ Range is valid")
        else:
            print("  ❌ Range is invalid - end_range_idx > final_idx")
            
    except Exception as e:
        print(f"  ❌ Error: {e}")

if __name__ == '__main__':
    test_prime_calculation()


