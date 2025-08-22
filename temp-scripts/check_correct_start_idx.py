#!/usr/bin/env python3
from sage.all import *

def check_correct_start_idx():
    """Find the correct start_idx for resuming."""
    print("🔍 Finding correct start_idx for resuming...")
    
    # Current values
    last_processed_prime = 733302529
    next_prime_after = next_prime(last_processed_prime)
    
    print(f"📊 Current values:")
    print(f"  Last processed prime: {last_processed_prime:,}")
    print(f"  Next prime after: {next_prime_after:,}")
    
    # Find the index of the next prime
    P = Primes(proof=False)
    next_prime_idx = P.rank(next_prime_after)
    
    print(f"  Index of next prime: {next_prime_idx:,}")
    
    # Check what prime is at index 30710999 (current start_idx + num_primes - 1)
    current_final_idx = 30710999
    prime_at_current_idx = P.unrank(current_final_idx)
    
    print(f"\n📊 Current calculation:")
    print(f"  Current start_idx: 30611000")
    print(f"  Current final_idx: {current_final_idx:,}")
    print(f"  Prime at final_idx: {prime_at_current_idx:,}")
    
    # Check if this makes sense
    if prime_at_current_idx > next_prime_after:
        print("  ✅ This should work - final prime > next prime")
    else:
        print("  ❌ This won't work - final prime <= next prime")
    
    # Calculate what the final prime should be if we start from next_prime_idx
    num_primes = 100000
    correct_final_idx = next_prime_idx + num_primes - 1
    correct_final_prime = P.unrank(correct_final_idx)
    
    print(f"\n📊 Correct calculation:")
    print(f"  Correct start_idx: {next_prime_idx:,}")
    print(f"  Correct final_idx: {correct_final_idx:,}")
    print(f"  Correct final_prime: {correct_final_prime:,}")
    
    print(f"\n📊 Summary:")
    print(f"  Current resume logic uses start_idx: 30611000")
    print(f"  Correct resume logic should use start_idx: {next_prime_idx:,}")
    print(f"  Difference: {next_prime_idx - 30611000:,}")

if __name__ == '__main__':
    check_correct_start_idx()


