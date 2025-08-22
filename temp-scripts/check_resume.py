#!/usr/bin/env python3
import sys
sys.path.append('src')

from funbuns.utils import resume_p

def check_resume():
    """Check what the resume logic is currently returning."""
    print("🔍 Checking resume logic...")
    
    try:
        last_prime, start_idx = resume_p(use_separate_runs=True)
        print(f"Last prime: {last_prime:,}")
        print(f"Start index: {start_idx:,}")
        
        # Check if this makes sense
        print(f"\n📊 Analysis:")
        print(f"  Current data ends at: 733,302,529")
        print(f"  Resume logic says: {last_prime:,}")
        print(f"  Difference: {last_prime - 733302529:,}")
        
        if last_prime == 733302529:
            print("  ✅ Last prime matches current data")
        else:
            print("  ❌ Last prime doesn't match current data")
            
        # Check if start_idx makes sense for the current data
        print(f"\n  Current unique primes: 30,611,000")
        print(f"  Resume start_idx: {start_idx:,}")
        print(f"  Difference: {start_idx - 30611000:,}")
        
        if start_idx == 30611000:
            print("  ✅ Start index matches current data")
        else:
            print("  ❌ Start index doesn't match current data")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == '__main__':
    check_resume()
