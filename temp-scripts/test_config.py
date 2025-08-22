#!/usr/bin/env python3
"""Test config reading functionality."""

from src.funbuns.utils import get_config

def test_config():
    """Test that config reading works properly."""
    
    print("🧪 Testing Configuration Reading")
    
    config = get_config()
    print(f"  Config loaded: {config}")
    
    # Test specific values
    buffer_size = config.get('buffer_size', 10000)
    use_separate_runs = config.get('use_separate_runs', True)
    small_primes_filename = config.get('small_primes_filename', 'small_primes.parquet')
    
    print(f"  buffer_size: {buffer_size}")
    print(f"  use_separate_runs: {use_separate_runs}")
    print(f"  small_primes_filename: {small_primes_filename}")
    
    # Verify that separate runs is the default
    if use_separate_runs:
        print("  ✅ Separate runs is enabled by default in config")
    else:
        print("  ❌ Separate runs is not enabled by default")

if __name__ == "__main__":
    test_config()
