#!/usr/bin/env python3
"""Check Polars build configuration and test separate runs functionality."""

import polars as pl
import sys

def check_polars_build():
    """Check Polars build information and flags."""
    print("🔍 Polars Build Information:")
    print(f"  Version: {pl.__version__}")
    
    # Check if it's a custom build
    if hasattr(pl, '__file__'):
        print(f"  Location: {pl.__file__}")
    
    # Test 64-bit integer handling
    print("\n🧪 Testing 64-bit Integer Handling:")
    
    # Test with a value just over 32-bit limit
    test_value = 2**31 + 1  # Just over 32-bit signed int limit
    large_value = 530_709_637  # The value that caused corruption
    
    try:
        # Create DataFrame with large integers
        df = pl.DataFrame({
            "test_32bit": [test_value],
            "test_corruption": [large_value],
            "calculation": [2**29 + large_value]  # Calculation that might overflow
        })
        
        print(f"  ✅ Created DataFrame with values:")
        print(f"    32-bit limit + 1: {test_value:,}")
        print(f"    Corruption value: {large_value:,}")
        print(f"    Large calculation: {2**29 + large_value:,}")
        
        # Check the actual values stored (DataFrame is already collected)
        retrieved_values = df.select([
            pl.col("test_32bit"),
            pl.col("test_corruption"), 
            pl.col("calculation")
        ])
        
        stored_32bit = retrieved_values["test_32bit"].item()
        stored_corruption = retrieved_values["test_corruption"].item()
        stored_calc = retrieved_values["calculation"].item()
        
        print(f"\n  📊 Retrieved values:")
        print(f"    32-bit test: {stored_32bit:,} ({'✅ correct' if stored_32bit == test_value else '❌ OVERFLOW'})")
        print(f"    Corruption test: {stored_corruption:,} ({'✅ correct' if stored_corruption == large_value else '❌ OVERFLOW'})")
        print(f"    Calculation: {stored_calc:,} ({'✅ correct' if stored_calc == 2**29 + large_value else '❌ OVERFLOW'})")
        
        # Test arithmetic operations
        print(f"\n  🔢 Testing arithmetic operations:")
        result_df = df.with_columns([
            (pl.col("test_corruption") + 1000).alias("add_test"),
            (pl.col("test_corruption") * 2).alias("mult_test"),
            (2**pl.lit(29) + pl.col("test_corruption")).alias("power_test")
        ])
        
        add_result = result_df["add_test"].item()
        mult_result = result_df["mult_test"].item()
        power_result = result_df["power_test"].item()
        
        expected_add = large_value + 1000
        expected_mult = large_value * 2
        expected_power = 2**29 + large_value
        
        print(f"    Addition: {add_result:,} ({'✅ correct' if add_result == expected_add else '❌ OVERFLOW'})")
        print(f"    Multiplication: {mult_result:,} ({'✅ correct' if mult_result == expected_mult else '❌ OVERFLOW'})")
        print(f"    Power calculation: {power_result:,} ({'✅ correct' if power_result == expected_power else '❌ OVERFLOW'})")
        
    except Exception as e:
        print(f"  ❌ Error testing large integers: {e}")
    
    # Check for compilation flags or build info
    print(f"\n🔧 Build Information:")
    try:
        # Try to get build info (may not be available in all versions)
        if hasattr(pl, 'build_info'):
            build_info = pl.build_info()
            print(f"  Build info: {build_info}")
        else:
            print("  Build info not available in this version")
    except:
        print("  Build info not accessible")
    
    # Check data types
    print(f"\n📋 Default Data Types:")
    test_df = pl.DataFrame({"int_col": [1, 2, 3]})
    print(f"  Default integer type: {test_df.dtypes[0]}")
    
    # Test explicit Int64
    test_df_64 = pl.DataFrame({"int64_col": pl.Series([1, 2, 3], dtype=pl.Int64)})
    print(f"  Explicit Int64 type: {test_df_64.dtypes[0]}")

def suggest_polars_rebuild():
    """Suggest how to rebuild Polars with proper 64-bit support."""
    print("\n🔨 Rebuilding Polars with 64-bit Support:")
    print("  If you need to rebuild Polars from source:")
    print("  1. Clone polars repo: git clone https://github.com/pola-rs/polars.git")
    print("  2. Build with explicit 64-bit support:")
    print("     cd polars/py-polars")
    print("     RUSTFLAGS='-C target-feature=+crt-static' maturin build --release")
    print("  3. Install the wheel:")
    print("     pip install target/wheels/polars-*.whl")
    print()
    print("  Your pyproject.toml shows you have a local build:")
    print("     polars @ file:///home/erpage159/fluid/byo/repos/polars/target/wheels/polars-1.32.0b1-cp39-abi3-manylinux_2_39_x86_64.whl")
    print("  You can update your funbuns pixi.toml to use this same wheel")

if __name__ == "__main__":
    check_polars_build()
    suggest_polars_rebuild()
