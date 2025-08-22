#!/usr/bin/env python3
"""Test separate runs functionality."""

import polars as pl
from pathlib import Path
import shutil

def test_separate_runs():
    """Test the separate run files functionality."""
    
    print("🧪 Testing Separate Run Files Functionality")
    
    # Clean up any existing test files
    test_runs_dir = Path("data/runs")
    if test_runs_dir.exists():
        print(f"  Cleaning up existing test runs directory...")
        shutil.rmtree(test_runs_dir)
    
    # Import functions
    from src.funbuns.utils import get_run_file, get_all_run_files, aggregate_run_files, show_run_files_summary
    
    print("\n📁 Creating test run files...")
    
    # Create some test data
    test_data_1 = pl.DataFrame({
        "p": [7, 11, 13],
        "m_k": [1, 2, 3], 
        "n_k": [1, 1, 1],
        "q_k": [5, 7, 5]
    })
    
    test_data_2 = pl.DataFrame({
        "p": [17, 19, 23],
        "m_k": [2, 3, 4],
        "n_k": [1, 1, 1], 
        "q_k": [13, 11, 7]
    })
    
    test_data_3 = pl.DataFrame({
        "p": [29, 31, 37],
        "m_k": [1, 2, 5],
        "n_k": [3, 3, 1],
        "q_k": [3, 3, 5]
    })
    
    # Write test run files
    run_file_1 = get_run_file("20250811_120000")
    run_file_2 = get_run_file("20250811_130000") 
    run_file_3 = get_run_file("20250811_140000")
    
    test_data_1.write_parquet(run_file_1)
    test_data_2.write_parquet(run_file_2)
    test_data_3.write_parquet(run_file_3)
    
    print(f"  ✅ Created {run_file_1.name} with {len(test_data_1)} rows")
    print(f"  ✅ Created {run_file_2.name} with {len(test_data_2)} rows")
    print(f"  ✅ Created {run_file_3.name} with {len(test_data_3)} rows")
    
    # Test getting all run files
    print("\n📊 Testing run file discovery...")
    all_runs = get_all_run_files()
    print(f"  Found {len(all_runs)} run files: {[f.name for f in all_runs]}")
    
    # Test summary
    print("\n📋 Testing run files summary...")
    show_run_files_summary()
    
    # Test aggregation
    print("\n🔄 Testing aggregation...")
    test_output = Path("data/test_aggregated.parquet")
    aggregate_run_files(output_file=test_output, delete_runs=False)
    
    # Verify aggregated result
    if test_output.exists():
        aggregated_df = pl.read_parquet(test_output)
        print(f"  ✅ Aggregated file created with {len(aggregated_df)} rows")
        print(f"  Prime range: {aggregated_df['p'].min()} to {aggregated_df['p'].max()}")
        
        # Check if sorted
        is_sorted = aggregated_df['p'].is_sorted()
        print(f"  Sorted correctly: {'✅' if is_sorted else '❌'}")
        
        # Clean up test file
        test_output.unlink()
        print(f"  🧹 Cleaned up test aggregated file")
    
    # Test CLI commands (dry run)
    print("\n🖥️  New CLI Commands Available:")
    print("  pixi run funbuns --separate-runs -n 1000  # Use separate run files")
    print("  pixi run funbuns --show-runs              # Show run files summary")  
    print("  pixi run funbuns --aggregate              # Merge run files into main file")
    
    print("\n✅ Separate runs functionality test complete!")
    
    # Clean up test files
    for run_file in all_runs:
        run_file.unlink()
    if test_runs_dir.exists() and not list(test_runs_dir.iterdir()):
        test_runs_dir.rmdir()
    print("🧹 Cleaned up test files")

if __name__ == "__main__":
    test_separate_runs()
