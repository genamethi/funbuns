# funbuns

A high-performance Python program for finding prime power partitions of the form `p = 2^m + q^n`, where `p` and `q` are primes and `m, n ≥ 1`.

## Overview

This project computes prime power partitions (decompositions) of length 2 for large sets of primes. For each prime `p`, it systematically searches for representations as `p = 2^m + q^n` where:
- `p` is the target prime
- `m ≥ 1` (power of 2)  
- `q` is a prime base
- `n ≥ 1` (power of the prime base `q`)

The algorithm efficiently explores the space by:
1. Computing `max_m = floor(log₂(p))` to bound the search
2. For each `m`, checking if `q_cand = p - 2^m` has an integer `n`-th root
3. Testing primality only on perfect `n`-th roots

## Features

- **Multiprocessing**: Utilizes all physical CPU cores for parallel prime analysis
- **Progress Tracking**: Smooth progress bar using Prime Number Theorem estimates
- **Resume Functionality**: Automatic checkpointing and resume from previous runs
- **Interactive Dashboard**: Altair-based visualizations for partition analysis
- **Efficient Algorithm**: Avoids expensive primality tests through perfect root filtering

## Installation

This project uses [pixi](https://pixi.sh) for environment management:

```bash
# Install dependencies and activate environment
pixi install
pixi shell
```

## Usage

### Basic Analysis

Analyze the first 1000 primes:
```bash
pixi run calcpp -n 1000
```

### Resume Previous Analysis

Continue from where you left off:
```bash
pixi run calcpp-resume -n 2000
```

### View Results Dashboard

Generate interactive visualizations:
```bash
pixi run calcpp-view
```

### Command Line Options

```bash
python -m funbuns -h
```

- `-n, --number`: Number of primes to analyze (required)
- `--resume`: Resume from previous progress using default data file  
- `--data-file`: Specify custom data file (overrides default)
- `--view`: Generate interactive dashboard from existing data

### Example Output

```
Using 8 physical cores
Processing up to prime 7919
Estimated computational work units: 971

Prime decomposition analysis: 100%|██████████| 971/971 [00:12<00:00, 79.2work/s]

Summary:
  Primes analyzed: 1000
  Largest prime: 7919
  Primes with partitions: 934
  Total partitions found: 2156

Partition Count Frequency Table:
  Partitions   # Primes   Percentage
  ------------ ---------- ----------
  0            66          6.60%
  1            543        54.30%
  2            301        30.10%
  3            78          7.80%
  4            11          1.10%
  5            1           0.10%
```

## Algorithm Details

The core algorithm for each prime `p`:

1. **Bound computation**: `max_m = floor(log₂(p))`
2. **Outer loop**: For `m ∈ [1, max_m]`
   - Compute `q_cand = p - 2^m`
   - Compute `max_n = floor(log₃(q_cand))`
3. **Inner loop**: For `n ∈ [1, max_n]`
   - Try `q = q_cand^(1/n)` (nth root)
   - If exact integer root exists and `q` is prime: record `(m, n, q)`

### Performance Characteristics

- **Time Complexity**: O(log²(p)) per prime
- **Space Complexity**: O(k) where k is the number of partitions found
- **Parallelization**: Embarrassingly parallel across primes
- **Memory Usage**: Minimal, processes one prime at a time per worker

## Data Format

Results are stored as pickle files containing:
```python
{
    'timestamp': '2024-01-01T12:00:00',
    'total_primes': 1000,
    'results': [
        {
            'prime': 17,
            'count': 1,
            'decomp': [(4, 1, 1)]  # 17 = 2^4 + 1^1 (but 1 not prime)
        },
        # ... more results
    ]
}
```

## Visualization Dashboard

The interactive dashboard provides:
- **Frequency Distribution**: Bar chart of partition counts
- **Frequency Table**: Textual breakdown with percentages  
- **Prime vs Partitions**: Scatter plot showing relationship
- **Pattern Analysis**: Distribution of `m` and `n` values
- **Data Explorer**: Interactive table for detailed inspection

## Technical Stack

- **Python 3.12+**: Core language
- **SageMath**: Prime generation, arbitrary precision arithmetic, primality testing
- **Polars**: Fast dataframe operations and data manipulation
- **Altair**: Interactive statistical visualizations
- **pixi**: Cross-platform package and environment management
- **Multiprocessing**: Parallel computation across physical cores

## Mathematical Background

This work explores additive number theory, specifically representations of primes as sums of prime powers. The algorithm systematically searches the solution space of the Diophantine equation:

```
p = 2^m + q^n
```

where the constraint to prime bases `q` makes this a restricted prime power partition problem.

## Version History

**v0.1.0** - Initial release
- Core algorithm implementation
- Multiprocessing support  
- Progress tracking with Prime Number Theorem estimates
- Resume functionality
- Interactive Altair dashboard
- Comprehensive frequency analysis

## Performance Notes

The progress bar uses `p/log(p)` (Prime Number Theorem) to estimate computational work, providing smooth visual feedback that accounts for the logarithmic difficulty scaling with prime size.

For large-scale computations (millions of primes), consider:
- Pre-computing small prime tables
- Caching powers of 2
- Memory-mapped result storage for very large datasets
