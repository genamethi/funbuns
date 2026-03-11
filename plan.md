# ℓ-adic Diophantine Analysis for funbuns

## Mathematical Framework

The equation `p = 2^m + q^n` has a natural ℓ-adic interpretation. For each prime ℓ, the ℓ-adic valuation `v_ℓ(p - 2^m)` tells us whether ℓ divides the remainder and with what multiplicity. When `p - 2^m = q^n` (a perfect prime power), the valuation profile is trivially concentrated: `v_q(r) = n` and `v_ℓ(r) = 0` for all ℓ ≠ q.

The interesting cases are the **obstructed primes** — the ~17.3% with zero-rows where no m yields a prime power remainder. For these, every remainder `r = p - 2^m` is composite. Understanding *why* these primes fail and *how close* they come to succeeding requires:

1. **Valuation profiles**: The vector `(v_2(r), v_3(r), v_5(r), v_7(r), ...)` for each remainder
2. **ℓ-adic distance from prime powers**: How "far" is r from being q^n? Measured by the number of distinct prime factors and the concentration of valuations
3. **Local obstructions**: Congruence conditions mod small primes that prevent p = 2^m + q^n

### Key Number-Theoretic Ideas

**Near-miss metric**: For r = p - 2^m = q1^a1 · q2^a2 · ... · qk^ak, define:
- `omega(r)` = k (number of distinct prime factors) — 1 means prime power (success!)
- `Omega(r)` = sum(ai) (total with multiplicity)
- `dominant_share(r)` = max(ai · log(qi)) / log(r) — how concentrated is r on one prime?
- When dominant_share → 1, r is "almost" a prime power

**ℓ-adic filtration**: Group primes by the valuation pattern of their *best* remainder (the one closest to a prime power). This reveals arithmetic progressions and congruence classes that create obstructions.

**Erdős–Kac connection**: For random n, omega(n) ~ Normal(log log n, √(log log n)). If the remainders p - 2^m behave differently, that reveals deep structure.

**Logarithmic depth (generalized)**: For r = q^n we have a single-dimensional "depth" = n = log_q(r). For r composite, define depth_ℓ = v_ℓ(r) for each prime ℓ. The vector (depth_ℓ) lives in a lattice — the "factorization lattice" of r. This is where ppparts homology tools connect: the topology of the factorization lattice.

---

## Implementation Plan

### Step 1: New `ladic.py` Analysis Module

Create `src/funbuns/ladic.py` with:

```python
# Core functions:
def compute_remainder_profile(p: int, m: int) -> dict:
    """Full factorization + valuation profile of p - 2^m"""

def classify_remainder(profile: dict) -> str:
    """'prime_power' | 'semiprime' | 'k_almost_prime' | ..."""

def near_miss_score(profile: dict) -> float:
    """How close to a prime power? dominant_share metric"""

def local_obstruction_check(p: int, moduli: list[int]) -> dict:
    """Check p = 2^m + q^n mod each modulus"""

def erdos_kac_comparison(omega_values: pl.Series, log_log_n_values: pl.Series) -> dict:
    """Compare omega distribution to Normal(log log n, sqrt(log log n))"""
```

**Schema for the analysis DataFrame:**
```
remainder_analysis = {
    'p': Int64,           # the prime
    'm': Int64,           # 2^m term
    'r': Int64,           # remainder = p - 2^m
    'omega': UInt8,       # distinct prime factors of r
    'big_omega': UInt8,   # prime factors with multiplicity
    'dominant_q': Int64,  # largest prime factor of r
    'dominant_exp': UInt8,# its exponent
    'dominant_share': Float64,  # concentration metric
    'mu': Int8,           # Möbius function value
    'is_prime_power': Boolean,
    'factorization': Utf8,  # serialized: "3^2·7·13" or similar
}
```

This uses lazy Polars evaluation over existing block files, streaming the results. SageMath's `factor()` handles the heavy factorization; the Rust plugin will accelerate the valuation step.

### Step 2: Valuation Computation Rust Plugin

Create a pyo3-polars plugin: `funbuns_native/`

```
funbuns_native/
├── Cargo.toml
├── src/
│   └── lib.rs          # Plugin entry points
│   └── valuations.rs   # v_ℓ(n) computation
│   └── factor.rs       # Trial division + Pollard's rho
```

**Plugin expressions exposed to Polars:**
- `v_ell(col("r"), ell=3)` → column of v_3(r) values
- `omega(col("r"))` → number of distinct prime factors
- `big_omega(col("r"))` → total prime factor count with multiplicity
- `dominant_share(col("r"))` → concentration metric
- `factorize(col("r"))` → struct column with factorization

Uses `pyo3-polars` crate for zero-copy integration with Arrow arrays. The key performance win: computing valuations for a batch of ℓ values across an entire column is embarrassingly parallel and avoids Python overhead.

### Step 3: Obstruction Analysis & ℓ-adic Filtration

New functions in `ladic.py`:

```python
def compute_ladic_filtration(df: pl.LazyFrame, primes: list[int]) -> pl.LazyFrame:
    """Add v_ℓ(r) columns for each prime ℓ in the list.
    Groups primes by their valuation patterns to find congruence obstructions."""

def find_local_obstructions(df: pl.LazyFrame) -> pl.DataFrame:
    """For obstructed primes (zero-row primes), analyze all m values
    and identify the minimum omega(p - 2^m) — the 'nearest miss'.
    Report congruence classes that concentrate obstructed primes."""

def gap_filling_analysis(p: int) -> pl.DataFrame:
    """For a specific obstructed prime, enumerate all m values,
    factorize each p - 2^m, and characterize the 'obstruction surface'."""
```

### Step 4: Probabilistic Number Theory Integration

Add to `ladic.py`:

```python
def erdos_kac_analysis(analysis_df: pl.DataFrame) -> dict:
    """Compare empirical omega distribution to Erdős–Kac prediction.
    Returns KS-statistic, chi-squared, and deviation profile."""

def density_by_almost_primality(analysis_df: pl.DataFrame) -> pl.DataFrame:
    """For k = 1, 2, 3, ..., compute fraction of remainders
    that are k-almost-prime. Compare to Hardy-Ramanujan predictions."""

def log_depth_distribution(analysis_df: pl.DataFrame) -> pl.DataFrame:
    """Distribution of max(v_ℓ(r) * log(ℓ)) / log(r) — the 'concentration'
    of the factorization. Prime powers have concentration = 1."""
```

### Step 5: Port ppparts Functions

Port the relevant homology and mathematical tools from ppparts into funbuns. Based on the TODO in `__main__.py` referencing "pppart+adics and some tda/ph":

- Factorization lattice tools (simplicial structure of divisor posets)
- Any existing ℓ-adic valuation code
- Topological data analysis primitives if relevant to filtration

### Step 6: Visualization Extensions

Extend `viewer.py` with new chart types:
- Omega distribution histogram vs Erdős–Kac prediction curve
- Near-miss heatmap: (p, m) colored by dominant_share
- ℓ-adic filtration treemap
- Congruence class distribution for obstructed primes

---

## File Changes Summary

| File | Action | Description |
|------|--------|-------------|
| `src/funbuns/ladic.py` | **Create** | Core ℓ-adic analysis module |
| `funbuns_native/Cargo.toml` | **Create** | Rust plugin project |
| `funbuns_native/src/lib.rs` | **Create** | Plugin entry + expression registration |
| `funbuns_native/src/valuations.rs` | **Create** | v_ℓ computation, omega, Omega |
| `src/funbuns/__main__.py` | **Edit** | Add `--ladic` CLI flag |
| `src/funbuns/viewer.py` | **Edit** | Add analysis charts |
| `setup.py` | **Edit** | Add new dependencies |
| `pixi.toml` | **Edit** | Add maturin build task for Rust plugin |
