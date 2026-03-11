#![allow(clippy::unused_unit)]

use polars::prelude::*;
use polars::prelude::arity::unary_elementwise_values;
use pyo3::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// ℓ-adic valuation: v_ℓ(n)
// ---------------------------------------------------------------------------

/// Compute ℓ-adic valuation of n: largest k such that ℓ^k | n.
#[inline]
fn v_ell_scalar(mut n: i64, ell: i64) -> i32 {
    if n == 0 {
        return -1; // convention: v(0) = ∞
    }
    if n < 0 {
        n = -n;
    }
    if ell < 2 {
        return 0;
    }
    let mut k: i32 = 0;
    while n % ell == 0 {
        n /= ell;
        k += 1;
    }
    k
}

#[derive(Deserialize)]
struct VEllKwargs {
    ell: i64,
}

/// Polars expression: v_ell(col, ell=p) -> Int32 column of ℓ-adic valuations.
#[polars_expr(output_type=Int32)]
fn v_ell(inputs: &[Series], kwargs: VEllKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let ell = kwargs.ell;
    let out: Int32Chunked = unary_elementwise_values(ca, |n| v_ell_scalar(n, ell));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Number of distinct prime factors: ω(n)
// ---------------------------------------------------------------------------

/// Trial division to count distinct prime factors.
#[inline]
fn omega_scalar(mut n: i64) -> u8 {
    if n <= 1 {
        return 0;
    }
    if n < 0 {
        n = -n;
    }
    let mut count: u8 = 0;

    // Factor out 2
    if n % 2 == 0 {
        count += 1;
        while n % 2 == 0 {
            n /= 2;
        }
    }

    // Odd factors
    let mut d: i64 = 3;
    while d * d <= n {
        if n % d == 0 {
            count += 1;
            while n % d == 0 {
                n /= d;
            }
        }
        d += 2;
    }
    if n > 1 {
        count += 1;
    }
    count
}

/// Polars expression: omega(col) -> UInt8 column of ω(n) values.
#[polars_expr(output_type=UInt8)]
fn omega(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: UInt8Chunked = unary_elementwise_values(ca, |n| omega_scalar(n));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Total prime factors with multiplicity: Ω(n)
// ---------------------------------------------------------------------------

#[inline]
fn big_omega_scalar(mut n: i64) -> u8 {
    if n <= 1 {
        return 0;
    }
    if n < 0 {
        n = -n;
    }
    let mut count: u8 = 0;

    while n % 2 == 0 {
        count += 1;
        n /= 2;
    }

    let mut d: i64 = 3;
    while d * d <= n {
        while n % d == 0 {
            count += 1;
            n /= d;
        }
        d += 2;
    }
    if n > 1 {
        count += 1;
    }
    count
}

/// Polars expression: big_omega(col) -> UInt8 column of Ω(n) values.
#[polars_expr(output_type=UInt8)]
fn big_omega(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: UInt8Chunked = unary_elementwise_values(ca, |n| big_omega_scalar(n));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Dominant factor analysis: share, prime (q), and exponent
// ---------------------------------------------------------------------------

/// Returns (dominant_q, dominant_exp, dominant_share) for n.
/// dominant_q: the prime whose v_q·ln(q)/ln(n) is maximal
/// dominant_exp: the exponent of that prime in the factorization
/// dominant_share: max(v_q·ln(q)) / ln(n)
#[inline]
fn dominant_triple(mut n: i64) -> (i64, u32, f64) {
    if n <= 1 {
        return if n == 1 { (1, 0, 1.0) } else { (0, 0, 0.0) };
    }
    if n < 0 {
        n = -n;
    }

    let log_n = (n as f64).ln();
    let mut max_share: f64 = 0.0;
    let mut dom_q: i64 = 0;
    let mut dom_e: u32 = 0;

    // Factor out 2
    if n % 2 == 0 {
        let mut e: u32 = 0;
        while n % 2 == 0 {
            e += 1;
            n /= 2;
        }
        let share = (e as f64) * (2.0_f64).ln() / log_n;
        if share > max_share {
            max_share = share;
            dom_q = 2;
            dom_e = e;
        }
    }

    let mut d: i64 = 3;
    while d * d <= n {
        if n % d == 0 {
            let mut e: u32 = 0;
            while n % d == 0 {
                e += 1;
                n /= d;
            }
            let share = (e as f64) * (d as f64).ln() / log_n;
            if share > max_share {
                max_share = share;
                dom_q = d;
                dom_e = e;
            }
        }
        d += 2;
    }
    if n > 1 {
        let share = (n as f64).ln() / log_n;
        if share > max_share {
            max_share = share;
            dom_q = n;
            dom_e = 1;
        }
    }

    (dom_q, dom_e, max_share)
}

#[inline]
fn dominant_share_scalar(n: i64) -> f64 {
    dominant_triple(n).2
}

/// Polars expression: dominant_share(col) -> Float64 column.
#[polars_expr(output_type=Float64)]
fn dominant_share(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: Float64Chunked = unary_elementwise_values(ca, |n| dominant_share_scalar(n));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Dominant prime: which prime factor has the largest share
// ---------------------------------------------------------------------------

#[inline]
fn dominant_q_scalar(n: i64) -> i64 {
    dominant_triple(n).0
}

/// Polars expression: dominant_q(col) -> Int64 column of dominant primes.
#[polars_expr(output_type=Int64)]
fn dominant_q(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: Int64Chunked = unary_elementwise_values(ca, |n| dominant_q_scalar(n));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Dominant exponent: exponent of the dominant prime factor
// ---------------------------------------------------------------------------

#[inline]
fn dominant_exp_scalar(n: i64) -> u8 {
    dominant_triple(n).1 as u8
}

/// Polars expression: dominant_exp(col) -> UInt8 column of dominant exponents.
#[polars_expr(output_type=UInt8)]
fn dominant_exp(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: UInt8Chunked = unary_elementwise_values(ca, |n| dominant_exp_scalar(n));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Möbius function: μ(n)
// ---------------------------------------------------------------------------

#[inline]
fn mobius_scalar(mut n: i64) -> i8 {
    if n <= 0 {
        return 0;
    }
    if n == 1 {
        return 1;
    }

    let mut num_factors: u32 = 0;

    if n % 2 == 0 {
        n /= 2;
        if n % 2 == 0 {
            return 0; // not squarefree
        }
        num_factors += 1;
    }

    let mut d: i64 = 3;
    while d * d <= n {
        if n % d == 0 {
            n /= d;
            if n % d == 0 {
                return 0; // not squarefree
            }
            num_factors += 1;
        }
        d += 2;
    }
    if n > 1 {
        num_factors += 1;
    }

    if num_factors % 2 == 0 {
        1
    } else {
        -1
    }
}

/// Polars expression: mobius(col) -> Int8 column of μ(n) values.
#[polars_expr(output_type=Int8)]
fn mobius(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: Int8Chunked = unary_elementwise_values(ca, |n| mobius_scalar(n));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Is prime power check
// ---------------------------------------------------------------------------

#[inline]
fn is_prime_power_scalar(n: i64) -> bool {
    if n <= 1 {
        return false;
    }
    omega_scalar(n) == 1
}

/// Polars expression: is_prime_power(col) -> Boolean column.
#[polars_expr(output_type=Boolean)]
fn is_prime_power(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: BooleanChunked = unary_elementwise_values(ca, |n| is_prime_power_scalar(n));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Largest prime factor
// ---------------------------------------------------------------------------

#[inline]
fn largest_prime_factor_scalar(mut n: i64) -> i64 {
    if n <= 1 {
        return 0;
    }
    if n < 0 {
        n = -n;
    }

    let mut largest: i64 = 0;

    if n % 2 == 0 {
        largest = 2;
        while n % 2 == 0 {
            n /= 2;
        }
    }

    let mut d: i64 = 3;
    while d * d <= n {
        if n % d == 0 {
            largest = d;
            while n % d == 0 {
                n /= d;
            }
        }
        d += 2;
    }
    if n > 1 {
        largest = n;
    }
    largest
}

/// Polars expression: largest_prime_factor(col) -> Int64 column.
#[polars_expr(output_type=Int64)]
fn largest_prime_factor(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: Int64Chunked = unary_elementwise_values(ca, |n| largest_prime_factor_scalar(n));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Python module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn funbuns_native(_py: Python, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Expressions are registered automatically via the #[polars_expr] macro.
    // The Python side uses `register_plugin_function` to bind them.
    Ok(())
}
