#![allow(clippy::unused_unit)]

use polars::prelude::*;
use polars::prelude::arity::unary_elementwise_values;
use pyo3::prelude::*;
use pyo3_polars::derive::polars_expr;
use rug::Integer;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Small-primes table for trial division (primes up to 2^16 cover sqrt(2^32))
// ---------------------------------------------------------------------------

const SMALL_PRIME_LIMIT: i64 = 65536;

/// Sieve primes up to SMALL_PRIME_LIMIT at startup.
fn small_primes() -> &'static [i64] {
    use std::sync::OnceLock;
    static PRIMES: OnceLock<Vec<i64>> = OnceLock::new();
    PRIMES.get_or_init(|| {
        let limit = SMALL_PRIME_LIMIT as usize;
        let mut sieve = vec![true; limit + 1];
        sieve[0] = false;
        if limit >= 1 { sieve[1] = false; }
        let mut i = 2;
        while i * i <= limit {
            if sieve[i] {
                let mut j = i * i;
                while j <= limit {
                    sieve[j] = false;
                    j += i;
                }
            }
            i += 1;
        }
        sieve.iter().enumerate()
            .filter(|(_, &is_p)| is_p)
            .map(|(i, _)| i as i64)
            .collect()
    })
}

// ---------------------------------------------------------------------------
// GMP-backed primality test
// ---------------------------------------------------------------------------

/// Fast primality test via GMP (deterministic for n < 2^64 with sufficient reps).
#[inline]
fn is_prime(n: i64) -> bool {
    if n < 2 { return false; }
    // 25 Miller-Rabin rounds: probability of false positive < 4^{-25}
    // For n < 3.3e24, this is actually deterministic.
    Integer::from(n).is_probably_prime(25) != rug::integer::IsPrime::No
}

// ---------------------------------------------------------------------------
// Factorization core: trial division with small primes, GMP primality on cofactor
// ---------------------------------------------------------------------------

/// Factor info collected during trial division.
struct FactorInfo {
    /// (prime, exponent) pairs
    factors: [(i64, u32); 16], // 16 distinct factors is plenty for i64
    nfactors: usize,
}

impl FactorInfo {
    #[inline]
    fn new() -> Self {
        FactorInfo {
            factors: [(0, 0); 16],
            nfactors: 0,
        }
    }

    /// Look up the exponent of `ell` in the factorization (0 if absent).
    #[inline]
    fn v_ell(&self, ell: i64) -> i32 {
        for i in 0..self.nfactors {
            if self.factors[i].0 == ell {
                return self.factors[i].1 as i32;
            }
        }
        0
    }

    #[inline]
    fn push(&mut self, p: i64, e: u32) {
        if self.nfactors < 16 {
            self.factors[self.nfactors] = (p, e);
            self.nfactors += 1;
        }
    }
}

/// Full factorization of n using small-primes trial division + GMP primality.
/// Returns factor info with all (prime, exponent) pairs.
#[inline]
fn factorize(mut n: i64) -> FactorInfo {
    let mut info = FactorInfo::new();
    if n <= 1 {
        return info;
    }
    if n < 0 {
        n = -n;
    }

    for &p in small_primes() {
        if p * p > n {
            break;
        }
        if n % p == 0 {
            let mut e: u32 = 0;
            while n % p == 0 {
                e += 1;
                n /= p;
            }
            info.push(p, e);
        }
    }

    // Cofactor: either 1, prime, or product of two large primes
    if n > 1 {
        if is_prime(n) {
            info.push(n, 1);
        } else {
            // n is composite with all prime factors > SMALL_PRIME_LIMIT.
            // For i64 values, this means n = p * q with p, q > 65536,
            // so n > 2^32. Since our remainders r = p - 2^m fit in i64,
            // we need to find the factor. Use Pollard's rho via rug.
            let factor = pollard_rho(n);
            if factor > 0 && factor < n {
                let other = n / factor;
                let (a, mut b) = if factor <= other {
                    (factor, other)
                } else {
                    (other, factor)
                };
                let ea: u32 = 1;
                let mut eb: u32 = 1;
                // Check if a divides b (i.e., a = b case or prime power)
                while b % a == 0 {
                    eb += 1;
                    b /= a;
                }
                if b == 1 {
                    // n = a^(ea+eb)
                    info.push(a, ea + eb - 1);
                } else {
                    info.push(a, ea);
                    info.push(b, eb);
                }
            } else {
                // Rho failed (shouldn't happen for i64), treat as prime
                info.push(n, 1);
            }
        }
    }

    info
}

/// Pollard's rho factoring using rug Integer.
fn pollard_rho(n: i64) -> i64 {
    let n_big = Integer::from(n);
    let mut x = Integer::from(2);
    let mut y = Integer::from(2);
    let mut d = Integer::from(1);
    let c = Integer::from(1);

    while d == 1 {
        x = (Integer::from(&x * &x) + &c) % &n_big;
        y = (Integer::from(&y * &y) + &c) % &n_big;
        y = (Integer::from(&y * &y) + &c) % &n_big;
        d = Integer::from(&x - &y).abs();
        d = d.gcd(&n_big);
    }

    if d == n_big {
        0 // failed
    } else {
        d.to_i64().unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// ℓ-adic valuation: v_ℓ(n)
// ---------------------------------------------------------------------------

#[inline]
fn v_ell_scalar(mut n: i64, ell: i64) -> i32 {
    if n == 0 {
        return -1;
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

#[polars_expr(output_type=Int32)]
fn v_ell(inputs: &[Series], kwargs: VEllKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let ell = kwargs.ell;
    let out: Int32Chunked = unary_elementwise_values(ca, |n| v_ell_scalar(n, ell));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Arithmetic helpers for power residue symbols
// ---------------------------------------------------------------------------

/// Greatest common divisor (Euclidean algorithm).
#[inline]
fn gcd_i64(mut a: i64, mut b: i64) -> i64 {
    a = a.abs();
    b = b.abs();
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

/// Modular exponentiation: base^exp mod modulus.
/// Uses i128 intermediate to avoid overflow.
#[inline]
fn powmod_i64(base: i64, mut exp: i64, modulus: i64) -> i64 {
    if modulus == 1 { return 0; }
    let m = modulus as i128;
    let mut result: i128 = 1;
    let mut b = (base % modulus) as i128;
    if b < 0 { b += m; }
    while exp > 0 {
        if exp & 1 == 1 {
            result = result * b % m;
        }
        exp >>= 1;
        if exp > 0 {
            b = b * b % m;
        }
    }
    result as i64
}

// ---------------------------------------------------------------------------
// n-th power residue symbol: the H^0 datum
//
// For r ∈ Z and prime l, exponent n ≥ 2:
//   1. v = v_l(r)                    (l-adic valuation)
//   2. u = r / l^v                   (unit part, coprime to l)
//   3. g = gcd(n, l-1)              (index of n-th powers in (Z/lZ)*)
//   4. symbol = u^((l-1)/g) mod l   (power residue symbol)
//
// r is an n-th power in Z_l iff v ≡ 0 mod n AND symbol = 1.
// (By Hensel's lemma, since l ∤ n for our filtration primes.)
//
// The symbol value lives in μ_g ⊂ (Z/lZ)* and records WHICH coset
// of the n-th powers r belongs to — this is the actual H^0 datum,
// not just the boolean.
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct PowerResidueKwargs {
    ell: i64,
    n: i64,
}

fn power_residue_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let _ = input_fields;
    let fields = vec![
        Field::new("v_ell".into(), DataType::Int32),
        Field::new("v_mod_n".into(), DataType::Int32),
        Field::new("unit_mod_ell".into(), DataType::Int64),
        Field::new("symbol".into(), DataType::Int64),
        Field::new("is_nth_power".into(), DataType::Boolean),
    ];
    Ok(Field::new("power_residue".into(), DataType::Struct(fields)))
}

#[polars_expr(output_type_func=power_residue_output)]
fn power_residue(inputs: &[Series], kwargs: PowerResidueKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let len = ca.len();
    let ell = kwargs.ell;
    let n = kwargs.n;

    // Precompute: g = gcd(n, l-1), exponent for symbol = (l-1)/g
    let g = gcd_i64(n, ell - 1);
    let pr_exp = (ell - 1) / g;

    let mut v_vals = Vec::with_capacity(len);
    let mut vm_vals = Vec::with_capacity(len);
    let mut unit_vals = Vec::with_capacity(len);
    let mut sym_vals = Vec::with_capacity(len);
    let mut is_np_vals = Vec::with_capacity(len);

    for opt_r in ca.into_iter() {
        let r = opt_r.unwrap_or(0);
        if r <= 0 {
            v_vals.push(if r == 0 { -1i32 } else { 0 });
            vm_vals.push(0i32);
            unit_vals.push(0i64);
            sym_vals.push(0i64);
            is_np_vals.push(false);
            continue;
        }

        // l-adic valuation
        let v = v_ell_scalar(r, ell);
        let v_mod_n = ((v as i64) % n) as i32;

        // Unit part: r / ell^v (guaranteed coprime to ell)
        let mut u = r;
        for _ in 0..v {
            u /= ell;
        }
        let u_mod = ((u % ell) + ell) % ell; // ensure positive

        // Power residue symbol: u^((l-1)/gcd(n,l-1)) mod l
        let sym = if u_mod == 0 {
            // Shouldn't happen (u is coprime to ell after dividing out v)
            0
        } else {
            powmod_i64(u_mod, pr_exp, ell)
        };

        let is_np = v_mod_n == 0 && sym == 1;

        v_vals.push(v);
        vm_vals.push(v_mod_n);
        unit_vals.push(u_mod);
        sym_vals.push(sym);
        is_np_vals.push(is_np);
    }

    let fields: Vec<Series> = vec![
        Series::new("v_ell".into(), v_vals),
        Series::new("v_mod_n".into(), vm_vals),
        Series::new("unit_mod_ell".into(), unit_vals),
        Series::new("symbol".into(), sym_vals),
        Series::new("is_nth_power".into(), is_np_vals),
    ];

    StructChunked::from_series("power_residue".into(), len, fields.iter())
        .map(|ca| ca.into_series())
}

/// Lean Step 2: just the symbol value u^((l-1)/gcd(n,l-1)) mod l.
///
/// Step 1 (v_ell) is already its own function. This computes the unit part
/// internally (divides out l) and returns the symbol in μ_{gcd(n,l-1)}.
/// Compose with v_ell in the pipeline to get the full local n-th power test.
#[polars_expr(output_type=Int64)]
fn power_residue_symbol(inputs: &[Series], kwargs: PowerResidueKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let ell = kwargs.ell;
    let n = kwargs.n;
    let g = gcd_i64(n, ell - 1);
    let pr_exp = (ell - 1) / g;

    let out: Int64Chunked = unary_elementwise_values(ca, |r| {
        if r <= 0 { return 0; }
        // Extract unit part: divide out all factors of ell
        let mut u = if r < 0 { -r } else { r };
        while u % ell == 0 { u /= ell; }
        let u_mod = u % ell;
        if u_mod == 0 { return 0; }
        powmod_i64(u_mod, pr_exp, ell)
    });

    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Selmer-based n-th power check
//
// For integer r and exponent n: factor r, check if all exponents are
// divisible by n. The Selmer set S = {l : l | r and v_l(r) % n ≠ 0}
// determines the obstruction — no arbitrary prime bound needed.
//
// Returns:
//   0  → r is a perfect n-th power (S is empty)
//   l  → first prime l where v_l(r) % n ≠ 0 (Selmer obstruction)
//  -1  → degenerate input (r ≤ 0, n ≤ 0)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct NthPowerKwargs {
    n: i64,
}

#[polars_expr(output_type=Int64)]
fn nth_power_check(inputs: &[Series], kwargs: NthPowerKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let n = kwargs.n;

    let out: Int64Chunked = unary_elementwise_values(ca, |r| {
        if r <= 0 || n <= 0 { return -1; }
        if n == 1 { return 0; }

        let info = factorize(if r < 0 { -r } else { r });

        for i in 0..info.nfactors {
            let (p, e) = info.factors[i];
            if (e as i64) % n != 0 {
                return p; // Selmer obstruction at this prime
            }
        }

        0 // perfect n-th power
    });

    Ok(out.into_series())
}

/// Like nth_power_check but returns the n-th root when r is a perfect n-th
/// power, or 0 when it's not. Useful for checking if the root is prime.
#[polars_expr(output_type=Int64)]
fn nth_root_or_zero(inputs: &[Series], kwargs: NthPowerKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let n = kwargs.n;

    let out: Int64Chunked = unary_elementwise_values(ca, |r| {
        if r <= 1 || n <= 0 { return 0; }
        if n == 1 { return r; }

        let info = factorize(if r < 0 { -r } else { r });

        // Check all exponents divisible by n
        for i in 0..info.nfactors {
            if (info.factors[i].1 as i64) % n != 0 {
                return 0; // not a perfect n-th power
            }
        }

        // Compute the n-th root: product of p^(e/n)
        let mut root: i64 = 1;
        for i in 0..info.nfactors {
            let (p, e) = info.factors[i];
            let exp_n = e as i64 / n;
            for _ in 0..exp_n {
                root = root.saturating_mul(p);
            }
        }

        root
    });

    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Number of distinct prime factors: ω(n)
// ---------------------------------------------------------------------------

#[inline]
fn omega_scalar(n: i64) -> u8 {
    factorize(n).nfactors as u8
}

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
fn big_omega_scalar(n: i64) -> u8 {
    let info = factorize(n);
    let mut total: u8 = 0;
    for i in 0..info.nfactors {
        total += info.factors[i].1 as u8;
    }
    total
}

#[polars_expr(output_type=UInt8)]
fn big_omega(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: UInt8Chunked = unary_elementwise_values(ca, |n| big_omega_scalar(n));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Dominant factor analysis: share, prime (q), and exponent
// ---------------------------------------------------------------------------

#[inline]
fn dominant_triple(n: i64) -> (i64, u32, f64) {
    if n <= 1 {
        return if n == 1 { (1, 0, 1.0) } else { (0, 0, 0.0) };
    }
    let abs_n = if n < 0 { -n } else { n };
    let log_n = (abs_n as f64).ln();
    let info = factorize(abs_n);

    let mut max_share: f64 = 0.0;
    let mut dom_q: i64 = 0;
    let mut dom_e: u32 = 0;

    for i in 0..info.nfactors {
        let (p, e) = info.factors[i];
        let share = (e as f64) * (p as f64).ln() / log_n;
        if share > max_share {
            max_share = share;
            dom_q = p;
            dom_e = e;
        }
    }

    (dom_q, dom_e, max_share)
}

#[polars_expr(output_type=Float64)]
fn dominant_share(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: Float64Chunked = unary_elementwise_values(ca, |n| dominant_triple(n).2);
    Ok(out.into_series())
}

#[polars_expr(output_type=Int64)]
fn dominant_q(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: Int64Chunked = unary_elementwise_values(ca, |n| dominant_triple(n).0);
    Ok(out.into_series())
}

#[polars_expr(output_type=UInt8)]
fn dominant_exp(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: UInt8Chunked = unary_elementwise_values(ca, |n| dominant_triple(n).1 as u8);
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Möbius function: μ(n)
// ---------------------------------------------------------------------------

#[inline]
fn mobius_scalar(n: i64) -> i8 {
    if n <= 0 {
        return 0;
    }
    if n == 1 {
        return 1;
    }
    let info = factorize(n);
    for i in 0..info.nfactors {
        if info.factors[i].1 > 1 {
            return 0; // not squarefree
        }
    }
    if info.nfactors % 2 == 0 { 1 } else { -1 }
}

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
    let info = factorize(if n < 0 { -n } else { n });
    info.nfactors == 1
}

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
fn largest_prime_factor_scalar(n: i64) -> i64 {
    if n <= 1 {
        return 0;
    }
    let info = factorize(if n < 0 { -n } else { n });
    let mut largest: i64 = 0;
    for i in 0..info.nfactors {
        if info.factors[i].0 > largest {
            largest = info.factors[i].0;
        }
    }
    largest
}

#[polars_expr(output_type=Int64)]
fn largest_prime_factor(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let out: Int64Chunked = unary_elementwise_values(ca, |n| largest_prime_factor_scalar(n));
    Ok(out.into_series())
}

// ---------------------------------------------------------------------------
// Full profile: single factorization, all outputs at once
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct FullProfileKwargs {
    filtration_primes: Vec<i64>,
}

/// Output type: Struct with fixed fields + dynamic v_ell fields.
/// Since we can't access kwargs in the output_type_func, we declare a
/// superset of common filtration primes. The actual fields produced at
/// runtime will match kwargs.filtration_primes.
fn full_profile_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let _ = input_fields;
    let fields = vec![
        Field::new("omega".into(), DataType::UInt8),
        Field::new("big_omega".into(), DataType::UInt8),
        Field::new("dominant_q".into(), DataType::Int64),
        Field::new("dominant_exp".into(), DataType::UInt8),
        Field::new("dominant_share".into(), DataType::Float64),
        Field::new("mu".into(), DataType::Int8),
        Field::new("is_prime_power".into(), DataType::Boolean),
        Field::new("v_2".into(), DataType::Int32),
        Field::new("v_3".into(), DataType::Int32),
        Field::new("v_5".into(), DataType::Int32),
        Field::new("v_7".into(), DataType::Int32),
        Field::new("v_11".into(), DataType::Int32),
        Field::new("v_13".into(), DataType::Int32),
    ];
    Ok(Field::new("full_profile".into(), DataType::Struct(fields)))
}

#[polars_expr(output_type_func=full_profile_output)]
fn full_profile(inputs: &[Series], kwargs: FullProfileKwargs) -> PolarsResult<Series> {
    let ca = inputs[0].i64()?;
    let len = ca.len();
    let filt_primes = &kwargs.filtration_primes;

    // Pre-allocate all output vectors
    let mut omega_vals = Vec::with_capacity(len);
    let mut big_omega_vals = Vec::with_capacity(len);
    let mut dom_q_vals = Vec::with_capacity(len);
    let mut dom_e_vals = Vec::with_capacity(len);
    let mut dom_share_vals = Vec::with_capacity(len);
    let mut mu_vals = Vec::with_capacity(len);
    let mut is_pp_vals = Vec::with_capacity(len);
    let mut v_ell_vecs: Vec<Vec<i32>> = filt_primes.iter().map(|_| Vec::with_capacity(len)).collect();

    for opt_n in ca.into_iter() {
        let n = opt_n.unwrap_or(0);
        let abs_n = if n < 0 { -n } else { n };

        if abs_n <= 1 {
            omega_vals.push(0u8);
            big_omega_vals.push(0u8);
            dom_q_vals.push(if n == 1 { 1i64 } else { 0 });
            dom_e_vals.push(0u8);
            dom_share_vals.push(if n == 1 { 1.0 } else { 0.0 });
            mu_vals.push(if n == 1 { 1i8 } else { 0 });
            is_pp_vals.push(false);
            for vv in v_ell_vecs.iter_mut() {
                vv.push(if n == 0 { -1 } else { 0 });
            }
            continue;
        }

        let info = factorize(abs_n);
        let log_n = (abs_n as f64).ln();

        // omega
        omega_vals.push(info.nfactors as u8);

        // big_omega
        let mut total_exp: u8 = 0;
        for i in 0..info.nfactors {
            total_exp += info.factors[i].1 as u8;
        }
        big_omega_vals.push(total_exp);

        // dominant triple
        let mut max_share: f64 = 0.0;
        let mut dq: i64 = 0;
        let mut de: u32 = 0;
        for i in 0..info.nfactors {
            let (p, e) = info.factors[i];
            let share = (e as f64) * (p as f64).ln() / log_n;
            if share > max_share {
                max_share = share;
                dq = p;
                de = e;
            }
        }
        dom_q_vals.push(dq);
        dom_e_vals.push(de as u8);
        dom_share_vals.push(max_share);

        // mobius
        let mut squarefree = true;
        for i in 0..info.nfactors {
            if info.factors[i].1 > 1 {
                squarefree = false;
                break;
            }
        }
        mu_vals.push(if !squarefree { 0 } else if info.nfactors % 2 == 0 { 1 } else { -1 });

        // is_prime_power
        is_pp_vals.push(info.nfactors == 1);

        // v_ell for each filtration prime (from factorization, no re-division)
        for (j, &ell) in filt_primes.iter().enumerate() {
            v_ell_vecs[j].push(info.v_ell(ell));
        }
    }

    // Build struct from individual Series
    let mut fields: Vec<Series> = vec![
        Series::new("omega".into(), omega_vals),
        Series::new("big_omega".into(), big_omega_vals),
        Series::new("dominant_q".into(), dom_q_vals),
        Series::new("dominant_exp".into(), dom_e_vals),
        Series::new("dominant_share".into(), dom_share_vals),
        Series::new("mu".into(), mu_vals),
        Series::new("is_prime_power".into(), is_pp_vals),
    ];
    for (j, ell) in filt_primes.iter().enumerate() {
        fields.push(Series::new(format!("v_{}", ell).into(), std::mem::take(&mut v_ell_vecs[j])));
    }

    StructChunked::from_series("full_profile".into(), len, fields.iter())
        .map(|ca| ca.into_series())
}

// ---------------------------------------------------------------------------
// Python module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn funbuns_native(_py: Python, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
