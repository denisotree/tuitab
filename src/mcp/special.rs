//! The distribution functions behind `tuitab_calc`'s statistics.
//!
//! Written here rather than taken from a statistics crate: the four distributions a
//! test or a threshold needs rest on two special functions, and a crate brings the
//! rest of its catalogue into the binary with them.  `tests/calc_tests.rs` holds these
//! to `statrs` across a grid of arguments, so the saving is not paid for in trust.
//!
//! - Student's t and binomial come from the regularized incomplete beta function;
//! - chi-square and Poisson from the regularized incomplete gamma function;
//! - quantiles invert a CDF by bisection on whichever tail is small, where the
//!   function keeps its digits.

/// Tolerance of the series and continued fractions.
const EPS: f64 = 1e-15;
/// A value closer to zero than this is replaced to keep Lentz's method off a division
/// by zero.
const TINY: f64 = 1e-300;
/// Iterations before a series is declared not to converge.  Enough for arguments in
/// the millions, whose series need on the order of their square root.
const MAX_ITER: usize = 100_000;

type Res<T> = Result<T, String>;

/// `ln Γ(x)` for `x > 0`: Lanczos approximation, g = 7, nine coefficients.
fn ln_gamma(x: f64) -> f64 {
    const G: f64 = 7.0;
    const C: [f64; 9] = [
        0.999_999_999_999_809_9,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    if x < 0.5 {
        // Reflection keeps the approximation in its accurate range.
        let pi = std::f64::consts::PI;
        return (pi / (pi * x).sin()).ln() - ln_gamma(1.0 - x);
    }
    let x = x - 1.0;
    let mut sum = C[0];
    for (i, c) in C.iter().enumerate().skip(1) {
        sum += c / (x + i as f64);
    }
    let t = x + G + 0.5;
    0.5 * (2.0 * std::f64::consts::PI).ln() + (x + 0.5) * t.ln() - t + sum.ln()
}

/// Regularized incomplete gamma: `(P(a, x), Q(a, x))`, lower and upper.  Each is
/// computed directly where it is small, so a far tail keeps its digits.
fn gamma_pq(a: f64, x: f64) -> Res<(f64, f64)> {
    if x <= 0.0 {
        return Ok((0.0, 1.0));
    }
    let front = a * x.ln() - x - ln_gamma(a);
    if x < a + 1.0 {
        // Series for P.
        let mut term = 1.0 / a;
        let mut sum = term;
        let mut n = a;
        for _ in 0..MAX_ITER {
            n += 1.0;
            term *= x / n;
            sum += term;
            if term.abs() < sum.abs() * EPS {
                let p = sum * front.exp();
                return Ok((p, 1.0 - p));
            }
        }
    } else {
        // Continued fraction for Q, modified Lentz.
        let mut b = x + 1.0 - a;
        let mut c = 1.0 / TINY;
        let mut d = 1.0 / b;
        let mut h = d;
        for i in 1..MAX_ITER {
            let an = -(i as f64) * (i as f64 - a);
            b += 2.0;
            d = an * d + b;
            if d.abs() < TINY {
                d = TINY;
            }
            c = b + an / c;
            if c.abs() < TINY {
                c = TINY;
            }
            d = 1.0 / d;
            let delta = d * c;
            h *= delta;
            if (delta - 1.0).abs() < EPS {
                let q = front.exp() * h;
                return Ok((1.0 - q, q));
            }
        }
    }
    Err("the incomplete gamma function did not converge for these arguments".into())
}

/// Continued fraction of the incomplete beta function, modified Lentz.
fn beta_fraction(a: f64, b: f64, x: f64) -> Res<f64> {
    let (qab, qap, qam) = (a + b, a + 1.0, a - 1.0);
    let mut c = 1.0;
    let mut d = 1.0 - qab * x / qap;
    if d.abs() < TINY {
        d = TINY;
    }
    d = 1.0 / d;
    let mut h = d;
    for m in 1..MAX_ITER {
        let m = m as f64;
        let m2 = 2.0 * m;
        let aa = m * (b - m) * x / ((qam + m2) * (a + m2));
        d = 1.0 + aa * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + aa / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        h *= d * c;
        let aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
        d = 1.0 + aa * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + aa / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let delta = d * c;
        h *= delta;
        if (delta - 1.0).abs() < EPS {
            return Ok(h);
        }
    }
    Err("the incomplete beta function did not converge for these arguments".into())
}

/// Regularized incomplete beta `I_x(a, b)`.
fn beta_inc(a: f64, b: f64, x: f64) -> Res<f64> {
    if x <= 0.0 {
        return Ok(0.0);
    }
    if x >= 1.0 {
        return Ok(1.0);
    }
    let front =
        (ln_gamma(a + b) - ln_gamma(a) - ln_gamma(b) + a * x.ln() + b * (1.0 - x).ln()).exp();
    // The fraction converges fast below the mean; above it, use the symmetry.
    if x < (a + 1.0) / (a + b + 2.0) {
        Ok(front * beta_fraction(a, b, x)? / a)
    } else {
        Ok(1.0 - front * beta_fraction(b, a, 1.0 - x)? / b)
    }
}

fn positive(name: &str, what: &str, v: f64) -> Res<()> {
    if v > 0.0 && v.is_finite() {
        Ok(())
    } else {
        Err(format!("{}() needs {} greater than 0", name, what))
    }
}

fn probability(name: &str, p: f64) -> Res<()> {
    if p > 0.0 && p < 1.0 {
        Ok(())
    } else {
        Err(format!(
            "{}() takes a probability strictly between 0 and 1",
            name
        ))
    }
}

/// Student's t: `P(T ≤ x)` with `df` degrees of freedom.
pub fn t_cdf(x: f64, df: f64) -> Res<f64> {
    positive("t_cdf", "degrees of freedom", df)?;
    // I_{df/(df+x²)}(df/2, 1/2) is the two-tailed mass beyond |x|.
    let tails = beta_inc(df / 2.0, 0.5, df / (df + x * x))?;
    Ok(if x > 0.0 {
        1.0 - tails / 2.0
    } else {
        tails / 2.0
    })
}

/// The `p` quantile of Student's t.
pub fn t_inv(p: f64, df: f64) -> Res<f64> {
    probability("t_inv", p)?;
    positive("t_inv", "degrees of freedom", df)?;
    if p == 0.5 {
        return Ok(0.0);
    }
    // Symmetric: solve on the lower tail, where the CDF is small and exact.
    let (target, sign) = if p < 0.5 { (p, 1.0) } else { (1.0 - p, -1.0) };
    let lower =
        |x: f64| -> Res<f64> { beta_inc(df / 2.0, 0.5, df / (df + x * x)).map(|t| t / 2.0) };
    let mut hi = 1.0;
    while lower(hi)? > target {
        hi *= 2.0;
        if hi > 1e300 {
            return Err("t_inv() is out of range for these arguments".into());
        }
    }
    let x = bisect(0.0, hi, |x| Ok(lower(x)? > target))?;
    Ok(-sign * x)
}

/// Chi-square: `P(X ≤ x)` with `df` degrees of freedom.
pub fn chi2_cdf(x: f64, df: f64) -> Res<f64> {
    positive("chi2_cdf", "degrees of freedom", df)?;
    Ok(gamma_pq(df / 2.0, x / 2.0)?.0)
}

/// The `p` quantile of chi-square.
pub fn chi2_inv(p: f64, df: f64) -> Res<f64> {
    probability("chi2_inv", p)?;
    positive("chi2_inv", "degrees of freedom", df)?;
    let upper = p > 0.5;
    // Below the target on the side being solved means the root is further right.
    let left_of_root = |x: f64| -> Res<bool> {
        let (lo, hi) = gamma_pq(df / 2.0, x / 2.0)?;
        Ok(if upper { hi > 1.0 - p } else { lo < p })
    };
    let mut hi = df.max(1.0);
    while left_of_root(hi)? {
        hi *= 2.0;
        if hi > 1e300 {
            return Err("chi2_inv() is out of range for these arguments".into());
        }
    }
    bisect(0.0, hi, left_of_root)
}

/// Poisson: `P(X ≤ k)` for mean `lambda`.
pub fn poisson_cdf(k: f64, lambda: f64) -> Res<f64> {
    if !(lambda >= 0.0 && lambda.is_finite()) {
        return Err("poisson_cdf() needs a mean of 0 or more".into());
    }
    let k = whole("poisson_cdf", "k", k)?;
    if k < 0.0 {
        return Ok(0.0);
    }
    if lambda == 0.0 {
        return Ok(1.0);
    }
    Ok(gamma_pq(k + 1.0, lambda)?.1)
}

/// Binomial: `P(X ≤ k)` in `n` trials with success probability `p`.
pub fn binom_cdf(k: f64, n: f64, p: f64) -> Res<f64> {
    let k = whole("binom_cdf", "k", k)?;
    let n = whole("binom_cdf", "n", n)?;
    if n < 0.0 {
        return Err("binom_cdf() needs n of 0 or more".into());
    }
    if !(0.0..=1.0).contains(&p) {
        return Err("binom_cdf() needs p from 0 to 1".into());
    }
    if k < 0.0 {
        return Ok(0.0);
    }
    if k >= n || p == 0.0 {
        return Ok(1.0);
    }
    if p == 1.0 {
        return Ok(0.0);
    }
    beta_inc(n - k, k + 1.0, 1.0 - p)
}

fn whole(name: &str, what: &str, v: f64) -> Res<f64> {
    if v.is_finite() && v.fract() == 0.0 {
        Ok(v)
    } else {
        Err(format!("{}() needs a whole number for {}", name, what))
    }
}

/// The boundary in `[lo, hi]` where `left_of_root` turns false, to double precision.
fn bisect(mut lo: f64, mut hi: f64, left_of_root: impl Fn(f64) -> Res<bool>) -> Res<f64> {
    for _ in 0..2_000 {
        let mid = lo + (hi - lo) / 2.0;
        if mid <= lo || mid >= hi {
            break;
        }
        if left_of_root(mid)? {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok(lo + (hi - lo) / 2.0)
}
