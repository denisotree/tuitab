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

// ── Rank tests: Mann–Whitney U and two-sample Kolmogorov–Smirnov ─────────────────────
//
// Both follow scipy.stats 1.18 (`mannwhitneyu`, `ks_2samp`) step for step — the same
// statistic, the same choice of method, the same exact and asymptotic p-values — so an
// answer here can be checked against the reference everyone else uses.

/// Which side the alternative hypothesis is on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Alternative {
    TwoSided,
    Less,
    Greater,
}

/// How the p-value is computed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Method {
    Auto,
    Exact,
    Asymptotic,
}

/// `C(n, k)` in floating point.
fn binom(n: f64, k: f64) -> f64 {
    if k < 0.0 || k > n {
        return 0.0;
    }
    let k = k.min(n - k);
    let mut out = 1.0;
    let mut i = 0.0;
    while i < k {
        out = out * (n - i) / (i + 1.0);
        i += 1.0;
    }
    out
}

/// Average ranks (1-based) of `values`, and the sizes of each group of ties.
fn average_ranks(values: &[f64]) -> (Vec<f64>, Vec<f64>) {
    let mut order: Vec<usize> = (0..values.len()).collect();
    order.sort_by(|&a, &b| values[a].total_cmp(&values[b]));
    let mut ranks = vec![0.0; values.len()];
    let mut ties = Vec::new();
    let mut i = 0;
    while i < order.len() {
        let mut j = i;
        while j + 1 < order.len() && values[order[j + 1]] == values[order[i]] {
            j += 1;
        }
        let rank = (i + j) as f64 / 2.0 + 1.0;
        for &k in &order[i..=j] {
            ranks[k] = rank;
        }
        ties.push((j - i + 1) as f64);
        i = j + 1;
    }
    (ranks, ties)
}

/// P(U = 0..=maxu) under the null for samples of `n1 <= n2`, by the recurrence scipy
/// uses (`_MWU.build_u_freqs_array`).
fn mwu_pmf(n1: usize, n2: usize, maxu: usize) -> Vec<f64> {
    let mut sigma = vec![0.0f64; maxu + 1];
    for d in 1..=n1 {
        let mut a = d;
        while a <= maxu {
            sigma[a] += d as f64;
            a += d;
        }
    }
    for d in (n2 + 1)..=(n2 + n1) {
        let mut a = d;
        while a <= maxu {
            sigma[a] -= d as f64;
            a += d;
        }
    }
    let mut configurations = vec![0.0f64; maxu + 1];
    configurations[0] = 1.0;
    for u in 1..=maxu {
        let mut total = 0.0;
        for (i, c) in configurations[..u].iter().enumerate() {
            total += c * sigma[u - i];
        }
        configurations[u] = total / u as f64;
    }
    let all = binom((n1 + n2) as f64, n1 as f64);
    configurations.into_iter().map(|c| c / all).collect()
}

/// P(U >= k) under the null, from the side of the distribution nearer zero.
fn mwu_sf(n1: usize, n2: usize, k: i64) -> f64 {
    let (n1, n2) = (n1.min(n2), n1.max(n2));
    let top = (n1 * n2) as i64;
    if k <= 0 {
        return 1.0;
    }
    if k > top {
        return 0.0;
    }
    let kc = top - k;
    if k < kc {
        let pmf = mwu_pmf(n1, n2, k as usize);
        let below: f64 = pmf[..k as usize].iter().sum();
        1.0 - below
    } else {
        let pmf = mwu_pmf(n1, n2, kc as usize);
        pmf.iter().sum()
    }
}

fn norm_sf(z: f64) -> f64 {
    0.5 * libm::erfc(z / std::f64::consts::SQRT_2)
}

/// Mann–Whitney U for `x` against `y`: the statistic U for `x`, and the p-value.
pub fn mannwhitney(
    x: &[f64],
    y: &[f64],
    alternative: Alternative,
    method: Method,
) -> Res<(f64, f64)> {
    if x.is_empty() || y.is_empty() {
        return Err("mannwhitney() needs two samples with at least one value each".into());
    }
    let (n1, n2) = (x.len(), y.len());
    let pooled: Vec<f64> = x.iter().chain(y).copied().collect();
    let (ranks, ties) = average_ranks(&pooled);
    let r1: f64 = ranks[..n1].iter().sum();
    let u1 = r1 - (n1 * (n1 + 1)) as f64 / 2.0;
    let u2 = (n1 * n2) as f64 - u1;
    let has_ties = ties.iter().any(|&t| t > 1.0);
    let method = match method {
        Method::Auto if n1 > 8 && n2 > 8 => Method::Asymptotic,
        Method::Auto if has_ties => Method::Asymptotic,
        Method::Auto => Method::Exact,
        m => m,
    };
    let p = match method {
        Method::Exact => {
            if n1.saturating_mul(n2) > 2_000_000 {
                return Err(
                    "mannwhitney(): the exact distribution of samples this large is out of \
                     reach; use asymptotic"
                        .into(),
                );
            }
            let (u, factor) = match alternative {
                Alternative::Greater => (u1, 1.0),
                Alternative::Less => (u2, 1.0),
                Alternative::TwoSided => (u1.max(u2), 2.0),
            };
            mwu_sf(n1, n2, u as i64) * factor
        }
        _ => {
            let (n1f, n2f) = (n1 as f64, n2 as f64);
            let n = n1f + n2f;
            let tie_term: f64 = ties.iter().map(|t| t * t * t - t).sum();
            let s = (n1f * n2f / 12.0 * ((n + 1.0) - tie_term / (n * (n - 1.0)))).sqrt();
            let mut numerator = u1 - n1f * n2f / 2.0;
            let sign = match alternative {
                Alternative::Greater => 1.0,
                Alternative::Less => -1.0,
                Alternative::TwoSided => {
                    if numerator > 0.0 {
                        1.0
                    } else if numerator < 0.0 {
                        -1.0
                    } else {
                        0.0
                    }
                }
            };
            numerator -= 0.5 * sign;
            let z = numerator / s;
            match alternative {
                Alternative::Less => 1.0 - norm_sf(z),
                Alternative::Greater => norm_sf(z),
                Alternative::TwoSided => 2.0 * norm_sf(z.abs()),
            }
        }
    };
    Ok((u1, p.clamp(0.0, 1.0)))
}

/// The two-sample KS statistic for an alternative: `(D, D⁻, D⁺)` folded the way
/// `ks_2samp` folds them.
fn ks_statistic(x: &[f64], y: &[f64], alternative: Alternative) -> f64 {
    let mut a = x.to_vec();
    let mut b = y.to_vec();
    a.sort_by(f64::total_cmp);
    b.sort_by(f64::total_cmp);
    let (n1, n2) = (a.len() as f64, b.len() as f64);
    let at_most = |s: &[f64], v: f64| s.partition_point(|&w| w <= v) as f64;
    let mut min_diff = f64::INFINITY;
    let mut max_diff = f64::NEG_INFINITY;
    for &v in a.iter().chain(&b) {
        let diff = at_most(&a, v) / n1 - at_most(&b, v) / n2;
        min_diff = min_diff.min(diff);
        max_diff = max_diff.max(diff);
    }
    let min_s = (-min_diff).clamp(0.0, 1.0);
    match alternative {
        Alternative::Less => min_s,
        Alternative::Greater => max_diff,
        Alternative::TwoSided => {
            if min_s > max_diff {
                min_s
            } else {
                max_diff
            }
        }
    }
}

fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// Pr(D_{n,n} >= h/n): paths outside both diagonals (scipy `_compute_prob_outside_square`).
fn prob_outside_square(n: usize, h: usize) -> f64 {
    let (n, h) = (n as f64, h as f64);
    let mut p = 0.0;
    let mut k = (n / h).floor();
    while k >= 0.0 {
        let mut p1 = 1.0;
        let mut j = 0.0;
        while j < h {
            p1 = (n - k * h - j) * p1 / (n + k * h + j + 1.0);
            j += 1.0;
        }
        p = p1 * (1.0 - p);
        k -= 1.0;
    }
    2.0 * p
}

/// The share of lattice paths from (0, 0) to (m, n) that leave the band
/// |n·x − m·y|/g < h — the two-sided exact p-value for unequal samples.  The column
/// carried along holds, per point, the share of paths to it that have already left.
fn outer_prob_inside_method(m: usize, n: usize, g: usize, h: usize) -> f64 {
    let (m, n) = if m < n { (n, m) } else { (m, n) };
    let mg = (m / g) as f64;
    let ng = (n / g) as f64;
    let hf = h as f64;
    let mut minj: usize = 0;
    let mut maxj: usize = ((hf / mg).ceil() as usize).min(n + 1);
    let mut curlen = maxj - minj;
    let len_a = (2 * maxj + 2).min(n + 1);
    let mut a = vec![1.0f64; len_a.max(1)];
    for v in a.iter_mut().take(maxj).skip(minj) {
        *v = 0.0;
    }
    for i in 1..=m {
        let (lastminj, lastlen) = (minj, curlen);
        let lo = ((ng * i as f64 - hf) / mg).floor() as i64 + 1;
        minj = (lo.max(0) as usize).min(n);
        maxj = (((ng * i as f64 + hf) / mg).ceil() as usize).min(n + 1);
        if maxj <= minj {
            return 1.0;
        }
        let mut val = if minj == 0 { 0.0 } else { 1.0 };
        for jj in 0..(maxj - minj) {
            let j = jj + minj;
            let carried = a.get(jj + minj - lastminj).copied().unwrap_or(1.0);
            val = (carried * i as f64 + val * j as f64) / (i + j) as f64;
            if jj < a.len() {
                a[jj] = val;
            }
        }
        curlen = maxj - minj;
        if lastlen > curlen {
            for v in a.iter_mut().skip(maxj - minj).take(lastlen - curlen) {
                *v = 1.0;
            }
        }
    }
    a[maxj - minj - 1]
}

/// Lattice paths from (0, 0) to (m, n) that cross the diagonal m·y <= n·x − h·g — the
/// one-sided exact count (scipy `_count_paths_outside_method`, after Hodges).
fn count_paths_outside(m: usize, n: usize, g: usize, h: usize) -> f64 {
    let (m, n) = if m < n { (n, m) } else { (m, n) };
    let mg = m / g;
    let ng = n / g;
    // Python's floor division, as in the reference: (mg - h) may be negative.
    let lxj = (n as i64 + (mg as i64 - h as i64).div_euclid(mg as i64)).max(0) as usize;
    let xj: Vec<f64> = (0..lxj).map(|j| (h + mg * j).div_ceil(ng) as f64).collect();
    if lxj == 0 {
        return binom((m + n) as f64, n as f64);
    }
    let mut b = vec![0.0f64; lxj];
    b[0] = 1.0;
    for j in 1..lxj {
        let mut bj = binom(xj[j] + j as f64, j as f64);
        for i in 0..j {
            bj -= binom(xj[j] - xj[i] + (j - i) as f64, (j - i) as f64) * b[i];
        }
        b[j] = bj;
    }
    (0..lxj)
        .map(|j| b[j] * binom((m as f64 - xj[j]) + (n - j) as f64, (n - j) as f64))
        .sum()
}

/// The exact two-sample p-value, or `None` when the arithmetic would not hold.
fn ks_exact(n1: usize, n2: usize, d: f64, alternative: Alternative) -> Option<f64> {
    let g = gcd(n1, n2);
    let lcm = (n1 / g) as f64 * n2 as f64;
    let h = (d * lcm).round() as usize;
    if h == 0 {
        return Some(1.0);
    }
    let p = match alternative {
        Alternative::TwoSided if n1 == n2 => prob_outside_square(n1, h),
        Alternative::TwoSided => outer_prob_inside_method(n1, n2, g, h),
        _ if n1 == n2 => (0..h)
            .map(|j| (n1 - j) as f64 / (n1 + j + 1) as f64)
            .product(),
        _ => {
            let paths = count_paths_outside(n1, n2, g, h);
            let all = binom((n1 + n2) as f64, n1 as f64);
            if !paths.is_finite() || !all.is_finite() || paths > all {
                return None;
            }
            paths / all
        }
    };
    (p.is_finite() && (0.0..=1.0).contains(&p)).then_some(p)
}

/// P(D_n <= d) for the one-sample KS statistic — Marsaglia, Tsang and Wang (2003),
/// exact; for a matrix too large to raise, Stephens' approximation to the limit.
fn kolmogorov_cdf(n: usize, d: f64) -> f64 {
    let nf = n as f64;
    if d <= 0.0 {
        return 0.0;
    }
    if d >= 1.0 {
        return 1.0;
    }
    let k = (nf * d) as usize + 1;
    let m = 2 * k - 1;
    if m > 600 {
        let lambda = (nf.sqrt() + 0.12 + 0.11 / nf.sqrt()) * d;
        let mut sum = 0.0;
        for j in 1..200 {
            let term = (-2.0 * (j * j) as f64 * lambda * lambda).exp();
            sum += if j % 2 == 1 { term } else { -term };
            if term < 1e-17 {
                break;
            }
        }
        return 1.0 - 2.0 * sum;
    }
    let h = k as f64 - nf * d;
    let mut hm = vec![0.0f64; m * m];
    for i in 0..m {
        for j in 0..m {
            hm[i * m + j] = if i + 1 >= j { 1.0 } else { 0.0 };
        }
    }
    for i in 0..m {
        hm[i * m] -= h.powi(i as i32 + 1);
        hm[(m - 1) * m + i] -= h.powi((m - i) as i32);
    }
    if 2.0 * h - 1.0 > 0.0 {
        hm[(m - 1) * m] += (2.0 * h - 1.0).powi(m as i32);
    }
    for i in 0..m {
        for j in 0..m {
            if i + 1 > j {
                for g in 1..=(i + 1 - j) {
                    hm[i * m + j] /= g as f64;
                }
            }
        }
    }
    // Q = H^n, keeping the scale in a decimal exponent.
    let (q, mut eq) = mat_pow(&hm, n, m);
    let mut s = q[(k - 1) * m + (k - 1)];
    for i in 1..=n {
        s = s * i as f64 / nf;
        if s < 1e-140 {
            s *= 1e140;
            eq -= 140;
        }
    }
    s * 10f64.powi(eq)
}

/// `a · b` for square matrices of side `m`, row-major.
fn mat_mul(a: &[f64], b: &[f64], m: usize) -> Vec<f64> {
    let mut c = vec![0.0f64; m * m];
    for i in 0..m {
        for l in 0..m {
            let ail = a[i * m + l];
            if ail == 0.0 {
                continue;
            }
            for j in 0..m {
                c[i * m + j] += ail * b[l * m + j];
            }
        }
    }
    c
}

/// `a^n`, scaled down by 10¹⁴⁰ whenever it grows past that, with the exponent kept.
fn mat_pow(a: &[f64], n: usize, m: usize) -> (Vec<f64>, i32) {
    if n == 1 {
        return (a.to_vec(), 0);
    }
    let (half, e) = mat_pow(a, n / 2, m);
    let mut q = mat_mul(&half, &half, m);
    let mut eq = 2 * e;
    if n % 2 == 1 {
        q = mat_mul(a, &q, m);
    }
    if q[(m / 2) * m + m / 2] > 1e140 {
        for v in q.iter_mut() {
            *v *= 1e-140;
        }
        eq += 140;
    }
    (q, eq)
}

/// Two-sample Kolmogorov–Smirnov for `x` against `y`: D and the p-value.
pub fn ks_2samp(x: &[f64], y: &[f64], alternative: Alternative, method: Method) -> Res<(f64, f64)> {
    if x.is_empty() || y.is_empty() {
        return Err("ks2() needs two samples with at least one value each".into());
    }
    let (n1, n2) = (x.len(), y.len());
    let d = ks_statistic(x, y, alternative);
    let mut method = match method {
        Method::Auto if n1.max(n2) <= 10_000 => Method::Exact,
        Method::Auto => Method::Asymptotic,
        m => m,
    };
    let mut p = f64::NAN;
    if method == Method::Exact {
        match ks_exact(n1, n2, d, alternative) {
            Some(exact) => p = exact,
            None => method = Method::Asymptotic,
        }
    }
    if method == Method::Asymptotic {
        let (m, n) = (n1.max(n2) as f64, n1.min(n2) as f64);
        let en = m * n / (m + n);
        p = match alternative {
            Alternative::TwoSided => 1.0 - kolmogorov_cdf(en.round() as usize, d),
            _ => {
                let z = en.sqrt() * d;
                (-2.0 * z * z - 2.0 * z * (m + 2.0 * n) / (m * n * (m + n)).sqrt() / 3.0).exp()
            }
        };
    }
    Ok((d, p.clamp(0.0, 1.0)))
}
