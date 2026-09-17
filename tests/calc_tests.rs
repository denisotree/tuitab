//! `tuitab_calc`: exact decimals, honest precision labels, named steps.

use serde_json::{json, Value};
use tuitab::mcp::calc::run;

/// The value and precision of a single expression.
fn calc(expr: &str) -> (String, String) {
    let out = run(&json!({ "expr": expr })).unwrap_or_else(|e| panic!("{expr}: {e}"));
    let result = &out["result"];
    (
        result["value"].as_str().unwrap().to_string(),
        result["precision"].as_str().unwrap().to_string(),
    )
}

fn assert_calc(expr: &str, value: &str, precision: &str) {
    assert_eq!(
        calc(expr),
        (value.to_string(), precision.to_string()),
        "{expr}"
    );
}

fn error(expr: &str) -> String {
    match run(&json!({ "expr": expr })) {
        Ok(v) => panic!("{expr} should fail, got {v}"),
        Err(e) => e,
    }
}

#[test]
fn decimal_arithmetic_has_no_binary_rounding() {
    assert_calc("0.1 + 0.2", "0.3", "exact");
    assert_calc("1 - 0.9", "0.1", "exact");
    assert_calc("3 * 0.1", "0.3", "exact");
    assert_calc("1 / 4", "0.25", "exact");
    assert_calc("7.5 % 2", "1.5", "exact");
    assert_calc("1.07^10", "1.96715135728956532249", "exact");
}

#[test]
fn lost_digits_are_labelled() {
    assert_calc("1/3", "0.3333333333333333333333333333", "rounded");
    // Its product with the divisor rounds back to 989 — still not exact.
    assert_calc("989 / 365.25", "2.707734428473648186173853525", "rounded");
    assert_calc("10 / 4", "2.5", "exact");
    assert_calc("1 / 8", "0.125", "exact");
    assert_calc("sqrt(2)", "1.4142135623730950488016887242", "rounded");
    assert_calc("sqrt(2.25)", "1.5", "exact");
    assert_calc("sin(pi / 6)", "0.5", "approximate");
    assert_calc("2^0.5", "1.4142135623731", "approximate");
    // Past the decimal's range the float takes over and says so.
    assert_calc("2^100", "1.26765060022823e30", "approximate");
    assert_calc("0.5^100", "7.88860904867761e-31", "approximate");
    // A rounded input keeps its label through later exact steps.
    assert_calc("round(1/3, 2)", "0.33", "rounded");
}

#[test]
fn operators_bind_as_on_paper() {
    assert_calc("-2^2", "-4", "exact");
    assert_calc("2^3^2", "512", "exact");
    assert_calc("2 ** 10", "1024", "exact");
    assert_calc("2 + 3 * 4", "14", "exact");
    assert_calc("if(3 > 2 and not false, 1, 1/0)", "1", "exact");
}

#[test]
fn math_functions() {
    assert_calc("round(2.5)", "3", "exact");
    assert_calc("round(-2.5)", "-3", "exact");
    assert_calc("round(1234.5678, 2)", "1234.57", "exact");
    assert_calc("round(1234.5678, -2)", "1200", "exact");
    assert_calc("factorial(20)", "2432902008176640000", "exact");
    assert_calc("gcd(12, 18, 24)", "6", "exact");
    assert_calc("lcm(4, 6)", "12", "exact");
    assert_calc("log(8, 2)", "3", "approximate");
    assert_calc("min(3, [1, 5])", "1", "exact");
}

#[test]
fn statistics_over_lists() {
    assert_calc("mean([1, 2, 3, 4])", "2.5", "exact");
    assert_calc("median(3, 1, 2, 10)", "2.5", "exact");
    assert_calc("stdev_p([2, 4, 4, 4, 5, 5, 7, 9])", "2", "exact");
    assert_calc(
        "stdev([2, 4, 4, 4, 5, 5, 7, 9])",
        "2.138089935299395077476427847",
        "rounded",
    );
    assert_calc("percentile([1, 2, 3, 4, 5], 0.9)", "4.6", "exact");
    assert_calc("count([1, 2], 3)", "3", "exact");
    // Reference values from Python's statistics.NormalDist.
    assert_calc("norm_cdf(1.96)", "0.97500210485178", "approximate");
    assert_calc("norm_cdf(-8)", "6.22096057427182e-16", "approximate");
    assert_calc("norm_inv(0.975)", "1.95996398454005", "approximate");
    assert_calc("norm_inv(1 - 0.05 / 6)", "2.39397979981851", "approximate");
    assert_calc("erf(1)", "0.842700792949715", "approximate");
    // A two-sided p-value of a two-proportion z-test.
    assert_calc(
        "2 * norm_cdf(-abs((0.1085427135678391959798994975 - 0.1) / sqrt(0.1042606516290726817042606516 * (1 - 0.1042606516290726817042606516) * (1/10000 + 1/9950))))",
        "0.0483622384150315",
        "approximate",
    );
}

/// Checked against a spreadsheet's PMT, FV, PV, NPV and IRR.
#[test]
fn finance_matches_spreadsheet_functions() {
    let starts = |expr: &str, prefix: &str| {
        let (value, _) = calc(expr);
        assert!(
            value.starts_with(prefix),
            "{expr} = {value}, expected {prefix}…"
        );
    };
    starts("pmt(0.01, 360, 250000)", "-2571.53149231376");
    assert_calc("pmt(0, 12, 1200)", "-100", "exact");
    starts("fv(0.05/12, 120, -100)", "15528.2279445667");
    starts("pv(0.08/12, 240, -500)", "59777.1458511880");
    starts("npv(0.1, -10000, 3000, 4200, 6800)", "1188.44341233522");
    starts("irr([-10000, 3000, 4200, 6800])", "0.1634056");
}

/// Lists of adjusted p-values, in the order given.  Worked by hand from the sorted
/// p-values 0.00096, 0.03, 0.0484, 0.1861 (m = 4):
/// Holm 4p, 3p, 2p, 1p with a running max; BH 4p/1, 4p/2, 4p/3, 4p/4 with a running min.
fn list(expr: &str) -> (Vec<String>, String) {
    let out = run(&json!({ "expr": expr })).unwrap_or_else(|e| panic!("{expr}: {e}"));
    let result = &out["result"];
    let values = result["value"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    (values, result["precision"].as_str().unwrap().to_string())
}

#[test]
fn multiple_comparison_corrections() {
    let ps = "[0.0484, 0.00096, 0.1861, 0.03]";
    assert_eq!(
        list(&format!("holm({ps})")),
        (
            vec![
                "0.0968".into(),
                "0.00384".into(),
                "0.1861".into(),
                "0.09".into()
            ],
            "exact".to_string()
        )
    );
    assert_eq!(
        list(&format!("bh({ps})")),
        (
            vec![
                "0.0645333333333333333333333333".into(),
                "0.00384".into(),
                "0.1861".into(),
                "0.06".into()
            ],
            "rounded".to_string()
        )
    );
    // Capped at 1, and a single p-value is its own adjustment.
    assert_eq!(list("holm(0.6, 0.7)").0, vec!["1", "1"]);
    assert_eq!(list("bh([0.2])").0, vec!["0.2"]);
    assert!(error("holm([0.5, 1.2])").contains("from 0 to 1"));
}

#[test]
fn steps_use_earlier_steps_by_name() {
    let out = run(&json!({"steps": [
        {"name": "rate", "expr": "0.12 / 12"},
        {"name": "payment", "expr": "pmt(rate, 360, 250000)"},
        {"expr": "round(payment, 2)"}
    ]}))
    .unwrap();

    let steps = out["steps"].as_array().unwrap();
    assert_eq!(steps[0]["name"], "rate");
    assert_eq!(steps[0]["value"], "0.01");
    assert_eq!(out["result"]["value"], "-2571.53");
    assert_eq!(out["result"], steps[2]);
}

#[test]
fn mistakes_are_named() {
    assert!(error("1/0").contains("division by zero"));
    assert!(error("sqrt(-1)").contains("negative"));
    assert!(error("2 = 2").contains("=="));
    assert!(error("sum(").contains("ends too early"));
    assert!(error("sin").contains("sin(…)"));
    assert!(error("nope(1)").contains("unknown function"));
    assert!(error("factorial(200)").contains("not a finite number"));

    let unknown = run(&json!({"steps": [
        {"name": "a", "expr": "1"},
        {"name": "b", "expr": "a + c"}
    ]}))
    .unwrap_err();
    assert!(
        unknown.starts_with("b (a + c)") && unknown.contains("earlier steps: a"),
        "{unknown}"
    );

    let reserved = run(&json!({"steps": [{"name": "sum", "expr": "1"}]})).unwrap_err();
    assert!(reserved.contains("built-in"), "{reserved}");
}

#[test]
fn the_server_runs_it() {
    let mut server = tuitab::mcp::Server::default();
    let call = |server: &mut tuitab::mcp::Server, message: Value| -> Value {
        tuitab::mcp::handle_message(server, &message.to_string()).unwrap()
    };

    let response = call(
        &mut server,
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
               "params": {"name": "tuitab_calc", "arguments": {"expr": "0.1 + 0.2"}}}),
    );
    assert_eq!(response["result"]["isError"], false);
    assert_eq!(
        response["result"]["structuredContent"]["result"]["value"],
        "0.3"
    );
}

// ── distributions, held to statrs ───────────────────────────────────────────

fn approx(expr: &str) -> f64 {
    let (value, precision) = calc(expr);
    assert_eq!(precision, "approximate", "{expr}");
    value.parse().unwrap_or_else(|_| panic!("{expr} = {value}"))
}

/// Agreement to 1e-12 absolutely, or to 1e-9 relatively for the small tail values
/// where an absolute bound says nothing.
fn close(expr: &str, reference: f64) {
    let got = approx(expr);
    let ok = (got - reference).abs() <= 1e-12 || (got - reference).abs() <= 1e-9 * reference.abs();
    assert!(ok, "{expr} = {got}, statrs says {reference}");
}

#[test]
fn distributions_match_statrs() {
    use statrs::distribution::{
        Binomial, ChiSquared, ContinuousCDF, DiscreteCDF, Poisson, StudentsT,
    };

    for df in [1.0, 2.0, 3.5, 10.0, 30.0, 1000.0] {
        let t = StudentsT::new(0.0, 1.0, df).unwrap();
        for x in [-40.0, -3.0, -0.5, 0.0, 0.7, 2.0, 12.0] {
            close(&format!("t_cdf({x}, {df})"), t.cdf(x));
        }
        // Quantiles are checked through statrs' CDF: its own inverse loses digits in
        // the far tails (df = 1, p = 1e-8 is off by 9%), its CDF does not.
        for p in [1e-8, 0.001, 0.025, 0.3, 0.9, 0.975, 0.9999] {
            let x = approx(&format!("t_inv({p}, {df})"));
            let back = t.cdf(x);
            assert!(
                (back - p).abs() <= 1e-9 * p.min(1.0 - p),
                "t_inv({p}, {df}) = {x}, cdf back {back}"
            );
        }

        let chi = ChiSquared::new(df).unwrap();
        for x in [0.001, 0.5, 1.0, 4.0, 11.0, 50.0, 2000.0] {
            close(&format!("chi2_cdf({x}, {df})"), chi.cdf(x));
        }
        for p in [1e-8, 0.001, 0.05, 0.5, 0.95, 0.999] {
            let x = approx(&format!("chi2_inv({p}, {df})"));
            let back = chi.cdf(x);
            assert!(
                (back - p).abs() <= 1e-9 * p.min(1.0 - p),
                "chi2_inv({p}, {df}) = {x}, cdf back {back}"
            );
        }
    }

    for lambda in [0.1, 1.0, 2.5, 30.0, 1000.0] {
        let poisson = Poisson::new(lambda).unwrap();
        for k in [0u64, 1, 3, 10, 40, 1200] {
            close(&format!("poisson_cdf({k}, {lambda})"), poisson.cdf(k));
        }
    }

    for (n, p) in [(1u64, 0.5), (10, 0.025), (10, 0.5), (200, 0.1), (5000, 0.9)] {
        let binomial = Binomial::new(p, n).unwrap();
        for k in [0u64, 1, 5, 20, 150, 4490] {
            if k <= n {
                close(&format!("binom_cdf({k}, {n}, {p})"), binomial.cdf(k));
            }
        }
    }
}

#[test]
fn distribution_edges_and_mistakes() {
    // The chi-square p-value of the ABC test: df = 2 has the closed form exp(-x/2).
    close(
        "1 - chi2_cdf(10.979359881795745, 2)",
        (-10.979359881795745f64 / 2.0).exp(),
    );
    assert_eq!(approx("t_inv(0.5, 7)"), 0.0);
    // Closed forms: t with df = 1 is Cauchy, t with df = 2 and chi-square with df = 2
    // have elementary quantiles.
    let p = 1e-8_f64;
    close(
        "t_inv(0.00000001, 1)",
        // -cot(πp), not tan(π(p - 0.5)): that one loses digits beside π/2.
        -1.0 / (std::f64::consts::PI * p).tan(),
    );
    close(
        "t_inv(0.00000001, 2)",
        (2.0 * p - 1.0) / (2.0 * p * (1.0 - p)).sqrt(),
    );
    close("chi2_inv(0.95, 2)", -2.0 * (0.05f64).ln());
    assert_eq!(approx("poisson_cdf(-1, 3)"), 0.0);
    assert_eq!(approx("poisson_cdf(4, 0)"), 1.0);
    assert_eq!(approx("binom_cdf(10, 10, 0.3)"), 1.0);
    assert_eq!(approx("binom_cdf(3, 10, 1)"), 0.0);

    assert!(error("t_cdf(1, 0)").contains("degrees of freedom"));
    assert!(error("chi2_inv(1, 3)").contains("strictly between 0 and 1"));
    assert!(error("binom_cdf(2.5, 10, 0.5)").contains("whole number"));
    assert!(error("binom_cdf(2, 10, 1.5)").contains("from 0 to 1"));
    assert!(error("poisson_cdf(2, -1)").contains("mean"));
}
