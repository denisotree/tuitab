//! Mann–Whitney U and two-sample Kolmogorov–Smirnov in `tuitab_calc`, held to
//! scipy 1.18.1 — and samples that come from a query rather than typed in.

use serde_json::{json, Value};
use std::path::PathBuf;
use tuitab::mcp::{handle_message, Server};

const AB: (&str, &str) = (
    "[1.1, 2.3, 3.0, 4.8, 5.5]",
    "[2.0, 6.1, 7.4, 8.2, 9.9, 10.5]",
);
const TIES: (&str, &str) = ("[1, 2, 2, 3, 3, 3, 4]", "[2, 3, 4, 4, 5, 5, 6, 7]");

fn one_vs_60() -> (String, String) {
    let base: Vec<String> = (0..60).map(|i| i.to_string()).collect();
    ("[100]".to_string(), format!("[{}]", base.join(", ")))
}

fn calc(server: &mut Server, args: Value) -> Value {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                         "params": {"name": "tuitab_calc", "arguments": args}});
    handle_message(server, &message.to_string()).unwrap()["result"].clone()
}

fn value(expr: &str) -> f64 {
    let out = calc(&mut Server::new(), json!({"expr": expr}));
    assert_eq!(out["isError"], json!(false), "{}: {}", expr, out);
    out["structuredContent"]["result"]["value"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap_or_else(|_| panic!("{} = {}", expr, out))
}

fn close(expr: &str, want: f64) {
    let got = value(expr);
    assert!(
        (got - want).abs() <= 1e-9 * want.abs().max(1e-3),
        "{} = {}, scipy says {}",
        expr,
        got,
        want
    );
}

#[test]
fn mannwhitney_matches_scipy() {
    let (ov_a, ov_b) = one_vs_60();
    let cases: Vec<(&str, &str, &str, [f64; 3])> = vec![
        // a, b, alternative, [auto, exact, asymptotic]
        (
            AB.0,
            AB.1,
            "two_sided",
            [0.0519480519481, 0.0519480519481, 0.0552342537181],
        ),
        (
            AB.0,
            AB.1,
            "less",
            [0.025974025974, 0.025974025974, 0.027617126859],
        ),
        (
            AB.0,
            AB.1,
            "greater",
            [0.984848484848, 0.984848484848, 0.982118116705],
        ),
        (
            TIES.0,
            TIES.1,
            "two_sided",
            [0.0252797368859, 0.0289044289044, 0.0252797368859],
        ),
        (
            TIES.0,
            TIES.1,
            "less",
            [0.0126398684429, 0.0144522144522, 0.0126398684429],
        ),
        (
            TIES.0,
            TIES.1,
            "greater",
            [0.990734738504, 0.993006993007, 0.990734738504],
        ),
        (
            &ov_a,
            &ov_b,
            "two_sided",
            [0.0327868852459, 0.0327868852459, 0.0938386390341],
        ),
        (
            &ov_a,
            &ov_b,
            "greater",
            [0.016393442623, 0.016393442623, 0.0469193195171],
        ),
    ];
    for (a, b, alt, want) in cases {
        for (method, p) in ["auto", "exact", "asymptotic"].iter().zip(want) {
            close(
                &format!("mannwhitney({}, {}, {}, {})", a, b, alt, method),
                p,
            );
        }
        close(&format!("mannwhitney({}, {}, {})", a, b, alt), want[0]);
    }
    close(&format!("mannwhitney_u({}, {})", AB.0, AB.1), 4.0);
    close(&format!("mannwhitney_u({}, {})", TIES.0, TIES.1), 8.5);
    close(&format!("mannwhitney_u({}, {})", ov_a, ov_b), 60.0);
}

#[test]
fn ks2_matches_scipy() {
    let (ov_a, ov_b) = one_vs_60();
    let cases: Vec<(&str, &str, &str, [f64; 3], f64)> = vec![
        // a, b, alternative, [auto, exact, asymp], D
        (
            AB.0,
            AB.1,
            "two_sided",
            [0.025974025974, 0.025974025974, 0.00925925925926],
            0.833333333333,
        ),
        (
            AB.0,
            AB.1,
            "greater",
            [0.012987012987, 0.012987012987, 0.0100925313804],
            0.833333333333,
        ),
        (
            TIES.0,
            TIES.1,
            "two_sided",
            [0.0870240870241, 0.0870240870241, 0.0618004216993],
            0.607142857143,
        ),
        (
            TIES.0,
            TIES.1,
            "greater",
            [0.043512043512, 0.043512043512, 0.0352238543313],
            0.607142857143,
        ),
        (
            &ov_a,
            &ov_b,
            "less",
            [0.016393442623, 0.016393442623, 0.0710189352973],
            1.0,
        ),
    ];
    for (a, b, alt, want, d) in cases {
        for (method, p) in ["auto", "exact", "asymp"].iter().zip(want) {
            close(&format!("ks2({}, {}, {}, {})", a, b, alt, method), p);
        }
        close(&format!("ks2_d({}, {}, {})", a, b, alt), d);
    }
}

#[test]
fn the_tests_refuse_what_they_cannot_read() {
    for expr in [
        "mannwhitney([1, 2], [3, 4], sideways)",
        "mannwhitney([1, 2], [3, 4], less, maybe)",
        "mannwhitney(1, [3, 4], less)",
        "mannwhitney([], [3, 4], less)",
        "ks2([1, 2], [3, 4], two_sided, asymptotic)",
    ] {
        let out = calc(&mut Server::new(), json!({"expr": expr}));
        assert_eq!(
            out["isError"],
            json!(true),
            "{} should be refused: {}",
            expr,
            out
        );
    }
}

fn csv(name: &str, text: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("rank-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

#[test]
fn lists_bind_a_query_column() {
    let path = csv("cpu.csv", "day,v\nmon,1\nmon,2\ntue,10\ntue,11\ntue,12\n");
    let out = calc(
        &mut Server::new(),
        json!({
            "lists": {
                "mon": {"source": path, "ops": [{"filter": {"col": "day", "op": "eq", "value": "mon"}}], "col": "v"},
                "tue": {"source": path, "ops": [{"filter": {"col": "day", "op": "eq", "value": "tue"}}], "col": "v"}
            },
            "steps": [
                {"name": "m", "expr": "mean(tue)"},
                {"expr": "mannwhitney(tue, mon, greater, exact)"}
            ]
        }),
    );
    assert_eq!(out["isError"], json!(false), "{}", out);
    let steps = &out["structuredContent"]["steps"];
    assert_eq!(steps[0]["value"], json!("11"), "{}", out);
    // 3 against 2, every tue above every mon: 1 / C(5, 2).
    let p: f64 = steps[1]["value"].as_str().unwrap().parse().unwrap();
    assert!((p - 0.1).abs() < 1e-12, "{}", p);
}

#[test]
fn a_list_name_that_shadows_a_function_is_refused() {
    let path = csv("shadow.csv", "v\n1\n");
    let out = calc(
        &mut Server::new(),
        json!({"lists": {"mean": {"source": path, "col": "v"}}, "expr": "1"}),
    );
    assert_eq!(out["isError"], json!(true), "{}", out);
}

#[test]
fn a_text_column_in_lists_is_refused() {
    let path = csv("text.csv", "v\na\nb\n");
    let out = calc(
        &mut Server::new(),
        json!({"lists": {"t": {"source": path, "col": "v"}}, "expr": "mean(t)"}),
    );
    assert_eq!(out["isError"], json!(true), "{}", out);
    assert!(
        out["content"][0]["text"].as_str().unwrap().contains("'v'"),
        "{}",
        out
    );
}

/// Samples large enough for the general lattice counts, and for the asymptotic
/// distribution of a thousand-point sample.
#[test]
fn larger_samples_match_scipy() {
    let list = |v: Vec<f64>| {
        format!(
            "[{}]",
            v.iter()
                .map(|x| format!("{:?}", x))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let a = list(
        (0..30)
            .map(|i| ((i * 7) % 31) as f64 + i as f64 / 10.0)
            .collect(),
    );
    let b = list(
        (0..45)
            .map(|i| ((i * 11) % 37) as f64 + i as f64 / 7.0 + 3.0)
            .collect(),
    );
    for (alt, auto, exact, asymp) in [
        (
            "two_sided",
            0.08625328340190708,
            0.08625328340190708,
            0.07987691239623707,
        ),
        (
            "greater",
            0.043128499633024786,
            0.043128499633024786,
            0.03785190106492146,
        ),
    ] {
        close(&format!("ks2({}, {}, {}, auto)", a, b, alt), auto);
        close(&format!("ks2({}, {}, {}, exact)", a, b, alt), exact);
        close(&format!("ks2({}, {}, {}, asymp)", a, b, alt), asymp);
    }
    for (alt, auto, exact) in [
        ("two_sided", 0.005537400984572714, 0.005055582324274859),
        ("less", 0.002768700492286357, 0.0025277911621374295),
        ("greater", 0.9973219713533094, 0.9975595842228226),
    ] {
        close(&format!("mannwhitney({}, {}, {}, auto)", a, b, alt), auto);
        close(&format!("mannwhitney({}, {}, {}, exact)", a, b, alt), exact);
    }

    // Three thousand points each: too long to type, so through `lists`.
    let mut text = String::from("a,b\n");
    for i in 0..3000 {
        text.push_str(&format!(
            "{:?},{:?}\n",
            i as f64 / 3000.0,
            i as f64 / 3000.0 + 0.06
        ));
    }
    let path = csv("big.csv", &text);
    let out = calc(
        &mut Server::new(),
        json!({
            "lists": {"a": {"source": path, "col": "a"}, "b": {"source": path, "col": "b"}},
            "steps": [
                {"expr": "ks2(a, b, two_sided, asymp)"},
                {"expr": "ks2(a, b, two_sided, exact)"}
            ]
        }),
    );
    assert_eq!(out["isError"], json!(false), "{}", out);
    let p = |i: usize| -> f64 {
        out["structuredContent"]["steps"][i]["value"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap()
    };
    for (got, want) in [
        (p(0), 3.4495502263606845e-05),
        (p(1), 3.599977572391281e-05),
    ] {
        assert!(
            (got - want).abs() <= 1e-9 * want.abs().max(1e-3) + 1e-12,
            "{} vs {}",
            got,
            want
        );
    }
}

fn steps(out: &Value) -> Vec<Value> {
    out["structuredContent"]["steps"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// Large samples answer in moments: the exact distribution is used while it can be
/// computed, and `auto` turns to the asymptotic one where it cannot.
#[test]
fn large_samples_answer_quickly() {
    let column = |name: &str, values: Vec<f64>| {
        let mut text = String::from("v\n");
        for v in values {
            text.push_str(&format!("{:?}\n", v));
        }
        csv(name, &text)
    };
    let few = column(
        "few.csv",
        (0..5).map(|i| i as f64 * 40_000.0 + 0.5).collect(),
    );
    let many = column("many.csv", (0..200_000).map(|i| i as f64).collect());
    let a = column("a3000.csv", (0..3000).map(|i| i as f64 / 3000.0).collect());
    let b = column(
        "b4000.csv",
        (0..4000).map(|i| i as f64 / 4000.0 + 0.01).collect(),
    );
    let started = std::time::Instant::now();
    let out = calc(
        &mut Server::new(),
        json!({
            "lists": {"few": {"source": few, "col": "v"}, "many": {"source": many, "col": "v"},
                      "a": {"source": a, "col": "v"}, "b": {"source": b, "col": "v"}},
            "steps": [
                {"expr": "mannwhitney(few, many, greater, exact)"},
                {"expr": "mannwhitney(few, many, greater, asymptotic)"},
                {"expr": "mannwhitney(few, many, greater)"},
                {"expr": "ks2(a, b, greater, auto)"},
                {"expr": "ks2(a, b, greater, asymp)"},
                {"expr": "mannwhitney(a, b, less, asymptotic)"}
            ]
        }),
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "took {:?}",
        started.elapsed()
    );
    assert_eq!(out["isError"], json!(false), "{}", out);
    let p: Vec<f64> = steps(&out)
        .iter()
        .map(|s| s["value"].as_str().unwrap().parse().unwrap())
        .collect();
    // Five against many: U / n2 tends to the sum of five uniforms (Irwin–Hall), and
    // U here is 2·n2, so P(U >= u) -> 1 − 27/120 = 0.775.  The normal approximation
    // is off in the third digit.
    assert!((p[0] - 0.775).abs() < 1e-4, "exact {}", p[0]);
    assert!((p[1] - 0.775).abs() > 1e-3, "asymptotic {}", p[1]);
    assert_eq!(p[0], p[2], "auto is exact for five against many");
    assert_eq!(
        p[3], p[4],
        "ks2 auto falls back to asymp past what floats hold"
    );

    // Exact over two large samples is refused rather than left to run for hours.
    let started = std::time::Instant::now();
    let out = calc(
        &mut Server::new(),
        json!({"lists": {"a": {"source": a, "col": "v"}, "b": {"source": b, "col": "v"}},
               "expr": "mannwhitney(a, b, less, exact)"}),
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    assert_eq!(out["isError"], json!(true), "{}", out);
    assert!(
        out["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("asymptotic"),
        "{}",
        out
    );
}

#[test]
fn mannwhitney_over_samples_all_equal_says_so() {
    let out = calc(
        &mut Server::new(),
        json!({"expr": "mannwhitney([1, 1, 1], [1, 1], two_sided, asymptotic)"}),
    );
    assert_eq!(out["isError"], json!(true), "{}", out);
    assert!(
        out["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("every value is the same"),
        "{}",
        out
    );
}

/// The two-sided asymptotic KS over large samples is a number, not a minute of matrix
/// powers: forty thousand against forty thousand, p as scipy's `ks_2samp` gives it.
#[test]
fn two_sided_ks_over_large_samples_answers_quickly() {
    let column = |name: &str, shift: f64| {
        let mut text = String::from("v\n");
        for i in 0..40_000 {
            text.push_str(&format!("{:?}\n", i as f64 / 40_000.0 + shift));
        }
        csv(name, &text)
    };
    let (x, y) = (column("ks-x40k.csv", 0.0), column("ks-y40k.csv", 0.0147));
    let started = std::time::Instant::now();
    let out = calc(
        &mut Server::new(),
        json!({
            "lists": {"x": {"source": x, "col": "v"}, "y": {"source": y, "col": "v"}},
            "expr": "ks2(x, y, two_sided, auto)"
        }),
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "took {:?}",
        started.elapsed()
    );
    let p: f64 = steps(&out)[0]["value"].as_str().unwrap().parse().unwrap();
    let scipy = 0.000_338_828_432_188_476_74;
    assert!((p - scipy).abs() / scipy < 1e-9, "p = {}", p);
}

/// `less` is `greater` with the samples swapped, deep in the tail too — not a
/// difference from one that cancels to zero.
#[test]
fn mannwhitney_less_keeps_its_tail() {
    let a: Vec<String> = (1..=100).map(|i| i.to_string()).collect();
    let b: Vec<String> = (1001..=1100).map(|i| i.to_string()).collect();
    let (a, b) = (format!("[{}]", a.join(", ")), format!("[{}]", b.join(", ")));
    let less = value(&format!("mannwhitney({}, {}, less, asymptotic)", a, b));
    let greater = value(&format!("mannwhitney({}, {}, greater, asymptotic)", b, a));
    assert!(less > 0.0, "less = {}", less);
    assert!(
        (less - greater).abs() <= 1e-9 * greater,
        "less {} greater {}",
        less,
        greater
    );
}

/// An exact KS p-value out of reach is refused, as `mannwhitney` refuses one, rather
/// than answered asymptotically under the name exact — or computed for minutes.
#[test]
fn ks2_refuses_an_exact_p_it_cannot_give() {
    let list = |n: usize, shift: f64| -> String {
        let v: Vec<String> = (0..n)
            .map(|i| format!("{}", i as f64 * 0.5 + shift))
            .collect();
        format!("[{}]", v.join(", "))
    };
    // One-sided: the count of paths overflows a float.
    let out = calc(
        &mut Server::new(),
        json!({"expr": format!("ks2({}, {}, less, exact)", list(600, 5.0), list(601, 0.0))}),
    );
    assert_eq!(out["isError"], json!(true), "{}", &out.to_string()[..200]);
    // Two-sided, sizes whose lattice is past what scipy computes exactly.
    let started = std::time::Instant::now();
    let out = calc(
        &mut Server::new(),
        json!({"expr": format!("ks2({}, {}, two_sided, exact)", list(50000, 0.0), list(49999, 0.1))}),
    );
    assert_eq!(out["isError"], json!(true), "{}", &out.to_string()[..200]);
    assert!(started.elapsed().as_secs() < 10);
    // Left to choose, the same samples are answered.
    let out = calc(
        &mut Server::new(),
        json!({"expr": format!("ks2({}, {}, less)", list(600, 5.0), list(601, 0.0))}),
    );
    assert_eq!(out["isError"], json!(false), "{}", &out.to_string()[..200]);
}

/// A NaN or an infinity in a list's column is no number for a statistic: it is left
/// out, as a missing value is.
#[test]
fn a_list_leaves_out_values_that_are_not_finite() {
    use polars::prelude::*;
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("rank-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("not-finite.parquet");
    let mut df = DataFrame::new_infer_height(vec![Series::new(
        "v".into(),
        [1.0, f64::NAN, 3.0, f64::INFINITY],
    )
    .into()])
    .unwrap();
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let out = calc(
        &mut Server::new(),
        json!({"lists": {"a": {"source": path, "col": "v"}}, "expr": "mean(a)"}),
    );
    assert_eq!(out["isError"], json!(false), "{}", out);
    assert_eq!(
        out["structuredContent"]["result"]["value"],
        json!("2"),
        "{}",
        out
    );
}

/// A list reads its own column: a table too wide to collect whole still gives one.
#[test]
fn a_list_collects_only_its_column() {
    let header: Vec<String> = (0..30).map(|c| format!("c{}", c)).collect();
    let mut text = header.join(",") + "\n";
    for r in 0..400 {
        let row: Vec<String> = (0..30).map(|c| format!("value {} {}", r, c)).collect();
        text.push_str(&row.join(","));
        text.push('\n');
    }
    text = text.replacen("c0,", "n,", 1);
    let text: String = text
        .lines()
        .enumerate()
        .map(|(i, l)| {
            if i == 0 {
                l.to_string()
            } else {
                l.replacen(&format!("value {} 0", i - 1), &format!("{}", i), 1)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let path = csv("wide.csv", &text);
    let mut tight = Server {
        memory_budget: Some(16384),
        ..Server::new()
    };
    let out = calc(
        &mut tight,
        json!({"lists": {"a": {"source": path, "col": "n"}}, "expr": "mean(a)"}),
    );
    assert_eq!(out["isError"], json!(false), "{}", out);
}
