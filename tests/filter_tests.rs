//! Row selection shared by the TUI's typed expressions and the MCP server's
//! structured predicates.

use std::path::{Path, PathBuf};
use tuitab::data::dataframe::DataFrame;
use tuitab::data::expression::{Expr, Value};
use tuitab::data::filter::{
    matching_rows, select_rows, Clause, Fallback, Operand, PredOp, Predicate,
};
use tuitab::data::io::load_file;

fn sample() -> DataFrame {
    load_file(Path::new("test_data/sample.csv"), None).unwrap()
}

fn eq(col: &str, value: &str) -> Predicate {
    Predicate {
        col: col.to_string(),
        op: PredOp::Eq,
        value: Operand::Literal(Value::String(value.to_string())),
    }
}

/// The reason this module exists: the old implementation fell back to the
/// per-row interpreter whenever the Polars result was *empty*, not when Polars
/// *failed*. A predicate that legitimately matched nothing was silently re-run
/// through a different evaluator with different semantics.
///
/// "No rows matched" is an answer.
#[test]
fn a_predicate_matching_nothing_returns_nothing() {
    let df = sample();
    let clauses = vec![Clause::One(eq("department", "Legal"))];
    assert_eq!(matching_rows(&df, &clauses).unwrap(), Vec::<usize>::new());
}

#[test]
fn an_expression_matching_nothing_returns_nothing_on_either_path() {
    let df = sample();
    let expr = Expr::parse(r#"department == "Legal""#).unwrap();

    assert_eq!(
        select_rows(&df, &expr, Fallback::Allowed).unwrap(),
        Vec::<usize>::new(),
        "the interpreter must not be consulted just because Polars found none"
    );
    assert_eq!(
        select_rows(&df, &expr, Fallback::Forbidden).unwrap(),
        Vec::<usize>::new()
    );
}

// ── OR ──────────────────────────────────────────────────────────────────────

#[test]
fn any_of_joins_predicates_with_or() {
    let df = sample();
    let clauses = vec![Clause::AnyOf(vec![
        eq("department", "HR"),
        eq("department", "Marketing"),
    ])];
    assert_eq!(
        matching_rows(&df, &clauses).unwrap().len(),
        8,
        "4 HR + 4 Marketing"
    );
}

#[test]
fn a_group_and_a_predicate_read_as_a_or_b_and_c() {
    let df = sample();
    let clauses = vec![
        Clause::AnyOf(vec![eq("department", "HR"), eq("department", "Marketing")]),
        Clause::One(Predicate {
            col: "age".to_string(),
            op: PredOp::Gt,
            value: Operand::Literal(Value::Number(40.0)),
        }),
    ];
    // Of the eight in HR or Marketing, only Noah Martin (44) is over 40.
    assert_eq!(matching_rows(&df, &clauses).unwrap().len(), 1);
}

/// `contains` inside a group is what forced the whole rewrite: it used to be
/// computed eagerly and separately, which left nothing to OR it with.
#[test]
fn contains_works_inside_an_or_group() {
    let df = sample();
    let clauses = vec![Clause::AnyOf(vec![
        Predicate {
            col: "name".to_string(),
            op: PredOp::Contains,
            value: Operand::Literal(Value::String("^Alice".to_string())),
        },
        eq("department", "HR"),
    ])];
    // Alice Johnson plus the four in HR.
    assert_eq!(matching_rows(&df, &clauses).unwrap().len(), 5);
}

#[test]
fn an_empty_any_of_matches_nothing() {
    let df = sample();
    let clauses = vec![Clause::AnyOf(vec![])];
    assert!(matching_rows(&df, &clauses).unwrap().is_empty());
}

// ── column against column ───────────────────────────────────────────────────

#[test]
fn a_predicate_can_compare_two_columns() {
    let df = sample();
    // Everyone's salary exceeds their age, and nobody's age exceeds their
    // salary — a comparison that needs both sides to be columns.
    let above = vec![Clause::One(Predicate {
        col: "salary".to_string(),
        op: PredOp::Gt,
        value: Operand::Column("age".to_string()),
    })];
    assert_eq!(matching_rows(&df, &above).unwrap().len(), 20);

    let below = vec![Clause::One(Predicate {
        col: "age".to_string(),
        op: PredOp::Gt,
        value: Operand::Column("salary".to_string()),
    })];
    assert!(matching_rows(&df, &below).unwrap().is_empty());
}

#[test]
fn an_unknown_column_is_named_along_with_the_ones_that_exist() {
    let df = sample();
    let clauses = vec![Clause::One(eq("dept", "HR"))];
    let message = matching_rows(&df, &clauses).unwrap_err();
    assert!(message.contains("dept"), "{}", message);
    assert!(message.contains("department"), "{}", message);

    // The other side of a column-vs-column comparison is checked too.
    let clauses = vec![Clause::One(Predicate {
        col: "salary".to_string(),
        op: PredOp::Gt,
        value: Operand::Column("bonus".to_string()),
    })];
    assert!(matching_rows(&df, &clauses).unwrap_err().contains("bonus"));
}

// ── the two surfaces agree ──────────────────────────────────────────────────

/// A structured predicate and the equivalent typed expression must select the
/// same rows — that is the whole claim of one filter language on two surfaces.
#[test]
fn structured_predicates_and_typed_expressions_agree() {
    let df = sample();

    let structured = matching_rows(
        &df,
        &[
            Clause::AnyOf(vec![eq("department", "HR"), eq("department", "Marketing")]),
            Clause::One(Predicate {
                col: "age".to_string(),
                op: PredOp::Gt,
                value: Operand::Literal(Value::Number(30.0)),
            }),
        ],
    )
    .unwrap();

    let typed =
        Expr::parse(r#"(department == "HR" or department == "Marketing") and age > 30"#).unwrap();
    let display = select_rows(&df, &typed, Fallback::Allowed).unwrap();
    let physical: Vec<usize> = display.into_iter().map(|i| df.row_order[i]).collect();

    assert_eq!(structured, physical);
    assert!(!structured.is_empty(), "a vacuous agreement proves nothing");
}

// ── the mask must describe every row ────────────────────────────────────────

/// A filter has to produce one verdict per row. A whole-frame aggregate
/// produces one verdict for the table, which applies to every row of it —
/// SQL's `WHERE (SELECT AVG(age) FROM t) > 30` keeps all rows or none.
///
/// Enumerating that single verdict as though it were the mask reported "1 row
/// matched", which is neither reading.
#[test]
fn a_whole_frame_condition_applies_to_every_row() {
    let df = sample();

    // The mean age is 36.65.
    let holds = Expr::parse("mean(age) > 30").unwrap();
    assert_eq!(
        select_rows(&df, &holds, Fallback::Forbidden).unwrap().len(),
        20,
        "a condition true of the table is true of all its rows"
    );

    let fails = Expr::parse("mean(age) > 100").unwrap();
    assert!(
        select_rows(&df, &fails, Fallback::Forbidden)
            .unwrap()
            .is_empty(),
        "and one that is false of the table keeps none"
    );
}

/// The empty-group case has to be answered by the filter, not by the length of
/// a mask that happens to be one.
#[test]
fn an_empty_any_of_matches_nothing_for_the_right_reason() {
    let df = sample();
    let rows = matching_rows(&df, &[Clause::AnyOf(vec![])]).unwrap();
    assert!(rows.is_empty());

    // And the same filter with a real predicate beside it still matches
    // nothing — an empty group is false, not ignored.
    let both = matching_rows(
        &df,
        &[
            Clause::AnyOf(vec![]),
            Clause::One(eq("department", "Engineering")),
        ],
    )
    .unwrap();
    assert!(both.is_empty(), "false AND anything is false");
}

/// When polars refuses an expression the model needs to know why, not that
/// something unspecified went wrong.
#[test]
fn a_refused_filter_reports_the_real_reason() {
    let df = sample();
    let clauses = vec![Clause::One(Predicate {
        col: "name".to_string(),
        op: PredOp::Gt,
        value: Operand::Literal(Value::Number(5.0)),
    })];

    let message = match matching_rows(&df, &clauses) {
        Err(e) => e,
        Ok(rows) => panic!(
            "comparing a name against a number must not silently match {} rows",
            rows.len()
        ),
    };
    assert!(
        message.contains("string") && message.contains("numeric"),
        "the model needs the actual reason, not a placeholder: {}",
        message
    );
}

/// The audit predicted that a number compared against a text column would be
/// coerced to text and match everything. It does not — polars refuses. Pinned
/// because the two outcomes need very different fixes, and a future change to
/// the literal path could turn the refusal into the silent version.
#[test]
fn a_number_against_a_text_column_is_refused_not_silently_stringified() {
    let df = sample();
    for op in [PredOp::Gt, PredOp::Eq, PredOp::Ne] {
        let clauses = vec![Clause::One(Predicate {
            col: "department".to_string(),
            op,
            value: Operand::Literal(Value::Number(30.0)),
        })];
        assert!(
            matching_rows(&df, &clauses).is_err(),
            "{:?} against a text column must be refused, not answered",
            op
        );
    }
}

// ── empty cells ─────────────────────────────────────────────────────────────

fn gaps() -> DataFrame {
    load_file(Path::new("test_data/gaps.csv"), None).unwrap()
}

fn missing(col: &str) -> Predicate {
    Predicate {
        col: col.to_string(),
        op: PredOp::IsEmpty,
        value: Operand::Literal(Value::Null),
    }
}

fn present(col: &str) -> Predicate {
    Predicate {
        col: col.to_string(),
        op: PredOp::NotEmpty,
        value: Operand::Literal(Value::Null),
    }
}

/// `is_empty` compared the column against a null literal, and in three-valued
/// logic `x == null` is null — never true. It matched nothing at all, on any
/// file, because polars reads an empty CSV field as null and tuitab does not
/// override that.
#[test]
fn is_empty_finds_the_blank_cells() {
    let df = gaps();
    // `name` is blank on rows 2 and 5 (physical 1 and 4).
    assert_eq!(
        matching_rows(&df, &[Clause::One(missing("name"))]).unwrap(),
        vec![1, 4]
    );
    // `team` on rows 3 and 5; `score` on row 4.
    assert_eq!(
        matching_rows(&df, &[Clause::One(missing("team"))]).unwrap(),
        vec![2, 4]
    );
    assert_eq!(
        matching_rows(&df, &[Clause::One(missing("score"))]).unwrap(),
        vec![3]
    );
}

/// Every row is either empty or not — a blank cell must not fall through both
/// predicates, which is what happened while `is_empty` matched nothing.
#[test]
fn empty_and_not_empty_partition_the_rows() {
    let df = gaps();
    for column in ["name", "team", "score"] {
        let blank = matching_rows(&df, &[Clause::One(missing(column))]).unwrap();
        let filled = matching_rows(&df, &[Clause::One(present(column))]).unwrap();

        assert_eq!(
            blank.len() + filled.len(),
            df.visible_row_count(),
            "'{}': {} blank + {} filled does not account for every row",
            column,
            blank.len(),
            filled.len()
        );
        assert!(
            blank.iter().all(|r| !filled.contains(r)),
            "'{}': a row cannot be both",
            column
        );
    }
}

/// `is_empty` on a numeric column used to compare it against `""`, which is a
/// string-versus-number refusal.
#[test]
fn is_empty_works_on_a_numeric_column() {
    let df = gaps();
    let blank = matching_rows(&df, &[Clause::One(missing("score"))]).unwrap();
    assert_eq!(blank, vec![3], "David has no score");
}

// ── the literal has to fit the column ───────────────────────────────────────

fn hires() -> DataFrame {
    load_file(Path::new("test_data/hires.csv"), None).unwrap()
}

fn compare(col: &str, op: PredOp, value: Value) -> Vec<Clause> {
    vec![Clause::One(Predicate {
        col: col.to_string(),
        op,
        value: Operand::Literal(value),
    })]
}

/// A model writing `"30"` where `30` was meant is a slip worth absorbing, not
/// a reason to refuse. The pre-refactor code said so in as many words; the
/// rewrite lost it.
#[test]
fn a_quoted_number_still_filters_a_numeric_column() {
    let df = hires();
    let quoted = matching_rows(
        &df,
        &compare("salary", PredOp::Gt, Value::String("80000".to_string())),
    )
    .unwrap();
    let plain = matching_rows(&df, &compare("salary", PredOp::Gt, Value::Number(80000.0))).unwrap();

    assert_eq!(quoted, plain, "the quotes must not change the answer");
    assert_eq!(quoted.len(), 2, "Bob and David earn over 80000");
}

/// A string that is not a number stays a type error — absorbing the slip must
/// not turn into guessing.
#[test]
fn a_non_numeric_string_against_a_numeric_column_is_still_refused() {
    let df = hires();
    assert!(matching_rows(
        &df,
        &compare("salary", PredOp::Gt, Value::String("a lot".to_string()))
    )
    .is_err());
}

/// Filtering by date was impossible: polars refuses to compare a Date column
/// with a string, and nothing cast either side. Dates are ordered the same way
/// their ISO text is, so comparing as text is exact.
#[test]
fn a_date_column_can_be_filtered_by_an_iso_string() {
    let mut df = hires();
    let col = df.column_index("hire_date").unwrap();
    // What `t` does in the terminal.
    df.set_column_type(col, tuitab::types::ColumnType::Date)
        .unwrap();

    let after = matching_rows(
        &df,
        &compare(
            "hire_date",
            PredOp::Ge,
            Value::String("2021-01-01".to_string()),
        ),
    )
    .unwrap();
    assert_eq!(after, vec![1, 3], "Bob in 2021 and David in 2022");

    let between = matching_rows(
        &df,
        &[Clause::One(Predicate {
            col: "hire_date".to_string(),
            op: PredOp::Between,
            value: Operand::List(vec![
                Value::String("2020-01-01".to_string()),
                Value::String("2021-12-31".to_string()),
            ]),
        })],
    )
    .unwrap();
    assert_eq!(between, vec![1, 2], "Bob and Carol");
}

/// JSON numbers arrive as `f64`. Above 2^53 that cannot tell neighbouring
/// integers apart, so an id lookup matched the wrong row — or two rows.
#[test]
fn a_large_integer_id_matches_only_itself() {
    let df = hires();
    let alice = matching_rows(
        &df,
        &compare("id", PredOp::Eq, Value::Number(9_007_199_254_740_993.0)),
    )
    .unwrap();
    assert_eq!(alice, vec![0], "exactly Alice, not Alice and Bob");
}

/// `select` used to build its frame by hand, skipping the type reconciliation
/// every other derived table gets — so the same column was reported as one type
/// before a projection and another after.
#[test]
fn a_projection_reports_the_same_types_as_its_source() {
    let df = sample();
    let kept = vec!["age".to_string(), "salary".to_string()];
    let projected = df.select_columns(&kept).unwrap();

    for name in &kept {
        let before = df.columns[df.column_index(name).unwrap()].col_type;
        let after = projected.columns[projected.column_index(name).unwrap()].col_type;
        assert_eq!(before, after, "'{}' changed type by being selected", name);
    }
}

/// The TUI's typed filter runs through the interpreter when Polars cannot lower it;
/// there `007` must stay `007`, not become 7.
#[test]
fn an_interpreted_filter_reads_a_text_column_as_written() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("filter-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("codes.csv");
    std::fs::write(&path, "sku\n007\nA12\n7\n").unwrap();
    let df = load_file(&path, None).unwrap();

    // concat() has no Polars form, so this runs row by row.
    let expr = Expr::parse("concat(sku, '') == '007'").unwrap();
    assert_eq!(select_rows(&df, &expr, Fallback::Allowed).unwrap(), vec![0]);
}
