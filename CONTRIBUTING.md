# Contributing to tuitab

Thank you for your interest in contributing!

## Quick start

```sh
git clone https://github.com/denisotree/tuitab
cd tuitab
cargo build        # debug build
cargo test         # run all tests
cargo run -- path/to/file.csv   # run with a file
```

Requirements: Rust stable (see `rust-version` in `Cargo.toml` for minimum).

## How to contribute

### Reporting bugs

Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md).
Include your OS, terminal emulator, tuitab version (`tuitab --version`), and the exact steps to reproduce.

### Requesting features

Use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md).
Explain the problem you're trying to solve, not just the solution you have in mind.

### Submitting a pull request

1. Fork the repository and create a branch from `main`:
   ```sh
   git checkout -b feat/my-feature
   ```
2. Make your changes. Keep commits focused — one logical change per commit.
3. Follow [Conventional Commits](https://www.conventionalcommits.org):
   ```
   feat: add export to JSON
   fix: histogram bins collapse when all values are equal
   docs: add pipe mode examples to README
   refactor: extract chart binning into separate module
   test: add sort stability test for equal values
   ```
4. Run the full check suite before pushing:
   ```sh
   cargo fmt
   cargo clippy --all-targets -- -D warnings
   cargo test
   ```
5. Open a PR against `main`. Fill in the PR template.

## Code style

- `cargo fmt` — enforced by CI, no exceptions
- `cargo clippy -- -D warnings` — all warnings are errors in CI
- No `unwrap()` or `expect()` on user-provided input — return `Result` or show an error in the status bar
- New features touching the UI should work at terminal widths down to 80 columns

## Project structure

```
src/
  app.rs          — main App struct, action dispatch
  types.rs        — AppMode, Action, ColumnType, ChartAgg enums
  event.rs        — keyboard → Action mapping
  sheet.rs        — Sheet and SheetStack
  data/           — DataFrame, loaders, expression engine, aggregators
  ui/             — ratatui rendering (table, charts, popups, bars)
docs/             — internal design documents
packaging/        — Homebrew, Debian, AUR packaging files
tests/            — integration tests
test_data/        — fixture files for tests
```

## Running specific tests

```sh
cargo test                          # all tests
cargo test --test data_tests        # single test file
cargo test histogram                # tests matching a pattern
```

## Performance note

The app uses Polars LazyFrame for filtering and aggregation. When adding new data transformations, prefer `.lazy()` + `.collect()` over iterating rows manually.

## Questions

Open a [Discussion](https://github.com/denisotree/tuitab/discussions) for anything that doesn't fit as a bug or feature request.
