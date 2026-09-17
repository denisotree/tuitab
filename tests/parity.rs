//! Keeping the terminal and the server in step.
//!
//! The two surfaces are meant to expose one engine. Nothing enforced that: a
//! new `Action` variant compiled fine and fell through the dispatcher's `_`
//! arm, so a feature could be added to the terminal and never reach the model —
//! or the reverse. Several of the defects found in this codebase were exactly
//! that, discovered by reading rather than by building.
//!
//! The match below is exhaustive on purpose. **Adding a variant to `Action`
//! breaks this file until someone classifies it**, which is the whole
//! mechanism: the question "and how does a model do this?" gets asked at the
//! moment the feature is written, not months later.
//!
//! The mirror match over the server's `Op` does the same in the other
//! direction.

use tuitab::mcp::pipeline::Op;
use tuitab::types::Action;

#[derive(Debug, PartialEq, Eq)]
enum Parity {
    /// Runs the same shared function as this MCP operation.
    Shared(&'static str),
    /// Not an operation of its own, but expressible with what the server has.
    Composable(&'static str),
    /// Belongs to the terminal alone, for the stated reason.
    UiOnly(&'static str),
    /// Should be reachable from the server and is not.  Carries an index into
    /// [`KNOWN_GAPS`] rather than a free string: a newly classified gap that nobody
    /// declared panics on the index, which is what makes the list below the register it
    /// claims to be instead of a comment.
    Gap(usize),
}

/// Where a terminal action stands relative to the server.
fn parity(action: &Action) -> Parity {
    match action {
        Action::SearchInput(_)
        | Action::SearchBackspace
        | Action::SearchForwardDelete
        | Action::SearchCursorLeft
        | Action::SearchCursorRight
        | Action::SearchCursorStart
        | Action::SearchCursorEnd
        | Action::SelectRegexInput(_)
        | Action::SelectRegexBackspace
        | Action::SelectRegexForwardDelete
        | Action::SelectRegexCursorLeft
        | Action::SelectRegexCursorRight
        | Action::SelectRegexCursorStart
        | Action::SelectRegexCursorEnd
        | Action::SelectRegexAutocomplete
        | Action::ExpressionInputChar(_)
        | Action::ExpressionBackspace
        | Action::ExpressionForwardDelete
        | Action::ExpressionCursorLeft
        | Action::ExpressionCursorRight
        | Action::ExpressionCursorStart
        | Action::ExpressionCursorEnd
        | Action::ExpressionAutocomplete
        | Action::ExpressionHistoryPrev
        | Action::ExpressionHistoryNext
        | Action::OpenPivotTableInput
        | Action::PivotInput(_)
        | Action::PivotBackspace
        | Action::PivotForwardDelete
        | Action::PivotCursorLeft
        | Action::PivotCursorRight
        | Action::PivotCursorStart
        | Action::PivotCursorEnd
        | Action::PivotAutocomplete
        | Action::PivotHistoryPrev
        | Action::PivotHistoryNext
        | Action::EditInput(_)
        | Action::EditBackspace
        | Action::EditForwardDelete
        | Action::EditCursorLeft
        | Action::EditCursorRight
        | Action::EditCursorStart
        | Action::EditCursorEnd
        | Action::PathInputChar(_)
        | Action::PathBackspace
        | Action::PathForwardDelete
        | Action::PathCursorLeft
        | Action::PathCursorRight
        | Action::PathCursorStart
        | Action::PathCursorEnd
        | Action::QueryInputChar(_)
        | Action::QueryBackspace
        | Action::QueryForwardDelete
        | Action::QueryCursorLeft
        | Action::QueryCursorRight
        | Action::QueryCursorStart
        | Action::QueryCursorEnd
        | Action::SavingInput(_)
        | Action::SavingBackspace
        | Action::SavingForwardDelete
        | Action::SavingCursorLeft
        | Action::SavingCursorRight
        | Action::SavingCursorStart
        | Action::SavingCursorEnd
        | Action::SavingAutocomplete
        | Action::RenameColumnInput(_)
        | Action::RenameColumnBackspace
        | Action::RenameColumnForwardDelete
        | Action::RenameColumnCursorLeft
        | Action::RenameColumnCursorRight
        | Action::RenameColumnCursorStart
        | Action::RenameColumnCursorEnd
        | Action::InsertColumnInput(_)
        | Action::InsertColumnBackspace
        | Action::InsertColumnForwardDelete
        | Action::InsertColumnCursorLeft
        | Action::InsertColumnCursorRight
        | Action::InsertColumnCursorStart
        | Action::InsertColumnCursorEnd
        | Action::ColFindInput(_)
        | Action::ColFindBackspace
        | Action::ColFindForwardDelete
        | Action::ColFindCursorLeft
        | Action::ColFindCursorRight
        | Action::ColFindCursorStart
        | Action::ColFindCursorEnd
        | Action::ColReplaceInput(_)
        | Action::ColReplaceBackspace
        | Action::ColReplaceForwardDelete
        | Action::ColReplaceCursorLeft
        | Action::ColReplaceCursorRight
        | Action::ColReplaceCursorStart
        | Action::ColReplaceCursorEnd
        | Action::ColSplitInput(_)
        | Action::ColSplitBackspace
        | Action::ColSplitForwardDelete
        | Action::ColSplitCursorLeft
        | Action::ColSplitCursorRight
        | Action::ColSplitCursorStart
        | Action::ColSplitCursorEnd
        | Action::BulkEditInput(_)
        | Action::BulkEditBackspace
        | Action::BulkEditForwardDelete
        | Action::BulkEditCursorLeft
        | Action::BulkEditCursorRight
        | Action::BulkEditCursorStart
        | Action::BulkEditCursorEnd
        | Action::JoinPathInput(_)
        | Action::JoinPathBackspace
        | Action::JoinPathForwardDelete
        | Action::JoinPathCursorLeft
        | Action::JoinPathCursorRight
        | Action::JoinPathCursorStart
        | Action::JoinPathCursorEnd
        | Action::JoinPathAutocomplete
        | Action::SelectRandomInputChar(_)
        | Action::SelectRandomBackspace
        | Action::SelectRandomForwardDelete
        | Action::SelectRandomCursorLeft
        | Action::SelectRandomCursorRight
        | Action::SelectRandomCursorStart
        | Action::SelectRandomCursorEnd
        | Action::TableNameChar(_)
        | Action::TableNameBackspace
        | Action::TableNameForwardDelete
        | Action::TableNameCursorLeft
        | Action::TableNameCursorRight
        | Action::TableNameCursorStart
        | Action::TableNameCursorEnd => Parity::UiOnly("editing a text field"),
        Action::MoveUp
        | Action::MoveDown
        | Action::PageUp
        | Action::PageDown
        | Action::WindowFnSelectUp
        | Action::WindowFnSelectDown
        | Action::WindowDirSelectUp
        | Action::WindowDirSelectDown
        | Action::WindowOrderSelectUp
        | Action::WindowOrderSelectDown
        | Action::SearchNext
        | Action::SearchPrev
        | Action::ChartAggSelectUp
        | Action::ChartAggSelectDown
        | Action::ChartCursorPrev
        | Action::ChartCursorNext
        | Action::ChartDrillDown
        | Action::TypeSelectUp
        | Action::TypeSelectDown
        | Action::CurrencySelectUp
        | Action::CurrencySelectDown
        | Action::ChoiceUp
        | Action::ChoiceDown
        | Action::PartitionSelectUp
        | Action::PartitionSelectDown
        | Action::AggregatorSelectUp
        | Action::AggregatorSelectDown
        | Action::CopyFormatSelectUp
        | Action::CopyFormatSelectDown
        | Action::JoinSourceUp
        | Action::JoinSourceDown
        | Action::JoinTypeUp
        | Action::JoinTypeDown
        | Action::JoinLeftKeyUp
        | Action::JoinLeftKeyDown
        | Action::JoinRightKeyUp
        | Action::JoinRightKeyDown
        | Action::DedupTiebreakerUp
        | Action::DedupTiebreakerDown
        | Action::JoinOverviewUp
        | Action::JoinOverviewDown
        | Action::SqlScroll(_)
        | Action::SqlScrollHome
        | Action::SqlScrollEnd => Parity::UiOnly("moving through a list"),
        Action::CancelWindowFnSelect
        | Action::CancelWindowDirSelect
        | Action::CancelWindowOrderSelect
        | Action::CancelSearch
        | Action::CancelSelectByRegex
        | Action::CancelExpression
        | Action::CancelPivotTable
        | Action::CancelChartAgg
        | Action::CancelTypeSelect
        | Action::CancelCurrencySelect
        | Action::CancelEdit
        | Action::CancelDocSearch
        | Action::CancelPathGoto
        | Action::CancelQuery
        | Action::CancelSaveShape
        | Action::CancelSql
        | Action::CancelTableName
        | Action::CancelOpenAs
        | Action::CancelSave
        | Action::CancelZPrefix
        | Action::CancelRenameColumn
        | Action::CancelInsertColumn
        | Action::CancelPartitionSelect
        | Action::CancelColOp
        | Action::CancelBulkEdit
        | Action::CancelAggregatorSelect
        | Action::CancelGPrefix
        | Action::CancelYPrefix
        | Action::CancelCopyFormat
        | Action::CloseHelp
        | Action::JoinSourceCancel
        | Action::JoinPathCancel
        | Action::JoinTypeCancel
        | Action::JoinLeftKeyCancel
        | Action::JoinRightKeyCancel
        | Action::CancelSPrefix
        | Action::CancelSelectRandom
        | Action::CancelDedupTiebreaker
        | Action::JoinOverviewCancel => Parity::UiOnly("dismissing a prompt"),
        Action::Quit => Parity::UiOnly("leaving the program"),
        Action::ConfirmQuitYes => Parity::UiOnly("leaving the program"),
        Action::ConfirmQuitNo => Parity::UiOnly("leaving the program"),
        Action::PopSheet => Parity::UiOnly("the sheet stack"),
        Action::Undo => Parity::UiOnly("undo history"),
        Action::Redo => Parity::UiOnly("undo history"),
        Action::MoveLeft => Parity::UiOnly("cursor"),
        Action::MoveRight => Parity::UiOnly("cursor"),
        Action::GoTop => Parity::UiOnly("cursor"),
        Action::GoBottom => Parity::UiOnly("cursor"),
        Action::SortAscending => Parity::Shared("sort"),
        Action::SortDescending => Parity::Shared("sort"),
        Action::OpenGroupBy => Parity::Shared("group_by"),
        Action::OpenWindowFnSelect => Parity::UiOnly("opens a picker"),
        Action::ApplyWindowFnSelect => Parity::Shared("window"),
        // The picker the two ranks get. `desc` is a field of the server's
        // `window` operation, so this asks for what MCP already accepts.
        Action::ApplyWindowDirSelect => Parity::Shared("window"),
        // The window's own ORDER BY — `order_by` on the server's `window`
        // operation. Neither surface can total by date without it.
        Action::ApplyWindowOrderSelect => Parity::Shared("window"),
        Action::AddSortKeyAscending => Parity::Shared("sort"),
        Action::AddSortKeyDescending => Parity::Shared("sort"),
        Action::TransposeRow => Parity::Shared("transpose"),
        Action::TransposeTable => Parity::Shared("transpose"),
        Action::DescribeSheet => Parity::Shared("tuitab_describe"),
        Action::DeduplicateByPinned => Parity::Shared("dedup"),
        Action::ResetSort => Parity::Composable("omitting the sort"),
        Action::ReloadFile => Parity::UiOnly("the server loads the file on every call"),
        Action::StartSearch => Parity::UiOnly("opens an input"),
        Action::ApplySearch => Parity::UiOnly("search highlights, it does not filter"),
        Action::ClearSearch => Parity::UiOnly("search highlights"),
        Action::SelectByValue => Parity::Shared("filter"),
        Action::StartSelectByRegex => Parity::UiOnly("opens an input"),
        Action::ApplySelectByRegex => Parity::Shared("filter"),
        Action::StartExpression => Parity::UiOnly("opens an input"),
        Action::ApplyExpression => Parity::Shared("compute"),
        Action::OpenFrequencyTable => Parity::Shared("frequency"),
        Action::ApplyPivotTable => Parity::Shared("pivot"),
        Action::OpenChart => Parity::UiOnly("a picture"),
        Action::ApplyChartAgg => Parity::UiOnly("a picture"),
        Action::OpenTypeSelect => Parity::UiOnly("opens a menu"),
        Action::ApplyTypeSelect => Parity::Gap(2),
        Action::ApplyCurrencySelect => Parity::UiOnly("column typing is a view concern"),
        Action::StartEdit => Parity::UiOnly("cell editing"),
        Action::ApplyEdit => Parity::UiOnly("cell editing"),
        Action::OpenRow => Parity::UiOnly("navigation"),
        Action::OpenCell => Parity::UiOnly("navigation"),
        Action::CycleViewMode => Parity::UiOnly("document projection is a view concern"),
        Action::ExpandColumn => Parity::UiOnly("document projection is a view concern"),
        Action::CopyNodePath => Parity::UiOnly("clipboard"),
        Action::StartDocSearch => Parity::UiOnly("opens an input"),
        Action::ApplyDocSearch => Parity::UiOnly("a document search hit list"),
        Action::StartPathGoto => Parity::UiOnly("opens an input"),
        Action::ApplyPathGoto => Parity::UiOnly("navigation"),
        Action::StartQuery => Parity::UiOnly("opens an input"),
        Action::ApplyQuery => Parity::Shared("tuitab_jq"),
        Action::ApplySaveShape => Parity::UiOnly("a save-time prompt"),
        Action::OpenAs => Parity::UiOnly("opens a menu"),
        Action::ApplyOpenAs => Parity::UiOnly("the server takes a format argument"),
        Action::ContractColumn => Parity::UiOnly("document projection is a view concern"),
        Action::SaveFile => Parity::UiOnly("opens the save prompt"),
        Action::ApplySave => Parity::Gap(3),
        Action::ApplyTableName => Parity::Shared("output.table"),
        // The terminal confirms a write with a keypress; the server confirms it with a
        // handshake it cannot fake — tuitab_write returns the SQL and a plan id,
        // tuitab_write_apply runs exactly that plan, and neither tool exists unless the
        // server was started with --mcp-write.
        Action::ApplySql => Parity::Shared("tuitab_write_apply"),
        Action::EnterZPrefix => Parity::UiOnly("a chord prefix"),
        Action::StartRenameColumn => Parity::UiOnly("opens an input"),
        // Renaming a sheet column is a view concern, but renaming a *table's* column
        // reaches the server through tuitab_write's 'alter'.
        Action::ApplyRenameColumn => Parity::Shared("tuitab_write's 'alter'"),
        Action::DeleteColumn => Parity::Composable("select, listing the columns to keep"),
        Action::StartInsertColumn => Parity::UiOnly("opens an input"),
        Action::ApplyInsertColumn => Parity::Shared("tuitab_write's 'alter'"),
        Action::SelectColumn => Parity::Shared("select"),
        Action::UnselectColumn => Parity::Shared("select"),
        Action::MoveColumnLeft => Parity::Gap(4),
        Action::MoveColumnRight => Parity::Gap(4),
        Action::AdjustColumnWidth => Parity::UiOnly("widths are a view concern"),
        Action::AdjustAllColumnWidths => Parity::UiOnly("widths are a view concern"),
        Action::IncreasePrecision => Parity::UiOnly("display precision"),
        Action::DecreasePrecision => Parity::UiOnly("display precision"),
        Action::CreatePctColumn => Parity::Shared("window"),
        Action::OpenPartitionSelect => Parity::UiOnly("opens a picker"),
        Action::ApplyPartitionedPct => Parity::Shared("window"),
        Action::TogglePartitionSelection => Parity::UiOnly("a picker"),
        Action::StartColReplace => Parity::UiOnly("opens an input"),
        Action::StartColRegexpReplace => Parity::UiOnly("opens an input"),
        Action::StartColSplit => Parity::UiOnly("opens an input"),
        Action::ColFindConfirm => Parity::UiOnly("a two-step input"),
        Action::ApplyColReplace => Parity::Gap(0),
        Action::ApplyColSplit => Parity::Gap(1),
        Action::ExitColumnMove => Parity::UiOnly("column order is a view concern"),
        Action::StartBulkEdit => Parity::UiOnly("cell editing"),
        Action::ApplyBulkEdit => Parity::Shared("tuitab_write's 'set'"),
        Action::OpenAggregatorSelect => Parity::UiOnly("opens a picker"),
        Action::ApplyAggregators => Parity::Composable("aggregate"),
        Action::ToggleAggregatorSelection => Parity::UiOnly("a picker"),
        Action::ClearAggregators => Parity::UiOnly("footer aggregates are a view concern"),
        Action::QuickAggregate => Parity::Composable("aggregate"),
        Action::SelectRow => Parity::UiOnly("row marking"),
        Action::UnselectRow => Parity::UiOnly("row marking"),
        Action::EnterGPrefix => Parity::UiOnly("a chord prefix"),
        Action::SelectAllRows => Parity::UiOnly("row marking"),
        Action::UnselectAllRows => Parity::UiOnly("row marking"),
        Action::ToggleAllSelection => Parity::UiOnly("row marking"),
        Action::PasteRows => Parity::UiOnly("clipboard"),
        Action::PasteCell => Parity::UiOnly("clipboard"),
        // A blank row to type into is a terminal idea, but the row itself reaches the
        // server as tuitab_write's 'insert'.
        Action::AddRowBelow => Parity::Shared("tuitab_write's 'insert'"),
        // The form is a way of typing a row; the row it produces is the same insert.
        Action::ApplyRowForm => Parity::Shared("tuitab_write's 'insert'"),
        Action::OpenRowForm
        | Action::RowFormInput(_)
        | Action::RowFormBackspace
        | Action::RowFormForwardDelete
        | Action::RowFormCursorLeft
        | Action::RowFormCursorRight
        | Action::RowFormCursorStart
        | Action::RowFormCursorEnd
        | Action::RowFormFieldUp
        | Action::RowFormFieldDown
        | Action::CancelRowForm => Parity::UiOnly("keystrokes inside the row form"),
        Action::DeleteSelectedRows => Parity::Composable("the inverse filter"),
        Action::EnterYPrefix => Parity::UiOnly("a chord prefix"),
        Action::CopyCurrentCell => Parity::UiOnly("clipboard"),
        Action::OpenCopyFormat(_) => Parity::UiOnly("clipboard"),
        Action::ApplyCopyFormat => Parity::UiOnly("clipboard"),
        Action::CreateSheetFromSelection => Parity::Shared("select"),
        Action::TogglePinColumn => Parity::UiOnly("pinning is a view concern"),
        Action::OpenMultiFrequencyTable => Parity::Shared("frequency"),
        Action::ShowHelp => Parity::UiOnly("help"),
        Action::OpenJoin => Parity::UiOnly("opens the wizard"),
        Action::JoinSourceApply => Parity::UiOnly("a wizard step"),
        Action::JoinPathApply => Parity::UiOnly("a wizard step"),
        Action::JoinTypeApply => Parity::UiOnly("a wizard step"),
        Action::JoinLeftKeyToggle => Parity::UiOnly("a wizard step"),
        Action::JoinLeftKeyToggleAll => Parity::UiOnly("a wizard step"),
        Action::JoinLeftKeyApply => Parity::Shared("join"),
        Action::JoinRightKeyToggle => Parity::UiOnly("a wizard step"),
        Action::JoinRightKeyApply => Parity::Shared("join"),
        Action::EnterSPrefix => Parity::UiOnly("a chord prefix"),
        Action::StartSelectRandom => Parity::UiOnly("opens an input"),
        Action::ApplySelectRandom => Parity::Shared("sample"),
        Action::SelectDuplicates => Parity::Shared("duplicates"),
        Action::StartSmartDedup => Parity::Shared("dedup"),
        Action::ApplyDedupTiebreaker => Parity::Shared("dedup"),
        Action::OpenExternalEditor => Parity::UiOnly("cell editing"),
        Action::JoinOverviewToggle => Parity::UiOnly("a wizard step"),
        Action::JoinOverviewApply => Parity::Shared("join"),
        Action::None => Parity::UiOnly("not an action"),
    }
}

/// And the other direction: every server operation reachable from the keyboard.
fn tui_reach(op: &Op) -> Parity {
    match op {
        Op::Filter(_) => Parity::Shared("| with an expression, and , for a value"),
        Op::Select(_) => Parity::Shared("zs to mark columns, then \""),
        Op::Sort(_) => Parity::Shared("[ and ], z[ and z] to add a key"),
        Op::Compute { .. } => Parity::Shared("="),
        Op::GroupBy { .. } => Parity::Shared("gb"),
        Op::Frequency { .. } => Parity::Shared("F, and gF for several columns"),
        Op::Pivot { .. } => Parity::Shared("W"),
        Op::Aggregate(_) => Parity::Shared("+ to mark aggregates, Z to compute one"),
        Op::Dedup { .. } => Parity::Shared("gD, and Shift+S D to choose a keeper"),
        Op::Duplicates { .. } => Parity::Shared("Shift+S d"),
        Op::Window(_) => Parity::Shared("zw, with zf and zF as shortcuts"),
        Op::Sample { .. } => Parity::Shared("Shift+S r"),
        Op::Transpose { .. } => Parity::Shared("T, and Enter for one row"),
        Op::Join(_) => Parity::Shared("J"),
        Op::Limit(_) => Parity::Composable("scrolling — a terminal shows what fits"),
    }
    // No `Gap` arm is written above because there is nothing to put in one today.  The
    // test below still has to be able to fail, so it matches on the verdict rather than
    // trusting that this function never returns one.
}

// ── the declared state of the union ─────────────────────────────────────────

/// Terminal actions that ought to be reachable from the server and are not.
///
/// A list, not a count, so a new entry has to be spelled out and an old one disappears
/// when it is closed. `Parity::Gap` indexes into this, so a gap classified without a
/// declaration here panics rather than passing quietly.
const KNOWN_GAPS: &[&str] = &[
    "find & replace in a column has no MCP operation",
    "splitting a column by a delimiter has no MCP operation",
    "changing a database column's declared type has no MCP operation — `alter` adds, \
     drops and renames, and `SchemaPlan::retypes` is only reachable from `t`",
    "saving a database to another file copies every table, index and trigger; \
     `output.path` writes one table from the current frame",
    "reordering a table's columns has no MCP operation — the terminal rebuilds the \
     table for it, so it is not a view concern",
];

#[test]
fn the_data_operations_are_shared() {
    // A representative few, so a refactor that quietly unhooks one is caught
    // here rather than by a user.
    for (action, op) in [
        (Action::SortAscending, "sort"),
        (Action::AddSortKeyDescending, "sort"),
        (Action::OpenGroupBy, "group_by"),
        (Action::TransposeTable, "transpose"),
        (Action::DeduplicateByPinned, "dedup"),
        (Action::ApplySelectRandom, "sample"),
        (Action::ApplyWindowFnSelect, "window"),
        (Action::CreatePctColumn, "window"),
        (Action::DescribeSheet, "tuitab_describe"),
        (Action::ApplyExpression, "compute"),
    ] {
        assert_eq!(
            parity(&action),
            Parity::Shared(op),
            "{:?} should run the same function as the server's {}",
            action,
            op
        );
    }
}

#[test]
fn every_server_operation_has_a_key() {
    for op in [
        Op::Sort(Vec::new()),
        Op::Aggregate(Vec::new()),
        Op::Transpose { row: None },
        Op::Limit(1),
    ] {
        if let Parity::Gap(i) = tui_reach(&op) {
            panic!("{}: no way to invoke it from the keyboard", KNOWN_GAPS[i]);
        }
    }
}

/// The gaps are declared rather than counted, so closing one means deleting a
/// line here and adding an operation — not adjusting a number.
#[test]
fn the_known_gaps_are_the_declared_ones() {
    // Every declared gap is claimed, and every claim points at a declared gap.  A gap
    // that gets closed has to be deleted from both, and one that appears has to be
    // added to both — neither can be done quietly.
    let claimed = [
        (Action::ApplyColReplace, 0),
        (Action::ApplyColSplit, 1),
        (Action::ApplyTypeSelect, 2),
        (Action::ApplySave, 3),
        (Action::MoveColumnLeft, 4),
        (Action::MoveColumnRight, 4),
    ];
    for (action, i) in &claimed {
        assert!(
            *i < KNOWN_GAPS.len(),
            "{:?} indexes past KNOWN_GAPS",
            action
        );
        assert_eq!(parity(action), Parity::Gap(*i), "{:?}", action);
    }
    for (i, why) in KNOWN_GAPS.iter().enumerate() {
        assert!(
            claimed.iter().any(|(_, j)| *j == i),
            "nothing produces gap {}: '{}' — delete it if it was closed",
            i,
            why
        );
    }
}

/// A reason is the point of the classification. An empty one would let a
/// variant be waved through.
#[test]
fn every_verdict_carries_a_reason() {
    for action in [
        Action::Quit,
        Action::OpenChart,
        Action::PasteRows,
        Action::TogglePinColumn,
        Action::SearchNext,
    ] {
        let why = match parity(&action) {
            Parity::UiOnly(w) | Parity::Shared(w) | Parity::Composable(w) => w,
            Parity::Gap(i) => KNOWN_GAPS[i],
        };
        assert!(
            !why.trim().is_empty(),
            "{:?} was classified with no reason",
            action
        );
    }
}
