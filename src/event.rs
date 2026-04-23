use crate::keymap::remap_char;
use crate::types::{Action, AppMode};
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

/// Map a raw crossterm key event to a semantic Action.
/// Returns Action::None for events that should be ignored.
pub fn handle_key_event(key: KeyEvent, mode: AppMode, can_pop: bool) -> Action {
    // Ignore key release and repeat events — only handle Press
    if key.kind != KeyEventKind::Press {
        return Action::None;
    }

    match mode {
        AppMode::Normal => match key.code {
            // Quit or pop sheet depending on stack depth
            KeyCode::Char('q') | KeyCode::Esc => {
                if can_pop {
                    Action::PopSheet
                } else {
                    Action::Quit
                }
            }
            KeyCode::Char('j') | KeyCode::Down => Action::MoveDown,
            KeyCode::Char('k') | KeyCode::Up => Action::MoveUp,
            KeyCode::Char('l') | KeyCode::Right => Action::MoveRight,
            KeyCode::Char('h') | KeyCode::Left => Action::MoveLeft,
            KeyCode::PageDown => Action::PageDown,
            KeyCode::Char('f') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::PageDown,
            KeyCode::PageUp => Action::PageUp,
            KeyCode::Char('b') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::PageUp,
            // 'g' now enters GPrefix mode (for gg=GoTop, gs=SelectAll, gu=UnselectAll)
            KeyCode::Home => Action::GoTop,
            KeyCode::End | KeyCode::Char('G') => Action::GoBottom,
            // Sorting
            KeyCode::Char('[') => Action::SortAscending,
            KeyCode::Char(']') => Action::SortDescending,
            KeyCode::Enter => Action::OpenRow,
            KeyCode::Char('r') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::Redo,
            KeyCode::Char('r') => Action::ResetSort,
            KeyCode::Char('R') => Action::ReloadFile,
            // Search (replaces old filter)
            KeyCode::Char('/') => Action::StartSearch,
            KeyCode::Char('n') => Action::SearchNext,
            KeyCode::Char('N') => Action::SearchPrev,
            KeyCode::Char('c') => Action::ClearSearch,
            // Select by value
            KeyCode::Char(',') => Action::SelectByValue,
            // Select by regex
            KeyCode::Char('|') => Action::StartSelectByRegex,
            // Frequency table
            KeyCode::Char('F') => Action::OpenFrequencyTable,
            // Charts
            KeyCode::Char('V') => Action::OpenChart,
            // Pin column
            KeyCode::Char('!') => Action::TogglePinColumn,
            KeyCode::Char('1') if key.modifiers.contains(KeyModifiers::SHIFT) => {
                Action::TogglePinColumn
            }
            // Describe sheet
            KeyCode::Char('I') => Action::DescribeSheet,
            // Type assignment prefix
            KeyCode::Char('t') => Action::OpenTypeSelect,
            KeyCode::Char('T') => Action::TransposeTable,
            KeyCode::Char('e') => Action::StartEdit,
            KeyCode::Char('E') => Action::OpenExternalEditor,
            // Undo / Redo
            KeyCode::Char('U') => Action::Undo,
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::SHIFT) => Action::Undo,
            // Save file (Ctrl+S)
            KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::SaveFile,
            // Aggregators
            KeyCode::Char('+') => Action::OpenAggregatorSelect,
            KeyCode::Char('-') => Action::ClearAggregators,
            // Quick aggregate — 'Z' (uppercase z)
            KeyCode::Char('Z') => Action::QuickAggregate,
            // Computed columns
            KeyCode::Char('=') => Action::StartExpression,
            // Column width adjustment
            KeyCode::Char('_') => Action::AdjustColumnWidth,
            // Row selection
            KeyCode::Char('s') => Action::SelectRow,
            KeyCode::Char('u') => Action::UnselectRow,
            KeyCode::Char('g') => Action::EnterGPrefix,
            // Column operations
            KeyCode::Char('z') => Action::EnterZPrefix,
            // Clipboard & delete
            KeyCode::Char('y') => Action::EnterYPrefix,
            KeyCode::Char('p') => Action::PasteRows,
            KeyCode::Char('d') => Action::DeleteSelectedRows,
            // Derived sheet
            KeyCode::Char('"') => Action::CreateSheetFromSelection,
            KeyCode::Char('W') => Action::OpenPivotTableInput,
            // JOIN wizard
            KeyCode::Char('J') => Action::OpenJoin,
            // Help
            KeyCode::Char('?') => Action::ShowHelp,
            // Non-English keyboard remapping — fall through all the above, then remap
            KeyCode::Char(c) => {
                let remapped = remap_char(c);
                if remapped != c {
                    // Re-dispatch through Normal mode handling (simple approach: re-check common bindings)
                    match remapped {
                        'q' => Action::Quit,
                        'k' => Action::MoveUp,
                        'j' => Action::MoveDown,
                        'h' => Action::MoveLeft,
                        'l' => Action::MoveRight,
                        'g' => Action::EnterGPrefix,
                        'z' => Action::EnterZPrefix,
                        'y' => Action::EnterYPrefix,
                        'p' => Action::PasteRows,
                        'd' => Action::DeleteSelectedRows,
                        's' => Action::SelectRow,
                        'u' => Action::UnselectRow,
                        '/' => Action::StartSearch,
                        '|' => Action::StartSelectByRegex,
                        '=' => Action::StartExpression,
                        '+' => Action::OpenAggregatorSelect,
                        '-' => Action::ClearAggregators,
                        'v' => Action::OpenChart,
                        'r' => Action::ResetSort,
                        'R' => Action::ReloadFile,
                        'i' => Action::DescribeSheet,
                        'e' => Action::StartEdit,
                        'F' => Action::OpenFrequencyTable,
                        'Z' => Action::QuickAggregate,
                        'U' => Action::Undo,
                        't' => Action::OpenTypeSelect,
                        'T' => Action::TransposeTable,
                        '!' => Action::TogglePinColumn,
                        'W' => Action::OpenPivotTableInput,
                        'J' => Action::OpenJoin,
                        _ => Action::None,
                    }
                } else {
                    Action::None
                }
            }
            _ => Action::None,
        },

        AppMode::PivotTableInput => match key.code {
            KeyCode::Char(c) => Action::PivotInput(c),
            KeyCode::Backspace => Action::PivotBackspace,
            KeyCode::Delete => Action::PivotForwardDelete,
            KeyCode::Left => Action::PivotCursorLeft,
            KeyCode::Right => Action::PivotCursorRight,
            KeyCode::Home => Action::PivotCursorStart,
            KeyCode::End => Action::PivotCursorEnd,
            KeyCode::Tab => Action::PivotAutocomplete,
            KeyCode::Up => Action::PivotHistoryPrev,
            KeyCode::Down => Action::PivotHistoryNext,
            KeyCode::Enter => Action::ApplyPivotTable,
            KeyCode::Esc => Action::CancelPivotTable,
            _ => Action::None,
        },

        // Chart aggregation selection popup
        AppMode::ChartAggSelect => match key.code {
            KeyCode::Up | KeyCode::Char('k') => Action::ChartAggSelectUp,
            KeyCode::Down | KeyCode::Char('j') => Action::ChartAggSelectDown,
            KeyCode::Enter => Action::ApplyChartAgg,
            KeyCode::Esc | KeyCode::Char('q') => Action::CancelChartAgg,
            _ => Action::None,
        },

        // Search input mode (/)
        AppMode::Searching => match key.code {
            KeyCode::Esc => Action::CancelSearch,
            KeyCode::Enter => Action::ApplySearch,
            KeyCode::Backspace => Action::SearchBackspace,
            KeyCode::Delete => Action::SearchForwardDelete,
            KeyCode::Left => Action::SearchCursorLeft,
            KeyCode::Right => Action::SearchCursorRight,
            KeyCode::Home => Action::SearchCursorStart,
            KeyCode::End => Action::SearchCursorEnd,
            KeyCode::Char(c) => Action::SearchInput(c),
            _ => Action::None,
        },

        // Select by regex input mode (|)
        AppMode::SelectByRegex => match key.code {
            KeyCode::Esc => Action::CancelSelectByRegex,
            KeyCode::Enter => Action::ApplySelectByRegex,
            KeyCode::Backspace => Action::SelectRegexBackspace,
            KeyCode::Delete => Action::SelectRegexForwardDelete,
            KeyCode::Left => Action::SelectRegexCursorLeft,
            KeyCode::Right => Action::SelectRegexCursorRight,
            KeyCode::Home => Action::SelectRegexCursorStart,
            KeyCode::End => Action::SelectRegexCursorEnd,
            KeyCode::Tab => Action::SelectRegexAutocomplete,
            KeyCode::Char(c) => Action::SelectRegexInput(c),
            _ => Action::None,
        },

        // Expression input mode (=)
        AppMode::ExpressionInput => match key.code {
            KeyCode::Esc => Action::CancelExpression,
            KeyCode::Enter => Action::ApplyExpression,
            KeyCode::Backspace => Action::ExpressionBackspace,
            KeyCode::Delete => Action::ExpressionForwardDelete,
            KeyCode::Left => Action::ExpressionCursorLeft,
            KeyCode::Right => Action::ExpressionCursorRight,
            KeyCode::Home => Action::ExpressionCursorStart,
            KeyCode::End => Action::ExpressionCursorEnd,
            KeyCode::Char(c) => Action::ExpressionInputChar(c),
            KeyCode::Tab => Action::ExpressionAutocomplete,
            KeyCode::Up => Action::ExpressionHistoryPrev,
            KeyCode::Down => Action::ExpressionHistoryNext,
            _ => Action::None,
        },

        // Type assignment — popup list
        AppMode::TypeSelect => match key.code {
            KeyCode::Up | KeyCode::Char('k') => Action::TypeSelectUp,
            KeyCode::Down | KeyCode::Char('j') => Action::TypeSelectDown,
            KeyCode::Enter => Action::ApplyTypeSelect,
            KeyCode::Esc | KeyCode::Char('q') => Action::CancelTypeSelect,
            _ => Action::None,
        },

        // Currency selection popup
        AppMode::CurrencySelect => match key.code {
            KeyCode::Up | KeyCode::Char('k') => Action::CurrencySelectUp,
            KeyCode::Down | KeyCode::Char('j') => Action::CurrencySelectDown,
            KeyCode::Enter => Action::ApplyCurrencySelect,
            KeyCode::Esc | KeyCode::Char('q') => Action::CancelCurrencySelect,
            _ => Action::None,
        },

        // Cell editing input
        AppMode::Editing => match key.code {
            KeyCode::Esc => Action::CancelEdit,
            KeyCode::Enter => Action::ApplyEdit,
            KeyCode::Backspace => Action::EditBackspace,
            KeyCode::Delete => Action::EditForwardDelete,
            KeyCode::Left => Action::EditCursorLeft,
            KeyCode::Right => Action::EditCursorRight,
            KeyCode::Home => Action::EditCursorStart,
            KeyCode::End => Action::EditCursorEnd,
            KeyCode::Char(c) => Action::EditInput(c),
            _ => Action::None,
        },

        // No input during loading or calculating
        AppMode::Loading | AppMode::Calculating => Action::None,

        // Aggregator selection popup
        AppMode::AggregatorSelect => match key.code {
            KeyCode::Enter => Action::ApplyAggregators,
            KeyCode::Up | KeyCode::Char('k') => Action::AggregatorSelectUp,
            KeyCode::Down | KeyCode::Char('j') => Action::AggregatorSelectDown,
            KeyCode::Char(' ') => Action::ToggleAggregatorSelection,
            KeyCode::Esc | KeyCode::Char('q') => Action::CancelAggregatorSelect,
            _ => Action::None,
        },

        // Save file popup
        AppMode::Saving => match key.code {
            KeyCode::Esc => Action::CancelSave,
            KeyCode::Enter => Action::ApplySave,
            KeyCode::Tab => Action::SavingAutocomplete,
            KeyCode::Backspace => Action::SavingBackspace,
            KeyCode::Delete => Action::SavingForwardDelete,
            KeyCode::Left => Action::SavingCursorLeft,
            KeyCode::Right => Action::SavingCursorRight,
            KeyCode::Home => Action::SavingCursorStart,
            KeyCode::End => Action::SavingCursorEnd,
            KeyCode::Char(c) => Action::SavingInput(c),
            _ => Action::None,
        },

        // Chart view
        AppMode::Chart => match key.code {
            KeyCode::Char('V') | KeyCode::Char('q') | KeyCode::Esc => Action::OpenChart, // Toggle off
            _ => Action::None,
        },

        // g-prefix modifier
        AppMode::GPrefix => match key.code {
            KeyCode::Char('g') => Action::GoTop,           // gg → go to top
            KeyCode::Char('s') => Action::SelectAllRows,   // gs → select all
            KeyCode::Char('u') => Action::UnselectAllRows, // gu → unselect all
            KeyCode::Char('_') => Action::AdjustAllColumnWidths, // g_ → adjust all column widths
            KeyCode::Char('F') => Action::OpenMultiFrequencyTable, // gF → multi frequency table
            KeyCode::Char('D') | KeyCode::Char('d') | KeyCode::Char('U') => {
                Action::DeduplicateByPinned
            }
            KeyCode::Esc => Action::CancelGPrefix,
            _ => Action::CancelGPrefix,
        },

        // z-prefix modifier
        AppMode::ZPrefix => match key.code {
            KeyCode::Char('e') => Action::StartRenameColumn,
            KeyCode::Char('d') => Action::DeleteColumn,
            KeyCode::Char('i') => Action::StartInsertColumn,
            KeyCode::Char('f') => Action::CreatePctColumn,
            KeyCode::Char('F') => Action::OpenPartitionSelect,
            KeyCode::Char('s') => Action::SelectColumn,
            KeyCode::Char('u') => Action::UnselectColumn,
            KeyCode::Left | KeyCode::Char('h') => Action::MoveColumnLeft,
            KeyCode::Right | KeyCode::Char('l') => Action::MoveColumnRight,
            // Precision: z> increase decimal places, z< decrease
            KeyCode::Char('>') | KeyCode::Char('.') => Action::IncreasePrecision,
            KeyCode::Char('<') | KeyCode::Char(',') => Action::DecreasePrecision,
            KeyCode::Esc => Action::CancelZPrefix,
            _ => Action::CancelZPrefix,
        },

        // Partition selection for zF
        AppMode::PartitionSelect => match key.code {
            KeyCode::Enter => Action::ApplyPartitionedPct,
            KeyCode::Up | KeyCode::Char('k') => Action::PartitionSelectUp,
            KeyCode::Down | KeyCode::Char('j') => Action::PartitionSelectDown,
            KeyCode::Char(' ') => Action::TogglePartitionSelection,
            KeyCode::Esc | KeyCode::Char('q') => Action::CancelPartitionSelect,
            _ => Action::None,
        },

        // Renaming column
        AppMode::RenamingColumn => match key.code {
            KeyCode::Esc => Action::CancelRenameColumn,
            KeyCode::Enter => Action::ApplyRenameColumn,
            KeyCode::Backspace => Action::RenameColumnBackspace,
            KeyCode::Delete => Action::RenameColumnForwardDelete,
            KeyCode::Left => Action::RenameColumnCursorLeft,
            KeyCode::Right => Action::RenameColumnCursorRight,
            KeyCode::Home => Action::RenameColumnCursorStart,
            KeyCode::End => Action::RenameColumnCursorEnd,
            KeyCode::Char(c) => Action::RenameColumnInput(c),
            _ => Action::None,
        },

        // Inserting column
        AppMode::InsertingColumn => match key.code {
            KeyCode::Esc => Action::CancelInsertColumn,
            KeyCode::Enter => Action::ApplyInsertColumn,
            KeyCode::Backspace => Action::InsertColumnBackspace,
            KeyCode::Delete => Action::InsertColumnForwardDelete,
            KeyCode::Left => Action::InsertColumnCursorLeft,
            KeyCode::Right => Action::InsertColumnCursorRight,
            KeyCode::Home => Action::InsertColumnCursorStart,
            KeyCode::End => Action::InsertColumnCursorEnd,
            KeyCode::Char(c) => Action::InsertColumnInput(c),
            _ => Action::None,
        },

        // Confirming Quit
        AppMode::ConfirmQuit => match key.code {
            KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => Action::ConfirmQuitYes,
            KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => Action::ConfirmQuitNo,
            _ => Action::None,
        },

        // Y-prefix copy mode
        AppMode::YPrefix => match key.code {
            KeyCode::Char('c') => Action::CopyCurrentCell,
            KeyCode::Char('r') => Action::OpenCopyFormat(crate::types::CopyPending::SmartRows),
            KeyCode::Char('z') => Action::OpenCopyFormat(crate::types::CopyPending::SmartColumn),
            KeyCode::Char('Z') => Action::OpenCopyFormat(crate::types::CopyPending::WholeColumn),
            KeyCode::Char('R') => Action::OpenCopyFormat(crate::types::CopyPending::WholeTable),
            KeyCode::Esc => Action::CancelYPrefix,
            _ => Action::CancelYPrefix,
        },

        // Copy format selection popup
        AppMode::CopyFormatSelect => match key.code {
            KeyCode::Up | KeyCode::Char('k') => Action::CopyFormatSelectUp,
            KeyCode::Down | KeyCode::Char('j') => Action::CopyFormatSelectDown,
            KeyCode::Enter => Action::ApplyCopyFormat,
            KeyCode::Esc => Action::CancelCopyFormat,
            _ => Action::None,
        },

        // Help overlay — any key closes it
        AppMode::Help => match key.code {
            KeyCode::Esc | KeyCode::Char('?') | KeyCode::Char('q') => Action::CloseHelp,
            _ => Action::CloseHelp,
        },

        // JOIN step 1: source selection
        AppMode::JoinSelectSource => match key.code {
            KeyCode::Up | KeyCode::Char('k') => Action::JoinSourceUp,
            KeyCode::Down | KeyCode::Char('j') => Action::JoinSourceDown,
            KeyCode::Enter => Action::JoinSourceApply,
            KeyCode::Esc | KeyCode::Char('q') => Action::JoinSourceCancel,
            _ => Action::None,
        },

        // JOIN step 1b: file path input
        AppMode::JoinInputPath => match key.code {
            KeyCode::Esc => Action::JoinPathCancel,
            KeyCode::Enter => Action::JoinPathApply,
            KeyCode::Tab => Action::JoinPathAutocomplete,
            KeyCode::Backspace => Action::JoinPathBackspace,
            KeyCode::Delete => Action::JoinPathForwardDelete,
            KeyCode::Left => Action::JoinPathCursorLeft,
            KeyCode::Right => Action::JoinPathCursorRight,
            KeyCode::Home => Action::JoinPathCursorStart,
            KeyCode::End => Action::JoinPathCursorEnd,
            KeyCode::Char(c) => Action::JoinPathInput(c),
            _ => Action::None,
        },

        // JOIN step 2: join type selection
        AppMode::JoinSelectType => match key.code {
            KeyCode::Up | KeyCode::Char('k') => Action::JoinTypeUp,
            KeyCode::Down | KeyCode::Char('j') => Action::JoinTypeDown,
            KeyCode::Enter => Action::JoinTypeApply,
            KeyCode::Esc | KeyCode::Char('q') => Action::JoinTypeCancel,
            _ => Action::None,
        },

        // JOIN step 3: left key columns
        AppMode::JoinSelectLeftKeys => match key.code {
            KeyCode::Up | KeyCode::Char('k') => Action::JoinLeftKeyUp,
            KeyCode::Down | KeyCode::Char('j') => Action::JoinLeftKeyDown,
            KeyCode::Char(' ') => Action::JoinLeftKeyToggle,
            KeyCode::Enter => Action::JoinLeftKeyApply,
            KeyCode::Esc | KeyCode::Char('q') => Action::JoinLeftKeyCancel,
            _ => Action::None,
        },

        // JOIN step 4: right key columns
        AppMode::JoinSelectRightKeys => match key.code {
            KeyCode::Up | KeyCode::Char('k') => Action::JoinRightKeyUp,
            KeyCode::Down | KeyCode::Char('j') => Action::JoinRightKeyDown,
            KeyCode::Char(' ') => Action::JoinRightKeyToggle,
            KeyCode::Enter => Action::JoinRightKeyApply,
            KeyCode::Esc | KeyCode::Char('q') => Action::JoinRightKeyCancel,
            _ => Action::None,
        },

        // JOIN overview multi-select
        AppMode::JoinOverviewSelect => match key.code {
            KeyCode::Up | KeyCode::Char('k') => Action::JoinOverviewUp,
            KeyCode::Down | KeyCode::Char('j') => Action::JoinOverviewDown,
            KeyCode::Char(' ') => Action::JoinOverviewToggle,
            KeyCode::Enter => Action::JoinOverviewApply,
            KeyCode::Esc | KeyCode::Char('q') => Action::JoinOverviewCancel,
            _ => Action::None,
        },
    }
}
