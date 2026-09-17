use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Application mode — determines which widgets are displayed and how input is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppMode {
    /// Normal table browsing mode
    Normal,
    /// User is typing a search pattern (/)
    Searching,
    /// User is typing a regex to select matching rows (|)
    SelectByRegex,
    /// User is typing an expression for a computed column (=)
    ExpressionInput,
    /// User is selecting a column type from a popup list
    TypeSelect,
    /// User is editing a cell value
    Editing,
    /// File is loading in background (Phase 10)
    Loading,
    /// User is selecting an aggregator to assign to current column
    AggregatorSelect,
    /// Waiting for second key after 'g' modifier (gg=GoTop, gs=SelectAll, gu=UnselectAll)
    GPrefix,
    /// Viewing full-screen chart
    Chart,
    /// User is typing a file name to save/export
    Saving,
    /// Waiting for second key after 'z' modifier (ze=Rename, zd=Delete, zi=Insert, z+Arrows=Move)
    ZPrefix,
    /// User is renaming a column
    RenamingColumn,
    /// User is inserting a column
    InsertingColumn,
    /// Application is performing a heavy calculation, overlay shown before action execution
    Calculating,
    /// Asking user for confirmation before quitting with unsaved changes
    ConfirmQuit,
    /// Waiting for second key after 'y' (yr=row, yc=cell, yz=column, yR=sel.rows, yZ=sel.col)
    YPrefix,
    /// User is selecting a copy format from the popup (entered via yr/yz/yR/yZ)
    CopyFormatSelect,
    /// User is selecting a currency for a Currency column
    CurrencySelect,
    /// User is typing a formula for a pivot table (Shift+W)
    PivotTableInput,
    /// Showing the ? help overlay
    Help,
    /// User is picking a window function (zw)
    WindowFnSelect,
    WindowDirSelect,
    WindowOrderSelect,
    /// User is selecting columns for partitioning (zF)
    PartitionSelect,
    /// User is selecting aggregation function for a contextual chart
    ChartAggSelect,
    /// JOIN wizard: step 1 — pick source sheet or browse for file
    JoinSelectSource,
    /// JOIN wizard: step 1b — type a file path
    JoinInputPath,
    /// JOIN wizard: step 2 — pick join type (INNER/LEFT/RIGHT/OUTER)
    JoinSelectType,
    /// JOIN wizard: step 3 — pick left-side key columns (multi-select)
    JoinSelectLeftKeys,
    /// JOIN wizard: step 4 — pick right-side key columns (multi-select)
    JoinSelectRightKeys,
    /// JOIN wizard (overview mode): multi-select items to chain-join
    JoinOverviewSelect,
    /// User is typing the find pattern for column replace (zr/zg)
    ColReplacingFind,
    /// User is typing the replacement string for column replace (zr/zg)
    ColReplacingReplace,
    /// User is typing the delimiter for column split (zx)
    ColSplitting,
    /// Column move mode — entered after z←/z→, repeated arrows reorder until Esc
    ColumnMove,
    /// Bulk-edit value for selected rows (ge)
    BulkEditing,
    /// Waiting for second key after Shift+S (Sr=random, Sd=duplicates, SD=smart dedup)
    SPrefix,
    /// Typing N for random row selection (Shift+S → r)
    SelectRandomInput,
    /// Picking tiebreaker column + direction for smart dedup (Shift+S → D, with pinned cols)
    DedupTiebreakerSelect,
    /// Choosing how a plain table becomes a JSON/YAML/TOML document on save
    SaveShapeSelect,
    /// Choosing which format to reopen the selected file as (zo on a directory sheet)
    OpenAsSelect,
    /// Typing a pattern to search the whole document, not just the visible projection
    DocSearching,
    /// Typing a document path to jump to (`gp`)
    PathInput,
    /// Typing a jq program to run over the document (`gq`)
    QueryInput,
    /// Reviewing the SQL that is about to be run against the source database
    SqlConfirm,
    /// Naming the table a new database will be given
    TableNameInput,
    /// Filling in a new row field by field, checked against the column types (`O`)
    RowForm,
}

/// Distinguishes a regular data sheet from derived views.
///
/// Stored on each [`crate::sheet::Sheet`] so the app knows how to refresh
/// the sheet when the parent data changes or when the user re-opens a view.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SheetType {
    /// A regular sheet containing loaded or user-edited data.
    Normal,
    /// A frequency-table aggregation derived from `group_cols` of a parent sheet.
    FrequencyTable { group_cols: Vec<String> },
    /// A pivot table derived from `index_cols`, `pivot_col`, and `formula`.
    PivotTable {
        index_cols: Vec<String>,
        pivot_col: String,
        formula: String,
    },
}

/// Semantic user action triggered by keyboard input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Quit,
    ConfirmQuitYes,
    ConfirmQuitNo,
    /// Pop the top sheet from the stack (or quit if it's the root sheet)
    PopSheet,
    Undo,
    Redo,
    MoveUp,
    MoveDown,
    MoveLeft,
    MoveRight,
    PageUp,
    PageDown,
    GoTop,
    GoBottom,
    SortAscending,
    SortDescending,
    /// Group by the pinned columns, computing the aggregates marked with `+`
    /// on the others (`gb`).
    OpenGroupBy,
    /// Open the window-function picker (`zw`).
    OpenWindowFnSelect,
    WindowFnSelectUp,
    WindowFnSelectDown,
    ApplyWindowFnSelect,
    CancelWindowFnSelect,
    WindowDirSelectUp,
    WindowDirSelectDown,
    ApplyWindowDirSelect,
    CancelWindowDirSelect,
    WindowOrderSelectUp,
    WindowOrderSelectDown,
    ApplyWindowOrderSelect,
    CancelWindowOrderSelect,
    /// Add the cursor column as a further, less significant sort key (`z[`).
    AddSortKeyAscending,
    /// Same, descending (`z]`).
    AddSortKeyDescending,
    TransposeRow,
    TransposeTable,
    DescribeSheet,
    DeduplicateByPinned,
    ResetSort,
    ReloadFile,

    // ── Search (/) ────────────────────────────────────────────────────────────
    StartSearch,
    SearchInput(char),
    SearchBackspace,
    SearchForwardDelete,
    SearchCursorLeft,
    SearchCursorRight,
    SearchCursorStart,
    SearchCursorEnd,
    ApplySearch,
    CancelSearch,
    SearchNext,
    SearchPrev,
    ClearSearch,

    // ── Select by value (,) ───────────────────────────────────────────────────
    SelectByValue,

    // ── Select by regex (|) ───────────────────────────────────────────────────
    StartSelectByRegex,
    SelectRegexInput(char),
    SelectRegexBackspace,
    SelectRegexForwardDelete,
    SelectRegexCursorLeft,
    SelectRegexCursorRight,
    SelectRegexCursorStart,
    SelectRegexCursorEnd,
    ApplySelectByRegex,
    CancelSelectByRegex,
    SelectRegexAutocomplete,

    // ── Expression / computed column (=) ──────────────────────────────────────
    StartExpression,
    ExpressionInputChar(char),
    ExpressionBackspace,
    ExpressionForwardDelete,
    ExpressionCursorLeft,
    ExpressionCursorRight,
    ExpressionCursorStart,
    ExpressionCursorEnd,
    ApplyExpression,
    CancelExpression,
    ExpressionAutocomplete,
    ExpressionHistoryPrev,
    ExpressionHistoryNext,

    // ── Frequency table ───────────────────────────────────────────────────────
    OpenFrequencyTable,

    // ── Pivot table (Shift+W) ─────────────────────────────────────────────────
    OpenPivotTableInput,
    ApplyPivotTable,
    CancelPivotTable,
    PivotInput(char),
    PivotBackspace,
    PivotForwardDelete,
    PivotCursorLeft,
    PivotCursorRight,
    PivotCursorStart,
    PivotCursorEnd,
    PivotAutocomplete,
    PivotHistoryPrev,
    PivotHistoryNext,

    // ── Charts ────────────────────────────────────────────────────────────────
    OpenChart,
    ChartAggSelectUp,
    ChartAggSelectDown,
    ApplyChartAgg,
    CancelChartAgg,
    ChartCursorPrev,
    ChartCursorNext,
    ChartDrillDown,

    // ── Column type assignment ────────────────────────────────────────────────
    OpenTypeSelect,
    TypeSelectUp,
    TypeSelectDown,
    ApplyTypeSelect,
    CancelTypeSelect,

    // ── Currency selection ────────────────────────────────────────────────────
    CurrencySelectUp,
    CurrencySelectDown,
    ApplyCurrencySelect,
    CancelCurrencySelect,

    // ── Cell editing ──────────────────────────────────────────────────────────
    StartEdit,
    ApplyEdit,
    CancelEdit,
    EditInput(char),
    EditBackspace,
    EditForwardDelete,
    EditCursorLeft,
    EditCursorRight,
    EditCursorStart,
    EditCursorEnd,

    // ── Table interactions ────────────────────────────────────────────────────
    /// Open the selected row (e.g. for directory browser F1)
    OpenRow,
    /// Dive into the node under the cursor cell (`zEnter` on a JSON/YAML/TOML sheet)
    OpenCell,
    /// Cycle how the anchored node is projected: records / key-value / scalars
    CycleViewMode,
    /// Expand the cursor column of containers into one column per child
    ExpandColumn,
    /// Copy the document path of the cell under the cursor (`yp`)
    CopyNodePath,
    /// Search the whole document tree, not just the rows on screen (`g/`)
    StartDocSearch,
    ApplyDocSearch,
    CancelDocSearch,
    /// Jump to a node by typing its document path (`gp`)
    StartPathGoto,
    ApplyPathGoto,
    CancelPathGoto,
    PathInputChar(char),
    PathBackspace,
    PathForwardDelete,
    PathCursorLeft,
    PathCursorRight,
    PathCursorStart,
    PathCursorEnd,
    /// Run a jq program over the document and open the result (`gq`)
    StartQuery,
    ApplyQuery,
    CancelQuery,
    QueryInputChar(char),
    QueryBackspace,
    QueryForwardDelete,
    QueryCursorLeft,
    QueryCursorRight,
    QueryCursorStart,
    QueryCursorEnd,
    /// Move the highlight in a single-choice popup
    ChoiceUp,
    ChoiceDown,
    /// Accept / dismiss the save-shape popup
    ApplySaveShape,
    CancelSaveShape,
    /// Reopen the selected file as an explicitly chosen format
    OpenAs,
    ApplyOpenAs,
    CancelOpenAs,
    /// Fold an expanded column back into a single container column
    ContractColumn,

    // ── Save/export ───────────────────────────────────────────────────────────
    SaveFile,
    SavingInput(char),
    SavingBackspace,
    SavingForwardDelete,
    SavingCursorLeft,
    SavingCursorRight,
    SavingCursorStart,
    SavingCursorEnd,
    ApplySave,
    CancelSave,
    SavingAutocomplete,

    // ── Naming the table a new database export creates ────────────────────────
    TableNameChar(char),
    TableNameBackspace,
    TableNameForwardDelete,
    TableNameCursorLeft,
    TableNameCursorRight,
    TableNameCursorStart,
    TableNameCursorEnd,
    ApplyTableName,
    CancelTableName,

    // ── Writing back into a SQLite/DuckDB file ────────────────────────────────
    /// Move through the statement list; the argument is a signed line delta.
    SqlScroll(i32),
    SqlScrollHome,
    SqlScrollEnd,
    /// Run the statements shown / walk away from them
    ApplySql,
    CancelSql,

    // ── Z Prefix (Column Operations) ──────────────────────────────────────────
    EnterZPrefix,
    CancelZPrefix,
    StartRenameColumn,
    RenameColumnInput(char),
    RenameColumnBackspace,
    RenameColumnForwardDelete,
    RenameColumnCursorLeft,
    RenameColumnCursorRight,
    RenameColumnCursorStart,
    RenameColumnCursorEnd,
    ApplyRenameColumn,
    CancelRenameColumn,
    DeleteColumn,
    StartInsertColumn,
    InsertColumnInput(char),
    InsertColumnBackspace,
    InsertColumnForwardDelete,
    InsertColumnCursorLeft,
    InsertColumnCursorRight,
    InsertColumnCursorStart,
    InsertColumnCursorEnd,
    ApplyInsertColumn,
    CancelInsertColumn,
    SelectColumn,
    UnselectColumn,
    MoveColumnLeft,
    MoveColumnRight,
    AdjustColumnWidth,
    AdjustAllColumnWidths,
    IncreasePrecision,
    DecreasePrecision,
    CreatePctColumn,
    OpenPartitionSelect,
    ApplyPartitionedPct,
    PartitionSelectUp,
    PartitionSelectDown,
    TogglePartitionSelection,
    CancelPartitionSelect,

    // ── Column string operations (zr, zg, zx) ────────────────────────────────
    StartColReplace,
    StartColRegexpReplace,
    StartColSplit,
    ColFindInput(char),
    ColFindBackspace,
    ColFindForwardDelete,
    ColFindCursorLeft,
    ColFindCursorRight,
    ColFindCursorStart,
    ColFindCursorEnd,
    ColFindConfirm,
    ColReplaceInput(char),
    ColReplaceBackspace,
    ColReplaceForwardDelete,
    ColReplaceCursorLeft,
    ColReplaceCursorRight,
    ColReplaceCursorStart,
    ColReplaceCursorEnd,
    ApplyColReplace,
    ColSplitInput(char),
    ColSplitBackspace,
    ColSplitForwardDelete,
    ColSplitCursorLeft,
    ColSplitCursorRight,
    ColSplitCursorStart,
    ColSplitCursorEnd,
    ApplyColSplit,
    CancelColOp,
    /// Exit column-move mode (Esc / Enter / any non-arrow key)
    ExitColumnMove,

    // ── Bulk edit (ge) ────────────────────────────────────────────────────────
    StartBulkEdit,
    BulkEditInput(char),
    BulkEditBackspace,
    BulkEditForwardDelete,
    BulkEditCursorLeft,
    BulkEditCursorRight,
    BulkEditCursorStart,
    BulkEditCursorEnd,
    ApplyBulkEdit,
    CancelBulkEdit,

    // ── Column aggregators ────────────────────────────────────────────────────
    OpenAggregatorSelect,
    ApplyAggregators,
    AggregatorSelectUp,
    AggregatorSelectDown,
    ToggleAggregatorSelection,
    ClearAggregators,
    CancelAggregatorSelect,
    /// Instantly compute & show a summary of the current column's values in the status bar (Z)
    QuickAggregate,

    // ── Row selection ─────────────────────────────────────────────────────────
    SelectRow,          // 's' — select/mark current row
    UnselectRow,        // 'u' — unselect current row
    EnterGPrefix,       // 'g' — wait for next key
    CancelGPrefix,      // Esc in GPrefix mode
    SelectAllRows,      // 'gs' — select all visible rows
    UnselectAllRows,    // 'gu' — unselect all visible rows
    ToggleAllSelection, // 'gt' — toggle: select all if not all selected, else unselect all

    // ── Clipboard & row operations ────────────────────────────────────────────
    PasteRows, // 'P' — paste rows from clipboard
    PasteCell, // 'p' — paste clipboard text into the cell under the cursor
    /// Add an empty row below the cursor (`o`)
    AddRowBelow,
    /// Open the new-row form (`O`) — one field per column, checked against its type
    OpenRowForm,
    RowFormInput(char),
    RowFormBackspace,
    RowFormForwardDelete,
    RowFormCursorLeft,
    RowFormCursorRight,
    RowFormCursorStart,
    RowFormCursorEnd,
    RowFormFieldUp,
    RowFormFieldDown,
    /// Enter in the form — insert the row, unless a field is still wrong
    ApplyRowForm,
    CancelRowForm,
    DeleteSelectedRows,          // 'd' — delete selected rows
    EnterYPrefix,                // 'y' — enter y-prefix mode for copy
    CancelYPrefix,               // Esc in YPrefix mode
    CopyCurrentCell,             // 'yc' — copy current cell value directly
    OpenCopyFormat(CopyPending), // yr/yz/yR/yZ — open format-selection popup
    CopyFormatSelectUp,
    CopyFormatSelectDown,
    ApplyCopyFormat,
    CancelCopyFormat,

    // ── Derived sheets ────────────────────────────────────────────────────────
    CreateSheetFromSelection, // '"' — create new sheet from selected rows

    // ── Table Column settings ─────────────────────────────────────────────────
    TogglePinColumn,
    OpenMultiFrequencyTable,

    // ── Help overlay ───────────────────────────────────────────────────────────
    ShowHelp,
    CloseHelp,

    // ── JOIN wizard ───────────────────────────────────────────────────────────
    OpenJoin,
    JoinSourceUp,
    JoinSourceDown,
    JoinSourceApply,
    JoinSourceCancel,
    JoinPathInput(char),
    JoinPathBackspace,
    JoinPathForwardDelete,
    JoinPathCursorLeft,
    JoinPathCursorRight,
    JoinPathCursorStart,
    JoinPathCursorEnd,
    JoinPathApply,
    JoinPathCancel,
    JoinPathAutocomplete,
    JoinTypeUp,
    JoinTypeDown,
    JoinTypeApply,
    JoinTypeCancel,
    JoinLeftKeyUp,
    JoinLeftKeyDown,
    JoinLeftKeyToggle,
    /// Every column as a left key, or none when all already are.
    JoinLeftKeyToggleAll,
    JoinLeftKeyApply,
    JoinLeftKeyCancel,
    JoinRightKeyUp,
    JoinRightKeyDown,
    JoinRightKeyToggle,
    JoinRightKeyApply,
    JoinRightKeyCancel,

    // ── Special select (Shift+S) ──────────────────────────────────────────────
    EnterSPrefix,
    CancelSPrefix,
    StartSelectRandom, // Sr — open input popup for N
    SelectRandomInputChar(char),
    SelectRandomBackspace,
    SelectRandomForwardDelete,
    SelectRandomCursorLeft,
    SelectRandomCursorRight,
    SelectRandomCursorStart,
    SelectRandomCursorEnd,
    ApplySelectRandom,
    CancelSelectRandom,
    SelectDuplicates, // Sd — select all rows that have an exact duplicate
    StartSmartDedup,  // SD — run smart deduplication
    DedupTiebreakerUp,
    DedupTiebreakerDown,
    ApplyDedupTiebreaker,
    CancelDedupTiebreaker,

    /// Open current cell value in $EDITOR for viewing/editing
    OpenExternalEditor,

    // ── JOIN overview multi-select ─────────────────────────────────────────────
    JoinOverviewUp,
    JoinOverviewDown,
    JoinOverviewToggle,
    JoinOverviewApply,
    JoinOverviewCancel,

    None,
}

/// An item offered in the JOIN source popup, derived from the current sheet's hierarchical context.
#[derive(Clone, Debug)]
pub enum JoinContextItem {
    SqliteTable {
        db_path: PathBuf,
        table_name: String,
    },
    DuckdbTable {
        db_path: PathBuf,
        table_name: String,
    },
    DirectoryFile {
        file_path: PathBuf,
    },
    XlsxSheet {
        xlsx_path: PathBuf,
        sheet_name: String,
    },
}

impl JoinContextItem {
    pub fn label(&self) -> String {
        match self {
            Self::SqliteTable { table_name, .. } => table_name.clone(),
            Self::DuckdbTable { table_name, .. } => table_name.clone(),
            Self::DirectoryFile { file_path } => file_path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
            Self::XlsxSheet { sheet_name, .. } => sheet_name.clone(),
        }
    }
}

/// Identifies which copy operation is pending when the format-select popup is open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyPending {
    /// yr — selected rows (column-selection aware) OR current row if nothing selected
    SmartRows,
    /// yz — current column values for selected rows (only when rows ARE selected)
    SmartColumn,
    /// yZ — entire current column (all visible rows)
    WholeColumn,
    /// yR — entire table (column-selection aware)
    WholeTable,
}

/// Inferred (or user-assigned) column data type used to pick the right sort comparator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    String,
    Integer,
    Float,
    /// Date (NaiveDate) — formats: YYYY-MM-DD, DD.MM.YYYY, etc.
    Date,
    /// Datetime (NaiveDateTime) — formats: YYYY-MM-DD HH:MM:SS, etc.
    Datetime,
    Boolean,
    Percentage,
    Currency,
    /// Integer bytes (i64), displayed as human-readable file size (e.g. "1.5 MB")
    FileSize,
}

impl ColumnType {
    pub fn all() -> &'static [ColumnType] {
        &[
            Self::String,
            Self::Integer,
            Self::Float,
            Self::Date,
            Self::Datetime,
            Self::Boolean,
            Self::Percentage,
            Self::Currency,
            Self::FileSize,
        ]
    }

    /// The type's name in prose: lower case, no icon.
    ///
    /// [`Self::display_name`] prefixes an icon for the type-picker menu, which
    /// makes it wrong for an error message or a JSON field.
    pub fn name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Integer => "integer",
            Self::Float => "float",
            Self::Date => "date",
            Self::Datetime => "datetime",
            Self::Boolean => "boolean",
            Self::Percentage => "percentage",
            Self::Currency => "currency",
            Self::FileSize => "filesize",
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::String => "s  String",
            Self::Integer => "#  Integer",
            Self::Float => "~  Float",
            Self::Date => "d  Date",
            Self::Datetime => "t  Datetime",
            Self::Boolean => "?  Boolean",
            Self::Percentage => "%  Percentage",
            Self::Currency => "$  Currency",
            Self::FileSize => "B  File size",
        }
    }

    pub fn icon(&self) -> char {
        match self {
            Self::String => 's',
            Self::Integer => '#',
            Self::Float => '~',
            Self::Date => 'd',
            Self::Datetime => 't',
            Self::Boolean => '?',
            Self::Percentage => '%',
            Self::Currency => '$',
            Self::FileSize => 'B',
        }
    }
}

/// Supported currency kinds for the Currency column type.
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CurrencyKind {
    USD,
    EUR,
    JPY,
    GBP,
    RUB,
    CNY,
    UAH,
    KZT,
    GEL,
    AMD,
    AUD,
    CAD,
    CHF,
    HKD,
    SGD,
    NOK,
    KRW,
    SEK,
    NZD,
    INR,
    TWD,
    ZAR,
    BRL,
    MXN,
}

impl CurrencyKind {
    pub fn all() -> &'static [CurrencyKind] {
        &[
            Self::USD,
            Self::EUR,
            Self::JPY,
            Self::GBP,
            Self::RUB,
            Self::CNY,
            Self::UAH,
            Self::KZT,
            Self::GEL,
            Self::AMD,
            Self::AUD,
            Self::CAD,
            Self::CHF,
            Self::HKD,
            Self::SGD,
            Self::NOK,
            Self::KRW,
            Self::SEK,
            Self::NZD,
            Self::INR,
            Self::TWD,
            Self::ZAR,
            Self::BRL,
            Self::MXN,
        ]
    }

    /// The currency symbol string
    pub fn symbol(self) -> &'static str {
        match self {
            Self::USD => "$",
            Self::EUR => "€",
            Self::JPY => "¥",
            Self::GBP => "£",
            Self::RUB => "₽",
            Self::CNY => "CN¥",
            Self::UAH => "₴",
            Self::KZT => "₸",
            Self::GEL => "₾",
            Self::AMD => "֏",
            Self::AUD => "A$",
            Self::CAD => "C$",
            Self::CHF => "CHF",
            Self::HKD => "HK$",
            Self::SGD => "S$",
            Self::NOK => "Nkr",
            Self::KRW => "₩",
            Self::SEK => "Skr",
            Self::NZD => "NZ$",
            Self::INR => "₹",
            Self::TWD => "NT$",
            Self::ZAR => "R",
            Self::BRL => "R$",
            Self::MXN => "MX$",
        }
    }

    /// Whether the symbol is a prefix (true) or suffix (false)
    pub fn is_prefix(self) -> bool {
        matches!(
            self,
            Self::USD
                | Self::GBP
                | Self::JPY
                | Self::CNY
                | Self::AUD
                | Self::CAD
                | Self::HKD
                | Self::SGD
                | Self::NZD
                | Self::INR
                | Self::TWD
                | Self::BRL
                | Self::MXN
                | Self::KRW
                | Self::CHF
        )
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::USD => "$  USD (Dollar)",
            Self::EUR => "€  EUR (Euro)",
            Self::JPY => "¥  JPY (Yen)",
            Self::GBP => "£  GBP (Pound)",
            Self::RUB => "₽  RUB (Rouble)",
            Self::CNY => "CN¥ CNY (Yuan)",
            Self::UAH => "₴  UAH (Hryvnia)",
            Self::KZT => "₸  KZT (Tenge)",
            Self::GEL => "₾  GEL (Lari)",
            Self::AMD => "֏  AMD (Dram)",
            Self::AUD => "A$ AUD (Australian Dollar)",
            Self::CAD => "C$ CAD (Canadian Dollar)",
            Self::CHF => "CHF CHF (Swiss Franc)",
            Self::HKD => "HK$ HKD (Hong Kong Dollar)",
            Self::SGD => "S$ SGD (Singapore Dollar)",
            Self::NOK => "Nkr NOK (Norwegian Krone)",
            Self::KRW => "₩  KRW (Won)",
            Self::SEK => "Skr SEK (Swedish Krona)",
            Self::NZD => "NZ$ NZD (New Zealand Dollar)",
            Self::INR => "₹  INR (Indian Rupee)",
            Self::TWD => "NT$ TWD (New Taiwan Dollar)",
            Self::ZAR => "R  ZAR (Rand)",
            Self::BRL => "R$ BRL (Real)",
            Self::MXN => "MX$ MXN (Mexican Peso)",
        }
    }
}

/// Aggregation function used in contextual charts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChartAgg {
    Sum,
    Count,
    Mean,
    Median,
    Min,
    Max,
}

impl ChartAgg {
    pub fn all() -> &'static [ChartAgg] {
        &[
            Self::Sum,
            Self::Count,
            Self::Mean,
            Self::Median,
            Self::Min,
            Self::Max,
        ]
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Count => "count",
            Self::Mean => "mean",
            Self::Median => "median",
            Self::Min => "min",
            Self::Max => "max",
        }
    }

    /// Apply this aggregation to a group. `count` = total rows in group, `vals` = parsed f64 values.
    pub fn apply_group(self, count: usize, vals: &[f64]) -> f64 {
        match self {
            Self::Count => count as f64,
            Self::Sum => vals.iter().sum(),
            Self::Mean => {
                if vals.is_empty() {
                    0.0
                } else {
                    vals.iter().sum::<f64>() / vals.len() as f64
                }
            }
            Self::Median => {
                if vals.is_empty() {
                    return 0.0;
                }
                let mut s = vals.to_vec();
                s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = s.len();
                if n.is_multiple_of(2) {
                    (s[n / 2 - 1] + s[n / 2]) / 2.0
                } else {
                    s[n / 2]
                }
            }
            Self::Min => vals.iter().cloned().fold(f64::INFINITY, f64::min),
            Self::Max => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        }
    }
}
