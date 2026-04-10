use serde::{Deserialize, Serialize};

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
    /// Waiting for second key after 'y' to select copy format (yy=row, yc=cell, yl=column)
    YPrefix,
    /// User is selecting a currency for a Currency column
    CurrencySelect,
    /// User is typing a formula for a pivot table (Shift+W)
    PivotTableInput,
    /// Showing the ? help overlay
    Help,
    /// User is selecting columns for partitioning (zF)
    PartitionSelect,
    /// User is selecting aggregation function for a contextual chart
    ChartAggSelect,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SheetType {
    Normal,
    FrequencyTable {
        group_cols: Vec<String>,
    },
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
    TransposeRow,
    TransposeTable,
    DescribeSheet,
    DeduplicateByPinned,
    ResetSort,

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
    SelectRow,       // 's' — select/mark current row
    UnselectRow,     // 'u' — unselect current row
    EnterGPrefix,    // 'g' — wait for next key
    CancelGPrefix,   // Esc in GPrefix mode
    SelectAllRows,   // 'gs' — select all visible rows
    UnselectAllRows, // 'gu' — unselect all visible rows

    // ── Clipboard & row operations ────────────────────────────────────────────
    CopySelectedRows,   // 'y' → enters YPrefix
    PasteRows,          // 'p' — paste rows from clipboard
    DeleteSelectedRows, // 'd' — delete selected rows
    EnterYPrefix,       // 'y' — enter y-prefix mode for copy
    CancelYPrefix,      // Esc in YPrefix mode
    CopyCurrentRow,     // 'yy' — copy current row as TSV
    CopyCurrentCell,    // 'yc' — copy current cell value
    CopyCurrentColumn,  // 'yl' — copy current column as newline-separated list

    // ── Derived sheets ────────────────────────────────────────────────────────
    CreateSheetFromSelection, // '"' — create new sheet from selected rows

    // ── Table Column settings ─────────────────────────────────────────────────
    TogglePinColumn,
    OpenMultiFrequencyTable,

    // ── Help overlay ───────────────────────────────────────────────────────────
    ShowHelp,
    CloseHelp,

    None,
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
        ]
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
