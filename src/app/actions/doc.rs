//! Actions for sheets backed by a document tree (JSON/JSONL/YAML/TOML): diving into
//! nodes and switching how the anchored node is projected.
//!
//! Editing lives elsewhere: scalar cells go through [`super::edit`], and whole nodes go
//! through the existing `$EDITOR` integration in [`crate::app`], which is doc-aware.

use crate::app::App;
use crate::data::doc::{NodePath, Seg};
use crate::types::AppMode;

/// Upper bound on collected matches.  A pattern like `.` on a big document would
/// otherwise build a sheet nobody can read and take a while doing it; the status line
/// says when the list was cut rather than implying it is complete.
const DOC_SEARCH_LIMIT: usize = 2000;

impl App {
    /// Physical row index under the cursor, or `None` when the sheet is empty.
    pub(crate) fn cursor_physical_row(&self) -> Option<usize> {
        let s = self.stack.active();
        let display_row = s.table_state.selected()?;
        if display_row >= s.dataframe.visible_row_count() {
            return None;
        }
        s.dataframe.row_order.get(display_row).copied()
    }

    /// Path of the node the cursor is on: the row's node, or the cell's node when
    /// `cell` is set (`zEnter` vs `Enter`).
    pub(crate) fn cursor_node_path(&self, cell: bool) -> Option<NodePath> {
        let s = self.stack.active();
        let doc = s.doc.as_ref()?;
        let row = self.cursor_physical_row()?;
        if cell {
            doc.path_of(row, s.cursor_col)
        } else {
            doc.row_paths.get(row).cloned()
        }
    }

    /// Push a sheet anchored at the node under the cursor.  Scalars have nothing to
    /// dive into and say so rather than pushing an empty sheet.
    pub(crate) fn dive_into_node(&mut self, cell: bool) {
        let Some(path) = self.cursor_node_path(cell) else {
            self.status_message = "No node to dive into".to_string();
            return;
        };
        let s = self.stack.active();
        let Some(doc) = s.doc.as_ref() else { return };

        match doc.dive(path) {
            Ok((df, state)) => {
                let title = state.breadcrumbs(&root_name(&s.title));
                let rows = df.visible_row_count();
                let source = s.source_path.clone();
                let mut sheet = crate::sheet::Sheet::new(title, df);
                sheet.doc = Some(state);
                sheet.source_path = source;
                self.stack.push(sheet);
                self.status_message = format!("{} rows", rows);
            }
            Err(e) => self.status_message = e.to_string(),
        }
    }

    /// Rebuild a doc-backed sheet's table from the shared tree.
    ///
    /// Called after popping back from a dive: the child may have edited a node this
    /// sheet renders, and the table is only a cached projection.  The cursor is kept
    /// where it was, since the row count rarely changes and losing the position on
    /// every `Esc` would be worse than the occasional off-by-one.
    pub(crate) fn refresh_doc_projection(&mut self) {
        let s = self.stack.active_mut();
        if s.doc.is_none() {
            return;
        }
        let row = s.table_state.selected();
        let col = s.cursor_col;
        let doc = s.doc.as_mut().expect("checked above");
        if let Ok(df) = doc.reproject() {
            s.dataframe = df;
            let rows = s.dataframe.visible_row_count();
            let cols = s.dataframe.columns.len();
            s.table_state
                .select(row.map(|r| r.min(rows.saturating_sub(1))));
            s.cursor_col = col.min(cols.saturating_sub(1));
            s.table_state.select_column(Some(s.cursor_col));
        }
    }

    /// Cycle the projection mode of the current sheet (`m`): records ↔ key/value ↔
    /// scalars, whichever make sense for the anchored node.
    pub(crate) fn cycle_view_mode(&mut self) {
        let s = self.stack.active_mut();
        let Some(doc) = s.doc.as_mut() else {
            self.status_message = "Not a JSON/YAML/TOML sheet".to_string();
            return;
        };
        let modes = doc.available_modes();
        if modes.len() < 2 {
            self.status_message = "No other view for this node".to_string();
            return;
        }
        let cur = modes.iter().position(|m| *m == doc.view.mode).unwrap_or(0);
        let next = modes[(cur + 1) % modes.len()];
        match doc.set_mode(next) {
            Ok(df) => {
                s.dataframe = df;
                s.reset_view_state();
                self.status_message = format!("View: {}", mode_name(next));
            }
            Err(e) => self.status_message = e.to_string(),
        }
    }

    /// Start typing a pattern to search the whole document (`g/`).
    ///
    /// The plain `/` searches what is on screen, which on a document sheet is one
    /// subtree rendered into cells — it cannot find a key three levels down.
    pub(crate) fn start_doc_search(&mut self) {
        if self.stack.active().doc.is_none() {
            self.mode = AppMode::Normal;
            self.status_message = "Document search needs a JSON/YAML/TOML sheet".to_string();
            return;
        }
        self.stack.active_mut().search_input.clear();
        self.mode = AppMode::DocSearching;
        self.status_message.clear();
    }

    /// Run the search and push a sheet of hits: one row per match, showing where it is
    /// and what is there.  Nothing is opened automatically — the user picks.
    pub(crate) fn apply_doc_search(&mut self) {
        self.mode = AppMode::Normal;
        let pattern = self.stack.active().search_input.as_str().to_string();
        if pattern.is_empty() {
            return;
        }
        let re = match regex::RegexBuilder::new(&pattern)
            .case_insensitive(true)
            .build()
        {
            Ok(re) => re,
            Err(e) => {
                self.status_message = format!("Bad pattern: {}", e);
                return;
            }
        };

        let Some(doc) = self.stack.active().doc.as_ref() else {
            return;
        };
        let handle = std::sync::Arc::clone(&doc.doc);
        let (hits, truncated) = {
            let Ok(guard) = handle.read() else {
                self.status_message = "Document lock poisoned".to_string();
                return;
            };
            // Collect one extra so "there are exactly 2000" is distinguishable from
            // "we stopped counting".
            let mut hits = crate::data::doc::search(&guard.root, &re, DOC_SEARCH_LIMIT + 1);
            let truncated = hits.len() > DOC_SEARCH_LIMIT;
            hits.truncate(DOC_SEARCH_LIMIT);
            let rows: Vec<(String, String, String, String)> = hits
                .iter()
                .map(|h| {
                    let node = guard.root.get(&h.path);
                    (
                        crate::data::doc::path_to_string(&h.path),
                        node.map(|n| n.render_compact(200)).unwrap_or_default(),
                        node.map(|n| n.type_name().to_string()).unwrap_or_default(),
                        if h.in_key { "key" } else { "value" }.to_string(),
                    )
                })
                .collect();
            ((hits, rows), truncated)
        };
        let (hits, rows) = hits;

        if rows.is_empty() {
            self.status_message = format!("No match for `{}` in the document", pattern);
            return;
        }

        let df = match hits_dataframe(&rows) {
            Ok(df) => df,
            Err(e) => {
                self.status_message = format!("Search failed: {}", e);
                return;
            }
        };
        let n = rows.len();
        let root_name = root_name(&self.stack.active().title);
        let mut sheet = crate::sheet::Sheet::new(format!("{} › /{}", root_name, pattern), df);
        sheet.source_path = self.stack.active().source_path.clone();
        let revision = handle.read().map(|d| d.revision).unwrap_or_default();
        sheet.doc_hits = Some(crate::sheet::DocHits {
            doc: handle,
            paths: hits.into_iter().map(|h| h.path).collect(),
            revision,
        });
        self.stack.push(sheet);
        self.status_message = if truncated {
            format!("{} matches (stopped at the first {})", n, DOC_SEARCH_LIMIT)
        } else {
            format!("{} matches", n)
        };
    }

    /// Open the node a search hit points at.  A container opens as itself; a scalar
    /// opens its parent so the value is seen in context.
    pub(crate) fn open_search_hit(&mut self) {
        let Some(row) = self.cursor_physical_row() else {
            return;
        };
        let s = self.stack.active();
        let Some(hits) = s.doc_hits.as_ref() else {
            return;
        };
        let Some(path) = hits.paths.get(row).cloned() else {
            return;
        };
        // Paths are absolute and were captured when the search ran.  A change since then
        // can renumber them, so a path may still resolve — to a different node.
        if hits.doc.read().map(|d| d.revision).unwrap_or_default() != hits.revision {
            self.status_message =
                "The document changed since this search — run g/ again".to_string();
            return;
        }
        let handle = std::sync::Arc::clone(&hits.doc);
        self.open_node_path(handle, &path);
    }

    /// Push a sheet showing `path`: a container as itself, a scalar's parent so the
    /// value is seen in context, with the cursor on it either way.  Shared by search
    /// hits and by `gp`.
    pub(crate) fn open_node_path(
        &mut self,
        handle: std::sync::Arc<std::sync::RwLock<crate::data::doc::Doc>>,
        path: &[Seg],
    ) {
        let source = self.stack.active().source_path.clone();
        let root_name = root_name(&self.stack.active().title);

        let (anchor, cursor_key) = {
            let Ok(guard) = handle.read() else { return };
            match guard.root.get(path) {
                Some(n) if n.is_container() => (path.to_vec(), None),
                Some(_) => {
                    let mut parent = path.to_vec();
                    let last = parent.pop();
                    (parent, last)
                }
                None => {
                    self.status_message = format!(
                        "No node at `{}`{}",
                        crate::data::doc::path_to_string(path),
                        deepest_existing(&guard.root, path)
                    );
                    return;
                }
            }
        };

        match crate::data::io::doc_io::DocState::open_at(handle, anchor) {
            Ok((df, state)) => {
                let title = state.breadcrumbs(&root_name);
                let mut sheet = crate::sheet::Sheet::new(title, df);
                sheet.source_path = source;
                sheet.doc = Some(state);
                // Even anchored at the root it shares the tree with the sheets below.
                sheet.derived_view = true;
                place_cursor(&mut sheet, cursor_key.as_ref());
                self.stack.push(sheet);
                self.status_message.clear();
            }
            Err(e) => self.status_message = e.to_string(),
        }
    }

    /// Prompt for a path to jump to (`gp`), prefilled with where the cursor is now so
    /// a nearby node is a small edit away.
    pub(crate) fn start_path_goto(&mut self) {
        if self.stack.active().doc.is_none() {
            self.mode = AppMode::Normal;
            self.status_message = "Go to path needs a JSON/YAML/TOML sheet".to_string();
            return;
        }
        // The `type` column of a key/value view addresses no node, so fall back to the
        // row's path rather than opening the prompt empty.
        let here = self
            .cursor_node_path(true)
            .or_else(|| self.cursor_node_path(false))
            .map(|p| crate::data::doc::path_to_string(&p))
            .unwrap_or_default();
        self.stack.active_mut().path_input = crate::ui::text_input::TextInput::with_value(here);
        self.mode = AppMode::PathInput;
        self.status_message.clear();
    }

    pub(crate) fn apply_path_goto(&mut self) {
        self.mode = AppMode::Normal;
        let text = self.stack.active().path_input.as_str().to_string();
        let path = match crate::data::doc::parse_path(&text) {
            Ok(p) => p,
            Err(e) => {
                self.status_message = format!("Bad path: {}", e);
                return;
            }
        };
        let Some(doc) = self.stack.active().doc.as_ref() else {
            return;
        };
        let handle = std::sync::Arc::clone(&doc.doc);

        // The prompt is prefilled with where the cursor already is, so confirming it
        // unchanged is the common case — and pushing a second sheet of the node already
        // on screen would grow the stack with duplicates.
        let target_anchor = {
            let Ok(guard) = handle.read() else { return };
            match guard.root.get(&path) {
                Some(n) if n.is_container() => Some(path.clone()),
                Some(_) => Some(path[..path.len().saturating_sub(1)].to_vec()),
                None => None,
            }
        };
        if target_anchor.as_ref() == Some(&doc.view.anchor) {
            let leaf = path.last().cloned();
            let s = self.stack.active_mut();
            place_cursor(s, leaf.as_ref());
            self.status_message.clear();
            return;
        }

        self.open_node_path(handle, &path);
    }

    /// Prompt for a jq program to run over the document (`gq`).
    pub(crate) fn start_query(&mut self) {
        if self.stack.active().doc.is_none() {
            self.mode = AppMode::Normal;
            self.status_message = "Queries need a JSON/YAML/TOML sheet".to_string();
            return;
        }
        self.mode = AppMode::QueryInput;
        self.status_message.clear();
    }

    /// Run the program and open its result as a sheet of its own.
    ///
    /// The result is a *new* document, not a view of the old one, so it deliberately
    /// does not inherit `source_path`: `Ctrl+S` on a query result must not default to
    /// overwriting the file the query was run against.
    pub(crate) fn apply_query(&mut self) {
        self.mode = AppMode::Normal;
        let program = self.stack.active().query_input.as_str().to_string();
        let Some(doc) = self.stack.active().doc.as_ref() else {
            return;
        };
        let format = doc.format();

        let result = {
            let Ok(guard) = doc.doc.read() else {
                self.status_message = "Document lock poisoned".to_string();
                return;
            };
            crate::data::query::run_jq(&guard.root, &program)
        };
        let root = match result {
            Ok(node) => node,
            Err(e) => {
                self.status_message = format!("Query failed: {}", e);
                return;
            }
        };

        // A query result keeps the source's format so saving it writes the kind of file
        // the user was already looking at, but carries no source text: it is not that
        // file any more, and its comments do not belong to this shape.
        let derived = crate::data::doc::Doc {
            format,
            root,
            path: None,
            source_text: None,
            multi_doc: false,
            revision: 0,
        };
        match crate::data::io::doc_io::DocState::from_doc(derived) {
            Ok((df, state)) => {
                let rows = df.visible_row_count();
                let title = format!("{} › {}", root_name(&self.stack.active().title), program);
                let mut sheet = crate::sheet::Sheet::new(title, df);
                sheet.doc = Some(state);
                sheet.save_name_hint = Some(query_save_name(
                    self.stack.active().source_path.as_deref(),
                    format,
                ));
                self.stack.push(sheet);
                self.status_message = if rows == 0 {
                    "no matches".to_string()
                } else {
                    format!("{} rows", rows)
                };
            }
            Err(e) => self.status_message = format!("Query failed: {}", e),
        }
    }

    /// Expand the cursor column of containers into one column per child (`(`).
    pub(crate) fn expand_column(&mut self) {
        self.reshape_columns(true);
    }

    /// Fold the innermost expansion covering the cursor column back (`)`).
    pub(crate) fn contract_column(&mut self) {
        self.reshape_columns(false);
    }

    fn reshape_columns(&mut self, expand: bool) {
        let s = self.stack.active_mut();
        let col = s.cursor_col;
        let Some(doc) = s.doc.as_mut() else {
            self.status_message = "Not a JSON/YAML/TOML sheet".to_string();
            return;
        };
        let result = if expand {
            doc.expand_column(col)
        } else {
            doc.contract_column(col)
        };
        match result {
            Ok(df) => {
                let ncols = df.columns.len();
                s.dataframe = df;
                // The column is replaced in place, so keeping the cursor index lands on
                // the first child — which is what the user was looking at.
                s.cursor_col = col.min(ncols.saturating_sub(1));
                s.table_state.select_column(Some(s.cursor_col));
                // A sort on a column that may no longer exist cannot be kept.
                s.sort_keys.clear();
                s.left_col = s.left_col.min(s.cursor_col);
                self.status_message = format!("{} columns", ncols);
            }
            Err(e) => self.status_message = e.to_string(),
        }
    }
}

/// Name the deepest prefix of `path` that does exist, so a typo says where it went
/// wrong instead of only that it did.
fn deepest_existing(root: &crate::data::doc::Node, path: &[Seg]) -> String {
    let mut best = 0;
    for n in 1..path.len() {
        if root.get(&path[..n]).is_some() {
            best = n;
        } else {
            break;
        }
    }
    if best == 0 {
        String::new()
    } else {
        format!(
            " — `{}` exists",
            crate::data::doc::path_to_string(&path[..best])
        )
    }
}

/// Put the cursor on the row (or key) the hit named, so the match is under the cursor
/// rather than merely somewhere on the sheet.
fn place_cursor(sheet: &mut crate::sheet::Sheet, seg: Option<&Seg>) {
    let Some(doc) = sheet.doc.as_ref() else {
        return;
    };
    let Some(seg) = seg else { return };
    match seg {
        Seg::Idx(i) => {
            if let Some(d) = sheet.dataframe.row_order.iter().position(|p| p == i) {
                sheet.table_state.select(Some(d));
            }
        }
        Seg::Key(k) => {
            // records: the key is a column; key/value: the key is a row
            if let Some(col) = sheet.dataframe.columns.iter().position(|c| &c.name == k) {
                sheet.cursor_col = col;
                sheet.table_state.select_column(Some(col));
            } else if let Some(row) = doc
                .row_paths
                .iter()
                .position(|p| matches!(p.last(), Some(Seg::Key(rk)) if rk == k))
            {
                if let Some(d) = sheet.dataframe.row_order.iter().position(|p| *p == row) {
                    sheet.table_state.select(Some(d));
                }
            }
        }
    }
}

fn hits_dataframe(
    rows: &[(String, String, String, String)],
) -> color_eyre::Result<crate::data::dataframe::DataFrame> {
    use polars::prelude::*;
    let col = |f: fn(&(String, String, String, String)) -> &String, name: &str| {
        Column::new(
            name.into(),
            rows.iter().map(f).cloned().collect::<Vec<String>>(),
        )
    };
    let df = polars::prelude::DataFrame::new(
        rows.len(),
        vec![
            col(|r| &r.0, "path"),
            col(|r| &r.1, "value"),
            col(|r| &r.2, "type"),
            col(|r| &r.3, "matched"),
        ],
    )?;
    crate::data::io::wrap_polars_df(df)
}

fn mode_name(m: crate::data::view::ViewMode) -> &'static str {
    use crate::data::view::ViewMode as V;
    match m {
        V::Records => "records",
        V::KeyValue => "key/value",
        V::Scalars => "scalars",
    }
}

/// A destination for a query result: the source's stem with `-query` appended, in the
/// source's format.  The sheet's title is its jq program, which is not a path.
fn query_save_name(source: Option<&std::path::Path>, format: crate::data::doc::Format) -> String {
    let stem = source
        .and_then(|p| p.file_stem())
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "query".to_string());
    format!("{}-query.{}", stem, format.name())
}

/// Strip any breadcrumb trail already on a sheet title so diving twice does not produce
/// `file › a › file › a › b`.
fn root_name(title: &str) -> String {
    title.split(" › ").next().unwrap_or(title).to_string()
}
