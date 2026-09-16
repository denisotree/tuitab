//! Wiring between the document tree and the rest of the app.
//!
//! [`DocState`] is what a sheet carries when its data came from JSON/JSONL/YAML/TOML:
//! a shared tree, the [`View`] projecting part of it, and the row/column bookkeeping
//! needed to turn a table cell back into a [`NodePath`].  Every sheet in a dive chain
//! shares the same tree, so an edit made three levels down is visible when you pop back.

use crate::data::dataframe::DataFrame;
use crate::data::doc::{Doc, Format, Node, NodePath, SaveOpts, Seg};
use crate::data::view::{cell_path, ColRole, View, ViewMode};
use crate::types::ColumnType;
use color_eyre::{eyre::eyre, Result};
use indexmap::IndexMap;
use polars::prelude::AnyValue;
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};

pub struct DocState {
    pub doc: Arc<RwLock<Doc>>,
    pub view: View,
    pub row_paths: Vec<NodePath>,
    pub col_roles: Vec<ColRole>,
}

impl DocState {
    pub fn open(path: &Path, format: Format) -> Result<(DataFrame, DocState)> {
        DocState::from_doc(Doc::load(path, format)?)
    }

    pub fn from_doc(doc: Doc) -> Result<(DataFrame, DocState)> {
        let view = View::auto(vec![], &doc.root);
        DocState::build(Arc::new(RwLock::new(doc)), view)
    }

    fn build(doc: Arc<RwLock<Doc>>, view: View) -> Result<(DataFrame, DocState)> {
        let proj = {
            let guard = doc.read().map_err(|_| eyre!("document lock poisoned"))?;
            view.project(&guard.root)?
        };
        Ok((
            proj.df,
            DocState {
                doc,
                view,
                row_paths: proj.row_paths,
                col_roles: proj.col_roles,
            },
        ))
    }

    /// Build a sheet anchored anywhere in a document you already hold a handle to.
    /// Used to jump straight to a search hit, where there is no parent sheet to dive
    /// from — only the tree and a path.
    pub fn open_at(doc: Arc<RwLock<Doc>>, anchor: NodePath) -> Result<(DataFrame, DocState)> {
        let view = {
            let guard = doc.read().map_err(|_| eyre!("document lock poisoned"))?;
            let node = guard
                .root
                .get(&anchor)
                .ok_or_else(|| eyre!("that node is gone"))?;
            View::auto(anchor, node)
        };
        DocState::build(doc, view)
    }

    /// Open a child sheet anchored at `anchor`, sharing this document.
    pub fn dive(&self, anchor: NodePath) -> Result<(DataFrame, DocState)> {
        let view = {
            let guard = self
                .doc
                .read()
                .map_err(|_| eyre!("document lock poisoned"))?;
            let node = guard
                .root
                .get(&anchor)
                .ok_or_else(|| eyre!("nothing to dive into"))?;
            if !node.is_container() {
                return Err(eyre!("{} is not a container", node.type_name()));
            }
            View::auto(anchor, node)
        };
        DocState::build(Arc::clone(&self.doc), view)
    }

    /// Re-run the projection after the tree changed shape.  Used when a whole node was
    /// replaced or the view mode changed — a plain cell edit patches in place instead.
    pub fn reproject(&mut self) -> Result<DataFrame> {
        let proj = {
            let guard = self
                .doc
                .read()
                .map_err(|_| eyre!("document lock poisoned"))?;
            self.view.project(&guard.root)?
        };
        self.row_paths = proj.row_paths;
        self.col_roles = proj.col_roles;
        Ok(proj.df)
    }

    pub fn set_mode(&mut self, mode: ViewMode) -> Result<DataFrame> {
        self.view.mode = mode;
        self.view.expanded.clear();
        self.reproject()
    }

    /// Replace the column with one column per child of its containers (`(`).
    pub fn expand_column(&mut self, col: usize) -> Result<DataFrame> {
        // Only record columns expand.  In key/value mode every row is a different node,
        // so there is no common set of children to make columns out of — diving is the
        // right move there, and the message has to say so rather than claim the column
        // holds no containers when it plainly does.
        let ColRole::Field(path) = self.column_role(col)? else {
            return Err(eyre!(
                "expand works on record columns — press Enter to dive in here"
            ));
        };
        if !self.column_has_container(col) {
            return Err(eyre!("nothing to expand — this column holds no containers"));
        }
        if !self.view.expand(path) {
            return Err(eyre!("already expanded"));
        }
        self.reproject()
    }

    /// Delete the given physical rows from the document.
    ///
    /// Unlike a column, a row *is* data: in records mode it is an array element, in
    /// key/value mode a key of the object.  Hiding it from the table without touching
    /// the tree — which is what the plain DataFrame path does — would look like a
    /// deletion and then quietly undo itself on the next save.
    ///
    /// Array indices are removed high-to-low so the earlier ones stay valid.
    pub fn delete_rows(&mut self, rows: &HashSet<usize>) -> Result<DataFrame> {
        let mut paths: Vec<NodePath> = rows
            .iter()
            .filter_map(|r| self.row_paths.get(*r).cloned())
            .collect();
        if paths.is_empty() {
            return Err(eyre!("nothing to delete"));
        }
        paths.sort_by(|a, b| match (a.last(), b.last()) {
            (Some(Seg::Idx(x)), Some(Seg::Idx(y))) => y.cmp(x),
            _ => std::cmp::Ordering::Equal,
        });
        {
            let mut guard = self
                .doc
                .write()
                .map_err(|_| eyre!("document lock poisoned"))?;
            for path in &paths {
                guard.root.remove(path)?;
            }
            guard.bump();
        }
        self.reproject()
    }

    /// Remove the key behind a record column from every record that has it (`zd`).
    ///
    /// Records are sparse, so a record without the key is skipped rather than being an
    /// error.  Returns the rebuilt table and how many records lost the key.
    pub fn delete_field(&mut self, col: usize) -> Result<(DataFrame, usize)> {
        let rel = self.field_path(col)?;
        let n = {
            let mut guard = self.write()?;
            let mut n = 0;
            for row in &self.row_paths {
                let path = join(row, &rel);
                if guard.root.get(&path).is_some() {
                    guard.root.remove(&path)?;
                    n += 1;
                }
            }
            if n == 0 {
                return Err(eyre!("no record holds this key"));
            }
            guard.bump();
            n
        };
        Ok((self.reproject()?, n))
    }

    /// Rename the key behind a record column in every record that has it (`ze`).
    ///
    /// All or nothing: a clash in any one record refuses the whole rename, since a
    /// half-renamed column would be two columns that each look like the other's typo.
    pub fn rename_field(&mut self, col: usize, new_key: &str) -> Result<(DataFrame, usize)> {
        let rel = self.field_key_path(col)?;
        let (parent_rel, old_key) = split_key(&rel);
        if new_key.is_empty() {
            return Err(eyre!("key cannot be empty"));
        }
        if new_key == old_key {
            return Err(eyre!("name unchanged"));
        }
        let n = {
            let mut guard = self.write()?;
            let mut targets = Vec::new();
            for (i, row) in self.row_paths.iter().enumerate() {
                let parent = join(row, parent_rel);
                let Some(Node::Obj(map)) = guard.root.get(&parent) else {
                    continue;
                };
                if !map.contains_key(old_key) {
                    continue;
                }
                if map.contains_key(new_key) {
                    return Err(eyre!("record {} already has a key `{}`", i + 1, new_key));
                }
                targets.push(join(row, &rel));
            }
            if targets.is_empty() {
                return Err(eyre!("no record holds this key"));
            }
            for path in &targets {
                rename_key(&mut guard.root, path, new_key)?;
            }
            guard.bump();
            targets.len()
        };
        // An expansion under the old name would otherwise stop applying.
        for p in &mut self.view.expanded {
            if p.len() >= rel.len() && p[..rel.len()] == rel[..] {
                p[rel.len() - 1] = Seg::Key(new_key.to_string());
            }
        }
        Ok((self.reproject()?, n))
    }

    /// Add an empty key to every record, next to the cursor column (`zi`).
    ///
    /// The key goes right before the cursor column's key in each record that has it,
    /// and at the end of the others.  On the bare `value` column it goes first.  The
    /// value is null — TOML cannot hold one, so there it is only written once filled.
    ///
    /// Returns the table, how many records got the key, and the new column's index.
    pub fn insert_field(&mut self, col: usize, key: &str) -> Result<(DataFrame, usize, usize)> {
        if self.view.mode != ViewMode::Records {
            return Err(eyre!("columns can only be added to a records view"));
        }
        if key.is_empty() {
            return Err(eyre!("key cannot be empty"));
        }
        let (parent_rel, before): (Vec<Seg>, Option<String>) = match self.column_role(col)? {
            ColRole::Field(_) => {
                let rel = self.field_key_path(col)?;
                let (parent, k) = split_key(&rel);
                (parent.to_vec(), Some(k.to_string()))
            }
            _ => (Vec::new(), None),
        };
        let n = {
            let mut guard = self.write()?;
            let mut parents = Vec::new();
            for row in &self.row_paths {
                let parent = join(row, &parent_rel);
                let Some(Node::Obj(map)) = guard.root.get(&parent) else {
                    continue;
                };
                if map.contains_key(key) {
                    return Err(eyre!("column `{}` already exists", key));
                }
                parents.push(parent);
            }
            if parents.is_empty() {
                return Err(eyre!("no record to add the key to"));
            }
            for parent in &parents {
                let Some(Node::Obj(map)) = guard.root.get_mut(parent) else {
                    continue;
                };
                let at = match &before {
                    Some(k) => map.get_index_of(k.as_str()).unwrap_or(map.len()),
                    None => 0,
                };
                map.shift_insert(at, key.to_string(), Node::Null);
            }
            guard.bump();
            parents.len()
        };
        let df = self.reproject()?;
        let new_col = ColRole::Field(join(&parent_rel, &[Seg::Key(key.to_string())]));
        let at = self
            .col_roles
            .iter()
            .position(|r| *r == new_col)
            .unwrap_or(col);
        Ok((df, n, at))
    }

    /// Swap the key behind column `col` with the one behind `other` in every record
    /// holding both (`z←` / `z→`).  Returns the table and where the moved column is now.
    ///
    /// Column order is the order keys are first seen across records, so a swap can
    /// leave it unchanged (the neighbour's position comes from a record without the
    /// moved key).  That is refused and rolled back rather than reported as a move.
    pub fn move_field(&mut self, col: usize, other: usize) -> Result<(DataFrame, usize)> {
        let rel = self.field_key_path(col)?;
        let other_rel = self
            .field_key_path(other)
            .map_err(|_| eyre!("cannot move past this column"))?;
        let (parent_rel, key) = split_key(&rel);
        let (other_parent, other_key) = split_key(&other_rel);
        if parent_rel != other_parent {
            return Err(eyre!("cannot move out of an expanded column"));
        }
        // A swap is its own inverse, so the rollback runs the same pass again.
        let swap = |st: &Self| -> Result<usize> {
            let mut guard = st.write()?;
            let mut swapped = 0;
            for row in &st.row_paths {
                let Some(Node::Obj(map)) = guard.root.get_mut(&join(row, parent_rel)) else {
                    continue;
                };
                if let (Some(a), Some(b)) = (map.get_index_of(key), map.get_index_of(other_key)) {
                    map.swap_indices(a, b);
                    swapped += 1;
                }
            }
            Ok(swapped)
        };
        if swap(self)? == 0 {
            return Err(eyre!("no record holds both keys, so their order is fixed"));
        }
        // Projected before the revision is bumped: a refused move must not tell open
        // search results that the document changed.
        let proj = {
            let guard = self
                .doc
                .read()
                .map_err(|_| eyre!("document lock poisoned"))?;
            self.view.project(&guard.root)?
        };
        let moved = ColRole::Field(rel.clone());
        match proj.col_roles.iter().position(|r| *r == moved) {
            Some(at) if at != col => {
                self.write()?.bump();
                self.row_paths = proj.row_paths;
                self.col_roles = proj.col_roles;
                Ok((proj.df, at))
            }
            _ => {
                swap(self)?;
                Err(eyre!(
                    "column order follows the first record holding each key — edit it with E"
                ))
            }
        }
    }

    /// Key under the cursor column, for prefilling a rename.
    pub fn field_key(&self, col: usize) -> Option<String> {
        match self.field_key_path(col).ok()?.last() {
            Some(Seg::Key(k)) => Some(k.clone()),
            _ => None,
        }
    }

    /// Path (relative to the row) of a record column, or why this column is not one.
    fn field_path(&self, col: usize) -> Result<Vec<Seg>> {
        match self.column_role(col)? {
            ColRole::Field(path) => Ok(path),
            ColRole::Row if self.view.mode == ViewMode::Records => Err(eyre!(
                "this column holds the non-object rows themselves — delete rows instead"
            )),
            _ => Err(eyre!(
                "a {} view has fixed columns — press m for records or E to edit the document",
                match self.view.mode {
                    ViewMode::KeyValue => "key/value",
                    _ => "list",
                }
            )),
        }
    }

    /// Like [`Self::field_path`], but only for a column that ends in an object key.
    fn field_key_path(&self, col: usize) -> Result<Vec<Seg>> {
        let path = self.field_path(col)?;
        match path.last() {
            Some(Seg::Key(_)) => Ok(path),
            _ => Err(eyre!("this column is a list position, not a key")),
        }
    }

    fn write(&self) -> Result<std::sync::RwLockWriteGuard<'_, Doc>> {
        self.doc
            .write()
            .map_err(|_| eyre!("document lock poisoned"))
    }

    /// Fold the innermost expansion covering this column back into one column (`)`).
    pub fn contract_column(&mut self, col: usize) -> Result<DataFrame> {
        let ColRole::Field(path) = self.column_role(col)? else {
            return Err(eyre!("nothing to contract here"));
        };
        if !self.view.contract_one(&path) {
            return Err(eyre!("this column is not expanded"));
        }
        self.reproject()
    }

    fn column_role(&self, col: usize) -> Result<ColRole> {
        self.col_roles
            .get(col)
            .cloned()
            .ok_or_else(|| eyre!("no such column"))
    }

    fn column_has_container(&self, col: usize) -> bool {
        (0..self.row_paths.len())
            .filter_map(|r| self.node_at(r, col))
            .any(|n| n.is_container())
    }

    /// Modes offered by `m` for the currently anchored node.
    pub fn available_modes(&self) -> Vec<ViewMode> {
        let Ok(guard) = self.doc.read() else {
            return vec![self.view.mode];
        };
        match guard.root.get(&self.view.anchor) {
            Some(node) => View::modes_for(node),
            None => vec![self.view.mode],
        }
    }

    pub fn path_of(&self, row: usize, col: usize) -> Option<NodePath> {
        cell_path(&self.row_paths, &self.col_roles, row, col)
    }

    pub fn node_at(&self, row: usize, col: usize) -> Option<Node> {
        let path = self.path_of(row, col)?;
        let guard = self.doc.read().ok()?;
        guard.root.get(&path).cloned()
    }

    /// Write an edited scalar into the tree.  Returns the text the table cell should
    /// now show, so the caller can patch the DataFrame without a full reprojection
    /// (which would drop the user's sort and selection).
    pub fn set_cell(&mut self, row: usize, col: usize, text: &str) -> Result<String> {
        let role = self
            .col_roles
            .get(col)
            .ok_or_else(|| eyre!("no such column"))?
            .clone();
        if role == ColRole::Type {
            return Err(eyre!("the type column is read-only"));
        }
        let path = self
            .path_of(row, col)
            .ok_or_else(|| eyre!("this cell does not address a node"))?;

        let mut guard = self
            .doc
            .write()
            .map_err(|_| eyre!("document lock poisoned"))?;

        if role == ColRole::Key {
            let out = rename_key(&mut guard.root, &path, text).map(|_| text.to_string());
            if out.is_ok() {
                guard.bump();
            }
            return out;
        }

        let old = guard.root.get(&path).cloned();
        if matches!(old, Some(ref n) if n.is_container()) {
            return Err(eyre!("press E to edit this container in $EDITOR"));
        }
        let new = Node::parse_scalar(text, old.as_ref());
        let shown = new.to_cell_string();
        guard.root.set(&path, new)?;
        guard.bump();
        Ok(shown)
    }

    /// Serialise the node under the cursor in the document's own format, for the
    /// "edit node as text" popup.
    pub fn node_text(&self, path: &[Seg]) -> Result<String> {
        let guard = self
            .doc
            .read()
            .map_err(|_| eyre!("document lock poisoned"))?;
        let node = guard
            .root
            .get(path)
            .ok_or_else(|| eyre!("no node at that path"))?;
        let fmt = text_edit_format(guard.format, node);
        crate::data::doc::serialize(node, fmt, false, &SaveOpts::default())
    }

    /// Parse the text from the "edit node as text" popup and replace the node.
    /// On a parse error nothing is written and the caller keeps the popup open.
    pub fn set_node_text(&mut self, path: &[Seg], text: &str) -> Result<()> {
        let fmt = {
            let guard = self
                .doc
                .read()
                .map_err(|_| eyre!("document lock poisoned"))?;
            let node = guard
                .root
                .get(path)
                .ok_or_else(|| eyre!("no node at that path"))?;
            text_edit_format(guard.format, node)
        };
        let parsed = Doc::from_str(text, fmt)?.root;
        let mut guard = self
            .doc
            .write()
            .map_err(|_| eyre!("document lock poisoned"))?;
        guard.root.set(path, parsed)?;
        guard.bump();
        Ok(())
    }

    pub fn format(&self) -> Format {
        self.doc.read().map(|d| d.format).unwrap_or(Format::Json)
    }

    pub fn save(&self, path: &Path, format: Format, opts: &SaveOpts) -> Result<()> {
        let guard = self
            .doc
            .read()
            .map_err(|_| eyre!("document lock poisoned"))?;
        guard.save_as(path, format, opts)
    }

    /// Save, wrapping the document if the target format cannot hold its shape.
    ///
    /// Only TOML needs this: its top level must be a table, so a JSON array of records
    /// becomes an array of tables named after the sheet.  Without the wrap, converting
    /// `data.json` to `data.toml` would just fail with nothing the user could do about
    /// it, and conversion is the whole point of writing through the tree.
    pub fn save_wrapped(
        &self,
        path: &Path,
        format: Format,
        opts: &SaveOpts,
        name: &str,
    ) -> Result<()> {
        let guard = self
            .doc
            .read()
            .map_err(|_| eyre!("document lock poisoned"))?;
        if format == Format::Toml && !matches!(guard.root, Node::Obj(_)) {
            let wrapped = Doc {
                format,
                source_text: None,
                root: Node::Obj(
                    [(toml_table_name(name), guard.root.clone())]
                        .into_iter()
                        .collect::<IndexMap<_, _>>(),
                ),
                path: None,
                multi_doc: false,
                revision: 0,
            };
            return wrapped.save_as(path, format, opts);
        }
        guard.save_as(path, format, opts)
    }

    /// What this document loses if written as `target`, phrased for the status line.
    ///
    /// The caveats themselves live with the formats, in [`crate::data::doc::caveats`] —
    /// this only formats them.
    pub fn conversion_loss(&self, target: Format) -> Option<String> {
        let guard = self.doc.read().ok()?;
        let lost = crate::data::doc::conversion_caveats(&guard, target);
        if lost.is_empty() {
            return None;
        }
        Some(format!(
            "{} cannot carry {}",
            target.name().to_uppercase(),
            lost.join(" or ")
        ))
    }

    /// Breadcrumb trail shown in the sheet title: `config.toml › servers › [1]`.
    pub fn breadcrumbs(&self, root_name: &str) -> String {
        let mut s = String::from(root_name);
        for seg in &self.view.anchor {
            s.push_str(" › ");
            match seg {
                Seg::Key(k) => s.push_str(k),
                Seg::Idx(i) => s.push_str(&format!("[{}]", i)),
            }
        }
        s
    }
}

/// A TOML fragment is usually not a valid TOML document on its own (an array, a scalar,
/// a nested table with no name), so node-level text editing of a TOML file goes through
/// JSON.  Whole tables stay in TOML, which is what a user editing a config expects.
fn text_edit_format(doc_format: Format, node: &Node) -> Format {
    match doc_format {
        Format::Toml if !matches!(node, Node::Obj(_)) => Format::Json,
        Format::Jsonl => Format::Json,
        other => other,
    }
}

fn join(base: &[Seg], rel: &[Seg]) -> NodePath {
    base.iter().chain(rel).cloned().collect()
}

/// Split a path ending in a key into its parent and the key.  Callers have checked the
/// last segment is a key.
fn split_key(path: &[Seg]) -> (&[Seg], &str) {
    match path.split_last() {
        Some((Seg::Key(k), parent)) => (parent, k.as_str()),
        _ => unreachable!("checked by field_key_path"),
    }
}

/// Rename an object key in place, keeping its position in the key order.
fn rename_key(root: &mut Node, path: &[Seg], new_key: &str) -> Result<()> {
    let Some((Seg::Key(old_key), parents)) = path.split_last().map(|(l, p)| (l.clone(), p)) else {
        return Err(eyre!("only object keys can be renamed"));
    };
    if new_key.is_empty() {
        return Err(eyre!("key cannot be empty"));
    }
    let parent = root
        .get_mut(parents)
        .ok_or_else(|| eyre!("parent node is gone"))?;
    let Node::Obj(map) = parent else {
        return Err(eyre!("parent is not an object"));
    };
    if old_key == new_key {
        return Ok(());
    }
    if map.contains_key(new_key) {
        return Err(eyre!("key `{}` already exists", new_key));
    }
    let idx = map
        .get_index_of(old_key.as_str())
        .ok_or_else(|| eyre!("key `{}` is gone", old_key))?;
    let (_, value) = map.shift_remove_index(idx).expect("index just looked up");
    map.shift_insert(idx, new_key.to_string(), value);
    Ok(())
}

// ── table → document ─────────────────────────────────────────────────────────

/// How a plain table (CSV, Parquet, SQL, pivot — anything with no document behind it)
/// is turned into a tree before being written as JSON/YAML/TOML.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Shape {
    /// `[{col: val}, …]` — the default, and the only shape that survives a round trip.
    #[default]
    Records,
    /// `{col: [val, …]}`
    Columns,
    /// `{a: b}` — only for a two-column table.
    KeyValue,
}

impl Shape {
    pub fn label(&self) -> &'static str {
        match self {
            Shape::Records => "records",
            Shape::Columns => "columns",
            Shape::KeyValue => "key/value",
        }
    }

    /// One-line example of what this shape produces, shown next to the label.
    pub fn hint(&self) -> &'static str {
        match self {
            Shape::Records => "[{col: val}, …]  — round-trips",
            Shape::Columns => "{col: [val, …]}",
            Shape::KeyValue => "{a: b} — first column becomes the key",
        }
    }

    /// Shapes that make sense for a table of `ncols` columns, best first.
    pub fn options(ncols: usize) -> Vec<Shape> {
        let mut v = vec![Shape::Records, Shape::Columns];
        if ncols == 2 {
            v.push(Shape::KeyValue);
        }
        v
    }
}

/// Build a document from a table.
///
/// `table_name` is only used for TOML, whose top level must be a table: records become
/// an array of tables under that name.
pub fn table_to_doc(df: &DataFrame, shape: Shape, format: Format, table_name: &str) -> Result<Doc> {
    let ncols = df.col_count();
    let nrows = df.visible_row_count();
    if ncols == 0 {
        return Err(eyre!("nothing to save: the sheet has no columns"));
    }
    let names: Vec<String> = df.columns.iter().map(|c| c.name.clone()).collect();

    let root = match shape {
        Shape::Records => {
            let rows: Vec<Node> = (0..nrows)
                .map(|r| {
                    let mut obj = IndexMap::new();
                    for (c, name) in names.iter().enumerate() {
                        let v = cell_node(df, r, c);
                        // Every column is kept on the first row so the output carries a
                        // full header even when later rows are sparse — the same reason
                        // VisiData passes keep_nulls only for row 0.
                        if r == 0 || !matches!(v, Node::Null) {
                            obj.insert(name.clone(), v);
                        }
                    }
                    Node::Obj(obj)
                })
                .collect();
            let arr = Node::Arr(rows);
            if format == Format::Toml {
                Node::Obj(
                    [(toml_table_name(table_name), arr)]
                        .into_iter()
                        .collect::<IndexMap<_, _>>(),
                )
            } else {
                arr
            }
        }
        Shape::Columns => Node::Obj(
            (0..ncols)
                .map(|c| {
                    let col: Vec<Node> = (0..nrows).map(|r| cell_node(df, r, c)).collect();
                    (names[c].clone(), Node::Arr(col))
                })
                .collect(),
        ),
        Shape::KeyValue => {
            if ncols != 2 {
                return Err(eyre!(
                    "key/value shape needs exactly 2 columns, this sheet has {}",
                    ncols
                ));
            }
            let mut obj = IndexMap::new();
            for r in 0..nrows {
                let key = match cell_node(df, r, 0) {
                    Node::Null => continue,
                    k => k.to_cell_string(),
                };
                obj.insert(key, cell_node(df, r, 1));
            }
            Node::Obj(obj)
        }
    };

    Ok(Doc {
        format,
        root,
        path: None,
        source_text: None,
        multi_doc: false,
        revision: 0,
    })
}

/// TOML array-of-tables needs a name; fall back to `rows` when the sheet title yields
/// nothing usable.  Keys are quoted by the serialiser if they are not bare-legal, so no
/// sanitising is needed beyond dropping whitespace and any extension the sheet title
/// carries (a sheet is usually named after its file).
fn toml_table_name(name: &str) -> String {
    let stem = name.rsplit('/').next().unwrap_or(name);
    let stem = stem.split('.').next().unwrap_or(stem);
    let cleaned: String = stem
        .trim()
        .chars()
        .map(|c| if c.is_whitespace() { '_' } else { c })
        .collect();
    if cleaned.is_empty() {
        "rows".to_string()
    } else {
        cleaned
    }
}

fn cell_node(df: &DataFrame, display_row: usize, col: usize) -> Node {
    match df.get_val(display_row, col) {
        AnyValue::Null => Node::Null,
        AnyValue::Boolean(b) => Node::Bool(b),
        AnyValue::Int8(i) => Node::Int(i as i64),
        AnyValue::Int16(i) => Node::Int(i as i64),
        AnyValue::Int32(i) => Node::Int(i as i64),
        AnyValue::Int64(i) => Node::Int(i),
        AnyValue::UInt8(i) => Node::Int(i as i64),
        AnyValue::UInt16(i) => Node::Int(i as i64),
        AnyValue::UInt32(i) => Node::Int(i as i64),
        AnyValue::UInt64(i) => Node::Int(i as i64),
        AnyValue::Float32(f) => Node::Float(f as f64),
        AnyValue::Float64(f) => Node::Float(f),
        AnyValue::String(s) => scalar_from_text(df, col, s),
        AnyValue::StringOwned(s) => scalar_from_text(df, col, s.as_str()),
        other => {
            let s = other.to_string();
            if s.is_empty() {
                Node::Null
            } else {
                Node::Str(s)
            }
        }
    }
}

/// A string cell in a column typed as a date keeps its text; everything else stays a
/// string.  We deliberately do not sniff numbers out of text columns — a zip code must
/// not become an integer on export.
fn scalar_from_text(df: &DataFrame, col: usize, s: &str) -> Node {
    if s.is_empty() {
        return Node::Null;
    }
    match df.columns.get(col).map(|c| c.col_type) {
        Some(ColumnType::Date) | Some(ColumnType::Datetime) => Node::DateTime(s.to_string()),
        _ => Node::Str(s.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::doc::Format;

    fn state(src: &str, f: Format) -> (DataFrame, DocState) {
        DocState::from_doc(Doc::from_str(src, f).unwrap()).unwrap()
    }

    #[test]
    fn editing_a_cell_writes_through_to_the_tree() {
        let (df, mut st) = state(r#"[{"a":1},{"a":2}]"#, Format::Json);
        assert_eq!(df.get_physical(0, 0), "1");
        assert_eq!(st.set_cell(0, 0, "42").unwrap(), "42");
        let out = st
            .doc
            .read()
            .unwrap()
            .to_string_as(
                Format::Json,
                &SaveOpts {
                    indent: false,
                    sort_keys: false,
                },
            )
            .unwrap();
        assert_eq!(out.trim(), r#"[{"a":42},{"a":2}]"#);
    }

    #[test]
    fn string_cells_stay_strings_when_edited() {
        let (_df, mut st) = state(r#"[{"v":"1.0"}]"#, Format::Json);
        st.set_cell(0, 0, "2.0").unwrap();
        let out = st
            .doc
            .read()
            .unwrap()
            .to_string_as(
                Format::Json,
                &SaveOpts {
                    indent: false,
                    sort_keys: false,
                },
            )
            .unwrap();
        assert_eq!(out.trim(), r#"[{"v":"2.0"}]"#, "must not become a number");
    }

    #[test]
    fn container_cells_refuse_inline_edits() {
        let (_df, mut st) = state(r#"{"db":{"host":"h"}}"#, Format::Json);
        let err = st.set_cell(0, 1, "nonsense").unwrap_err().to_string();
        assert!(err.contains("E to edit"), "{}", err);
    }

    #[test]
    fn key_column_renames_and_keeps_position() {
        let (_df, mut st) = state("a = 1\nb = 2\nc = 3\n", Format::Toml);
        st.set_cell(1, 0, "bb").unwrap();
        let out = st
            .doc
            .read()
            .unwrap()
            .to_string_as(Format::Toml, &SaveOpts::default())
            .unwrap();
        let keys: Vec<&str> = out
            .lines()
            .filter_map(|l| l.split(" =").next())
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(keys, vec!["a", "bb", "c"], "{}", out);
    }

    #[test]
    fn renaming_onto_an_existing_key_is_refused() {
        let (_df, mut st) = state("a = 1\nb = 2\n", Format::Toml);
        assert!(st.set_cell(1, 0, "a").is_err());
    }

    #[test]
    fn diving_shares_the_tree_so_edits_are_visible_from_the_parent() {
        let (_df, parent) = state(r#"{"servers":[{"host":"a"}]}"#, Format::Json);
        let (child_df, mut child) = parent.dive(vec![Seg::Key("servers".into())]).unwrap();
        assert_eq!(child_df.get_physical(0, 0), "a");
        child.set_cell(0, 0, "b").unwrap();
        let out = parent
            .doc
            .read()
            .unwrap()
            .to_string_as(
                Format::Json,
                &SaveOpts {
                    indent: false,
                    sort_keys: false,
                },
            )
            .unwrap();
        assert_eq!(out.trim(), r#"{"servers":[{"host":"b"}]}"#);
    }

    #[test]
    fn node_text_edit_replaces_a_whole_subtree() {
        let (_df, mut st) = state(r#"{"db":{"host":"h"}}"#, Format::Json);
        let path = vec![Seg::Key("db".into())];
        assert_eq!(
            st.node_text(&path).unwrap().trim(),
            "{\n  \"host\": \"h\"\n}"
        );
        st.set_node_text(&path, r#"{"host":"h2","port":5432}"#)
            .unwrap();
        let out = st
            .doc
            .read()
            .unwrap()
            .to_string_as(
                Format::Json,
                &SaveOpts {
                    indent: false,
                    sort_keys: false,
                },
            )
            .unwrap();
        assert_eq!(out.trim(), r#"{"db":{"host":"h2","port":5432}}"#);
    }

    #[test]
    fn a_bad_node_text_edit_changes_nothing() {
        let (_df, mut st) = state(r#"{"db":{"host":"h"}}"#, Format::Json);
        let path = vec![Seg::Key("db".into())];
        assert!(st.set_node_text(&path, "{not json").is_err());
        let out = st
            .doc
            .read()
            .unwrap()
            .to_string_as(
                Format::Json,
                &SaveOpts {
                    indent: false,
                    sort_keys: false,
                },
            )
            .unwrap();
        assert_eq!(out.trim(), r#"{"db":{"host":"h"}}"#);
    }

    #[test]
    fn switching_view_mode_reprojects() {
        let (df, mut st) = state(r#"{"a":1,"b":2}"#, Format::Json);
        assert_eq!(df.df.width(), 3, "key/value by default");
        let df = st.set_mode(ViewMode::Records).unwrap();
        assert_eq!(df.df.width(), 2, "records shows one column per key");
        assert_eq!(df.df.height(), 1);
    }

    fn json(st: &DocState) -> String {
        st.doc
            .read()
            .unwrap()
            .to_string_as(
                Format::Json,
                &SaveOpts {
                    indent: false,
                    sort_keys: false,
                },
            )
            .unwrap()
            .trim()
            .to_string()
    }

    fn names(df: &DataFrame) -> Vec<String> {
        df.columns.iter().map(|c| c.name.clone()).collect()
    }

    #[test]
    fn deleting_a_column_removes_the_key_from_sparse_records() {
        let (_df, mut st) = state(r#"[{"a":1,"b":2},{"a":3},{"b":4,"a":5}]"#, Format::Json);
        let (df, n) = st.delete_field(1).unwrap();
        assert_eq!(n, 2, "the record without `b` is skipped, not an error");
        assert_eq!(json(&st), r#"[{"a":1},{"a":3},{"a":5}]"#);
        assert_eq!(names(&df), vec!["a"]);
        assert_eq!(st.col_roles.len(), df.columns.len());
    }

    #[test]
    fn deleting_the_only_key_leaves_empty_records() {
        let (_df, mut st) = state(r#"[{"a":1},{"a":2}]"#, Format::Json);
        let (df, _) = st.delete_field(0).unwrap();
        assert_eq!(json(&st), r#"[{},{}]"#);
        assert_eq!(df.visible_row_count(), 2, "rows survive their last key");
        assert_eq!(st.col_roles.len(), df.columns.len());
    }

    #[test]
    fn deleting_an_expanded_child_column_reaches_the_nested_key() {
        let (_df, mut st) = state(r#"[{"m":{"x":1,"y":2}},{"m":{"x":3}}]"#, Format::Json);
        st.expand_column(0).unwrap();
        st.delete_field(1).unwrap(); // m.y
        let (df, _) = st.delete_field(0).unwrap(); // m.x
        assert_eq!(json(&st), r#"[{"m":{}},{"m":{}}]"#);
        assert_eq!(
            names(&df),
            vec!["m"],
            "an emptied expansion shows its parent"
        );
    }

    #[test]
    fn column_ops_refuse_columns_that_are_not_keys() {
        // key/value: every column is structural
        let (_df, mut st) = state(r#"{"a":1,"b":2}"#, Format::Json);
        for col in 0..3 {
            assert!(st.delete_field(col).is_err());
            assert!(st.rename_field(col, "z").is_err());
        }
        assert!(st.insert_field(0, "z").is_err());
        // list of scalars: the one column is the rows
        let (_df, mut st) = state("[1,2]", Format::Json);
        assert!(st.delete_field(0).is_err());
        assert!(st.insert_field(0, "z").is_err());
        // the bare column of non-object rows in a records view
        let (_df, mut st) = state(r#"[{"a":1},7]"#, Format::Json);
        assert!(st.delete_field(0).is_err());
        assert!(st.rename_field(0, "z").is_err());
        // an expanded list position is not a key
        let (_df, mut st) = state(r#"[{"t":["p","q"]}]"#, Format::Json);
        st.expand_column(0).unwrap();
        assert!(st.rename_field(1, "z").is_err());
        assert_eq!(json(&st), r#"[{"t":["p","q"]}]"#, "refusals change nothing");
    }

    #[test]
    fn renaming_a_column_renames_the_key_in_place_everywhere() {
        let (_df, mut st) = state(r#"[{"a":1,"b":2,"c":3},{"c":4},{"b":5}]"#, Format::Json);
        let (df, n) = st.rename_field(1, "bb").unwrap();
        assert_eq!(n, 2);
        assert_eq!(json(&st), r#"[{"a":1,"bb":2,"c":3},{"c":4},{"bb":5}]"#);
        assert_eq!(names(&df), vec!["a", "bb", "c"]);
    }

    #[test]
    fn a_rename_clashing_in_one_record_changes_nothing() {
        let src = r#"[{"a":1},{"a":2,"b":3}]"#;
        let (_df, mut st) = state(src, Format::Json);
        let err = st.rename_field(0, "b").err().unwrap().to_string();
        assert!(err.contains("record 2"), "{}", err);
        assert_eq!(json(&st), src);
        assert!(st.rename_field(0, "a").is_err(), "unchanged name");
        assert!(st.rename_field(0, "").is_err());
    }

    #[test]
    fn renaming_an_expanded_column_keeps_it_expanded() {
        let (_df, mut st) = state(r#"[{"m":{"x":{"k":1}}}]"#, Format::Json);
        st.expand_column(0).unwrap();
        st.expand_column(0).unwrap();
        // m.x.k — rename `m` is not reachable, rename the leaf
        let (df, _) = st.rename_field(0, "kk").unwrap();
        assert_eq!(names(&df), vec!["m.x.kk"]);
        assert_eq!(json(&st), r#"[{"m":{"x":{"kk":1}}}]"#);
    }

    #[test]
    fn inserting_a_column_adds_a_null_key_before_the_cursor_key() {
        let (_df, mut st) = state(r#"[{"a":1,"b":2},{"b":3},7]"#, Format::Json);
        let (df, n, at) = st.insert_field(2, "new").unwrap();
        assert_eq!(at, 2); // cursor on `b` (col 0 is bare)
        assert_eq!(n, 2, "the scalar row has nowhere to hold a key");
        assert_eq!(
            json(&st),
            r#"[{"a":1,"new":null,"b":2},{"new":null,"b":3},7]"#
        );
        assert_eq!(names(&df), vec!["value", "a", "new", "b"]);
        assert_eq!(st.col_roles.len(), df.columns.len());
        // and the new cell is editable into a typed value
        st.set_cell(0, 2, "42").unwrap();
        assert!(
            json(&st).starts_with(r#"[{"a":1,"new":42,"#),
            "{}",
            json(&st)
        );
    }

    #[test]
    fn inserting_an_existing_or_empty_key_is_refused() {
        let src = r#"[{"a":1},{"b":2}]"#;
        let (_df, mut st) = state(src, Format::Json);
        assert!(st.insert_field(0, "b").is_err(), "exists in another record");
        assert!(st.insert_field(0, "").is_err());
        assert_eq!(json(&st), src);
        let (_df, mut st) = state("[]", Format::Json);
        assert!(st.insert_field(0, "a").is_err(), "no records at all");
    }

    #[test]
    fn inserting_on_a_lone_object_in_records_mode_works() {
        let (_df, mut st) = state(r#"{"a":1}"#, Format::Json);
        st.set_mode(ViewMode::Records).unwrap();
        st.insert_field(0, "z").unwrap();
        assert_eq!(json(&st), r#"{"z":null,"a":1}"#);
    }

    #[test]
    fn moving_a_column_swaps_the_keys_in_each_record() {
        let (_df, mut st) = state(r#"[{"a":1,"b":2},{"a":3,"b":4}]"#, Format::Json);
        let (df, at) = st.move_field(0, 1).unwrap();
        assert_eq!(at, 1);
        assert_eq!(names(&df), vec!["b", "a"]);
        assert_eq!(json(&st), r#"[{"b":2,"a":1},{"b":4,"a":3}]"#);
    }

    #[test]
    fn a_move_the_key_order_cannot_show_is_rolled_back() {
        // columns a, c, b: `c`'s place comes from record 0, which has no `b`
        let src = r#"[{"a":1,"c":2},{"a":3,"b":4,"c":5}]"#;
        let (_df, mut st) = state(src, Format::Json);
        assert!(st.move_field(2, 1).is_err());
        assert_eq!(json(&st), src);
        assert_eq!(
            st.doc.read().unwrap().revision,
            0,
            "open search hits stay valid"
        );
        let (_df, mut st) = state(r#"[{"a":1},{"b":2}]"#, Format::Json);
        assert!(st.move_field(0, 1).is_err(), "no record holds both");
    }

    #[test]
    fn column_ops_write_through_to_toml() {
        let (_df, mut st) = state(
            "[[s]]\nh = \"a\"\np = 1\n\n[[s]]\nh = \"b\"\n",
            Format::Toml,
        );
        let (_df, child) = st.dive(vec![Seg::Key("s".into())]).unwrap();
        st = child;
        st.rename_field(0, "host").unwrap();
        st.delete_field(1).unwrap();
        let out = st
            .doc
            .read()
            .unwrap()
            .to_string_as(Format::Toml, &SaveOpts::default())
            .unwrap();
        assert!(
            out.contains("host = \"a\"") && out.contains("host = \"b\""),
            "{}",
            out
        );
        assert!(!out.contains("p = 1") && !out.contains("h ="), "{}", out);
    }

    #[test]
    fn table_to_toml_records_becomes_an_array_of_tables() {
        let (df, _) = state(r#"[{"a":1},{"a":2}]"#, Format::Json);
        let doc = table_to_doc(&df, Shape::Records, Format::Toml, "my sheet").unwrap();
        let out = doc
            .to_string_as(Format::Toml, &SaveOpts::default())
            .unwrap();
        assert!(out.contains("[[my_sheet]]"), "{}", out);
        assert_eq!(out.matches("[[my_sheet]]").count(), 2, "{}", out);
    }

    #[test]
    fn table_to_json_keeps_the_full_header_on_the_first_row_only() {
        let (df, _) = state(r#"[{"a":1,"b":null},{"a":2,"b":null}]"#, Format::Json);
        let doc = table_to_doc(&df, Shape::Records, Format::Json, "x").unwrap();
        let out = doc
            .to_string_as(
                Format::Json,
                &SaveOpts {
                    indent: false,
                    sort_keys: false,
                },
            )
            .unwrap();
        assert_eq!(out.trim(), r#"[{"a":1,"b":null},{"a":2}]"#);
    }

    #[test]
    fn key_value_shape_needs_two_columns() {
        let (df, _) = state(r#"[{"a":1,"b":2,"c":3}]"#, Format::Json);
        assert!(table_to_doc(&df, Shape::KeyValue, Format::Json, "x").is_err());
    }
}
