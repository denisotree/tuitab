//! Projection of a [`Doc`] subtree into a table.
//!
//! A [`View`] names *which* node is being shown (`anchor`) and *how* (`mode`).  Building
//! it produces a throwaway [`DataFrame`] for rendering plus the bookkeeping needed to map
//! a table cell back to the [`NodePath`] it came from — that mapping is what makes editing
//! write into the tree rather than into the table.

use crate::data::dataframe::DataFrame;
use crate::data::doc::{Node, NodePath, Seg};
use color_eyre::{eyre::eyre, Result};
use indexmap::IndexSet;
use polars::prelude::*;

/// How a subtree is laid out as rows and columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewMode {
    /// Array of objects → one row per element, columns are the union of keys.
    Records,
    /// Object → one row per key; columns are `key` / `value` / `type`.
    ///
    /// This is deliberately different from VisiData, which shows an object as a single
    /// very wide row.  A 40-key config file is readable as 40 rows and unreadable as
    /// one row of 40 columns.  The cost: the `value` column is heterogeneous by
    /// construction, so typing, sorting and aggregating it are meaningless.
    KeyValue,
    /// Array of scalars → a single column.
    Scalars,
}

/// What a column of the projection means, and therefore where an edit to it goes.
#[derive(Debug, Clone, PartialEq)]
pub enum ColRole {
    /// Cell node is at `path` relative to the row node.  Normally one segment; an
    /// expanded column carries the whole chain (`meta`, `ok` for `meta.ok`).
    Field(Vec<Seg>),
    /// Cell node is the row node itself.
    Row,
    /// The object key naming this row; editing it renames the key.
    Key,
    /// Read-only type name of the row node.
    Type,
}

#[derive(Debug, Clone)]
pub struct View {
    pub anchor: NodePath,
    pub mode: ViewMode,
    /// Paths (relative to the row node) shown as their children instead of as one
    /// container cell — the state behind `(` / `)`.  Kept on the view, not on the
    /// columns, so it survives every reprojection.
    pub expanded: Vec<Vec<Seg>>,
}

/// A built projection: the table plus the mapping back into the tree.
pub struct Projection {
    pub df: DataFrame,
    /// Absolute path of each row's node, indexed by physical row.
    pub row_paths: Vec<NodePath>,
    /// Role of each column, indexed by column.
    pub col_roles: Vec<ColRole>,
}

impl View {
    /// Pick the layout that fits `node`, matching the DWIM rules of the loaders it
    /// replaces: arrays of objects become records, objects become key/value rows,
    /// everything else becomes a single column.
    pub fn auto_mode(node: &Node) -> ViewMode {
        match node {
            Node::Arr(items) => {
                if items.iter().any(|n| matches!(n, Node::Obj(_))) {
                    ViewMode::Records
                } else {
                    ViewMode::Scalars
                }
            }
            Node::Obj(_) => ViewMode::KeyValue,
            _ => ViewMode::Scalars,
        }
    }

    pub fn auto(anchor: NodePath, node: &Node) -> View {
        View {
            mode: View::auto_mode(node),
            anchor,
            expanded: Vec::new(),
        }
    }

    /// Modes worth offering for `node`, in the order the `m` key cycles them.
    pub fn modes_for(node: &Node) -> Vec<ViewMode> {
        match node {
            Node::Arr(_) => vec![ViewMode::Records, ViewMode::Scalars, ViewMode::KeyValue],
            Node::Obj(_) => vec![ViewMode::KeyValue, ViewMode::Records],
            _ => vec![ViewMode::Scalars],
        }
    }

    pub fn project(&self, root: &Node) -> Result<Projection> {
        let node = root
            .get(&self.anchor)
            .ok_or_else(|| eyre!("no node at the anchor path"))?;
        match self.mode {
            ViewMode::Records => self.project_records(node),
            ViewMode::KeyValue => self.project_keyvalue(node),
            ViewMode::Scalars => self.project_scalars(node),
        }
    }

    fn project_records(&self, node: &Node) -> Result<Projection> {
        // Rows: array elements, or the single object itself when a lone record is
        // viewed as a table of one.
        let (rows, row_paths): (Vec<&Node>, Vec<NodePath>) = match node {
            Node::Arr(items) => (
                items.iter().collect(),
                (0..items.len())
                    .map(|i| child_path(&self.anchor, Seg::Idx(i)))
                    .collect(),
            ),
            other => (vec![other], vec![self.anchor.clone()]),
        };

        // Columns are discovered incrementally, in order of first appearance, exactly
        // as VisiData's JsonSheet does — rows are legitimately sparse and heterogeneous.
        let mut keys: IndexSet<String> = IndexSet::new();
        for r in &rows {
            if let Node::Obj(m) = r {
                for k in m.keys() {
                    keys.insert(k.clone());
                }
            }
        }
        // Rows that are not objects (scalars, nested arrays) still need somewhere to go.
        // That extra column is identified by position, never by name: an object key
        // literally called `value` must stay an ordinary field, or editing it would
        // replace the whole row instead of one key.
        let needs_bare_col = rows.iter().any(|r| !matches!(r, Node::Obj(_))) || keys.is_empty();

        let mut names: Vec<String> = Vec::with_capacity(keys.len() + 1);
        let mut col_roles: Vec<ColRole> = Vec::with_capacity(keys.len() + 1);
        let mut cells: Vec<Vec<Option<&Node>>> = Vec::with_capacity(keys.len() + 1);

        if needs_bare_col {
            names.push(unique_name(DEFAULT_COLNAME, &keys));
            col_roles.push(ColRole::Row);
            cells.push(
                rows.iter()
                    .map(|r| {
                        if matches!(r, Node::Obj(_)) {
                            None
                        } else {
                            Some(*r)
                        }
                    })
                    .collect(),
            );
        }
        for key in &keys {
            let path = vec![Seg::Key(key.clone())];
            // An expanded column is replaced in place by its children, so the expansion
            // reads as a wider version of the same column rather than a new group at
            // the end.  Children of children are expanded too, recursively.
            self.push_column(&path, &rows, &mut names, &mut col_roles, &mut cells);
        }

        Ok(Projection {
            df: build_df(&names, &cells)?,
            row_paths,
            col_roles,
        })
    }

    /// Emit one column for `path`, or — if it is expanded — the columns of its children.
    fn push_column<'a>(
        &self,
        path: &[Seg],
        rows: &[&'a Node],
        names: &mut Vec<String>,
        col_roles: &mut Vec<ColRole>,
        cells: &mut Vec<Vec<Option<&'a Node>>>,
    ) {
        let values: Vec<Option<&Node>> = rows.iter().map(|r| r.get(path)).collect();

        if self.is_expanded(path) {
            let children = child_segments(&values);
            if !children.is_empty() {
                for seg in children {
                    let mut child = path.to_vec();
                    child.push(seg);
                    self.push_column(&child, rows, names, col_roles, cells);
                }
                return;
            }
            // Nothing to expand into any more (the values stopped being containers);
            // fall through and show the column as it is rather than dropping it.
        }

        names.push(crate::data::doc::path_to_string(path));
        col_roles.push(ColRole::Field(path.to_vec()));
        cells.push(values);
    }

    fn is_expanded(&self, path: &[Seg]) -> bool {
        self.expanded.iter().any(|p| p == path)
    }

    /// Mark `path` expanded.  Returns false if it was already.
    pub fn expand(&mut self, path: Vec<Seg>) -> bool {
        if self.is_expanded(&path) {
            return false;
        }
        self.expanded.push(path);
        true
    }

    /// Collapse `path` and everything expanded beneath it.  Returns false if nothing
    /// was expanded there.
    pub fn contract(&mut self, path: &[Seg]) -> bool {
        let before = self.expanded.len();
        self.expanded
            .retain(|p| !(p.len() >= path.len() && &p[..path.len()] == path));
        self.expanded.len() != before
    }

    /// Collapse one level: the deepest expansion that is a prefix of `path`, so `)` on
    /// `a.b.c` folds `a.b` back into a single column and leaves `a` expanded.
    pub fn contract_one(&mut self, path: &[Seg]) -> bool {
        let Some(target) = self
            .expanded
            .iter()
            .filter(|p| p.len() <= path.len() && path[..p.len()] == p[..])
            .max_by_key(|p| p.len())
            .cloned()
        else {
            return false;
        };
        self.contract(&target)
    }

    fn project_keyvalue(&self, node: &Node) -> Result<Projection> {
        let Node::Obj(map) = node else {
            // A non-object anchored in key/value mode degrades to a single row rather
            // than failing — the user may have switched modes on the wrong node.
            return self.project_scalars(node);
        };
        let row_paths: Vec<NodePath> = map
            .keys()
            .map(|k| child_path(&self.anchor, Seg::Key(k.clone())))
            .collect();

        let key_col: Vec<String> = map.keys().cloned().collect();
        let value_col: Vec<Option<&Node>> = map.values().map(Some).collect();
        let type_col: Vec<String> = map.values().map(|v| v.type_name().to_string()).collect();

        let df = polars::prelude::DataFrame::new(
            map.len(),
            vec![
                Column::new("key".into(), &key_col),
                node_series("value", &value_col),
                Column::new("type".into(), &type_col),
            ],
        )?;

        Ok(Projection {
            df: wrap(df)?,
            row_paths,
            col_roles: vec![ColRole::Key, ColRole::Row, ColRole::Type],
        })
    }

    fn project_scalars(&self, node: &Node) -> Result<Projection> {
        let (items, row_paths): (Vec<&Node>, Vec<NodePath>) = match node {
            Node::Arr(v) => (
                v.iter().collect(),
                (0..v.len())
                    .map(|i| child_path(&self.anchor, Seg::Idx(i)))
                    .collect(),
            ),
            other => (vec![other], vec![self.anchor.clone()]),
        };
        let name = self
            .anchor
            .last()
            .map(|s| match s {
                Seg::Key(k) => k.clone(),
                Seg::Idx(i) => format!("[{}]", i),
            })
            .unwrap_or_else(|| DEFAULT_COLNAME.to_string());
        let cells: Vec<Vec<Option<&Node>>> = vec![items.into_iter().map(Some).collect()];
        Ok(Projection {
            df: build_df(&[name], &cells)?,
            row_paths,
            col_roles: vec![ColRole::Row],
        })
    }
}

/// Column name used for rows that are not objects, mirroring VisiData's
/// `options.default_colname`.
pub const DEFAULT_COLNAME: &str = "value";

/// Segments to expand a column into, derived from every value in it.
///
/// Objects contribute the union of their keys in order of first appearance; arrays
/// contribute indices up to the longest one.  Mixed columns are allowed — a row that is
/// not a container simply has no value under the child columns.  Returns empty when no
/// value is a container, which is what makes `(` a no-op on a scalar column.
fn child_segments(values: &[Option<&Node>]) -> Vec<Seg> {
    let mut keys: IndexSet<String> = IndexSet::new();
    let mut max_len = 0usize;
    for v in values.iter().flatten() {
        match v {
            Node::Obj(m) => keys.extend(m.keys().cloned()),
            Node::Arr(a) => max_len = max_len.max(a.len()),
            _ => {}
        }
    }
    // Objects win when a column mixes both: named columns beat positional ones.
    if !keys.is_empty() {
        return keys.into_iter().map(Seg::Key).collect();
    }
    (0..max_len).map(Seg::Idx).collect()
}

/// `base`, or `base_1`, `base_2`… — the first that `taken` does not hold.
pub(crate) fn unique_name(base: &str, taken: &IndexSet<String>) -> String {
    if !taken.contains(base) {
        return base.to_string();
    }
    (1..)
        .map(|n| format!("{}_{}", base, n))
        .find(|c| !taken.contains(c))
        .unwrap()
}

fn child_path(base: &[Seg], seg: Seg) -> NodePath {
    let mut p = base.to_vec();
    p.push(seg);
    p
}

/// Resolve the tree path of one cell.  `None` for columns that do not address a node
/// (the read-only `type` column).
pub fn cell_path(
    row_paths: &[NodePath],
    col_roles: &[ColRole],
    row: usize,
    col: usize,
) -> Option<NodePath> {
    let base = row_paths.get(row)?;
    match col_roles.get(col)? {
        ColRole::Row | ColRole::Key => Some(base.clone()),
        ColRole::Field(path) => Some(base.iter().chain(path).cloned().collect()),
        ColRole::Type => None,
    }
}

// ── DataFrame construction ───────────────────────────────────────────────────

fn build_df(names: &[String], cells: &[Vec<Option<&Node>>]) -> Result<DataFrame> {
    let height = cells.first().map(|c| c.len()).unwrap_or(0);
    let cols: Vec<Column> = names
        .iter()
        .zip(cells)
        .map(|(name, col)| node_series(name, col))
        .collect();
    wrap(polars::prelude::DataFrame::new(height, cols)?)
}

/// Build one Polars column from a column of tree nodes.
///
/// Homogeneous scalar columns get a real numeric/boolean dtype so that sorting,
/// filtering and aggregation behave; anything mixed (and anything containing a
/// container) falls back to the compact text rendering.
fn node_series(name: &str, col: &[Option<&Node>]) -> Column {
    let mut any_int = false;
    let mut any_float = false;
    let mut any_bool = false;
    let mut any_other = false;
    for n in col.iter().flatten() {
        match n {
            Node::Int(_) => any_int = true,
            Node::Float(_) => any_float = true,
            Node::Bool(_) => any_bool = true,
            Node::Null => {}
            _ => any_other = true,
        }
    }

    if !any_other {
        if any_bool && !any_int && !any_float {
            let v: Vec<Option<bool>> = col
                .iter()
                .map(|n| match n {
                    Some(Node::Bool(b)) => Some(*b),
                    _ => None,
                })
                .collect();
            return Column::new(name.into(), v);
        }
        if any_int && !any_float && !any_bool {
            let v: Vec<Option<i64>> = col
                .iter()
                .map(|n| match n {
                    Some(Node::Int(i)) => Some(*i),
                    _ => None,
                })
                .collect();
            return Column::new(name.into(), v);
        }
        if (any_int || any_float) && !any_bool {
            let v: Vec<Option<f64>> = col
                .iter()
                .map(|n| match n {
                    Some(Node::Int(i)) => Some(*i as f64),
                    Some(Node::Float(f)) => Some(*f),
                    _ => None,
                })
                .collect();
            return Column::new(name.into(), v);
        }
    }

    let v: Vec<Option<String>> = col
        .iter()
        .map(|n| match n {
            None | Some(Node::Null) => None,
            Some(node) => Some(node.render_compact(RENDER_LIMIT)),
        })
        .collect();
    Column::new(name.into(), v)
}

/// Upper bound on how much of a container is rendered into a cell.  The table view
/// clips to the real column width; this only keeps a huge subtree from being
/// materialised into a string in the first place.
const RENDER_LIMIT: usize = 512;

fn wrap(pdf: polars::prelude::DataFrame) -> Result<DataFrame> {
    crate::data::io::wrap_polars_df(pdf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::doc::{Doc, Format};
    use crate::types::ColumnType;

    fn root_of(src: &str, f: Format) -> Node {
        Doc::from_str(src, f).unwrap().root
    }

    #[test]
    fn array_of_objects_becomes_records_with_union_of_keys() {
        let root = root_of(r#"[{"a":1,"b":2},{"a":3,"c":4}]"#, Format::Json);
        let view = View::auto(vec![], &root);
        assert_eq!(view.mode, ViewMode::Records);
        let p = view.project(&root).unwrap();
        let names: Vec<String> = p.df.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(
            names,
            vec!["a", "b", "c"],
            "keys in order of first sighting"
        );
        assert_eq!(p.df.df.height(), 2);
        // second row has no `b`
        assert_eq!(p.df.get_physical(1, 1), "");
        assert_eq!(
            cell_path(&p.row_paths, &p.col_roles, 1, 2),
            Some(vec![Seg::Idx(1), Seg::Key("c".into())])
        );
    }

    #[test]
    fn toml_document_opens_as_key_value_rows() {
        let root = root_of(
            "name = \"x\"\nport = 8080\n\n[db]\nhost = \"h\"\n",
            Format::Toml,
        );
        let view = View::auto(vec![], &root);
        assert_eq!(view.mode, ViewMode::KeyValue);
        let p = view.project(&root).unwrap();
        assert_eq!(p.df.df.height(), 3, "one row per top-level key");
        let names: Vec<String> = p.df.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(names, vec!["key", "value", "type"]);
        // the nested table renders compactly rather than as an empty cell
        assert_eq!(p.df.get_physical(2, 1), "{1} host=h");
        assert_eq!(p.df.get_physical(2, 2), "dict");
        assert_eq!(
            cell_path(&p.row_paths, &p.col_roles, 2, 1),
            Some(vec![Seg::Key("db".into())])
        );
        assert_eq!(
            cell_path(&p.row_paths, &p.col_roles, 2, 2),
            None,
            "type column addresses no node"
        );
    }

    #[test]
    fn homogeneous_numeric_column_keeps_a_numeric_dtype() {
        let root = root_of(r#"[{"n":1},{"n":2},{"n":3}]"#, Format::Json);
        let p = View::auto(vec![], &root).project(&root).unwrap();
        assert_eq!(p.df.df.dtypes()[0], DataType::Int64);
        assert_eq!(p.df.columns[0].col_type, ColumnType::Integer);
    }

    #[test]
    fn mixed_column_falls_back_to_text() {
        let root = root_of(r#"[{"n":1},{"n":"two"}]"#, Format::Json);
        let p = View::auto(vec![], &root).project(&root).unwrap();
        assert_eq!(p.df.df.dtypes()[0], DataType::String);
        assert_eq!(p.df.get_physical(1, 0), "two");
    }

    #[test]
    fn array_of_scalars_becomes_one_column() {
        let root = root_of("[1, 2, 3]", Format::Json);
        let view = View::auto(vec![], &root);
        assert_eq!(view.mode, ViewMode::Scalars);
        let p = view.project(&root).unwrap();
        assert_eq!(p.df.df.width(), 1);
        assert_eq!(p.df.df.height(), 3);
        assert_eq!(
            cell_path(&p.row_paths, &p.col_roles, 2, 0),
            Some(vec![Seg::Idx(2)])
        );
    }

    #[test]
    fn records_with_scalar_rows_get_a_default_column() {
        let root = root_of(r#"[{"a":1}, 7]"#, Format::Json);
        let p = View {
            anchor: vec![],
            mode: ViewMode::Records,
            expanded: Vec::new(),
        }
        .project(&root)
        .unwrap();
        let names: Vec<String> = p.df.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(names, vec![DEFAULT_COLNAME, "a"]);
        assert_eq!(p.df.get_physical(1, 0), "7");
        assert_eq!(
            cell_path(&p.row_paths, &p.col_roles, 1, 0),
            Some(vec![Seg::Idx(1)])
        );
    }

    #[test]
    fn anchoring_into_a_subtree_projects_only_that_subtree() {
        let root = root_of(r#"{"servers":[{"host":"a"},{"host":"b"}]}"#, Format::Json);
        let anchor = vec![Seg::Key("servers".into())];
        let node = root.get(&anchor).unwrap();
        let p = View::auto(anchor.clone(), node).project(&root).unwrap();
        assert_eq!(p.df.df.height(), 2);
        assert_eq!(
            cell_path(&p.row_paths, &p.col_roles, 1, 0),
            Some(vec![
                Seg::Key("servers".into()),
                Seg::Idx(1),
                Seg::Key("host".into())
            ])
        );
    }

    #[test]
    fn expanding_a_column_replaces_it_with_its_children_in_place() {
        let root = root_of(
            r#"[{"id":1,"meta":{"ok":true,"n":2}},{"id":2,"meta":{"ok":false}}]"#,
            Format::Json,
        );
        let mut view = View::auto(vec![], &root);
        assert!(view.expand(vec![Seg::Key("meta".into())]));
        let p = view.project(&root).unwrap();

        let names: Vec<String> = p.df.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(
            names,
            vec!["id", "meta.ok", "meta.n"],
            "children sit where the parent was"
        );
        assert_eq!(p.df.get_physical(0, 2), "2");
        assert_eq!(
            p.df.get_physical(1, 2),
            "",
            "row without the key is empty, not an error"
        );
        assert_eq!(
            cell_path(&p.row_paths, &p.col_roles, 0, 1),
            Some(vec![
                Seg::Idx(0),
                Seg::Key("meta".into()),
                Seg::Key("ok".into())
            ]),
            "an expanded cell still addresses its real node, so it stays editable"
        );
    }

    #[test]
    fn expanding_a_list_column_uses_the_longest_row() {
        let root = root_of(r#"[{"t":["a","b","c"]},{"t":["x"]}]"#, Format::Json);
        let mut view = View::auto(vec![], &root);
        view.expand(vec![Seg::Key("t".into())]);
        let p = view.project(&root).unwrap();
        let names: Vec<String> = p.df.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(names, vec!["t[0]", "t[1]", "t[2]"]);
        assert_eq!(
            p.df.get_physical(1, 1),
            "",
            "short row has no second element"
        );
    }

    #[test]
    fn expansion_nests_and_contracts_one_level_at_a_time() {
        let root = root_of(r#"[{"a":{"b":{"c":1}}}]"#, Format::Json);
        let mut view = View::auto(vec![], &root);
        view.expand(vec![Seg::Key("a".into())]);
        view.expand(vec![Seg::Key("a".into()), Seg::Key("b".into())]);
        let names = |v: &View| -> Vec<String> {
            v.project(&root)
                .unwrap()
                .df
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect()
        };
        assert_eq!(names(&view), vec!["a.b.c"]);

        // `)` on a.b.c folds a.b, leaving a expanded
        assert!(view.contract_one(&[
            Seg::Key("a".into()),
            Seg::Key("b".into()),
            Seg::Key("c".into())
        ]));
        assert_eq!(names(&view), vec!["a.b"]);

        assert!(view.contract_one(&[Seg::Key("a".into()), Seg::Key("b".into())]));
        assert_eq!(names(&view), vec!["a"]);

        assert!(
            !view.contract_one(&[Seg::Key("a".into())]),
            "nothing left to fold"
        );
    }

    #[test]
    fn contracting_a_parent_drops_deeper_expansions_too() {
        let root = root_of(r#"[{"a":{"b":{"c":1}}}]"#, Format::Json);
        let mut view = View::auto(vec![], &root);
        view.expand(vec![Seg::Key("a".into())]);
        view.expand(vec![Seg::Key("a".into()), Seg::Key("b".into())]);
        assert!(view.contract(&[Seg::Key("a".into())]));
        assert!(view.expanded.is_empty(), "{:?}", view.expanded);
    }

    #[test]
    fn expanding_a_column_of_scalars_leaves_it_alone() {
        let root = root_of(r#"[{"n":1},{"n":2}]"#, Format::Json);
        let mut view = View::auto(vec![], &root);
        view.expand(vec![Seg::Key("n".into())]);
        let p = view.project(&root).unwrap();
        let names: Vec<String> = p.df.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(names, vec!["n"], "a scalar column has no children to show");
    }
}
