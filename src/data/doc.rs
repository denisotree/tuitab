//! Document tree — the shared in-memory model for JSON / JSONL / YAML / TOML.
//!
//! All four formats parse into one [`Node`] tree.  The table shown on screen is a
//! *projection* of a subtree (see [`crate::data::view`]), not a copy: editing a cell
//! writes back into the tree by [`NodePath`], and saving re-serialises the tree into
//! whichever format the target file asks for.  Cross-format conversion therefore falls
//! out for free — `config.toml` can be saved as `config.yaml` with no extra code.
//!
//! Known ceilings, all deliberate:
//! - datetimes are carried as RFC-3339 text so TOML round-trips exactly; converting to
//!   JSON emits a string, because JSON has no date type;
//! - YAML anchors, tags and merge keys are resolved at load time and not restored;
//! - YAML comments and formatting are lost; no Rust YAML library round-trips them.
//!   TOML keeps its comments when a TOML file is saved back as TOML (see [`retoml`]).

use color_eyre::{eyre::eyre, Result};
use indexmap::IndexMap;
use std::path::Path;

/// A concrete serialisation format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    /// Newline-delimited JSON (also `.ndjson`).
    Jsonl,
    Yaml,
    Toml,
}

impl Format {
    /// Map a file extension (without the dot, any case) to a format.
    pub fn from_ext(ext: &str) -> Option<Format> {
        match ext.to_lowercase().as_str() {
            "json" => Some(Format::Json),
            "jsonl" | "ndjson" | "ldjson" => Some(Format::Jsonl),
            "yaml" | "yml" => Some(Format::Yaml),
            "toml" => Some(Format::Toml),
            _ => None,
        }
    }

    /// Map an explicit `--type` argument to a format.
    pub fn from_name(name: &str) -> Option<Format> {
        Format::from_ext(name)
    }

    pub fn name(&self) -> &'static str {
        match self {
            Format::Json => "json",
            Format::Jsonl => "jsonl",
            Format::Yaml => "yaml",
            Format::Toml => "toml",
        }
    }
}

/// Something a target format cannot carry across a conversion.
///
/// Formats differ in what they can express, and a conversion that quietly drops
/// something is worse than one that says so.  Rather than growing a branch in the save
/// path per format, each quirk is one entry in [`CAVEATS`]: a description and a
/// predicate saying when it applies.  A format with nothing to declare simply has no
/// entry — most do not.
pub struct Caveat {
    /// Completes the sentence "<FORMAT> cannot carry ...".
    pub lost: &'static str,
    /// Whether this applies to `doc` being written as `target`.
    pub applies: fn(doc: &Doc, target: Format) -> bool,
}

/// Every known one-way conversion.  Add an entry when a new format has a quirk;
/// nothing else needs to change.
pub const CAVEATS: &[Caveat] = &[
    Caveat {
        // YAML is the only format with a document separator; everywhere else several
        // documents can only become several array elements.
        lost: "document separators",
        applies: |doc, target| doc.multi_doc && target != Format::Yaml,
    },
    Caveat {
        // JSONL is a record stream by definition: there is no way to write "this is one
        // document", so a non-array document returns as a stream of one record.
        lost: "the difference between a document and a one-record stream",
        applies: |doc, target| target == Format::Jsonl && !matches!(doc.root, Node::Arr(_)),
    },
    Caveat {
        // Only a TOML file written back as TOML goes through its own source text.
        lost: "comments",
        applies: |doc, target| {
            doc.format == Format::Toml
                && target != Format::Toml
                && doc
                    .source_text
                    .as_deref()
                    .is_some_and(|s| s.lines().any(|l| l.trim_start().starts_with('#')))
        },
    },
];

/// What `doc` would lose if written as `target`.
pub fn conversion_caveats(doc: &Doc, target: Format) -> Vec<&'static str> {
    CAVEATS
        .iter()
        .filter(|c| (c.applies)(doc, target))
        .map(|c| c.lost)
        .collect()
}

/// Guess a structured format from the contents of `text`.
///
/// `bracket_only` restricts the guess to JSON and JSONL, which announce themselves
/// unambiguously with a leading `[` or `{`.  Callers pass it for files with no extension
/// at all, where the existing default is CSV and a wrong guess would be a regression;
/// files with an unrecognised extension would otherwise just fail to open, so there
/// anything we can parse is a win.
///
/// TOML is tried before YAML on purpose.  TOML is the stricter grammar, so it rarely
/// matches something that is not TOML — whereas YAML happily parses `name = "x"` as one
/// long scalar string and would claim every TOML file it saw.
pub fn sniff(text: &str, bracket_only: bool) -> Option<Format> {
    let first = text
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with("//") && !l.starts_with('#'))?;

    if first.starts_with('{') && first.ends_with('}') {
        // A complete object on line one means one record per line unless the file turns
        // out to be a single pretty-printed object.
        if text.lines().filter(|l| l.trim().starts_with('{')).count() > 1 {
            return Some(Format::Jsonl);
        }
        return Some(Format::Json);
    }
    if first.starts_with('[') || first.starts_with('{') {
        return Some(Format::Json);
    }
    if bracket_only {
        return None;
    }
    if text.parse::<toml_edit::DocumentMut>().is_ok() {
        return Some(Format::Toml);
    }
    // A bare scalar is not evidence of YAML — every plain text file is one.
    if let Ok((node, _)) = parse_yaml(text) {
        if node.is_container() {
            return Some(Format::Yaml);
        }
    }
    None
}

/// One hit from a document-wide search.
pub struct Hit {
    pub path: NodePath,
    /// What matched: the key naming this node, or the value in it.
    pub in_key: bool,
}

/// Walk the whole tree and collect every node whose key or scalar value matches.
///
/// Containers are matched on their key only — matching them on their rendered contents
/// would report every ancestor of every hit, which buries the hit itself.  Stops at
/// `limit` so a pattern like `.` on a large document cannot lock the UI up; the caller
/// says so rather than pretending the list is complete.
pub fn search(root: &Node, re: &regex::Regex, limit: usize) -> Vec<Hit> {
    let mut out = Vec::new();
    walk(root, &mut Vec::new(), re, limit, &mut out);
    out
}

fn walk(node: &Node, path: &mut NodePath, re: &regex::Regex, limit: usize, out: &mut Vec<Hit>) {
    if out.len() >= limit {
        return;
    }
    match node {
        Node::Obj(map) => {
            for (k, v) in map {
                if out.len() >= limit {
                    return;
                }
                path.push(Seg::Key(k.clone()));
                if re.is_match(k) {
                    out.push(Hit {
                        path: path.clone(),
                        in_key: true,
                    });
                }
                walk(v, path, re, limit, out);
                path.pop();
            }
        }
        Node::Arr(items) => {
            for (i, v) in items.iter().enumerate() {
                path.push(Seg::Idx(i));
                walk(v, path, re, limit, out);
                path.pop();
            }
        }
        scalar => {
            if re.is_match(&scalar.to_cell_string()) {
                out.push(Hit {
                    path: path.clone(),
                    in_key: false,
                });
            }
        }
    }
}

/// One node of the document tree.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    /// RFC-3339 text.  Only TOML produces these on load.
    DateTime(String),
    Arr(Vec<Node>),
    Obj(IndexMap<String, Node>),
}

/// One step of a path into the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seg {
    Key(String),
    Idx(usize),
}

/// Address of a node inside a [`Doc`], from the root.
pub type NodePath = Vec<Seg>;

/// Render a path the way it is shown in breadcrumbs and expanded column names.
///
/// A key that would be ambiguous in dotted form — one containing `.`, `[` or `]`, or an
/// empty one — is written `["like this"]`, so what [`parse_path`] reads back is the path
/// that went in.  Without that, copying the path of a key called `a.b` would produce
/// something that silently resolves elsewhere.
pub fn path_to_string(path: &[Seg]) -> String {
    let mut s = String::new();
    for seg in path {
        match seg {
            Seg::Key(k) if is_simple_key(k) => {
                if !s.is_empty() {
                    s.push('.');
                }
                s.push_str(k);
            }
            Seg::Key(k) => s.push_str(&format!("[{:?}]", k)),
            Seg::Idx(i) => s.push_str(&format!("[{}]", i)),
        }
    }
    s
}

/// Read a `\`-escaped string body up to its closing quote, returning it and the rest.
fn read_quoted(body: &str) -> Option<(String, &str)> {
    let mut out = String::new();
    let mut chars = body.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => {
                let (_, esc) = chars.next()?;
                out.push(esc);
            }
            '"' => return Some((out, &body[i + 1..])),
            other => out.push(other),
        }
    }
    None
}

fn is_simple_key(k: &str) -> bool {
    !k.is_empty() && !k.contains(['.', '[', ']', '"'])
}

/// Parse `servers[1].host` — or `["awkward.key"].x` — back into a path.
pub fn parse_path(text: &str) -> Result<NodePath> {
    let mut out = NodePath::new();
    let mut rest = text.trim();
    while !rest.is_empty() {
        rest = rest.strip_prefix('.').unwrap_or(rest);
        if let Some(after) = rest.strip_prefix('[') {
            // A quoted key may itself contain `]`, so the closing quote has to be found
            // before the closing bracket, not after it.
            if let Some(body) = after.strip_prefix('"') {
                let (key, tail) = read_quoted(body)
                    .ok_or_else(|| eyre!("unterminated quoted key in `{}`", text))?;
                rest = tail
                    .strip_prefix(']')
                    .ok_or_else(|| eyre!("expected `]` after a quoted key in `{}`", text))?;
                out.push(Seg::Key(key));
                continue;
            }
            let (inner, tail) = after
                .split_once(']')
                .ok_or_else(|| eyre!("unclosed `[` in `{}`", text))?;
            out.push(Seg::Idx(inner.trim().parse::<usize>().map_err(|_| {
                eyre!("`{}` is neither an index nor a quoted key", inner.trim())
            })?));
            rest = tail;
            continue;
        }
        let end = rest.find(['.', '[']).unwrap_or(rest.len());
        let (key, tail) = rest.split_at(end);
        if key.is_empty() {
            return Err(eyre!("empty path segment in `{}`", text));
        }
        out.push(Seg::Key(key.to_string()));
        rest = tail;
    }
    Ok(out)
}

/// A parsed document plus the format and path it came from.
pub struct Doc {
    pub format: Format,
    pub root: Node,
    pub path: Option<std::path::PathBuf>,
    /// The text this document was parsed from, kept so a TOML file can be written back
    /// through its own source and keep its comments.  `None` for documents built from a
    /// table rather than read from a file.
    pub source_text: Option<String>,
    /// True when a YAML file held several `---`-separated documents, in which case
    /// `root` is an `Arr` of them and saving back to YAML re-emits the separators.
    pub multi_doc: bool,
    /// Bumped on every change to `root`.  Anything holding paths captured earlier — a
    /// search hit list, say — compares this to know whether they still mean what they
    /// meant: deleting one array element silently renumbers every later index.
    pub revision: u64,
}

impl Doc {
    pub fn load(path: &Path, format: Format) -> Result<Doc> {
        let text = std::fs::read_to_string(path)?;
        let mut doc = Doc::from_str(&text, format)?;
        doc.path = Some(path.to_path_buf());
        Ok(doc)
    }

    /// Record that `root` changed.  Call after every mutation.
    pub fn bump(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }

    /// True when this document can be written back through its own source text, keeping
    /// comments and layout.  Only same-format TOML saves qualify.
    pub fn can_keep_comments(&self, target: Format) -> bool {
        target == Format::Toml && self.format == Format::Toml && self.source_text.is_some()
    }

    pub fn from_str(text: &str, format: Format) -> Result<Doc> {
        let (root, multi_doc) = match format {
            Format::Json => (parse_json(text)?, false),
            Format::Jsonl => (parse_jsonl(text)?, false),
            Format::Yaml => parse_yaml(text)?,
            Format::Toml => (parse_toml(text)?, false),
        };
        Ok(Doc {
            format,
            root,
            path: None,
            // Only TOML is written back through its own text (comments and layout);
            // for every other format this would be a second copy of the whole file.
            source_text: (format == Format::Toml).then(|| text.to_string()),
            multi_doc,
            revision: 0,
        })
    }

    /// Serialise the whole tree into `format`.
    ///
    /// A TOML document written back as TOML goes through its own source text so
    /// comments and layout survive — losing a config file's comments because one value
    /// was edited is not an acceptable trade.
    pub fn to_string_as(&self, format: Format, opts: &SaveOpts) -> Result<String> {
        if self.can_keep_comments(format) && !opts.sort_keys {
            if let Some(src) = self.source_text.as_deref() {
                if let Ok(text) = retoml(src, &self.root) {
                    return Ok(text);
                }
                // A source that no longer parses (edited on disk under us) is not worth
                // failing the save over; fall through to a clean re-serialisation.
            }
        }
        serialize(&self.root, format, self.multi_doc, opts)
    }

    pub fn save_as(&self, path: &Path, format: Format, opts: &SaveOpts) -> Result<()> {
        std::fs::write(path, self.to_string_as(format, opts)?)?;
        Ok(())
    }
}

/// Serialisation knobs, mirroring VisiData's `json_indent` / `json_sort_keys` /
/// `json_ensure_ascii`.
#[derive(Debug, Clone)]
pub struct SaveOpts {
    pub indent: bool,
    pub sort_keys: bool,
}

impl Default for SaveOpts {
    fn default() -> Self {
        SaveOpts {
            indent: true,
            sort_keys: false,
        }
    }
}

// ── tree access ──────────────────────────────────────────────────────────────

impl Node {
    /// Short type name, shown in the `type` column of a key/value view.
    pub fn type_name(&self) -> &'static str {
        match self {
            Node::Null => "null",
            Node::Bool(_) => "bool",
            Node::Int(_) => "int",
            Node::Float(_) => "float",
            Node::Str(_) => "str",
            Node::DateTime(_) => "datetime",
            Node::Arr(_) => "list",
            Node::Obj(_) => "dict",
        }
    }

    pub fn is_container(&self) -> bool {
        matches!(self, Node::Arr(_) | Node::Obj(_))
    }

    pub fn get(&self, path: &[Seg]) -> Option<&Node> {
        let mut cur = self;
        for seg in path {
            cur = match (cur, seg) {
                (Node::Obj(m), Seg::Key(k)) => m.get(k)?,
                (Node::Arr(v), Seg::Idx(i)) => v.get(*i)?,
                _ => return None,
            };
        }
        Some(cur)
    }

    pub fn get_mut(&mut self, path: &[Seg]) -> Option<&mut Node> {
        let mut cur = self;
        for seg in path {
            cur = match (cur, seg) {
                (Node::Obj(m), Seg::Key(k)) => m.get_mut(k)?,
                (Node::Arr(v), Seg::Idx(i)) => v.get_mut(*i)?,
                _ => return None,
            };
        }
        Some(cur)
    }

    /// Replace the node at `path`.  Missing object keys are created; missing array
    /// indices are an error (we never grow arrays implicitly).
    pub fn set(&mut self, path: &[Seg], value: Node) -> Result<()> {
        let Some((last, parents)) = path.split_last() else {
            *self = value;
            return Ok(());
        };
        let parent = self
            .get_mut(parents)
            .ok_or_else(|| eyre!("no node at {}", path_to_string(parents)))?;
        match (parent, last) {
            (Node::Obj(m), Seg::Key(k)) => {
                m.insert(k.clone(), value);
                Ok(())
            }
            (Node::Arr(v), Seg::Idx(i)) => {
                let slot = v
                    .get_mut(*i)
                    .ok_or_else(|| eyre!("index {} out of range", i))?;
                *slot = value;
                Ok(())
            }
            _ => Err(eyre!("cannot set {} on this node", path_to_string(path))),
        }
    }

    /// Remove the node at `path` from its parent: a key from an object, an element from
    /// an array.  Removing the root is refused — there would be nothing left to show.
    pub fn remove(&mut self, path: &[Seg]) -> Result<()> {
        let Some((last, parents)) = path.split_last() else {
            return Err(eyre!("cannot remove the whole document"));
        };
        let parent = self
            .get_mut(parents)
            .ok_or_else(|| eyre!("no node at {}", path_to_string(parents)))?;
        match (parent, last) {
            (Node::Obj(m), Seg::Key(k)) => m
                .shift_remove(k)
                .map(|_| ())
                .ok_or_else(|| eyre!("no key `{}`", k)),
            (Node::Arr(v), Seg::Idx(i)) => {
                if *i >= v.len() {
                    return Err(eyre!("index {} out of range", i));
                }
                v.remove(*i);
                Ok(())
            }
            _ => Err(eyre!("cannot remove {}", path_to_string(path))),
        }
    }

    /// Compact one-line rendering for a table cell, in the spirit of VisiData's
    /// `iterchars`: `{3} host=localhost port=5432 ssl=true` / `[2] alpha; beta`.
    /// Stops as soon as `max_width` display chars have been produced.
    pub fn render_compact(&self, max_width: usize) -> String {
        let mut out = String::new();
        self.render_into(&mut out, max_width);
        if out.chars().count() > max_width && max_width > 0 {
            out = out.chars().take(max_width.saturating_sub(1)).collect();
            out.push('…');
        }
        out
    }

    fn render_into(&self, out: &mut String, max_width: usize) {
        if out.chars().count() > max_width {
            return;
        }
        match self {
            Node::Null => {}
            Node::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Node::Int(i) => out.push_str(&i.to_string()),
            Node::Float(f) => out.push_str(&fmt_float(*f)),
            Node::Str(s) => out.push_str(s),
            Node::DateTime(s) => out.push_str(s),
            Node::Arr(v) => {
                out.push_str(&format!("[{}]", v.len()));
                for (i, item) in v.iter().enumerate() {
                    if out.chars().count() > max_width {
                        return;
                    }
                    out.push_str(if i == 0 { " " } else { "; " });
                    item.render_into(out, max_width);
                }
            }
            Node::Obj(m) => {
                out.push_str(&format!("{{{}}}", m.len()));
                for (k, v) in m.iter() {
                    if out.chars().count() > max_width {
                        return;
                    }
                    out.push(' ');
                    out.push_str(k);
                    out.push('=');
                    v.render_into(out, max_width);
                }
            }
        }
    }

    /// Plain scalar text, used when a scalar cell is displayed or copied.
    /// Containers fall back to the compact rendering.
    pub fn to_cell_string(&self) -> String {
        match self {
            Node::Null => String::new(),
            Node::Bool(b) => (if *b { "true" } else { "false" }).to_string(),
            Node::Int(i) => i.to_string(),
            Node::Float(f) => fmt_float(*f),
            Node::Str(s) => s.clone(),
            Node::DateTime(s) => s.clone(),
            _ => self.render_compact(usize::MAX),
        }
    }

    /// Parse user-typed text back into a node.
    ///
    /// `old` is the node being replaced; its type is preserved where that is the least
    /// surprising outcome — editing a string cell that happens to read `1.0` must not
    /// silently turn a version number into a float.
    pub fn parse_scalar(text: &str, old: Option<&Node>) -> Node {
        match old {
            Some(Node::Str(_)) => return Node::Str(text.to_string()),
            Some(Node::DateTime(_)) if !text.is_empty() => return Node::DateTime(text.to_string()),
            _ => {}
        }
        let t = text.trim();
        if t.is_empty() || t == "null" || t == "~" {
            return Node::Null;
        }
        if t == "true" {
            return Node::Bool(true);
        }
        if t == "false" {
            return Node::Bool(false);
        }
        if let Ok(i) = t.parse::<i64>() {
            return Node::Int(i);
        }
        if let Ok(f) = t.parse::<f64>() {
            return Node::Float(f);
        }
        Node::Str(text.to_string())
    }
}

/// Format a float without a trailing `.0` for whole values, matching how the rest of
/// tuitab renders numbers.
pub(crate) fn fmt_float(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{:.1}", f)
    } else {
        f.to_string()
    }
}

// ── writing TOML back through its own source ─────────────────────────────────

/// Re-emit `src` with the values from `root` applied to it, keeping every comment and
/// every scrap of layout that still has somewhere to live.
///
/// Keys that survived keep their decoration; keys the user removed go, keys they added
/// are appended.  Arrays and tables whose length changed are rebuilt, which loses any
/// comment *inside* them — rare enough to accept, and far better than dropping the
/// comments of the whole file because one value moved.
pub fn retoml(src: &str, root: &Node) -> Result<String> {
    let mut doc: toml_edit::DocumentMut = src
        .parse()
        .map_err(|e| eyre!("source is no longer valid TOML: {}", e))?;
    let Node::Obj(map) = root else {
        return Err(eyre!("TOML requires a table at the top level"));
    };
    apply_table(map, doc.as_table_mut());
    reorder_table(doc.as_table_mut(), map);
    Ok(doc.to_string())
}

fn apply_table(map: &IndexMap<String, Node>, tbl: &mut dyn toml_edit::TableLike) {
    let existing: Vec<String> = tbl.iter().map(|(k, _)| k.to_string()).collect();
    for k in existing {
        if !map.contains_key(&k) {
            tbl.remove(&k);
        }
    }
    for (k, node) in map {
        // TOML has no null, and the from-scratch writer drops such keys.  This writer
        // must do the same or the two would disagree — and writing `b = ""` in place of
        // a null would quietly change the meaning of a config file.
        if matches!(node, Node::Null) {
            tbl.remove(k);
            continue;
        }
        match tbl.get_mut(k) {
            Some(item) => apply_item(node, item),
            None => {
                tbl.insert(k, node_to_item(node));
            }
        }
    }
}

/// Put a table back into the tree's key order.
///
/// Needed because a newly inserted key lands at the end, and a rename looks like a
/// removal plus an insertion — without this, renaming a key would move its line to the
/// bottom of the file.  `TableLike` only offers alphabetical sorting, hence the two
/// concrete call sites.
fn key_order(map: &IndexMap<String, Node>) -> std::collections::HashMap<&str, usize> {
    map.keys()
        .enumerate()
        .map(|(i, k)| (k.as_str(), i))
        .collect()
}

fn reorder_table(tbl: &mut toml_edit::Table, map: &IndexMap<String, Node>) {
    let order = key_order(map);
    tbl.sort_values_by(|k1, _, k2, _| order.get(k1.get()).cmp(&order.get(k2.get())));
}

fn reorder_inline(tbl: &mut toml_edit::InlineTable, map: &IndexMap<String, Node>) {
    let order = key_order(map);
    tbl.sort_values_by(|k1, _, k2, _| order.get(k1.get()).cmp(&order.get(k2.get())));
}

fn apply_item(node: &Node, item: &mut toml_edit::Item) {
    match node {
        Node::Obj(map) => {
            if let Some(tbl) = item.as_table_mut() {
                apply_table(map, tbl);
                reorder_table(tbl, map);
                return;
            }
            if let Some(tbl) = item.as_inline_table_mut() {
                apply_table(map, tbl);
                reorder_inline(tbl, map);
                return;
            }
        }
        Node::Arr(items) => {
            if let toml_edit::Item::ArrayOfTables(aot) = item {
                // Only reusable while it is still an array of the same number of
                // tables; anything else is rebuilt below.
                if aot.len() == items.len() && items.iter().all(|n| matches!(n, Node::Obj(_))) {
                    for (t, n) in aot.iter_mut().zip(items) {
                        if let Node::Obj(map) = n {
                            apply_table(map, t);
                            reorder_table(t, map);
                        }
                    }
                    return;
                }
            }
            if let Some(arr) = item.as_array_mut() {
                // Nulls are dropped from arrays, so a null anywhere means the length
                // changes and the array has to be rebuilt rather than updated in place.
                if arr.len() == items.len() && !items.iter().any(|n| matches!(n, Node::Null)) {
                    for (v, n) in arr.iter_mut().zip(items) {
                        apply_value(n, v);
                    }
                    return;
                }
            }
        }
        _ => {
            if let toml_edit::Item::Value(v) = item {
                apply_value(node, v);
                return;
            }
        }
    }
    *item = node_to_item(node);
}

/// Replace a value, carrying its decoration across so the whitespace and any trailing
/// `# comment` on that line stay put.
fn apply_value(node: &Node, slot: &mut toml_edit::Value) {
    match node {
        Node::Obj(map) => {
            if let Some(tbl) = slot.as_inline_table_mut() {
                apply_table(map, tbl);
                reorder_inline(tbl, map);
                return;
            }
        }
        Node::Arr(items) => {
            if let Some(arr) = slot.as_array_mut() {
                if arr.len() == items.len() && !items.iter().any(|n| matches!(n, Node::Null)) {
                    for (v, n) in arr.iter_mut().zip(items) {
                        apply_value(n, v);
                    }
                    return;
                }
            }
        }
        _ => {}
    }
    let decor = slot.decor().clone();
    let mut fresh = node_to_toml_value(node);
    *fresh.decor_mut() = decor;
    *slot = fresh;
}

fn node_to_item(node: &Node) -> toml_edit::Item {
    match node {
        Node::Obj(map) => {
            let mut t = toml_edit::Table::new();
            for (k, v) in map {
                if matches!(v, Node::Null) {
                    continue;
                }
                t.insert(k, node_to_item(v));
            }
            toml_edit::Item::Table(t)
        }
        Node::Null => toml_edit::Item::None,
        // An array of tables is what `[[name]]` means; without this it would come out
        // as one long inline array, which is legal TOML but not what anyone writes.
        Node::Arr(items)
            if !items.is_empty() && items.iter().all(|n| matches!(n, Node::Obj(_))) =>
        {
            let mut aot = toml_edit::ArrayOfTables::new();
            for n in items {
                if let Node::Obj(map) = n {
                    let mut t = toml_edit::Table::new();
                    for (k, v) in map {
                        if matches!(v, Node::Null) {
                            continue;
                        }
                        t.insert(k, node_to_item(v));
                    }
                    aot.push(t);
                }
            }
            toml_edit::Item::ArrayOfTables(aot)
        }
        other => toml_edit::Item::Value(node_to_toml_value(other)),
    }
}

fn node_to_toml_value(node: &Node) -> toml_edit::Value {
    use toml_edit::Value as V;
    match node {
        // Unreachable: every caller drops nulls before getting here, in both the key
        // and the array case.  Kept total rather than panicking on a future caller.
        Node::Null => V::from(""),
        Node::Bool(b) => V::from(*b),
        Node::Int(i) => V::from(*i),
        Node::Float(f) => V::from(*f),
        Node::Str(s) => V::from(s.as_str()),
        Node::DateTime(s) => match s.parse::<toml_edit::Datetime>() {
            Ok(d) => V::from(d),
            Err(_) => V::from(s.as_str()),
        },
        Node::Arr(items) => {
            let mut a = toml_edit::Array::new();
            for i in items {
                if matches!(i, Node::Null) {
                    continue;
                }
                a.push(node_to_toml_value(i));
            }
            V::Array(a)
        }
        Node::Obj(map) => {
            let mut t = toml_edit::InlineTable::new();
            for (k, v) in map {
                if matches!(v, Node::Null) {
                    continue;
                }
                t.insert(k, node_to_toml_value(v));
            }
            V::InlineTable(t)
        }
    }
}

// ── parsing ──────────────────────────────────────────────────────────────────

fn parse_json(text: &str) -> Result<Node> {
    let v: serde_json::Value = serde_json::from_str(text)?;
    Ok(from_json(v))
}

fn parse_jsonl(text: &str) -> Result<Node> {
    let mut rows = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        // blank lines and `//` / `#` comments are skipped, as VisiData does
        if line.is_empty() || line.starts_with("//") || line.starts_with('#') {
            continue;
        }
        let v: serde_json::Value =
            serde_json::from_str(line).map_err(|e| eyre!("line {}: {}", n + 1, e))?;
        rows.push(from_json(v));
    }
    Ok(Node::Arr(rows))
}

fn parse_yaml(text: &str) -> Result<(Node, bool)> {
    use serde::Deserialize;
    let mut docs = Vec::new();
    for de in serde_yaml_ng::Deserializer::from_str(text) {
        let v = serde_yaml_ng::Value::deserialize(de)?;
        if matches!(v, serde_yaml_ng::Value::Null) && docs.is_empty() && text.trim().is_empty() {
            continue;
        }
        docs.push(from_yaml(v));
    }
    match docs.len() {
        0 => Ok((Node::Null, false)),
        1 => Ok((docs.pop().unwrap(), false)),
        _ => Ok((Node::Arr(docs), true)),
    }
}

fn parse_toml(text: &str) -> Result<Node> {
    let doc: toml_edit::DocumentMut = text.parse().map_err(|e| eyre!("{}", e))?;
    Ok(item_to_node(doc.as_item()))
}

fn from_json(v: serde_json::Value) -> Node {
    match v {
        serde_json::Value::Null => Node::Null,
        serde_json::Value::Bool(b) => Node::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Node::Int(i)
            } else {
                Node::Float(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        serde_json::Value::String(s) => Node::Str(s),
        serde_json::Value::Array(a) => Node::Arr(a.into_iter().map(from_json).collect()),
        serde_json::Value::Object(o) => {
            Node::Obj(o.into_iter().map(|(k, v)| (k, from_json(v))).collect())
        }
    }
}

fn from_yaml(v: serde_yaml_ng::Value) -> Node {
    use serde_yaml_ng::Value as Y;
    match v {
        Y::Null => Node::Null,
        Y::Bool(b) => Node::Bool(b),
        Y::Number(n) => {
            if let Some(i) = n.as_i64() {
                Node::Int(i)
            } else {
                Node::Float(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        Y::String(s) => Node::Str(s),
        Y::Sequence(s) => Node::Arr(s.into_iter().map(from_yaml).collect()),
        Y::Mapping(m) => Node::Obj(
            m.into_iter()
                .map(|(k, v)| (yaml_key_to_string(k), from_yaml(v)))
                .collect(),
        ),
        // tagged values are flattened to their payload; the tag itself is dropped
        Y::Tagged(t) => from_yaml(t.value),
    }
}

/// YAML permits non-string mapping keys; we render them as text, which is lossy on
/// save (`1:` becomes `"1":`).  Acceptable: non-string keys are rare in config files.
fn yaml_key_to_string(k: serde_yaml_ng::Value) -> String {
    match k {
        serde_yaml_ng::Value::String(s) => s,
        other => match from_yaml(other) {
            Node::Null => String::new(),
            n => n.to_cell_string(),
        },
    }
}

fn item_to_node(item: &toml_edit::Item) -> Node {
    match item {
        toml_edit::Item::Value(v) => toml_value_to_node(v),
        toml_edit::Item::Table(t) => Node::Obj(
            t.iter()
                .map(|(k, v)| (k.to_string(), item_to_node(v)))
                .collect(),
        ),
        toml_edit::Item::ArrayOfTables(a) => Node::Arr(
            a.iter()
                .map(|t| {
                    Node::Obj(
                        t.iter()
                            .map(|(k, v)| (k.to_string(), item_to_node(v)))
                            .collect(),
                    )
                })
                .collect(),
        ),
        toml_edit::Item::None => Node::Null,
    }
}

fn toml_value_to_node(v: &toml_edit::Value) -> Node {
    use toml_edit::Value as V;
    match v {
        V::String(s) => Node::Str(s.value().clone()),
        V::Integer(i) => Node::Int(*i.value()),
        V::Float(f) => Node::Float(*f.value()),
        V::Boolean(b) => Node::Bool(*b.value()),
        V::Datetime(d) => Node::DateTime(d.value().to_string()),
        V::Array(a) => Node::Arr(a.iter().map(toml_value_to_node).collect()),
        V::InlineTable(t) => Node::Obj(
            t.iter()
                .map(|(k, v)| (k.to_string(), toml_value_to_node(v)))
                .collect(),
        ),
    }
}

// ── serialising ──────────────────────────────────────────────────────────────

pub fn serialize(root: &Node, format: Format, multi_doc: bool, opts: &SaveOpts) -> Result<String> {
    let root = if opts.sort_keys { &sorted(root) } else { root };
    match format {
        Format::Json => {
            let v = to_json(root);
            Ok(if opts.indent {
                serde_json::to_string_pretty(&v)?
            } else {
                serde_json::to_string(&v)?
            } + "\n")
        }
        Format::Jsonl => {
            let items: Vec<&Node> = match root {
                Node::Arr(v) => v.iter().collect(),
                other => vec![other],
            };
            let mut out = String::new();
            for item in items {
                out.push_str(&serde_json::to_string(&to_json(item))?);
                out.push('\n');
            }
            Ok(out)
        }
        Format::Yaml => {
            if multi_doc {
                if let Node::Arr(docs) = root {
                    let mut out = String::new();
                    for d in docs {
                        out.push_str("---\n");
                        out.push_str(&serde_yaml_ng::to_string(&to_yaml(d))?);
                    }
                    return Ok(out);
                }
            }
            Ok(serde_yaml_ng::to_string(&to_yaml(root))?)
        }
        Format::Toml => {
            let Node::Obj(map) = root else {
                return Err(eyre!(
                    "TOML requires a table at the top level, got {}",
                    root.type_name()
                ));
            };
            // Built with the same node_to_item the source-preserving writer uses, so
            // the two paths cannot drift apart on questions like what to do with nulls.
            let mut doc = toml_edit::DocumentMut::new();
            for (k, v) in map {
                if matches!(v, Node::Null) {
                    continue;
                }
                doc.insert(k, node_to_item(v));
            }
            Ok(doc.to_string())
        }
    }
}

fn sorted(n: &Node) -> Node {
    match n {
        Node::Obj(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            Node::Obj(
                keys.into_iter()
                    .map(|k| (k.clone(), sorted(&m[k])))
                    .collect(),
            )
        }
        Node::Arr(v) => Node::Arr(v.iter().map(sorted).collect()),
        other => other.clone(),
    }
}

fn to_json(n: &Node) -> serde_json::Value {
    use serde_json::Value as J;
    match n {
        Node::Null => J::Null,
        Node::Bool(b) => J::Bool(*b),
        Node::Int(i) => J::Number((*i).into()),
        Node::Float(f) => serde_json::Number::from_f64(*f)
            .map(J::Number)
            .unwrap_or(J::Null),
        // JSON has no date type, so a datetime degrades to its RFC-3339 text
        Node::Str(s) | Node::DateTime(s) => J::String(s.clone()),
        Node::Arr(v) => J::Array(v.iter().map(to_json).collect()),
        Node::Obj(m) => J::Object(m.iter().map(|(k, v)| (k.clone(), to_json(v))).collect()),
    }
}

fn to_yaml(n: &Node) -> serde_yaml_ng::Value {
    use serde_yaml_ng::Value as Y;
    match n {
        Node::Null => Y::Null,
        Node::Bool(b) => Y::Bool(*b),
        Node::Int(i) => Y::Number((*i).into()),
        Node::Float(f) => Y::Number((*f).into()),
        Node::Str(s) | Node::DateTime(s) => Y::String(s.clone()),
        Node::Arr(v) => Y::Sequence(v.iter().map(to_yaml).collect()),
        Node::Obj(m) => Y::Mapping(
            m.iter()
                .map(|(k, v)| (Y::String(k.clone()), to_yaml(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(pairs: &[(&str, Node)]) -> Node {
        Node::Obj(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    #[test]
    fn json_roundtrip_preserves_key_order() {
        let src = r#"{"zebra": 1, "apple": 2, "mango": [1, 2, 3]}"#;
        let doc = Doc::from_str(src, Format::Json).unwrap();
        let out = doc
            .to_string_as(Format::Json, &SaveOpts::default())
            .unwrap();
        let keys: Vec<&str> = out
            .lines()
            .filter_map(|l| l.trim().split('"').nth(1))
            .collect();
        assert_eq!(keys[0], "zebra", "key order must survive: {}", out);
        assert_eq!(Doc::from_str(&out, Format::Json).unwrap().root, doc.root);
    }

    #[test]
    fn toml_datetime_survives_roundtrip() {
        let src = "released = 1979-05-27T07:32:00Z\nname = \"x\"\n";
        let doc = Doc::from_str(src, Format::Toml).unwrap();
        assert!(matches!(
            doc.root.get(&[Seg::Key("released".into())]),
            Some(Node::DateTime(_))
        ));
        let out = doc
            .to_string_as(Format::Toml, &SaveOpts::default())
            .unwrap();
        assert!(
            out.contains("1979-05-27T07:32:00Z"),
            "datetime must not become a quoted string: {}",
            out
        );
        // and converting to JSON degrades it to text rather than corrupting it
        let js = doc
            .to_string_as(Format::Json, &SaveOpts::default())
            .unwrap();
        assert!(js.contains("\"1979-05-27T07:32:00Z\""), "{}", js);
    }

    #[test]
    fn yaml_multidoc_becomes_array_and_comes_back() {
        let src = "a: 1\n---\na: 2\n";
        let doc = Doc::from_str(src, Format::Yaml).unwrap();
        assert!(doc.multi_doc);
        assert_eq!(
            doc.root,
            Node::Arr(vec![
                obj(&[("a", Node::Int(1))]),
                obj(&[("a", Node::Int(2))]),
            ])
        );
        let out = doc
            .to_string_as(Format::Yaml, &SaveOpts::default())
            .unwrap();
        assert_eq!(out.matches("---").count(), 2, "{}", out);
        assert_eq!(Doc::from_str(&out, Format::Yaml).unwrap().root, doc.root);
    }

    #[test]
    fn jsonl_skips_blanks_and_comments() {
        let src = "// header\n{\"a\":1}\n\n{\"a\":2}\n";
        let doc = Doc::from_str(src, Format::Jsonl).unwrap();
        assert_eq!(
            doc.root,
            Node::Arr(vec![
                obj(&[("a", Node::Int(1))]),
                obj(&[("a", Node::Int(2))])
            ])
        );
        let out = doc
            .to_string_as(Format::Jsonl, &SaveOpts::default())
            .unwrap();
        assert_eq!(out, "{\"a\":1}\n{\"a\":2}\n");
    }

    #[test]
    fn toml_drops_nulls_and_rejects_non_table_root() {
        let root = obj(&[("keep", Node::Int(1)), ("drop", Node::Null)]);
        let doc = Doc {
            format: Format::Toml,
            root,
            path: None,
            source_text: None,
            multi_doc: false,
            revision: 0,
        };
        let out = doc
            .to_string_as(Format::Toml, &SaveOpts::default())
            .unwrap();
        assert!(out.contains("keep"), "{}", out);
        assert!(!out.contains("drop"), "null key must be dropped: {}", out);

        let arr = Doc {
            format: Format::Toml,
            root: Node::Arr(vec![Node::Int(1)]),
            path: None,
            source_text: None,
            multi_doc: false,
            revision: 0,
        };
        assert!(arr
            .to_string_as(Format::Toml, &SaveOpts::default())
            .is_err());
    }

    #[test]
    fn set_and_get_by_path() {
        let mut root = obj(&[(
            "servers",
            Node::Arr(vec![obj(&[("host", Node::Str("a".into()))])]),
        )]);
        let p = vec![
            Seg::Key("servers".into()),
            Seg::Idx(0),
            Seg::Key("host".into()),
        ];
        assert_eq!(root.get(&p), Some(&Node::Str("a".into())));
        root.set(&p, Node::Str("b".into())).unwrap();
        assert_eq!(root.get(&p), Some(&Node::Str("b".into())));
        assert_eq!(path_to_string(&p), "servers[0].host");

        // growing an array implicitly is refused rather than silently ignored
        let bad = vec![Seg::Key("servers".into()), Seg::Idx(9)];
        assert!(root.set(&bad, Node::Null).is_err());
    }

    #[test]
    fn parse_scalar_keeps_string_typed_cells_as_strings() {
        let old = Node::Str("1.0".into());
        assert_eq!(
            Node::parse_scalar("2.0", Some(&old)),
            Node::Str("2.0".into())
        );
        assert_eq!(Node::parse_scalar("2.0", None), Node::Float(2.0));
        assert_eq!(Node::parse_scalar("7", None), Node::Int(7));
        assert_eq!(Node::parse_scalar("", None), Node::Null);
        assert_eq!(Node::parse_scalar("true", None), Node::Bool(true));
    }

    #[test]
    fn render_compact_matches_visidata_shape_and_clips() {
        let n = obj(&[
            ("host", Node::Str("localhost".into())),
            ("port", Node::Int(5432)),
        ]);
        assert_eq!(n.render_compact(100), "{2} host=localhost port=5432");
        assert_eq!(
            Node::Arr(vec![Node::Str("alpha".into()), Node::Str("beta".into())])
                .render_compact(100),
            "[2] alpha; beta"
        );
        let clipped = n.render_compact(10);
        assert_eq!(clipped.chars().count(), 10, "{:?}", clipped);
        assert!(clipped.ends_with('…'));
    }

    #[test]
    fn sniffing_prefers_the_stricter_grammar() {
        // TOML must win: YAML would happily read this as one long scalar string
        assert_eq!(sniff("name = \"x\"\nport = 1\n", false), Some(Format::Toml));
        assert_eq!(sniff("name: x\nport: 1\n", false), Some(Format::Yaml));
        assert_eq!(sniff("- a\n- b\n", false), Some(Format::Yaml));
        assert_eq!(sniff("[{\"a\":1}]", false), Some(Format::Json));
        assert_eq!(sniff("{\"a\":1}\n{\"a\":2}\n", false), Some(Format::Jsonl));
        assert_eq!(sniff("{\n  \"a\": 1\n}\n", false), Some(Format::Json));
    }

    #[test]
    fn sniffing_refuses_plain_text_and_delimited_data() {
        assert_eq!(
            sniff("just some prose\n", false),
            None,
            "a scalar is not YAML"
        );
        assert_eq!(sniff("", false), None);
        // a CSV must never be mistaken for a document, extension or not
        assert_eq!(sniff("a,b,c\n1,2,3\n", false), None);
        assert_eq!(sniff("a,b,c\n1,2,3\n", true), None);
    }

    #[test]
    fn bracket_only_sniffing_ignores_yaml_and_toml() {
        // used for extension-less files, where the established default is CSV and a
        // wrong guess would be a regression
        assert_eq!(sniff("name: x\n", true), None);
        assert_eq!(sniff("name = \"x\"\n", true), None);
        assert_eq!(sniff("[1, 2]", true), Some(Format::Json));
    }

    #[test]
    fn editing_a_toml_value_keeps_every_comment() {
        let src = "\
# top of file
name = \"demo\"   # what it is called
port = 8080

# the database section
[db]
host = \"localhost\"
pool = 10
";
        let mut doc = Doc::from_str(src, Format::Toml).unwrap();
        doc.root
            .set(&[Seg::Key("port".into())], Node::Int(9090))
            .unwrap();
        doc.root
            .set(
                &[Seg::Key("db".into()), Seg::Key("host".into())],
                Node::Str("db.internal".into()),
            )
            .unwrap();

        let out = doc
            .to_string_as(Format::Toml, &SaveOpts::default())
            .unwrap();
        assert!(out.contains("# top of file"), "{}", out);
        assert!(out.contains("# the database section"), "{}", out);
        assert!(
            out.contains("# what it is called"),
            "trailing comment: {}",
            out
        );
        assert!(out.contains("port = 9090"), "{}", out);
        assert!(out.contains("host = \"db.internal\""), "{}", out);
        assert!(out.contains("pool = 10"), "untouched keys stay: {}", out);
    }

    #[test]
    fn removing_and_adding_keys_is_reflected_without_losing_the_rest() {
        let src = "# keep me\na = 1\nb = 2\n";
        let mut doc = Doc::from_str(src, Format::Toml).unwrap();
        let Node::Obj(map) = &mut doc.root else {
            unreachable!()
        };
        map.shift_remove("b");
        map.insert("c".into(), Node::Str("new".into()));

        let out = doc
            .to_string_as(Format::Toml, &SaveOpts::default())
            .unwrap();
        assert!(out.contains("# keep me"), "{}", out);
        assert!(out.contains("a = 1"), "{}", out);
        assert!(!out.contains("b = 2"), "removed key is gone: {}", out);
        assert!(out.contains("c = \"new\""), "added key is there: {}", out);
    }

    #[test]
    fn arrays_of_tables_keep_their_comments_when_the_length_holds() {
        let src = "\
# servers follow
[[server]]
host = \"a\"  # the first one

[[server]]
host = \"b\"
";
        let mut doc = Doc::from_str(src, Format::Toml).unwrap();
        doc.root
            .set(
                &[
                    Seg::Key("server".into()),
                    Seg::Idx(1),
                    Seg::Key("host".into()),
                ],
                Node::Str("bb".into()),
            )
            .unwrap();
        let out = doc
            .to_string_as(Format::Toml, &SaveOpts::default())
            .unwrap();
        assert!(out.contains("# servers follow"), "{}", out);
        assert!(out.contains("# the first one"), "{}", out);
        assert!(out.contains("host = \"bb\""), "{}", out);
    }

    #[test]
    fn a_document_built_from_scratch_still_serialises_normally() {
        // no source text to write back through
        let doc = Doc {
            format: Format::Toml,
            root: Node::Obj([("a".to_string(), Node::Int(1))].into_iter().collect()),
            path: None,
            source_text: None,
            multi_doc: false,
            revision: 0,
        };
        assert_eq!(
            doc.to_string_as(Format::Toml, &SaveOpts::default())
                .unwrap()
                .trim(),
            "a = 1"
        );
    }

    #[test]
    fn converting_to_another_format_does_not_go_through_the_toml_source() {
        let doc = Doc::from_str("# c\na = 1\n", Format::Toml).unwrap();
        let out = doc
            .to_string_as(Format::Json, &SaveOpts::default())
            .unwrap();
        assert!(!out.contains("# c"), "JSON has no comments: {}", out);
        assert!(out.contains("\"a\""), "{}", out);
    }

    #[test]
    fn a_renamed_key_stays_on_its_own_line_and_the_rest_keeps_its_comments() {
        let src = "# header\na = 1\nb = 2  # about b\nc = 3\n";
        let mut doc = Doc::from_str(src, Format::Toml).unwrap();
        let Node::Obj(map) = &mut doc.root else {
            unreachable!()
        };
        let idx = map.get_index_of("b").unwrap();
        let (_, v) = map.shift_remove_index(idx).unwrap();
        map.shift_insert(idx, "bb".into(), v);

        let out = doc
            .to_string_as(Format::Toml, &SaveOpts::default())
            .unwrap();
        let keys: Vec<&str> = out
            .lines()
            .filter(|l| l.contains('='))
            .filter_map(|l| l.split(" =").next())
            .collect();
        assert_eq!(keys, vec!["a", "bb", "c"], "order holds: {}", out);
        assert!(out.contains("# header"), "{}", out);
        // Known limit: a rename is a removal plus an insertion as far as the source is
        // concerned, so the comment that sat on that key's line goes with it.
        assert!(!out.contains("# about b"), "documented loss: {}", out);
    }

    /// There are two TOML writers now — through the source text and from scratch — and
    /// they must not disagree about anything.  Nulls are the case where they could:
    /// the clean path drops the key, so the source-preserving path has to as well.
    #[test]
    fn both_toml_writers_agree_about_nulls() {
        let src = "a = 1\nb = 2\nlist = [1, 2, 3]\n";
        let mut with_source = Doc::from_str(src, Format::Toml).unwrap();
        with_source
            .root
            .set(&[Seg::Key("b".into())], Node::Null)
            .unwrap();
        with_source
            .root
            .set(&[Seg::Key("list".into()), Seg::Idx(1)], Node::Null)
            .unwrap();

        let mut from_scratch = Doc::from_str(src, Format::Toml).unwrap();
        from_scratch.source_text = None;
        from_scratch.root = with_source.root.clone();

        let a = with_source
            .to_string_as(Format::Toml, &SaveOpts::default())
            .unwrap();
        let b = from_scratch
            .to_string_as(Format::Toml, &SaveOpts::default())
            .unwrap();

        assert!(!a.contains("b ="), "null key must be dropped: {}", a);
        assert_eq!(
            Doc::from_str(&a, Format::Toml).unwrap().root,
            Doc::from_str(&b, Format::Toml).unwrap().root,
            "the two writers disagree:\n--- source path ---\n{}\n--- clean path ---\n{}",
            a,
            b
        );
    }

    #[test]
    fn paths_round_trip_through_text_including_awkward_keys() {
        for path in [
            vec![
                Seg::Key("servers".into()),
                Seg::Idx(1),
                Seg::Key("host".into()),
            ],
            vec![Seg::Key("a.b".into()), Seg::Key("c".into())],
            vec![Seg::Key("has[bracket]".into())],
            vec![Seg::Idx(0), Seg::Idx(2)],
            vec![],
        ] {
            let text = path_to_string(&path);
            assert_eq!(parse_path(&text).unwrap(), path, "round trip of `{}`", text);
        }
        assert_eq!(
            path_to_string(&[
                Seg::Key("servers".into()),
                Seg::Idx(1),
                Seg::Key("host".into())
            ]),
            "servers[1].host"
        );
        assert_eq!(path_to_string(&[Seg::Key("a.b".into())]), "[\"a.b\"]");
    }

    #[test]
    fn a_malformed_path_is_rejected_rather_than_guessed() {
        assert!(parse_path("servers[1").is_err(), "unclosed bracket");
        assert!(
            parse_path("servers[x]").is_err(),
            "not an index, not quoted"
        );
        assert!(parse_path("a..b").is_err(), "empty segment");
    }

    #[test]
    fn conversion_caveats_fire_only_where_they_apply() {
        // a plain JSON document has nothing to declare about JSON or YAML
        let plain = Doc::from_str(r#"[{"a":1}]"#, Format::Json).unwrap();
        assert!(conversion_caveats(&plain, Format::Json).is_empty());
        assert!(conversion_caveats(&plain, Format::Yaml).is_empty());
        // it is already a list, so JSONL costs it nothing
        assert!(conversion_caveats(&plain, Format::Jsonl).is_empty());

        // an object written as JSONL comes back a stream
        let obj = Doc::from_str(r#"{"a":1}"#, Format::Json).unwrap();
        assert_eq!(
            conversion_caveats(&obj, Format::Jsonl),
            vec!["the difference between a document and a one-record stream"]
        );

        // multi-document YAML keeps its separators only in YAML
        let multi = Doc::from_str("a: 1\n---\na: 2\n", Format::Yaml).unwrap();
        assert!(conversion_caveats(&multi, Format::Yaml).is_empty());
        assert_eq!(
            conversion_caveats(&multi, Format::Json),
            vec!["document separators"]
        );

        // a commented TOML keeps its comments only in TOML
        let commented = Doc::from_str("# note\na = 1\n", Format::Toml).unwrap();
        assert!(conversion_caveats(&commented, Format::Toml).is_empty());
        assert_eq!(
            conversion_caveats(&commented, Format::Yaml),
            vec!["comments"]
        );
        // one without comments has nothing to lose
        let bare = Doc::from_str("a = 1\n", Format::Toml).unwrap();
        assert!(conversion_caveats(&bare, Format::Yaml).is_empty());
    }

    #[test]
    fn several_caveats_are_reported_together() {
        // a multi-document YAML written as JSONL loses its separators *and* becomes a
        // stream — the mechanism has to report both, not the first one it finds
        let multi = Doc::from_str("a: 1\n---\na: 2\n", Format::Yaml).unwrap();
        let mut d = multi;
        d.root = Node::Obj(Default::default()); // not an array any more
        let lost = conversion_caveats(&d, Format::Jsonl);
        assert_eq!(lost.len(), 2, "{:?}", lost);
    }
}
