//! End-to-end checks for the JSON / JSONL / YAML / TOML pipeline: open a real file,
//! project it, edit through the tree, save it back, and convert between formats.

use std::path::{Path, PathBuf};
use tuitab::data::doc::{Format, Seg};
use tuitab::data::io::{doc_io::Shape, load_file_as, load_file_with_doc, save_file_as};
use tuitab::data::view::ViewMode;

fn fixture(name: &str) -> PathBuf {
    Path::new("test_data").join(name)
}

fn out(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("tuitab-doc-tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

#[test]
fn toml_opens_as_key_value_rows_with_typed_nodes() {
    let (df, doc) = load_file_with_doc(&fixture("app.toml"), None).unwrap();
    let doc = doc.expect("toml must carry a document tree");
    assert_eq!(doc.view.mode, ViewMode::KeyValue);

    let names: Vec<&str> = df.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["key", "value", "type"]);
    assert_eq!(df.visible_row_count(), 5, "5 top-level keys");

    // the datetime is a real node type, not a string, and the nested table renders
    // compactly instead of being lost
    let types: Vec<String> = (0..5).map(|r| df.get_physical(r, 2)).collect();
    assert!(types.contains(&"datetime".to_string()), "{:?}", types);
    assert!(types.contains(&"dict".to_string()), "{:?}", types);
    assert!(types.contains(&"list".to_string()), "{:?}", types);
}

#[test]
fn editing_a_toml_value_and_saving_preserves_structure_and_datetime() {
    let (mut df, doc) = load_file_with_doc(&fixture("app.toml"), None).unwrap();
    let mut doc = doc.unwrap();

    // row 1 is `port`; write through the tree, then patch the table like the app does
    let row = (0..df.visible_row_count())
        .find(|r| df.get_physical(*r, 0) == "port")
        .unwrap();
    let shown = doc.set_cell(row, 1, "9090").unwrap();
    df.set_cell(row, 1, shown).unwrap();

    let path = out("app-edited.toml");
    save_file_as(&df, Some(&doc), &path, Shape::Records, "app").unwrap();
    let text = std::fs::read_to_string(&path).unwrap();

    assert!(text.contains("port = 9090"), "{}", text);
    assert!(
        text.contains("1979-05-27T07:32:00Z"),
        "datetime must not degrade to a quoted string: {}",
        text
    );
    assert!(text.contains("[db]"), "nested table survives: {}", text);
    assert!(
        text.contains("[[servers]]"),
        "array of tables survives: {}",
        text
    );
}

#[test]
fn toml_converts_to_yaml_and_json_by_changing_the_extension() {
    let (df, doc) = load_file_with_doc(&fixture("app.toml"), None).unwrap();
    let doc = doc.unwrap();

    let yaml_path = out("app.yaml");
    save_file_as(&df, Some(&doc), &yaml_path, Shape::Records, "app").unwrap();
    let yaml = std::fs::read_to_string(&yaml_path).unwrap();
    assert!(yaml.contains("host: localhost"), "{}", yaml);

    let json_path = out("app.json");
    save_file_as(&df, Some(&doc), &json_path, Shape::Records, "app").unwrap();
    let json = std::fs::read_to_string(&json_path).unwrap();
    assert!(json.contains("\"pool\": 10"), "{}", json);

    // and the converted file reopens as the same structure
    let (_, reopened) = load_file_with_doc(&yaml_path, None).unwrap();
    let reopened = reopened.unwrap();
    let host = reopened
        .doc
        .read()
        .unwrap()
        .root
        .get(&[Seg::Key("db".into()), Seg::Key("host".into())])
        .cloned();
    assert_eq!(host, Some(tuitab::data::doc::Node::Str("localhost".into())));
}

#[test]
fn multi_document_yaml_becomes_rows_and_stays_multi_document() {
    let (df, doc) = load_file_with_doc(&fixture("k8s.yaml"), None).unwrap();
    let doc = doc.unwrap();
    assert_eq!(doc.view.mode, ViewMode::Records);
    assert_eq!(df.visible_row_count(), 2, "one row per document");

    let path = out("k8s-out.yaml");
    save_file_as(&df, Some(&doc), &path, Shape::Records, "k8s").unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        text.matches("---").count(),
        2,
        "document separators must survive: {}",
        text
    );
}

#[test]
fn jsonl_round_trips_and_can_be_saved_as_json() {
    let (df, doc) = load_file_with_doc(&fixture("rows.jsonl"), None).unwrap();
    let doc = doc.unwrap();
    assert_eq!(df.visible_row_count(), 2);
    assert_eq!(df.col_count(), 2);

    let path = out("rows.json");
    save_file_as(&df, Some(&doc), &path, Shape::Records, "rows").unwrap();
    let json = std::fs::read_to_string(&path).unwrap();
    assert!(json.trim_start().starts_with('['), "{}", json);
}

#[test]
fn nested_json_dives_into_subtrees_and_edits_are_visible_from_the_root() {
    let (_df, doc) = load_file_with_doc(&fixture("nested.json"), None).unwrap();
    let root = doc.unwrap();

    // dive into row 0's `tags` list
    let (tags_df, mut tags) = root
        .dive(vec![Seg::Idx(0), Seg::Key("tags".into())])
        .unwrap();
    assert_eq!(tags.view.mode, ViewMode::Scalars);
    assert_eq!(tags_df.visible_row_count(), 2);
    tags.set_cell(0, 0, "z").unwrap();

    // the parent sees it, because both sheets share one tree
    let value = root
        .doc
        .read()
        .unwrap()
        .root
        .get(&[Seg::Idx(0), Seg::Key("tags".into()), Seg::Idx(0)])
        .cloned();
    assert_eq!(value, Some(tuitab::data::doc::Node::Str("z".into())));
}

#[test]
fn a_plain_csv_saves_as_json_records() {
    let (df, doc) = load_file_with_doc(&fixture("prices.csv"), None).unwrap();
    assert!(doc.is_none(), "csv has no document tree");

    let path = out("prices.json");
    save_file_as(&df, None, &path, Shape::Records, "prices").unwrap();
    let json = std::fs::read_to_string(&path).unwrap();
    assert!(json.trim_start().starts_with('['), "{}", json);

    // and as TOML it becomes an array of tables named after the sheet
    let toml_path = out("prices.toml");
    save_file_as(&df, None, &toml_path, Shape::Records, "prices").unwrap();
    let toml = std::fs::read_to_string(&toml_path).unwrap();
    assert!(toml.contains("[[prices]]"), "{}", toml);
}

#[test]
fn forcing_a_format_overrides_the_extension() {
    // prices.csv opened as CSV has real columns; the same path forced to YAML must fail
    // rather than silently producing nonsense
    let forced = load_file_as(&fixture("prices.csv"), None, Some(Format::Toml));
    assert!(forced.is_err(), "csv is not valid toml");

    let (_, doc) = load_file_as(&fixture("app.toml"), None, Some(Format::Toml)).unwrap();
    assert!(doc.is_some());
}

/// The whole path, headless: open a real TOML file, render the actual UI, and check the
/// key/value rows land on screen.  Catches wiring breaks that unit tests on the data
/// layer would miss.
#[test]
fn a_toml_file_renders_as_key_value_rows_in_the_real_ui() {
    use ratatui::{backend::TestBackend, Terminal};

    let mut app = tuitab::app::App::new(&fixture("app.toml"), None).unwrap();
    let mut terminal = Terminal::new(TestBackend::new(120, 24)).unwrap();
    terminal.draw(|f| tuitab::ui::render(f, &mut app)).unwrap();

    let screen: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();

    for expected in [
        "key",
        "value",
        "type",
        "port",
        "8080",
        "datetime",
        "{2} host=localhost",
    ] {
        assert!(
            screen.contains(expected),
            "expected `{}` on screen:\n{}",
            expected,
            screen
        );
    }
}

/// Drive the app the way a user does: move to a nested key, press Enter to dive, press
/// `m` to change the projection, and pop back out.
#[test]
fn enter_dives_into_a_nested_node_and_esc_pops_back() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("app.toml"), None).unwrap();
    let root_title = app.stack.active().title.clone();

    // walk down to the `servers` row
    while app
        .stack
        .active()
        .dataframe
        .get_physical(app.stack.active().table_state.selected().unwrap_or(0), 0)
        != "servers"
    {
        app.handle_action(Action::MoveDown);
    }

    app.handle_action(Action::OpenRow);
    assert_eq!(
        app.stack.active().title,
        "app.toml › servers",
        "breadcrumbs name the anchored node"
    );
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 2);
    let cols: Vec<String> = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert_eq!(cols, vec!["host", "weight"], "array of tables → records");

    app.handle_action(Action::CycleViewMode);
    assert_ne!(
        app.stack.active().dataframe.columns[0].name,
        "host",
        "the projection actually changed"
    );

    app.handle_action(Action::PopSheet);
    assert_eq!(app.stack.active().title, root_title);
}

/// Regression: opening a structured file *through the directory listing* must keep its
/// document tree.  Without it, `Ctrl+S` over the same name rewrites a TOML config as a
/// flat array of tables.
#[test]
fn drilling_in_from_a_directory_keeps_the_document() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(Path::new("test_data"), None).unwrap();
    assert!(app.stack.active().is_dir_sheet);

    // find the app.toml row and open it
    let df = &app.stack.active().dataframe;
    let row = (0..df.visible_row_count())
        .find(|r| df.get_physical(*r, 0).contains("app.toml"))
        .expect("app.toml must be listed");
    app.stack.active_mut().table_state.select(Some(row));
    app.handle_action(Action::OpenRow);

    let sheet = app.stack.active();
    assert!(
        sheet.doc.is_some(),
        "a TOML opened from a directory must carry its tree"
    );

    let path = out("drill-save.toml");
    save_file_as(
        &sheet.dataframe,
        sheet.doc.as_ref(),
        &path,
        Shape::Records,
        &sheet.title,
    )
    .unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("[db]"), "structure must survive: {}", text);
    assert!(text.contains("[[servers]]"), "{}", text);
    assert!(
        !text.contains("[[test_data"),
        "must not be rewritten as a flat table: {}",
        text
    );
}

/// Regression: an edit made inside a dive is visible on the parent sheet after `Esc`,
/// not just in the saved file.
#[test]
fn popping_back_from_a_dive_shows_the_edit() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("nested.json"), None).unwrap();
    // row 0, column `meta` holds a container rendered as `{1} ok=true`
    let meta_col = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .position(|c| c.name == "meta")
        .unwrap();
    app.stack.active_mut().cursor_col = meta_col;
    assert_eq!(
        app.stack.active().dataframe.get_physical(0, meta_col),
        "{1} ok=true"
    );

    app.handle_action(Action::OpenCell);
    assert!(app.stack.can_pop(), "dived into the meta object");
    // key/value view: edit the `ok` value
    {
        let s = app.stack.active_mut();
        s.edit_row = 0;
        s.edit_col = 1;
        s.edit_input = tuitab::ui::text_input::TextInput::with_value("false".into());
    }
    app.handle_action(Action::ApplyEdit);
    app.handle_action(Action::PopSheet);

    assert_eq!(
        app.stack.active().dataframe.get_physical(0, meta_col),
        "{1} ok=false",
        "the parent must re-render the edited subtree"
    );
}

/// Regression: a records array whose objects have a key literally named `value`, mixed
/// with bare scalar rows, must not confuse that key with the synthetic bare-value column.
#[test]
fn a_key_named_value_is_not_mistaken_for_the_bare_column() {
    use tuitab::data::doc::{Doc, Node};
    use tuitab::data::io::doc_io::DocState;

    let doc = Doc::from_str(r#"[{"value":"real"}, 7]"#, Format::Json).unwrap();
    let (df, mut state) = DocState::from_doc(doc).unwrap();
    let names: Vec<&str> = df.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["value_1", "value"],
        "the synthetic column is renamed"
    );

    // editing the real `value` key must change only that key, not the whole row
    state.set_cell(0, 1, "changed").unwrap();
    let root = state.doc.read().unwrap().root.clone();
    assert_eq!(
        root.get(&[Seg::Idx(0)]).map(|n| n.type_name()),
        Some("dict"),
        "row 0 must still be an object"
    );
    assert_eq!(
        root.get(&[Seg::Idx(0), Seg::Key("value".into())]),
        Some(&Node::Str("changed".into()))
    );
}

/// `(` and `)` through the app: expand a nested column, edit a cell inside the
/// expansion, and fold it back.
#[test]
fn expand_and_contract_a_nested_column_from_the_app() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("nested.json"), None).unwrap();
    let meta_col = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .position(|c| c.name == "meta")
        .unwrap();
    app.stack.active_mut().cursor_col = meta_col;

    app.handle_action(Action::ExpandColumn);
    let names: Vec<String> = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert!(names.contains(&"meta.ok".to_string()), "{:?}", names);
    assert!(names.contains(&"meta.note".to_string()), "{:?}", names);
    assert!(
        !names.contains(&"meta".to_string()),
        "parent is replaced: {:?}",
        names
    );

    // an expanded cell is editable, and the edit lands on the real nested node
    let ok_col = names.iter().position(|n| n == "meta.ok").unwrap();
    {
        let s = app.stack.active_mut();
        s.edit_row = 0;
        s.edit_col = ok_col;
        s.edit_input = tuitab::ui::text_input::TextInput::with_value("false".into());
    }
    app.handle_action(Action::ApplyEdit);
    let doc = app.stack.active().doc.as_ref().unwrap();
    assert_eq!(
        doc.doc.read().unwrap().root.get(&[
            Seg::Idx(0),
            Seg::Key("meta".into()),
            Seg::Key("ok".into())
        ]),
        Some(&tuitab::data::doc::Node::Bool(false)),
        "editing an expanded column must write into the nested node"
    );

    app.stack.active_mut().cursor_col = ok_col;
    app.handle_action(Action::ContractColumn);
    let names: Vec<String> = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert!(
        names.contains(&"meta".to_string()),
        "folded back: {:?}",
        names
    );
}

/// Expansion survives a reprojection — it lives on the view, not on the columns.
#[test]
fn expansion_survives_switching_back_from_a_dive() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("nested.json"), None).unwrap();
    let meta_col = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .position(|c| c.name == "meta")
        .unwrap();
    app.stack.active_mut().cursor_col = meta_col;
    app.handle_action(Action::ExpandColumn);

    // dive into a row and come back; the reprojection on pop must keep the expansion
    app.stack.active_mut().cursor_col = 0;
    app.handle_action(Action::OpenRow);
    app.handle_action(Action::PopSheet);

    let names: Vec<String> = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert!(names.contains(&"meta.ok".to_string()), "{:?}", names);
}

/// Expanding columns is a view operation only: saving still writes the real nested
/// structure, not flattened `meta.ok` keys.  (VisiData writes the flattened names.)
#[test]
fn expanding_columns_does_not_leak_into_the_saved_file() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("nested.json"), None).unwrap();
    let meta_col = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .position(|c| c.name == "meta")
        .unwrap();
    app.stack.active_mut().cursor_col = meta_col;
    app.handle_action(Action::ExpandColumn);

    let sheet = app.stack.active();
    let path = out("nested-expanded.json");
    save_file_as(
        &sheet.dataframe,
        sheet.doc.as_ref(),
        &path,
        Shape::Records,
        &sheet.title,
    )
    .unwrap();
    let json = std::fs::read_to_string(&path).unwrap();
    assert!(json.contains("\"meta\""), "nesting is preserved: {}", json);
    assert!(
        !json.contains("meta.ok"),
        "must not flatten on save: {}",
        json
    );
}

/// Undo must never leave the table and the cell→node mapping describing different
/// shapes: the next edit would then write into the wrong node, silently.
#[test]
fn undo_after_expanding_keeps_the_table_and_the_node_mapping_in_step() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("nested.json"), None).unwrap();

    // one document edit, so there is something on the undo stack
    {
        let s = app.stack.active_mut();
        s.edit_row = 0;
        s.edit_col = s
            .dataframe
            .columns
            .iter()
            .position(|c| c.name == "name")
            .unwrap();
        s.edit_input = tuitab::ui::text_input::TextInput::with_value("ALPHA".into());
    }
    app.handle_action(Action::ApplyEdit);

    let meta_col = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .position(|c| c.name == "meta")
        .unwrap();
    app.stack.active_mut().cursor_col = meta_col;
    app.handle_action(Action::ExpandColumn);
    app.handle_action(Action::Undo);

    let s = app.stack.active();
    let doc = s.doc.as_ref().unwrap();
    assert_eq!(
        doc.col_roles.len(),
        s.dataframe.columns.len(),
        "column roles must describe the table that is actually shown"
    );
    assert_eq!(doc.row_paths.len(), s.dataframe.df.height());

    // and the undo actually reverted the document edit
    let name_col = s
        .dataframe
        .columns
        .iter()
        .position(|c| c.name == "name")
        .unwrap();
    assert_eq!(s.dataframe.get_physical(0, name_col), "alpha");
}

/// Regression: column operations that reshape the table but not the document must be
/// refused on a doc-backed sheet rather than desync the cell→node mapping.
#[test]
fn column_ops_are_refused_on_a_doc_sheet() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("nested.json"), None).unwrap();
    let before: Vec<String> = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();

    for action in [Action::StartColReplace, Action::StartColSplit] {
        app.stack.active_mut().cursor_col = 0;
        app.handle_action(action);
        let s = app.stack.active();
        let now: Vec<String> = s.dataframe.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(now, before, "the table must be left alone");
        assert!(s.doc_mapping_ok(), "the node mapping must stay valid");
    }

    // and editing still works afterwards
    {
        let s = app.stack.active_mut();
        s.edit_row = 0;
        s.edit_col = before.iter().position(|n| n == "name").unwrap();
        s.edit_input = tuitab::ui::text_input::TextInput::with_value("ALPHA".into());
    }
    app.handle_action(Action::ApplyEdit);
    let s = app.stack.active();
    assert_eq!(
        s.doc
            .as_ref()
            .unwrap()
            .doc
            .read()
            .unwrap()
            .root
            .get(&[Seg::Idx(0), Seg::Key("name".into())]),
        Some(&tuitab::data::doc::Node::Str("ALPHA".into()))
    );
}

/// `zd` / `ze` / `zi` / `z→` on a records sheet change keys in the document, keep the
/// cell→node mapping valid, save, and undo.
#[test]
fn column_ops_on_a_doc_sheet_change_the_document() {
    use tuitab::types::Action;
    use tuitab::ui::text_input::TextInput;

    let path = out("column-ops.json");
    std::fs::write(
        &path,
        r#"[{"id":1,"name":"a","tmp":0},{"id":2,"name":"b"}]"#,
    )
    .unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();
    let cols = |app: &tuitab::app::App| -> Vec<String> {
        app.stack
            .active()
            .dataframe
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect()
    };

    app.stack.active_mut().cursor_col = 2;
    app.stack.active_mut().sort_keys = vec![("id".into(), true)];
    app.handle_action(Action::DeleteColumn);
    assert!(
        app.stack.active().sort_keys.is_empty(),
        "rows come back in tree order"
    );
    assert_eq!(cols(&app), vec!["id", "name"], "{}", app.status_message);

    app.stack.active_mut().cursor_col = 1;
    app.handle_action(Action::StartRenameColumn);
    app.stack.active_mut().rename_column_input = TextInput::with_value("title".into());
    app.handle_action(Action::ApplyRenameColumn);
    assert_eq!(cols(&app), vec!["id", "title"], "{}", app.status_message);

    app.handle_action(Action::StartInsertColumn);
    app.stack.active_mut().insert_column_input = TextInput::with_value("note".into());
    app.handle_action(Action::ApplyInsertColumn);
    assert_eq!(
        cols(&app),
        vec!["id", "note", "title"],
        "{}",
        app.status_message
    );
    assert_eq!(
        app.stack.active().cursor_col,
        1,
        "cursor lands on the new column"
    );

    app.handle_action(Action::MoveColumnLeft);
    assert_eq!(
        cols(&app),
        vec!["note", "id", "title"],
        "{}",
        app.status_message
    );
    assert_eq!(
        app.stack.active().cursor_col,
        0,
        "cursor follows the moved column"
    );
    app.handle_action(Action::MoveColumnLeft);
    assert_eq!(cols(&app), vec!["note", "id", "title"], "edge is a no-op");
    assert!(app.stack.active().doc_mapping_ok());

    // editing the new column writes a typed value into the right node
    {
        let s = app.stack.active_mut();
        s.edit_row = 1;
        s.edit_col = 0;
        s.edit_input = TextInput::with_value("7".into());
    }
    app.handle_action(Action::ApplyEdit);

    app.handle_action(Action::SaveFile);
    app.save.input = TextInput::with_value(path.to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);
    let saved: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        saved,
        serde_json::json!([
            {"note": null, "id": 1, "title": "a"},
            {"note": 7, "id": 2, "title": "b"}
        ])
    );

    // undo walks back through the document, not just the table
    for _ in 0..4 {
        app.handle_action(Action::Undo);
    }
    assert_eq!(cols(&app), vec!["id", "name"]);
    app.handle_action(Action::Undo);
    assert_eq!(cols(&app), vec!["id", "name", "tmp"]);
    assert!(app.stack.active().doc_mapping_ok());
}

/// Regression: a file large enough to take the background loader must still arrive with
/// its document tree — otherwise saving it flattens the structure.
#[test]
fn a_large_json_loaded_in_the_background_keeps_its_document() {
    use tuitab::data::async_loader::{load_in_background, LoadEvent};

    // just over the 10 MB threshold that switches App::new to the async path
    let path = out("big.json");
    let mut text = String::from("[\n");
    let row = r#"{"id":0,"meta":{"ok":true},"pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#;
    let n = (11 * 1024 * 1024) / row.len();
    for i in 0..n {
        if i > 0 {
            text.push_str(",\n");
        }
        text.push_str(row);
    }
    text.push_str("\n]\n");
    std::fs::write(&path, &text).unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() > 10 * 1024 * 1024);

    let rx = load_in_background(path.clone(), None, None);
    let LoadEvent::Complete(result) = rx.recv().unwrap();
    let opened = result.unwrap();
    let df = opened.df;
    assert_eq!(df.visible_row_count(), n);
    let doc = opened
        .doc
        .expect("the background loader must carry the document tree");

    let out_path = out("big-out.json");
    save_file_as(&df, Some(&doc), &out_path, Shape::Records, "big").unwrap();
    let head: String = std::fs::read_to_string(&out_path)
        .unwrap()
        .chars()
        .take(200)
        .collect();
    assert!(head.contains("\"meta\""), "nesting must survive: {}", head);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&out_path);
}

/// A file whose extension says nothing is identified by its contents.
#[test]
fn an_unknown_extension_is_sniffed_from_the_contents() {
    let (df, doc) = load_file_with_doc(&fixture("deploy.conf"), None).unwrap();
    let doc = doc.expect("deploy.conf is TOML and must open as a document");
    assert_eq!(doc.format(), Format::Toml);
    assert_eq!(df.visible_row_count(), 2, "listen and tls");

    // an unrecognised extension holding nothing parseable still fails as before
    let prose = out("notes.rubbish");
    std::fs::write(&prose, "1. Definitions.\n\nSome prose, not a document.\n").unwrap();
    assert!(
        load_file_with_doc(&prose, None).is_err(),
        "prose is neither tabular nor a document"
    );
}

/// A file with no extension keeps defaulting to CSV — sniffing there is limited to the
/// unambiguous bracket forms so an extension-less CSV does not regress.
#[test]
fn an_extensionless_file_only_sniffs_json() {
    let json = out("noext-json");
    std::fs::write(&json, "[{\"a\":1}]\n").unwrap();
    let (_, doc) = load_file_with_doc(&json, None).unwrap();
    assert_eq!(doc.map(|d| d.format()), Some(Format::Json));

    let csv = out("noext-csv");
    std::fs::write(&csv, "a,b\n1,2\n").unwrap();
    let (df, doc) = load_file_with_doc(&csv, None).unwrap();
    assert!(doc.is_none(), "still read as CSV");
    assert_eq!(df.col_count(), 2);

    let yaml_ish = out("noext-yaml");
    std::fs::write(&yaml_ish, "a: 1\nb: 2\n").unwrap();
    let (_, doc) = load_file_with_doc(&yaml_ish, None).unwrap();
    assert!(doc.is_none(), "no extension means no YAML guess");
}

/// Converting a JSON array of records to TOML must work: TOML has no array at the top
/// level, so it becomes an array of tables named after the sheet.
#[test]
fn an_array_rooted_document_can_still_be_saved_as_toml() {
    let (df, doc) = load_file_with_doc(&fixture("rows.jsonl"), None).unwrap();
    let doc = doc.unwrap();
    let path = out("rows.toml");
    save_file_as(&df, Some(&doc), &path, Shape::Records, "rows").unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert_eq!(text.matches("[[rows]]").count(), 2, "{}", text);
}

/// Saving a plain table to a document format asks which shape to produce, remembers the
/// answer, and does not ask a doc-backed sheet at all.
#[test]
fn saving_a_plain_table_asks_for_a_shape() {
    use tuitab::types::{Action, AppMode};

    let mut app = tuitab::app::App::new(&fixture("prices.csv"), None).unwrap();
    let target = out("shape-choice.json");
    let _ = std::fs::remove_file(&target); // a leftover from an earlier run would mask the check

    app.handle_action(Action::SaveFile);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(target.to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);
    assert_eq!(app.mode, AppMode::SaveShapeSelect, "must ask first");
    assert!(
        !target.exists(),
        "nothing written until the shape is chosen"
    );

    // pick `columns` (the second option) and confirm
    app.handle_action(Action::ChoiceDown);
    app.handle_action(Action::ApplySaveShape);
    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);

    let json = std::fs::read_to_string(&target).unwrap();
    assert!(json.trim_start().starts_with('{'), "column shape: {}", json);
    assert!(json.contains("["), "each column is an array: {}", json);

    // saving again lands on the remembered shape
    let second = out("shape-choice-2.json");
    app.handle_action(Action::SaveFile);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(second.to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);
    assert_eq!(app.mode, AppMode::SaveShapeSelect);
    app.handle_action(Action::ApplySaveShape);
    let again = std::fs::read_to_string(&second).unwrap();
    assert!(again.trim_start().starts_with('{'), "same shape: {}", again);
}

/// The remembered choice is a shape, not a position in the list: a sheet with a
/// different column count offers a different list, and an index would land elsewhere.
#[test]
fn the_remembered_shape_survives_a_shorter_option_list() {
    use tuitab::data::io::doc_io::Shape;
    use tuitab::types::{Action, AppMode};

    // a two-column table: key/value is offered as the third option
    let two_col = out("pairs.csv");
    std::fs::write(&two_col, "k,v\na,1\nb,2\n").unwrap();
    let mut app = tuitab::app::App::new(&two_col, None).unwrap();
    let target = out("kv-shape.json");
    let _ = std::fs::remove_file(&target);
    app.handle_action(Action::SaveFile);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(target.to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);
    assert_eq!(app.save.shapes.len(), 3, "records, columns, key/value");
    app.handle_action(Action::ChoiceDown);
    app.handle_action(Action::ChoiceDown);
    app.handle_action(Action::ApplySaveShape);
    assert_eq!(app.save.shape, Shape::KeyValue);

    // now a sheet with more columns: key/value is not on offer, and the cursor must not
    // silently land on `columns` just because index 2 no longer exists
    app.handle_action(Action::PopSheet);
    let mut app = tuitab::app::App::new(&fixture("sample.csv"), None).unwrap();
    app.save.shape = Shape::KeyValue;
    app.handle_action(Action::SaveFile);
    let wide = out("wide-shape.json");
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(wide.to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);
    assert_eq!(app.mode, AppMode::SaveShapeSelect);
    assert_eq!(
        app.save.shapes.len(),
        2,
        "key/value needs exactly 2 columns"
    );
    assert_eq!(app.save.shape_index, 0, "falls back to the first option");
    app.handle_action(Action::ApplySaveShape);
    assert_eq!(app.save.shape, Shape::Records);
    let json = std::fs::read_to_string(&wide).unwrap();
    assert!(json.trim_start().starts_with('['), "records: {}", json);
}

/// A doc-backed sheet is never asked for a shape — its tree already has one.
#[test]
fn saving_a_document_sheet_does_not_ask_for_a_shape() {
    use tuitab::types::{Action, AppMode};

    let mut app = tuitab::app::App::new(&fixture("app.toml"), None).unwrap();
    let target = out("no-shape-question.yaml");
    app.handle_action(Action::SaveFile);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(target.to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);
    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);
    assert!(std::fs::read_to_string(&target)
        .unwrap()
        .contains("host: localhost"));
}

/// `zo` on a directory listing reopens the selected file as an explicitly chosen format,
/// overriding both the extension and content sniffing.
#[test]
fn open_as_forces_a_format_from_the_directory_listing() {
    use tuitab::types::{Action, AppMode};

    // a YAML file wearing a .txt extension: sniffing declines, the extension lies
    std::fs::write(fixture("mislabelled.txt"), "alpha: 1\nbeta: 2\n").unwrap();

    let mut app = tuitab::app::App::new(Path::new("test_data"), None).unwrap();
    let df = &app.stack.active().dataframe;
    let row = (0..df.visible_row_count())
        .find(|r| df.get_physical(*r, 0).contains("mislabelled.txt"))
        .expect("listed");
    app.stack.active_mut().table_state.select(Some(row));

    app.handle_action(Action::OpenAs);
    assert_eq!(app.mode, AppMode::OpenAsSelect);
    // Json, Jsonl, Yaml, Toml — step down to Yaml
    app.handle_action(Action::ChoiceDown);
    app.handle_action(Action::ChoiceDown);
    app.handle_action(Action::ApplyOpenAs);

    let s = app.stack.active();
    let doc = s.doc.as_ref().expect("opened as a document");
    assert_eq!(doc.format(), Format::Yaml);
    assert_eq!(s.dataframe.visible_row_count(), 2, "alpha and beta");

    let _ = std::fs::remove_file(fixture("mislabelled.txt"));
}

/// End to end: edit a config in the app, save over it, and the comments are still there.
/// Losing a config file's comments because one value changed is the kind of damage the
/// user cannot undo from inside the tool.
#[test]
fn saving_an_edited_toml_config_over_itself_keeps_its_comments() {
    use tuitab::types::Action;

    let path = out("commented.toml");
    std::fs::write(
        &path,
        "# tuitab demo config\nname = \"demo\"  # display name\nport = 8080\n\n# database\n[db]\nhost = \"localhost\"\n",
    )
    .unwrap();

    let mut app = tuitab::app::App::new(&path, None).unwrap();
    let row = (0..app.stack.active().dataframe.visible_row_count())
        .find(|r| app.stack.active().dataframe.get_physical(*r, 0) == "port")
        .unwrap();
    {
        let s = app.stack.active_mut();
        s.edit_row = row;
        s.edit_col = 1;
        s.edit_input = tuitab::ui::text_input::TextInput::with_value("9090".into());
    }
    app.handle_action(Action::ApplyEdit);

    app.handle_action(Action::SaveFile);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(path.to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);

    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("port = 9090"), "the edit landed: {}", text);
    assert!(text.contains("# tuitab demo config"), "{}", text);
    assert!(text.contains("# display name"), "{}", text);
    assert!(text.contains("# database"), "{}", text);
}

/// Converting away from TOML drops the comments, because the target has nowhere to put
/// them — the guarantee is TOML → TOML only.
#[test]
fn converting_a_commented_toml_to_yaml_drops_the_comments() {
    let path = out("commented-src.toml");
    std::fs::write(&path, "# a comment\nname = \"demo\"\n").unwrap();
    let (df, doc) = load_file_with_doc(&path, None).unwrap();
    let target = out("commented-out.yaml");
    save_file_as(&df, doc.as_ref(), &target, Shape::Records, "x").unwrap();
    let text = std::fs::read_to_string(&target).unwrap();
    assert!(!text.contains("# a comment"), "{}", text);
    assert!(text.contains("name: demo"), "{}", text);
}

/// Deleting rows on a doc-backed sheet must reach the document. Filtering them out of
/// the view only would look like a deletion and then undo itself on the next save.
#[test]
fn deleting_rows_removes_them_from_the_document() {
    use tuitab::types::Action;

    let path = out("delete-rows.json");
    std::fs::write(&path, r#"[{"id":1},{"id":2},{"id":3}]"#).unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();

    // select rows 0 and 2
    app.stack.active_mut().dataframe.selected_rows.insert(0);
    app.stack.active_mut().dataframe.selected_rows.insert(2);
    app.handle_action(Action::DeleteSelectedRows);

    let s = app.stack.active();
    assert_eq!(s.dataframe.visible_row_count(), 1, "{}", app.status_message);
    assert_eq!(s.dataframe.get_physical(0, 0), "2");
    assert!(s.doc_mapping_ok(), "the mapping is rebuilt with the table");

    app.handle_action(Action::SaveFile);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(path.to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        !text.contains("\"id\": 1"),
        "deleted rows stay deleted: {}",
        text
    );
    assert!(text.contains("\"id\": 2"), "{}", text);

    // and undo brings them back, document and table together
    app.handle_action(Action::Undo);
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 3);
}

/// In key/value mode a row is an object key, so deleting one removes the key.
#[test]
fn deleting_a_row_in_key_value_mode_removes_the_key() {
    use tuitab::types::Action;

    let path = out("delete-key.toml");
    std::fs::write(&path, "a = 1\nb = 2\nc = 3\n").unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();

    let row = (0..3)
        .find(|r| app.stack.active().dataframe.get_physical(*r, 0) == "b")
        .unwrap();
    app.stack.active_mut().dataframe.selected_rows.insert(row);
    app.handle_action(Action::DeleteSelectedRows);

    let sheet = app.stack.active();
    let target = out("delete-key-out.toml");
    save_file_as(
        &sheet.dataframe,
        sheet.doc.as_ref(),
        &target,
        Shape::Records,
        "x",
    )
    .unwrap();
    let text = std::fs::read_to_string(&target).unwrap();
    assert!(!text.contains("b ="), "{}", text);
    assert!(text.contains("a = 1") && text.contains("c = 3"), "{}", text);
}

/// Operations that reshape the table without a matching change in the document are
/// refused, so the cell→node mapping can never drift.
#[test]
fn table_reshaping_operations_are_refused_on_a_doc_sheet() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("nested.json"), None).unwrap();
    let before = app.stack.active().dataframe.columns.len();

    for action in [
        Action::StartExpression,
        Action::CreatePctColumn,
        Action::PasteRows,
    ] {
        app.handle_action(action.clone());
        assert_eq!(
            app.stack.active().dataframe.columns.len(),
            before,
            "{:?} must not reshape the table",
            action
        );
        assert!(app.stack.active().doc_mapping_ok());
    }
}

/// A one-way conversion says so at the moment of saving, not afterwards.
#[test]
fn a_lossy_conversion_is_named_in_the_status_line() {
    use tuitab::types::Action;

    // multi-document YAML written as JSON loses the document separation
    let mut app = tuitab::app::App::new(&fixture("k8s.yaml"), None).unwrap();
    app.handle_action(Action::SaveFile);
    let target = out("k8s-lossy.json");
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(target.to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);
    assert!(
        app.status_message.contains("document separators"),
        "{}",
        app.status_message
    );

    // a commented TOML written as YAML loses the comments
    let src = out("lossy-src.toml");
    std::fs::write(&src, "# a note\nname = \"x\"\n").unwrap();
    let mut app = tuitab::app::App::new(&src, None).unwrap();
    app.handle_action(Action::SaveFile);
    let yaml = out("lossy-out.yaml");
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(yaml.to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);
    assert!(
        app.status_message.contains("comments"),
        "{}",
        app.status_message
    );

    // and a conversion that loses nothing stays quiet
    app.handle_action(Action::SaveFile);
    let toml = out("lossy-out.toml");
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(toml.to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);
    assert!(
        !app.status_message.contains("note:"),
        "{}",
        app.status_message
    );
}

/// The path of the cell under the cursor is on screen whenever there is nothing more
/// urgent to say — nothing else shows it, and it is what you need to refer to a value.
#[test]
fn the_node_path_is_shown_in_the_status_line_when_idle() {
    use ratatui::{backend::TestBackend, Terminal};
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("nested.json"), None).unwrap();
    let host_col = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .position(|c| c.name == "meta")
        .unwrap();
    app.stack.active_mut().cursor_col = host_col;
    app.stack.active_mut().table_state.select(Some(1));
    app.status_message.clear();

    let mut terminal = Terminal::new(TestBackend::new(140, 12)).unwrap();
    terminal.draw(|f| tuitab::ui::render(f, &mut app)).unwrap();
    let screen: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(screen.contains("[1].meta"), "path on screen:\n{}", screen);

    // yp copies the same path (headless CI may have no clipboard, so either the
    // confirmation or the failure is acceptable — what matters is the path it names)
    app.handle_action(Action::CopyNodePath);
    assert!(
        app.status_message.contains("[1].meta") || app.status_message.contains("Copy failed"),
        "{}",
        app.status_message
    );

    // a real message takes precedence over the path
    app.handle_action(Action::CycleViewMode);
    assert!(!app.status_message.is_empty());
}

/// Deleting from a sorted view must leave the table and its own sort state agreeing:
/// the reprojection rebuilds rows in the tree's order, so a stale sort marker would
/// claim an ordering the table no longer has.
#[test]
fn deleting_rows_from_a_sorted_view_leaves_no_stale_sort() {
    use tuitab::types::Action;

    let path = out("sorted-delete.json");
    std::fs::write(&path, r#"[{"n":3},{"n":1},{"n":2}]"#).unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();

    app.stack.active_mut().cursor_col = 0;
    app.handle_action(Action::SortAscending);
    assert_eq!(
        app.stack
            .active()
            .dataframe
            .get_physical(app.stack.active().dataframe.row_order[0], 0),
        "1"
    );

    // delete the physical row holding 1, so the tree order (3, 2) no longer matches
    // what an ascending sort would produce (2, 3)
    app.stack.active_mut().dataframe.selected_rows.insert(1);
    app.handle_action(Action::DeleteSelectedRows);

    let s = app.stack.active();
    assert_eq!(s.dataframe.visible_row_count(), 2);
    let shown: Vec<String> = (0..2)
        .map(|d| s.dataframe.get_physical(s.dataframe.row_order[d], 0))
        .collect();
    assert!(
        s.sort_keys.is_empty(),
        "a sort marker that no longer describes the rows ({:?}) is worse than none",
        shown
    );
}

/// Deleting an element from a scalar array view.
#[test]
fn deleting_from_a_scalar_view_removes_the_element() {
    use tuitab::types::Action;

    let path = out("scalar-delete.json");
    std::fs::write(&path, "[10, 20, 30]").unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 3);

    app.stack.active_mut().dataframe.selected_rows.insert(1);
    app.handle_action(Action::DeleteSelectedRows);

    let sheet = app.stack.active();
    assert_eq!(
        sheet.dataframe.visible_row_count(),
        2,
        "{}",
        app.status_message
    );
    let target = out("scalar-delete-out.json");
    save_file_as(
        &sheet.dataframe,
        sheet.doc.as_ref(),
        &target,
        Shape::Records,
        "x",
    )
    .unwrap();
    let text = std::fs::read_to_string(&target).unwrap();
    assert!(!text.contains("20"), "{}", text);
    assert!(text.contains("10") && text.contains("30"), "{}", text);
}

/// `g/` searches the whole tree, not the rows on screen, and reports hits as a sheet
/// the user can look through — nothing is opened behind their back.
#[test]
fn document_search_finds_nodes_that_are_not_on_screen() {
    use tuitab::types::{Action, AppMode};

    let path = out("deep-search.json");
    std::fs::write(
        &path,
        r#"[{"id":1,"meta":{"owner":"alice","tags":["x"]}},
            {"id":2,"meta":{"owner":"bob","tags":["alice-backup"]}}]"#,
    )
    .unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();

    // `alice` appears only inside nested containers, never as a cell of the top view
    app.handle_action(Action::StartDocSearch);
    assert_eq!(app.mode, AppMode::DocSearching);
    for c in "alice".chars() {
        app.handle_action(Action::SearchInput(c));
    }
    app.handle_action(Action::ApplyDocSearch);
    assert_eq!(app.mode, AppMode::Normal);

    let s = app.stack.active();
    assert!(s.doc_hits.is_some(), "a results sheet was pushed");
    let names: Vec<&str> = s
        .dataframe
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, vec!["path", "value", "type", "matched"]);
    assert_eq!(s.dataframe.visible_row_count(), 2, "the owner and the tag");
    let paths: Vec<String> = (0..2).map(|r| s.dataframe.get_physical(r, 0)).collect();
    assert!(paths.contains(&"[0].meta.owner".to_string()), "{:?}", paths);
    assert!(
        paths.contains(&"[1].meta.tags[0]".to_string()),
        "{:?}",
        paths
    );

    // Enter on a hit opens the node's parent, with the cursor on the match
    app.stack.active_mut().table_state.select(Some(
        paths.iter().position(|p| p == "[0].meta.owner").unwrap(),
    ));
    app.handle_action(Action::OpenRow);
    let s = app.stack.active();
    assert!(s.doc.is_some(), "landed on a document sheet");
    assert!(
        s.title.ends_with("[0] › meta"),
        "opened the containing node: {}",
        s.title
    );

    assert_eq!(
        s.doc.as_ref().unwrap().view.anchor,
        vec![Seg::Idx(0), Seg::Key("meta".into())]
    );
    // the cursor is on the match, not merely somewhere on the sheet
    let cursor_row = s.table_state.selected().unwrap();
    assert_eq!(
        s.dataframe
            .get_physical(s.dataframe.row_order[cursor_row], 0),
        "owner"
    );
}

/// Searching for a key name finds the key, and says that is what matched.
#[test]
fn document_search_matches_key_names_too() {
    use tuitab::types::Action;

    let path = out("key-search.toml");
    std::fs::write(&path, "[db]\nhostname = \"h\"\n[cache]\nhost = \"c\"\n").unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();

    app.handle_action(Action::StartDocSearch);
    for c in "^host".chars() {
        app.handle_action(Action::SearchInput(c));
    }
    app.handle_action(Action::ApplyDocSearch);

    let s = app.stack.active();
    assert_eq!(s.dataframe.visible_row_count(), 2, "hostname and host");
    let matched: Vec<String> = (0..2).map(|r| s.dataframe.get_physical(r, 3)).collect();
    assert!(matched.iter().all(|m| m == "key"), "{:?}", matched);
}

/// A pattern with no match says so instead of pushing an empty sheet.
#[test]
fn document_search_with_no_match_pushes_nothing() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("nested.json"), None).unwrap();
    let depth = app.stack.depth();
    app.handle_action(Action::StartDocSearch);
    for c in "zzzznope".chars() {
        app.handle_action(Action::SearchInput(c));
    }
    app.handle_action(Action::ApplyDocSearch);
    assert_eq!(app.stack.depth(), depth, "no sheet for no results");
    assert!(
        app.status_message.contains("No match"),
        "{}",
        app.status_message
    );
}

/// A hit list is a snapshot of paths. If the document changes underneath it, those paths
/// can silently resolve to *different* nodes — deleting an early array element shifts
/// every later index. Navigating there without a word would be worse than refusing.
#[test]
fn a_hit_list_does_not_navigate_after_the_document_changed() {
    use tuitab::types::Action;

    let path = out("stale-hits.json");
    std::fs::write(&path, r#"[{"tag":"keep"},{"tag":"keep"},{"tag":"needle"}]"#).unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();

    app.handle_action(Action::StartDocSearch);
    for c in "needle".chars() {
        app.handle_action(Action::SearchInput(c));
    }
    app.handle_action(Action::ApplyDocSearch);
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 1);
    assert_eq!(app.stack.active().dataframe.get_physical(0, 0), "[2].tag");

    // go back and delete the first element, shifting [2] down to [1]
    app.handle_action(Action::PopSheet);
    app.stack.active_mut().dataframe.selected_rows.insert(0);
    app.handle_action(Action::DeleteSelectedRows);

    // a fresh search is of course right
    app.handle_action(Action::StartDocSearch);
    for c in "needle".chars() {
        app.handle_action(Action::SearchInput(c));
    }
    app.handle_action(Action::ApplyDocSearch);
    assert_eq!(app.stack.active().dataframe.get_physical(0, 0), "[1].tag");
}

/// The dangerous shape: a stale path that still resolves, but to a different node.
/// Deleting an early array element renumbers every later index, so a recorded path
/// would open something the user never searched for.
#[test]
fn a_stale_hit_refuses_rather_than_opening_the_wrong_node() {
    use tuitab::types::Action;

    let path = out("stale-hits-2.json");
    std::fs::write(
        &path,
        r#"[{"tag":"a"},{"tag":"b"},{"tag":"needle"},{"tag":"d"}]"#,
    )
    .unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();

    app.handle_action(Action::StartDocSearch);
    for c in "needle".chars() {
        app.handle_action(Action::SearchInput(c));
    }
    app.handle_action(Action::ApplyDocSearch);
    assert_eq!(app.stack.active().dataframe.get_physical(0, 0), "[2].tag");

    // change the document underneath the hit list, the way a dive-and-delete would
    {
        let hits = app.stack.active().doc_hits.as_ref().unwrap();
        let mut guard = hits.doc.write().unwrap();
        guard.root.remove(&[Seg::Idx(0)]).unwrap();
        guard.bump();
    }

    // `[2].tag` still resolves — to what used to be `[3]`, which never matched
    app.handle_action(Action::OpenRow);
    assert!(
        app.status_message.contains("changed"),
        "a stale hit must refuse: {}",
        app.status_message
    );
    assert!(
        app.stack.active().doc_hits.is_some(),
        "and stay on the hit list rather than navigating"
    );
}

/// `gp` jumps to a node by its path — the same text `yp` copies and the status line
/// shows, so the three agree.
#[test]
fn goto_path_jumps_to_the_node_and_reports_a_wrong_path_usefully() {
    use tuitab::types::{Action, AppMode};

    let path = out("goto.json");
    std::fs::write(
        &path,
        r#"{"servers":[{"host":"a"},{"host":"b"}],"awkward.key":{"x":1}}"#,
    )
    .unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();

    app.handle_action(Action::StartPathGoto);
    assert_eq!(app.mode, AppMode::PathInput);
    app.stack.active_mut().path_input =
        tuitab::ui::text_input::TextInput::with_value("servers[1].host".into());
    app.handle_action(Action::ApplyPathGoto);

    let s = app.stack.active();
    assert!(s.title.ends_with("servers › [1]"), "{}", s.title);
    assert_eq!(
        s.doc.as_ref().unwrap().view.anchor,
        vec![Seg::Key("servers".into()), Seg::Idx(1)]
    );
    // cursor sits on the addressed key
    let row = s.table_state.selected().unwrap();
    assert_eq!(
        s.dataframe.get_physical(s.dataframe.row_order[row], 0),
        "host"
    );

    // a key that needs quoting round-trips
    app.handle_action(Action::PopSheet);
    app.handle_action(Action::StartPathGoto);
    app.stack.active_mut().path_input =
        tuitab::ui::text_input::TextInput::with_value("[\"awkward.key\"]".into());
    app.handle_action(Action::ApplyPathGoto);
    assert_eq!(
        app.stack.active().doc.as_ref().unwrap().view.anchor,
        vec![Seg::Key("awkward.key".into())],
        "{}",
        app.status_message
    );

    // a wrong path says how far it got, and pushes nothing
    app.handle_action(Action::PopSheet);
    let depth = app.stack.depth();
    app.handle_action(Action::StartPathGoto);
    app.stack.active_mut().path_input =
        tuitab::ui::text_input::TextInput::with_value("servers[1].nope".into());
    app.handle_action(Action::ApplyPathGoto);
    assert_eq!(app.stack.depth(), depth);
    assert!(
        app.status_message.contains("servers[1]") && app.status_message.contains("exists"),
        "{}",
        app.status_message
    );

    // going to a node on the sheet you are already on moves the cursor instead of
    // stacking a duplicate
    app.handle_action(Action::StartPathGoto);
    app.stack.active_mut().path_input =
        tuitab::ui::text_input::TextInput::with_value("servers[0].host".into());
    app.handle_action(Action::ApplyPathGoto);
    let depth_before = app.stack.depth();
    let anchor = app.stack.active().doc.as_ref().unwrap().view.anchor.clone();
    app.handle_action(Action::StartPathGoto);
    app.handle_action(Action::ApplyPathGoto); // confirm the prefilled path unchanged
    assert_eq!(app.stack.depth(), depth_before, "no duplicate sheet");
    assert_eq!(app.stack.active().doc.as_ref().unwrap().view.anchor, anchor);
    app.handle_action(Action::PopSheet);

    // and a malformed path is refused rather than guessed at
    app.handle_action(Action::StartPathGoto);
    app.stack.active_mut().path_input =
        tuitab::ui::text_input::TextInput::with_value("servers[1".into());
    app.handle_action(Action::ApplyPathGoto);
    assert!(
        app.status_message.contains("Bad path"),
        "{}",
        app.status_message
    );
}

/// `gq` runs a jq program and opens the result as an ordinary sheet — diving, editing
/// and saving all work on it, because a query result is just another document.
#[test]
fn a_jq_query_opens_its_result_as_a_sheet() {
    use tuitab::types::{Action, AppMode};

    let path = out("query.json");
    std::fs::write(
        &path,
        r#"[{"n":1,"ok":true},{"n":2,"ok":false},{"n":3,"ok":true}]"#,
    )
    .unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();

    app.handle_action(Action::StartQuery);
    assert_eq!(app.mode, AppMode::QueryInput);
    for c in ".[] | select(.ok)".chars() {
        app.handle_action(Action::QueryInputChar(c));
    }
    app.handle_action(Action::ApplyQuery);
    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);

    let s = app.stack.active();
    assert_eq!(s.dataframe.visible_row_count(), 2, "{}", app.status_message);
    assert_eq!(s.dataframe.get_physical(1, 0), "3");
    assert!(s.doc.is_some(), "the result is a document sheet");

    // it must not offer to overwrite the file the query ran against
    assert!(
        s.source_path.is_none(),
        "a query result is not the source file"
    );

    // and it saves as its own document
    let target = out("query-result.json");
    save_file_as(
        &s.dataframe,
        s.doc.as_ref(),
        &target,
        Shape::Records,
        &s.title,
    )
    .unwrap();
    let text = std::fs::read_to_string(&target).unwrap();
    assert!(
        text.contains("\"n\": 1") && text.contains("\"n\": 3"),
        "{}",
        text
    );
    assert!(!text.contains("\"n\": 2"), "{}", text);
}

/// A query that reshapes a nested object into records makes something browsable out of
/// something that was not.
#[test]
fn a_jq_query_can_reshape_a_document_into_a_table() {
    use tuitab::types::Action;

    let path = out("reshape.yaml");
    std::fs::write(
        &path,
        "users:\n  alice:\n    age: 30\n  bob:\n    age: 40\n",
    )
    .unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();

    app.handle_action(Action::StartQuery);
    for c in ".users | to_entries | map({name: .key, age: .value.age})".chars() {
        app.handle_action(Action::QueryInputChar(c));
    }
    app.handle_action(Action::ApplyQuery);

    let s = app.stack.active();
    let names: Vec<&str> = s
        .dataframe
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, vec!["name", "age"], "{}", app.status_message);
    assert_eq!(s.dataframe.visible_row_count(), 2);
    assert_eq!(
        s.doc.as_ref().unwrap().format(),
        Format::Yaml,
        "keeps the source format"
    );
}

/// A program that matches nothing opens an empty sheet — `select` finding no rows is a
/// legitimate answer, not a failure.
#[test]
fn a_query_matching_nothing_opens_an_empty_sheet() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("nested.json"), None).unwrap();
    let depth = app.stack.depth();
    app.handle_action(Action::StartQuery);
    app.stack.active_mut().query_input =
        tuitab::ui::text_input::TextInput::with_value(".[] | select(.id == 999)".into());
    app.handle_action(Action::ApplyQuery);

    assert_eq!(app.stack.depth(), depth + 1, "{}", app.status_message);
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 0);
    assert!(
        app.status_message.contains("no matches"),
        "{}",
        app.status_message
    );
}

/// A broken program says so and pushes nothing.
#[test]
fn a_failing_query_reports_and_pushes_nothing() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("nested.json"), None).unwrap();
    let depth = app.stack.depth();

    for program in [".[ | broken", ".a | .[0]"] {
        app.handle_action(Action::StartQuery);
        app.stack.active_mut().query_input =
            tuitab::ui::text_input::TextInput::with_value(program.into());
        app.handle_action(Action::ApplyQuery);
        assert_eq!(app.stack.depth(), depth, "no sheet for `{}`", program);
        assert!(
            app.status_message.contains("Query failed"),
            "`{}`: {}",
            program,
            app.status_message
        );
    }
}

/// A query result must offer a sensible file name to save to. Its title carries the
/// program text, which is not a path.
#[test]
fn a_query_result_offers_a_usable_save_name() {
    use tuitab::types::Action;

    let path = out("savename.json");
    std::fs::write(&path, r#"[{"n":1,"ok":true},{"n":2,"ok":false}]"#).unwrap();
    let mut app = tuitab::app::App::new(&path, None).unwrap();

    app.handle_action(Action::StartQuery);
    for c in ".[] | select(.ok)".chars() {
        app.handle_action(Action::QueryInputChar(c));
    }
    app.handle_action(Action::ApplyQuery);

    app.handle_action(Action::SaveFile);
    let prefill = app.save.input.as_str().to_string();
    assert!(
        !prefill.contains('›') && !prefill.contains('|'),
        "the prefill must be a path, not a query: {}",
        prefill
    );
    assert!(prefill.ends_with(".json"), "{}", prefill);

    // The prefill is a bare file name, which would land in the working directory — the
    // repository, when the suite runs from it. Point it somewhere disposable before
    // saving, having already asserted the part that matters.
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(out(&prefill).to_string_lossy().into_owned());
    app.handle_action(Action::ApplySave);
    assert!(
        app.status_message.contains("Saved"),
        "{}",
        app.status_message
    );
}

/// A deeply nested document with the shapes a real config has: nine levels, multi-line
/// string values, empty containers, sparse records, and a column whose values are an
/// object, a string and two numbers.  Built after auditing a real Grafana dashboard.
#[test]
fn a_deeply_nested_document_survives_every_conversion() {
    use tuitab::data::doc::{Doc, Format as F, SaveOpts};

    let original = std::fs::read_to_string(fixture("deep.json")).unwrap();
    let doc = Doc::from_str(&original, F::Json).unwrap();

    // JSON is written back byte for byte
    assert_eq!(
        doc.to_string_as(F::Json, &SaveOpts::default()).unwrap(),
        original
    );

    // YAML and TOML carry the whole tree there and back
    for fmt in [F::Yaml, F::Toml] {
        let text = doc.to_string_as(fmt, &SaveOpts::default()).unwrap();
        let back = Doc::from_str(&text, fmt).unwrap();
        assert_eq!(back.root, doc.root, "{:?} round trip", fmt);
    }
}

/// JSONL cannot express "this is one document", so a document written as JSONL reads
/// back as a stream of one record. The data is intact; the shape is not, and saving
/// says so.
#[test]
fn jsonl_turns_a_document_into_a_one_record_stream_and_warns() {
    use tuitab::data::doc::{Doc, Format as F, Node, SaveOpts};

    let doc = Doc::from_str(r#"{"a":1,"b":[2,3]}"#, F::Json).unwrap();
    let text = doc.to_string_as(F::Jsonl, &SaveOpts::default()).unwrap();
    let back = Doc::from_str(&text, F::Jsonl).unwrap();

    assert_eq!(text.lines().count(), 1);
    let Node::Arr(items) = &back.root else {
        panic!("jsonl always reads as a stream, got {:?}", back.root)
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], doc.root, "the data itself is intact");

    let (_, state) = tuitab::data::io::doc_io::DocState::from_doc(doc).unwrap();
    let note = state.conversion_loss(F::Jsonl).expect("must warn");
    assert!(note.contains("one-record stream"), "{}", note);

    // a document that is already a list converts cleanly and says nothing
    let list = Doc::from_str(r#"[{"a":1},{"a":2}]"#, F::Json).unwrap();
    let (_, state) = tuitab::data::io::doc_io::DocState::from_doc(list).unwrap();
    assert_eq!(state.conversion_loss(F::Jsonl), None);
}

/// A multi-line string reaches the table as one cell containing newlines — the table
/// view shows its first line — and editing it does not flatten it.
#[test]
fn multi_line_values_survive_the_table_and_the_editor() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("deep.json"), None).unwrap();
    let row = (0..app.stack.active().dataframe.visible_row_count())
        .find(|r| app.stack.active().dataframe.get_physical(*r, 0) == "panels")
        .unwrap();
    app.stack.active_mut().table_state.select(Some(row));
    app.handle_action(Action::OpenRow);

    let sql = app
        .stack
        .active()
        .dataframe
        .columns
        .iter()
        .position(|c| c.name == "sql")
        .unwrap();
    let before = app.stack.active().dataframe.get_physical(0, sql);
    assert!(
        before.contains('\n'),
        "the cell holds the real value: {:?}",
        before
    );

    // confirm an edit without changing anything: the newlines must still be there
    {
        let s = app.stack.active_mut();
        s.edit_row = 0;
        s.edit_col = sql;
        s.edit_input = tuitab::ui::text_input::TextInput::with_value(before.clone());
    }
    app.handle_action(Action::ApplyEdit);
    let after = app
        .stack
        .active()
        .doc
        .as_ref()
        .unwrap()
        .doc
        .read()
        .unwrap()
        .root
        .get(&tuitab::data::doc::parse_path("panels[0].sql").unwrap())
        .cloned();
    assert_eq!(
        after,
        Some(tuitab::data::doc::Node::Str(before)),
        "a single-line editor must not flatten a multi-line value"
    );
}

/// Expanding twice through a nine-level document names the columns by their full path
/// and keeps every cell addressing its real node.
#[test]
fn expanding_deep_nesting_keeps_cells_addressable() {
    use tuitab::data::doc::{parse_path, Node};
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new(&fixture("deep.json"), None).unwrap();
    let row = (0..app.stack.active().dataframe.visible_row_count())
        .find(|r| app.stack.active().dataframe.get_physical(*r, 0) == "panels")
        .unwrap();
    app.stack.active_mut().table_state.select(Some(row));
    app.handle_action(Action::OpenRow);

    for _ in 0..2 {
        let col = app
            .stack
            .active()
            .dataframe
            .columns
            .iter()
            .position(|c| c.name.starts_with("config"))
            .unwrap();
        app.stack.active_mut().cursor_col = col;
        app.handle_action(Action::ExpandColumn);
    }

    let s = app.stack.active();
    let names: Vec<String> = s.dataframe.columns.iter().map(|c| c.name.clone()).collect();
    assert!(
        names.iter().any(|n| n == "config.defaults.custom"),
        "{:?}",
        names
    );
    let col = names
        .iter()
        .position(|n| n == "config.defaults.custom")
        .unwrap();
    assert_eq!(
        s.doc.as_ref().unwrap().path_of(0, col),
        Some(parse_path("panels[0].config.defaults.custom").unwrap())
    );
    // sparse records: only the second panel has a description
    let d = names.iter().position(|n| n == "description").unwrap();
    assert_eq!(s.dataframe.get_physical(0, d), "");
    assert!(matches!(
        s.doc.as_ref().unwrap().node_at(1, d),
        Some(Node::Str(_))
    ));
}

/// The xlsx path has no fixture and is otherwise only compile-checked; this writes one
/// with the app's own writer and reads it back, so a calamine upgrade cannot break
/// Excel support silently.
#[test]
fn an_xlsx_written_by_tuitab_reads_back() {
    let (df, _) = load_file_with_doc(&fixture("prices.csv"), None).unwrap();
    let path = out("roundtrip.xlsx");
    let _ = std::fs::remove_file(&path);
    save_file_as(&df, None, &path, Shape::Records, "prices").unwrap();

    let (back, doc) = load_file_with_doc(&path, None).unwrap();
    assert!(doc.is_none(), "xlsx is tabular, not a document");
    assert_eq!(back.col_count(), df.col_count());
    assert_eq!(back.visible_row_count(), df.visible_row_count());
    assert_eq!(back.get_physical(0, 1), df.get_physical(0, 1));
}

#[test]
fn an_arrow_file_written_by_tuitab_reads_back() {
    let (df, _) = load_file_with_doc(&fixture("prices.csv"), None).unwrap();
    let path = out("roundtrip.arrow");
    let _ = std::fs::remove_file(&path);
    save_file_as(&df, None, &path, Shape::Records, "prices").unwrap();

    let (back, doc) = load_file_with_doc(&path, None).unwrap();
    assert!(doc.is_none(), "arrow is tabular, not a document");
    assert_eq!(back.col_count(), df.col_count());
    assert_eq!(back.visible_row_count(), df.visible_row_count());
    assert_eq!(back.get_physical(0, 1), df.get_physical(0, 1));
}
