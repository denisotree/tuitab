use crate::app::App;
use crate::data::column::ColumnMeta;
use crate::data::io::db_write;
use crate::types::{Action, AppMode};

/// Carry what the user set up on each column across a reload, matched by name.
///
/// The metadata lives inside the frame, so replacing the frame drops pins, widths,
/// aggregators and precision — everything the user arranged before pressing save.  Names
/// come from the same table, so matching by name is exact; a column the save dropped is
/// simply not found, and there are never new ones.
///
/// A type assigned by hand is carried with its `db_retype` tag, never without: the tag
/// is what tells the next save that the column is a *reading* of the data rather than
/// the data, and the pair coming apart is what would make a reload plan a whole-table
/// UPDATE of the rendering.  Only the types no declared type can express are carried —
/// Integer, Float and String come back from the table itself.
fn restore_column_meta(df: &mut crate::data::dataframe::DataFrame, old: &[ColumnMeta]) {
    use crate::types::ColumnType;
    for i in 0..df.columns.len() {
        let Some(prev) = old.iter().find(|p| p.name == df.columns[i].name) else {
            continue;
        };
        let carried = matches!(
            prev.col_type,
            ColumnType::Date
                | ColumnType::Datetime
                | ColumnType::Boolean
                | ColumnType::Percentage
                | ColumnType::Currency
                | ColumnType::FileSize
        )
        .then_some(prev.col_type);
        let retype = prev.db_retype;
        let backup = prev.backup_datetime_str.clone();

        let col = &mut df.columns[i];
        col.pinned = prev.pinned;
        col.width = prev.width;
        col.width_mode = prev.width_mode;
        col.default_width = prev.default_width;
        col.precision = prev.precision;
        col.currency = prev.currency;
        col.selected = prev.selected;
        col.aggregators = prev.aggregators.clone();
        col.expression = prev.expression.clone();
        col.backup_datetime_str = backup;

        if let Some(t) = carried {
            // Applied, not assigned: the values have just been re-read as text, and the
            // type is what converts them.  A column whose new contents no longer parse
            // keeps what the reload gave it — and then must not claim the type either.
            if df.set_column_type(i, t).is_ok() {
                df.columns[i].db_retype = retype;
            }
        }
    }
}

impl App {
    /// Route a save whose target is a database file.
    ///
    /// Onto the file the sheet came from, the edits become UPDATE/INSERT/DELETE against
    /// the original table and the user reads them first.  Onto a different database
    /// file, the whole source is copied — every table, index and trigger — and the same
    /// statements are applied to the copy, which touches nothing the user has, so there
    /// is nothing to confirm.
    fn save_into_database(&mut self, path: std::path::PathBuf) -> Option<Action> {
        let sheet = self.stack.active();
        let src = sheet
            .table_source
            .clone()
            .expect("checked by the caller before routing here");

        let plan = match db_write::build_plan(&src, &sheet.dataframe) {
            Ok(plan) => plan,
            Err(e) => {
                self.save.error = Some(format!("Error: {}", e));
                return None;
            }
        };

        if db_write::same_file(&path, &src.db_path) {
            if plan.is_empty() {
                self.mode = AppMode::Normal;
                self.save.error = None;
                // A plan with nothing to run never reaches the popup, so anything it had
                // to say about what it is *not* writing has to be said here or nowhere.
                self.status_message = if plan.warnings.is_empty() {
                    format!("No changes to write to '{}'", src.table)
                } else {
                    format!(
                        "No changes to write to '{}' — {}",
                        src.table,
                        plan.warnings.join("; ")
                    )
                };
                return None;
            }
            self.sql.plan = Some(plan);
            self.sql.scroll = 0;
            self.sql.path = Some(path);
            self.mode = AppMode::SqlConfirm;
            return None;
        }

        match db_write::copy_db(&src, &path).and_then(|_| db_write::apply(&src.at(&path), &plan)) {
            Ok(()) => {
                self.mode = AppMode::Normal;
                self.save.error = None;
                self.status_message = format!(
                    "Copied the database to {} and applied {}",
                    path.display(),
                    plan.summary()
                );
            }
            Err(e) => self.save.error = Some(format!("Error: {}", e)),
        }
        None
    }
    /// Make the table the user just named, out of a sheet that has no table of its own.
    ///
    /// A brand-new file, or a new table in an existing one, is written straight away:
    /// nothing of the user's is at stake, which is the same rule that lets a copy into a
    /// fresh file skip confirmation.  Replacing a table that is already there does have
    /// something at stake, so that goes through the popup with its `DROP TABLE` visible.
    fn create_into_database(&mut self, path: std::path::PathBuf, table: String) -> Option<Action> {
        let kind = db_write::kind_for_path(&path);
        let existed = path.exists();
        let sheet = self.stack.active();

        let (plan, src) = match db_write::create_plan(kind, &path, &table, &sheet.dataframe) {
            Ok(pair) => pair,
            Err(e) => {
                self.save.error = Some(format!("Error: {}", e));
                return None;
            }
        };

        if plan.rebuild {
            self.sql.plan = Some(plan);
            self.sql.pending_source = Some(src);
            self.sql.path = Some(path);
            self.sql.scroll = 0;
            self.mode = AppMode::SqlConfirm;
            return None;
        }

        match db_write::apply(&src, &plan) {
            Ok(()) => {
                let summary = plan.summary();
                self.adopt_created_table(src, &path, &table);
                self.mode = AppMode::Normal;
                self.save.error = None;
                self.status_message =
                    format!("Created '{}' in {} — {}", table, path.display(), summary);
            }
            Err(e) => {
                if !existed {
                    db_write::remove_new_file(&path);
                }
                self.save.error = Some(format!("Error: {}", e));
            }
        }
        None
    }

    /// Point the sheet at the table it has just made, then re-read it.
    ///
    /// The reload is what turns an ordinary sheet into a writeback sheet: it comes back
    /// with row identities, origin tags and declared types, so the next `Ctrl+S` goes
    /// down the usual path with its usual confirmation.  A document-backed sheet does
    /// not adopt — its tree is the real thing and a table is only an export of it.
    fn adopt_created_table(
        &mut self,
        src: db_write::TableSource,
        path: &std::path::Path,
        table: &str,
    ) {
        if self.stack.active().doc.is_some() {
            return;
        }
        let kind = src.kind;
        let sheet = self.stack.active_mut();
        sheet.table_source = Some(src);
        sheet.source_path = Some(path.to_path_buf());
        // `--type` described the file the rows came from, not the database they now live in.
        sheet.forced_type = None;
        // A drill-down or a page of search hits saved into a new database is that
        // database's table now.
        sheet.derived_view = false;
        sheet.doc_hits = None;
        sheet.container = Some(table.to_string());
        sheet.title = format!("{} :: {}", path.display(), table);
        // The same two fields a drilled-into table gets, so the JOIN picker offers this
        // database's other tables without the file having to be reopened first.
        match kind {
            db_write::DbKind::Sqlite => sheet.sqlite_source_path = Some(path.to_path_buf()),
            db_write::DbKind::DuckDb => sheet.duckdb_source_path = Some(path.to_path_buf()),
        }
        let _ = self.reload_db_sheet();
    }

    pub(crate) fn handle_io_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::SaveFile => {
                self.save.error = None;
                self.save.table_name = None;
                let hint = self.stack.active().save_name_hint.clone();
                let default_path = self
                    .stack
                    .active()
                    .source_path
                    .as_deref()
                    .and_then(|p| {
                        std::env::current_dir()
                            .ok()
                            .and_then(|cwd| {
                                p.strip_prefix(&cwd)
                                    .ok()
                                    .map(|r| r.to_string_lossy().into_owned())
                            })
                            .or_else(|| Some(p.to_string_lossy().into_owned()))
                    })
                    .or(hint)
                    .unwrap_or_else(|| self.stack.active().title.clone());

                self.save.autocomplete_candidates.clear();
                self.save.autocomplete_prefix.clear();
                self.save.autocomplete_idx = 0;
                self.save.input = crate::ui::text_input::TextInput::with_value(default_path);
                self.mode = AppMode::Saving;
                None
            }
            Action::SavingInput(c) => {
                self.save.input.insert_char(c);
                None
            }
            Action::SavingBackspace => {
                self.save.input.delete_backward();
                None
            }
            Action::SavingForwardDelete => {
                self.save.input.delete_forward();
                None
            }
            Action::SavingCursorLeft => {
                self.save.input.move_cursor_left();
                None
            }
            Action::SavingCursorRight => {
                self.save.input.move_cursor_right();
                None
            }
            Action::SavingCursorStart => {
                self.save.input.move_cursor_start();
                None
            }
            Action::SavingCursorEnd => {
                self.save.input.move_cursor_end();
                None
            }
            Action::ApplySave => {
                // A path parked by the shape popup means the question has been answered
                // already; taking it here is what stops the two from bouncing forever.
                let answered = self.save.pending_path.take();
                let path = answered
                    .clone()
                    .unwrap_or_else(|| crate::app::expand_tilde(self.save.input.as_str()));
                // A sheet that came from a database table is saved by changing that
                // table, not by dumping the rows into a new file — unless the user
                // pointed the save somewhere else entirely, which is an export.
                if self.stack.active().table_source.is_some() && db_write::is_db_path(&path) {
                    return self.save_into_database(path);
                }
                // Two kinds of sheet come out of a database with no `table_source`, and
                // both would otherwise fall into the *creation* branch below and offer to
                // `DROP TABLE` the very thing they were read from.  A view survives that
                // by accident (the DROP fails); a WITHOUT ROWID table does not, and the
                // overview would write the database's own table listing back into it.
                if db_write::is_db_path(&path) {
                    let sheet = self.stack.active();
                    let overview = sheet
                        .sqlite_db_path
                        .as_ref()
                        .or(sheet.duckdb_db_path.as_ref());
                    let table = sheet
                        .sqlite_source_path
                        .as_ref()
                        .or(sheet.duckdb_source_path.as_ref());
                    let refusal = match (overview, table) {
                        (Some(p), _) if db_write::same_file(p, &path) => Some(
                            "This is the list of tables in the database, not a table — \
                             open one and save that.",
                        ),
                        (_, Some(p)) if db_write::same_file(p, &path) => Some(
                            "This table or view is read-only; save to a different file \
                             instead.",
                        ),
                        _ => None,
                    };
                    if let Some(why) = refusal {
                        self.save.error = Some(why.to_string());
                        return None;
                    }
                }
                // A sheet with no table behind it *makes* one, and a table needs a name.
                // Both branches must come before the document-shape question below, or a
                // .sqlite target would fall through to the plain exporter.
                if db_write::is_db_path(&path) {
                    if let Some(table) = self.save.table_name.clone() {
                        return self.create_into_database(path, table);
                    }
                    let stem = path
                        .file_stem()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| "data".to_string());
                    self.save.pending_path = Some(path);
                    self.save.table_input = crate::ui::text_input::TextInput::with_value(stem);
                    self.save.error = None;
                    self.mode = AppMode::TableNameInput;
                    return None;
                }
                // A table with no document behind it can become several different
                // documents, and only the user knows which one they meant — ask, once,
                // and remember the answer for the rest of the session.
                if answered.is_none() && self.needs_shape_for(&path) {
                    self.save.pending_path = Some(path);
                    self.save.shapes = crate::data::io::doc_io::Shape::options(
                        self.stack.active().dataframe.col_count(),
                    );
                    // Restore the cursor onto the remembered shape, not its old position.
                    self.save.shape_index = self
                        .save
                        .shapes
                        .iter()
                        .position(|s| *s == self.save.shape)
                        .unwrap_or(0);
                    self.mode = AppMode::SaveShapeSelect;
                    return None;
                }
                let shape = self.chosen_shape();
                let sheet = self.stack.active();
                // A doc-backed sheet is written by re-serialising its tree, so structure
                // survives and changing the extension converts between formats.
                let result = crate::data::io::save_file_as(
                    &sheet.dataframe,
                    sheet.doc.as_ref(),
                    &path,
                    shape,
                    &sheet.title,
                );
                let loss = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .and_then(crate::data::doc::Format::from_ext)
                    .and_then(|fmt| {
                        self.stack
                            .active()
                            .doc
                            .as_ref()
                            .and_then(|d| d.conversion_loss(fmt))
                    });
                match result {
                    Ok(_) => {
                        self.mode = AppMode::Normal;
                        self.status_message = match loss {
                            Some(note) => {
                                format!("Saved to {} — note: {}", self.save.input.as_str(), note)
                            }
                            None => {
                                format!("Saved successfully to: {}", self.save.input.as_str())
                            }
                        };
                        self.save.error = None;
                    }
                    Err(e) => {
                        self.save.error = Some(format!("Error: {}", e));
                    }
                }
                None
            }
            Action::CancelSave => {
                self.mode = AppMode::Normal;
                self.save.error = None;
                None
            }
            Action::TableNameChar(c) => {
                self.save.table_input.insert_char(c);
                None
            }
            Action::TableNameBackspace => {
                self.save.table_input.delete_backward();
                None
            }
            Action::TableNameForwardDelete => {
                self.save.table_input.delete_forward();
                None
            }
            Action::TableNameCursorLeft => {
                self.save.table_input.move_cursor_left();
                None
            }
            Action::TableNameCursorRight => {
                self.save.table_input.move_cursor_right();
                None
            }
            Action::TableNameCursorStart => {
                self.save.table_input.move_cursor_start();
                None
            }
            Action::TableNameCursorEnd => {
                self.save.table_input.move_cursor_end();
                None
            }
            Action::ApplyTableName => {
                let name = self.save.table_input.as_str().trim().to_string();
                // A name that clashes with an existing table is deliberately allowed —
                // it shows up as a DROP TABLE in the confirmation, which is a better
                // place to see it than a validation message.
                if let Err(problem) = db_write::validate_table_name(&name) {
                    self.save.error =
                        Some(format!("{}{}", problem[..1].to_uppercase(), &problem[1..]));
                    return None;
                }
                self.save.table_name = Some(name);
                self.save.error = None;
                // Re-entered rather than returned: returning would pass the action down
                // the handler chain instead of running it.
                self.mode = AppMode::Saving;
                self.handle_io_action(Action::ApplySave)
            }
            Action::CancelTableName => {
                self.save.pending_path = None;
                self.save.error = None;
                self.mode = AppMode::Saving;
                None
            }
            Action::SqlScroll(delta) => {
                let max = self.sql_max_scroll();
                let next = self.sql.scroll as i64 + i64::from(delta);
                self.sql.scroll = next.clamp(0, max as i64) as usize;
                None
            }
            Action::SqlScrollHome => {
                self.sql.scroll = 0;
                None
            }
            Action::SqlScrollEnd => {
                self.sql.scroll = self.sql_max_scroll();
                None
            }
            Action::ApplySql => {
                self.apply_sql_plan();
                None
            }
            Action::CancelSql => {
                self.sql.plan = None;
                self.sql.path = None;
                self.sql.pending_source = None;
                self.sql.scroll = 0;
                // Esc goes back to the filename prompt, so a name answered for the old
                // path must not be silently reused for the next one.
                self.save.table_name = None;
                // Back to the filename prompt rather than straight to the table, so
                // "actually, save it elsewhere" costs one keystroke.
                self.mode = AppMode::Saving;
                None
            }
            Action::ChoiceUp => {
                self.save.shape_index = self.save.shape_index.saturating_sub(1);
                None
            }
            Action::ChoiceDown => {
                let len = if self.mode == AppMode::OpenAsSelect {
                    crate::app::OPEN_AS_FORMATS.len()
                } else {
                    self.save.shapes.len()
                };
                self.save.shape_index = (self.save.shape_index + 1).min(len.saturating_sub(1));
                None
            }
            Action::ApplySaveShape => {
                if let Some(s) = self.save.shapes.get(self.save.shape_index) {
                    self.save.shape = *s;
                }
                // Returning Some() here would only pass the action further down the
                // handler chain, not run it — the save has to be re-entered directly.
                self.mode = AppMode::Saving;
                self.handle_io_action(Action::ApplySave)
            }
            Action::CancelSaveShape => {
                self.save.pending_path = None;
                self.mode = AppMode::Saving;
                None
            }
            Action::OpenAs => {
                if self.stack.active().is_dir_sheet {
                    self.save.shape_index = 0;
                    self.save.shapes.clear();
                    self.mode = AppMode::OpenAsSelect;
                } else {
                    self.mode = AppMode::Normal;
                    self.status_message = "Open as… works on a directory listing".to_string();
                }
                None
            }
            Action::ApplyOpenAs => {
                let fmt = crate::app::OPEN_AS_FORMATS[self.save.shape_index];
                self.mode = AppMode::Normal;
                self.open_directory_row_as(Some(fmt));
                None
            }
            Action::CancelOpenAs => {
                self.mode = AppMode::Normal;
                None
            }
            Action::SavingAutocomplete => {
                self.saving_autocomplete();
                None
            }
            other => Some(other),
        }
    }

    /// How far the statement list can scroll, as of the last frame drawn.
    fn sql_max_scroll(&self) -> usize {
        self.sql.max_scroll.get()
    }

    /// Run the statements the user has just read, then reload the sheet from the file.
    ///
    /// Reloading rather than patching the frame in place: an INSERT's key is assigned by
    /// the database and is unknown here, so a second save would insert the same rows
    /// again.  It also shows what DEFAULTs, triggers and type coercion actually did.
    fn apply_sql_plan(&mut self) {
        let Some(plan) = self.sql.plan.take() else {
            self.mode = AppMode::Normal;
            return;
        };
        // A create-plan brings its own source: the sheet does not have one yet, which is
        // exactly what it is about to be given.
        let creating = self.sql.pending_source.is_some();
        let src = match self
            .sql
            .pending_source
            .take()
            .or_else(|| self.stack.active().table_source.clone())
        {
            Some(src) => src,
            None => {
                self.mode = AppMode::Normal;
                self.status_message = "This sheet is no longer backed by a table".to_string();
                return;
            }
        };
        let path = self.sql.path.clone();

        match db_write::apply(&src, &plan) {
            Ok(()) => {
                let summary = plan.summary();
                self.sql.path = None;
                self.sql.scroll = 0;
                self.mode = AppMode::Normal;
                self.save.error = None;
                if creating {
                    let table = src.table.clone();
                    let where_ = src.db_path.clone();
                    self.adopt_created_table(src, &where_, &table);
                    self.status_message =
                        format!("Created '{}' in {} — {}", table, where_.display(), summary);
                    return;
                }
                match self.reload_db_sheet() {
                    Ok(()) => {
                        self.status_message = format!("Wrote {} to '{}'", summary, src.table);
                    }
                    Err(e) => {
                        self.status_message = format!(
                            "Wrote {} to '{}', but reloading the table failed: {}",
                            summary, src.table, e
                        );
                    }
                }
            }
            Err(e) => {
                // A create that failed on a file it had just made leaves a zero-byte
                // database behind, which the next attempt would read as "already there".
                if creating {
                    if let Some(p) = path.as_deref() {
                        if plan.schema > 0 && !plan.rebuild {
                            db_write::remove_new_file(p);
                        }
                    }
                }
                self.sql.path = None;
                self.sql.scroll = 0;
                self.mode = AppMode::Saving;
                self.save.error = Some(format!("Error: {}", e));
            }
        }
    }

    /// Re-read the active sheet's table from its database, keeping the view where it is.
    ///
    /// A view, a `WITHOUT ROWID` table or a DuckDB table with its own `rowid` column has
    /// no `table_source` — it reads but cannot be addressed row by row — so for those the
    /// database and the name come from where the sheet was opened.
    pub(crate) fn reload_db_sheet(&mut self) -> color_eyre::Result<()> {
        use crate::data::io::db_write::DbKind;
        let s = self.stack.active();
        let (kind, db_path, table) = match (&s.table_source, &s.container) {
            (Some(src), _) => (src.kind, src.db_path.clone(), src.table.clone()),
            (None, Some(name)) => match (&s.sqlite_source_path, &s.duckdb_source_path) {
                (Some(p), _) => (DbKind::Sqlite, p.clone(), name.clone()),
                (None, Some(p)) => (DbKind::DuckDb, p.clone(), name.clone()),
                (None, None) => return Ok(()),
            },
            (None, None) => return Ok(()),
        };
        let (df, reloaded) = match kind {
            DbKind::Sqlite => crate::data::io::load_sqlite_table_full(&db_path, &table)?,
            DbKind::DuckDb => crate::data::io::load_duckdb_table_full(&db_path, &table)?,
        };

        let sheet = self.stack.active_mut();
        let old_meta = std::mem::take(&mut sheet.dataframe.columns);
        // Selection is by physical row, which the reload renumbers; the row identity it
        // was really about is the rowid, and that survives the write.  Column selection
        // travels with `ColumnMeta` below, so leaving rows behind would be the odd one.
        let selected_ids: Vec<i64> = match sheet.dataframe.db_rows.as_ref() {
            Some(db) => sheet
                .dataframe
                .selected_rows
                .iter()
                .filter_map(|&phys| db.ids.get(phys).copied().flatten())
                .collect(),
            None => Vec::new(),
        };
        sheet.dataframe = df;
        sheet.table_source = reloaded;
        restore_column_meta(&mut sheet.dataframe, &old_meta);
        if !selected_ids.is_empty() {
            if let Some(db) = sheet.dataframe.db_rows.as_ref() {
                let wanted: std::collections::HashSet<i64> = selected_ids.into_iter().collect();
                // A row the save deleted simply is not found.
                sheet.dataframe.selected_rows = db
                    .ids
                    .iter()
                    .enumerate()
                    .filter(|(_, id)| id.is_some_and(|i| wanted.contains(&i)))
                    .map(|(phys, _)| phys)
                    .collect();
            }
        }
        // A save that shrank the table would otherwise leave the view scrolled past its
        // last row, same as `reload_file` guards against.
        sheet.top_row = 0;
        // The old frames address rows by keys that predate the write.
        sheet.undo_stack.clear();
        sheet.redo_stack.clear();
        sheet.reapply_sort();
        let vis = sheet.dataframe.visible_row_count();
        sheet.scroll_state = ratatui::widgets::ScrollbarState::new(vis.saturating_sub(1));
        let sel = sheet
            .table_state
            .selected()
            .unwrap_or(0)
            .min(vis.saturating_sub(1));
        sheet.table_state.select(Some(sel));
        // A save that dropped a column leaves the cursor past the end, and
        // `columns[cursor_col]` is indexed unguarded in the status bar, the column-move
        // handler and the renderer.
        sheet.clamp_cursor();
        Ok(())
    }

    /// True when saving to `path` has to ask for a shape first: the target is one of
    /// the document formats and this sheet has no document to re-serialise.
    fn needs_shape_for(&self, path: &std::path::Path) -> bool {
        if self.stack.active().doc.is_some() {
            return false;
        }
        path.extension()
            .and_then(|e| e.to_str())
            .and_then(crate::data::doc::Format::from_ext)
            .is_some()
    }

    fn chosen_shape(&self) -> crate::data::io::doc_io::Shape {
        self.save.shape
    }

    pub(super) fn saving_autocomplete(&mut self) {
        let input = self.save.input.as_str().to_owned();

        let path = std::path::Path::new(&input);
        let (dir, prefix) = if input.ends_with('/') {
            (path, "")
        } else {
            let dir = path.parent().unwrap_or(std::path::Path::new("."));
            let prefix = path
                .file_name()
                .map(|f| f.to_str().unwrap_or(""))
                .unwrap_or("");
            (dir, prefix)
        };

        let dir_str = if dir == std::path::Path::new("") {
            std::path::Path::new(".")
        } else {
            dir
        };
        let expanded_dir = crate::app::expand_tilde(dir_str.to_str().unwrap_or("."));

        let full_prefix = input.trim_end_matches(prefix).to_string();
        if self.save.autocomplete_prefix != full_prefix
            || self.save.autocomplete_candidates.is_empty()
        {
            self.save.autocomplete_prefix = full_prefix.clone();
            self.save.autocomplete_idx = 0;

            let mut candidates: Vec<String> = std::fs::read_dir(&expanded_dir)
                .into_iter()
                .flatten()
                .filter_map(|e| e.ok())
                .map(|e| {
                    let name = e.file_name().to_string_lossy().into_owned();
                    let is_dir = e.file_type().map(|t| t.is_dir()).unwrap_or(false);
                    if is_dir {
                        format!("{}/", name)
                    } else {
                        name
                    }
                })
                .filter(|name| name.starts_with(prefix))
                .collect();

            candidates.sort();
            self.save.autocomplete_candidates = candidates;
        }

        if self.save.autocomplete_candidates.is_empty() {
            return;
        }

        let common = crate::app::longest_common_prefix(&self.save.autocomplete_candidates);
        let current_suffix = self
            .save
            .input
            .as_str()
            .strip_prefix(&self.save.autocomplete_prefix)
            .unwrap_or("");

        if common.len() > current_suffix.len() {
            let new_value = format!("{}{}", self.save.autocomplete_prefix, common);
            self.save.input = crate::ui::text_input::TextInput::with_value(new_value);
        } else {
            self.save.autocomplete_idx =
                (self.save.autocomplete_idx + 1) % self.save.autocomplete_candidates.len();
            let completion = &self.save.autocomplete_candidates[self.save.autocomplete_idx];
            let new_value = format!("{}{}", self.save.autocomplete_prefix, completion);
            self.save.input = crate::ui::text_input::TextInput::with_value(new_value);
        }
    }
}
