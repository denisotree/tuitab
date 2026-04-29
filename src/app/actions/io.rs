use crate::app::App;
use crate::types::{Action, AppMode};

impl App {
    pub(crate) fn handle_io_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::SaveFile => {
                self.save.error = None;
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
                let path = crate::app::expand_tilde(self.save.input.as_str());
                match crate::data::io::save_file(&self.stack.active().dataframe, &path) {
                    Ok(_) => {
                        self.mode = AppMode::Normal;
                        self.status_message =
                            format!("Saved successfully to: {}", self.save.input.as_str());
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
            Action::SavingAutocomplete => {
                self.saving_autocomplete();
                None
            }
            other => Some(other),
        }
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
