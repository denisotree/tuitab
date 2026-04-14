/// A universal text input widget state that supports moving the cursor and editing mid-string.
#[derive(Debug, Clone, Default)]
pub struct TextInput {
    /// The string content
    pub content: String,
    /// Absolute char index of the cursor (0 <= cursor <= content.chars().count())
    pub cursor: usize,
}

impl TextInput {
    /// Create an empty `TextInput` with the cursor at position 0.
    pub fn new() -> Self {
        Self {
            content: String::new(),
            cursor: 0,
        }
    }

    /// Create a `TextInput` pre-filled with `value`; cursor placed at the end.
    pub fn with_value(value: String) -> Self {
        let len = value.chars().count();
        Self {
            content: value,
            cursor: len,
        }
    }

    #[allow(dead_code)]
    /// Return `true` if the input contains no characters.
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }

    /// Clear the content and reset the cursor to position 0.
    pub fn clear(&mut self) {
        self.content.clear();
        self.cursor = 0;
    }

    /// Borrow the current content as a `&str`.
    pub fn as_str(&self) -> &str {
        &self.content
    }

    /// Return the cursor position as a `u16` for use with
    /// [`ratatui::Frame::set_cursor_position`].
    pub fn cursor_pos(&self) -> u16 {
        self.cursor as u16
    }

    /// Insert `c` at the current cursor position and advance the cursor by one.
    pub fn insert_char(&mut self, c: char) {
        let byte_idx = self.cursor_byte_index();
        self.content.insert(byte_idx, c);
        self.cursor += 1;
    }

    /// Delete the character immediately before the cursor (Backspace behaviour).
    pub fn delete_backward(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
            let byte_idx = self.cursor_byte_index();
            self.content.remove(byte_idx);
        }
    }

    /// Delete the character at the cursor position (Delete key behaviour).
    pub fn delete_forward(&mut self) {
        if self.cursor < self.content.chars().count() {
            let byte_idx = self.cursor_byte_index();
            self.content.remove(byte_idx);
        }
    }

    /// Move the cursor one character to the left (no-op at position 0).
    pub fn move_cursor_left(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
        }
    }

    /// Move the cursor one character to the right (no-op at end of content).
    pub fn move_cursor_right(&mut self) {
        if self.cursor < self.content.chars().count() {
            self.cursor += 1;
        }
    }

    /// Move the cursor to position 0 (Home key behaviour).
    pub fn move_cursor_start(&mut self) {
        self.cursor = 0;
    }

    /// Move the cursor past the last character (End key behaviour).
    pub fn move_cursor_end(&mut self) {
        self.cursor = self.content.chars().count();
    }

    /// Converts the `self.cursor` (which is a char index) to a byte index
    /// for correctly indexing into the underlying `String`.
    fn cursor_byte_index(&self) -> usize {
        self.content
            .char_indices()
            .nth(self.cursor)
            .map(|(i, _)| i)
            .unwrap_or_else(|| self.content.len())
    }
}
