use arboard::Clipboard;
use color_eyre::Result;

/// Copy rows as TSV text to the system clipboard.
/// First row is the header; subsequent rows are data.
pub fn copy_to_clipboard(headers: &[&str], rows: &[Vec<String>]) -> Result<()> {
    let mut text = headers.join("\t");
    text.push('\n');
    for row in rows {
        text.push_str(&row.join("\t"));
        text.push('\n');
    }
    let mut cb = Clipboard::new().map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    cb.set_text(text).map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    Ok(())
}

/// Copy a plain text string to the system clipboard.
pub fn copy_text(text: &str) -> Result<()> {
    let mut cb = Clipboard::new().map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    cb.set_text(text.to_string()).map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    Ok(())
}

/// Read TSV-formatted text from the system clipboard.
pub fn paste_from_clipboard() -> Result<String> {
    let mut cb = Clipboard::new().map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    let text = cb.get_text().map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    Ok(text)
}
