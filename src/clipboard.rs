use arboard::Clipboard;
use color_eyre::Result;

/// Copy rows as TSV text to the system clipboard.
pub fn copy_to_clipboard(headers: &[&str], rows: &[Vec<String>]) -> Result<()> {
    copy_tsv(headers, rows)
}

/// Copy rows as TSV (tab-separated values with header).
pub fn copy_tsv(headers: &[&str], rows: &[Vec<String>]) -> Result<()> {
    let mut text = headers.join("\t");
    text.push('\n');
    for row in rows {
        text.push_str(&row.join("\t"));
        text.push('\n');
    }
    copy_text(&text)
}

/// Copy rows as CSV (RFC 4180: cells with comma/quote/newline are quoted).
pub fn copy_csv(headers: &[&str], rows: &[Vec<String>]) -> Result<()> {
    fn csv_cell(s: &str) -> String {
        if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
            format!("\"{}\"", s.replace('"', "\"\""))
        } else {
            s.to_string()
        }
    }
    let mut text = headers
        .iter()
        .map(|h| csv_cell(h))
        .collect::<Vec<_>>()
        .join(",");
    text.push('\n');
    for row in rows {
        text.push_str(
            &row.iter()
                .map(|v| csv_cell(v))
                .collect::<Vec<_>>()
                .join(","),
        );
        text.push('\n');
    }
    copy_text(&text)
}

/// Copy rows as a JSON array of objects (string values).
pub fn copy_json(headers: &[&str], rows: &[Vec<String>]) -> Result<()> {
    let objects: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let map: serde_json::Map<String, serde_json::Value> = headers
                .iter()
                .zip(row.iter())
                .map(|(h, v)| (h.to_string(), serde_json::Value::String(v.clone())))
                .collect();
            serde_json::Value::Object(map)
        })
        .collect();
    let text = serde_json::to_string_pretty(&serde_json::Value::Array(objects))
        .map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    copy_text(&text)
}

/// Copy rows as a Markdown table.
pub fn copy_markdown(headers: &[&str], rows: &[Vec<String>]) -> Result<()> {
    fn md_cell(s: &str) -> String {
        s.replace('|', "\\|").replace('\n', " ").replace('\r', "")
    }
    let header_row = format!(
        "| {} |",
        headers
            .iter()
            .map(|h| md_cell(h))
            .collect::<Vec<_>>()
            .join(" | ")
    );
    let sep_row = format!(
        "| {} |",
        headers
            .iter()
            .map(|_| "---")
            .collect::<Vec<_>>()
            .join(" | ")
    );
    let mut lines = vec![header_row, sep_row];
    for row in rows {
        lines.push(format!(
            "| {} |",
            row.iter()
                .map(|v| md_cell(v))
                .collect::<Vec<_>>()
                .join(" | ")
        ));
    }
    copy_text(&lines.join("\n"))
}

/// Copy column values as newline-separated text.
pub fn copy_column_newline(values: &[String]) -> Result<()> {
    copy_text(&values.join("\n"))
}

/// Copy column values as a comma-separated list.
pub fn copy_column_comma(values: &[String]) -> Result<()> {
    copy_text(&values.join(", "))
}

/// Copy column values as a comma-separated list with single quotes.
pub fn copy_column_comma_quoted(values: &[String]) -> Result<()> {
    let quoted: Vec<String> = values.iter().map(|v| format!("'{}'", v)).collect();
    copy_text(&quoted.join(", "))
}

/// Copy a plain text string to the system clipboard.
pub fn copy_text(text: &str) -> Result<()> {
    let mut cb = Clipboard::new().map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    cb.set_text(text.to_string())
        .map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    Ok(())
}

/// Read TSV-formatted text from the system clipboard.
pub fn paste_from_clipboard() -> Result<String> {
    let mut cb = Clipboard::new().map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    let text = cb
        .get_text()
        .map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    Ok(text)
}
