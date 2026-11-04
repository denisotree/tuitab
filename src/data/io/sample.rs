//! JSON Lines read as a table, a whole number of lines at a time.  A file too large to
//! load at once opens with its first piece, and the rest arrives in the background
//! until the file ends or the table reaches the memory cap.

use super::{memory_budget, Opened};
use crate::data::dataframe::DataFrame;
use crate::data::doc::{jsonl_records, Format};
use crate::data::view::records_table;
use color_eyre::{eyre::eyre, Result};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

/// How much of its file a sheet holds, when that is not all of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sample {
    pub file: PathBuf,
    /// Bytes read, up to the end of the last whole line.
    pub read: u64,
    /// The file's size.
    pub size: u64,
}

impl Sample {
    /// `first 256.0 MB of 16.0 GB`.
    pub fn label(&self) -> String {
        format!(
            "first {} of {}",
            super::format_file_size_pub(self.read),
            super::format_file_size_pub(self.size)
        )
    }

    /// How far through the file, in whole percent.
    pub fn percent(&self) -> u64 {
        self.read * 100 / self.size.max(1)
    }
}

/// How [`super::open_target_from`] reads a JSON Lines file a piece at a time.
#[derive(Debug, Clone, Copy)]
pub struct Sampling {
    /// Files of this many bytes or more open with their first piece, the rest read in
    /// the background; smaller ones are read to the end before they open.
    pub from: u64,
    /// Bytes per piece: the first opens the sheet, the rest are added to it.
    pub chunk: u64,
    /// Bytes the table may hold before no more is read.
    pub cap: u64,
}

/// Bytes per piece.
const CHUNK: u64 = 8 << 20;

impl Default for Sampling {
    /// Past a sixteenth of the memory budget — where the MCP server starts streaming —
    /// the sheet opens with its first 8 MB, and reading stops once the table holds
    /// three quarters of the budget, the rest left for the sheets made from it.  A
    /// 16 GB log read to its end in 145 s: 35.4 million rows, a 12.2 GB table, a 16 GB
    /// footprint.
    fn default() -> Self {
        let budget = memory_budget();
        Sampling {
            from: super::stream_from(budget),
            chunk: CHUNK,
            cap: budget / 4 * 3,
        }
    }
}

impl Sampling {
    /// Never a piece at a time: the whole file, as before.
    pub fn whole() -> Self {
        Sampling {
            from: u64::MAX,
            ..Sampling::default()
        }
    }
}

/// Rows read past the first piece, as a table, and how many bytes of the file that
/// makes.  The reader builds the table, so adding it is all the sheet has to do.
pub struct Growth {
    /// `None` for the end of the file, when what is left holds no records.
    pub block: Option<DataFrame>,
    pub read: u64,
}

/// The pieces still to come.  Dropping it stops the reader.
pub struct Tail {
    pub rx: mpsc::Receiver<Result<Growth, String>>,
    /// Bytes the table may hold before reading stops.
    pub cap: u64,
    /// The columns as the last piece left them: changed since, they are the user's.
    pub columns: Vec<(String, polars::prelude::DataType)>,
}

/// Each column's name and type.
pub fn columns_of(df: &DataFrame) -> Vec<(String, polars::prelude::DataType)> {
    df.df
        .columns()
        .iter()
        .map(|c| (c.name().to_string(), c.dtype().clone()))
        .collect()
}

/// Text read a whole number of lines at a time.
struct Lines<R> {
    source: BufReader<R>,
    /// Bytes after the last newline read, which begin the next piece.
    carry: Vec<u8>,
    /// Bytes handed out.
    read: u64,
    /// Lines handed out, for naming a bad one.
    line: usize,
}

impl<R: Read> Lines<R> {
    fn new(source: R) -> Self {
        Lines {
            source: BufReader::new(source),
            carry: Vec::new(),
            read: 0,
            line: 0,
        }
    }

    /// About `chunk` bytes ending at a newline — more when one line is longer — or
    /// what is left at the end, with the number of lines before them.
    fn next(&mut self, chunk: u64) -> std::io::Result<Option<(String, usize)>> {
        let mut buf = std::mem::take(&mut self.carry);
        (&mut self.source).take(chunk).read_to_end(&mut buf)?;
        loop {
            if let Some(i) = buf.iter().rposition(|&b| b == b'\n') {
                self.carry = buf.split_off(i + 1);
                break;
            }
            let before = buf.len();
            (&mut self.source).take(chunk).read_to_end(&mut buf)?;
            if buf.len() == before {
                break;
            }
        }
        if buf.is_empty() {
            return Ok(None);
        }
        let first_line = self.line;
        self.line += buf.iter().filter(|&&b| b == b'\n').count();
        self.read += buf.len() as u64;
        // Refused as every other reader refuses it, by line: replaced quietly, a save
        // would write the replacement.
        let text = String::from_utf8(buf).map_err(|e| {
            let at = e.utf8_error().valid_up_to();
            let before = e.as_bytes()[..at].iter().filter(|&&b| b == b'\n').count();
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("line {}: not valid UTF-8", first_line + before + 1),
            )
        })?;
        Ok(Some((text, first_line)))
    }

    /// The next piece that holds records, as a table.  One of blank lines alone would
    /// be a table of no rows and a made-up column.
    fn table(&mut self, chunk: u64) -> Result<Option<DataFrame>> {
        while let Some((text, first_line)) = self.next(chunk)? {
            let records = jsonl_records(&text, first_line)?;
            if !records.is_empty() {
                return Ok(Some(records_table(&records)?));
            }
        }
        Ok(None)
    }

    /// Every piece left, added to `df`.
    fn read_to_end(&mut self, chunk: u64, mut df: DataFrame) -> Result<DataFrame> {
        while let Some(block) = self.table(chunk)? {
            df.push_rows(block).map_err(|e| eyre!(e))?;
        }
        Ok(df)
    }
}

/// JSON Lines as a table, a piece at a time: no tree of the whole is ever built, so the
/// table is all it costs.  Past `sampling.from` bytes only the first piece is read
/// before the sheet opens, and the rest follows in the background.
pub(super) fn jsonl_of(
    path: &Path,
    forced: Option<&str>,
    sampling: Sampling,
) -> Result<Option<Opened>> {
    let format = match forced {
        Some(name) => Format::from_name(name),
        None => Format::of_file(path),
    };
    if format != Some(Format::Jsonl) {
        return Ok(None);
    }
    let size = std::fs::metadata(path).map_or(0, |m| m.len());
    let mut lines = Lines::new(std::fs::File::open(path)?);
    let first = lines.table(sampling.chunk)?;
    let first = match first {
        Some(df) => df,
        None => records_table(&[])?,
    };
    if size < sampling.from || lines.read >= size {
        let df = lines.read_to_end(sampling.chunk, first)?;
        return Ok(Some(Opened {
            df,
            ..Default::default()
        }));
    }
    let read = lines.read;
    let columns = columns_of(&first);
    Ok(Some(Opened {
        df: first,
        sample: Some(Sample {
            file: path.to_path_buf(),
            read,
            size,
        }),
        tail: Some(read_on(lines, sampling, columns)),
        ..Default::default()
    }))
}

/// JSON Lines text as a table, a piece at a time.
pub fn jsonl_table(text: &str) -> Result<DataFrame> {
    read_all(Lines::new(text.as_bytes()))
}

/// A JSON Lines file as a table, read to its end a piece at a time.
pub(super) fn jsonl_file(path: &Path) -> Result<DataFrame> {
    read_all(Lines::new(std::fs::File::open(path)?))
}

fn read_all<R: Read>(mut lines: Lines<R>) -> Result<DataFrame> {
    let first = lines.table(CHUNK)?.map_or_else(|| records_table(&[]), Ok)?;
    lines.read_to_end(CHUNK, first)
}

/// Read the rest of the file in the background, one piece waiting at most.
fn read_on(
    mut lines: Lines<std::fs::File>,
    sampling: Sampling,
    columns: Vec<(String, polars::prelude::DataType)>,
) -> Tail {
    let (tx, rx) = mpsc::sync_channel(1);
    std::thread::spawn(move || loop {
        let sent = match lines.table(sampling.chunk) {
            Ok(Some(block)) => tx.send(Ok(Growth {
                block: Some(block),
                read: lines.read,
            })),
            Ok(None) => {
                // Blank lines after the last record still count as read.
                let _ = tx.send(Ok(Growth {
                    block: None,
                    read: lines.read,
                }));
                break;
            }
            Err(e) => {
                let _ = tx.send(Err(e.to_string()));
                break;
            }
        };
        if sent.is_err() {
            break; // the sheet is gone
        }
    });
    Tail {
        rx,
        cap: sampling.cap,
        columns,
    }
}
