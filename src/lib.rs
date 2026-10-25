#![doc = include_str!("../README.md")]
// Library root: re-exports for integration tests and external access.
pub mod app;
pub(crate) mod app_state;
pub mod clipboard;
pub mod data;
pub mod event;
pub mod keymap;
pub mod mcp;
pub mod sheet;
#[cfg(test)]
mod test;
pub mod theme;
pub mod types;
pub mod ui;

use clap::Parser;
use color_eyre::Result;
use std::path::PathBuf;

/// TuiTab — Terminal tabular data explorer
#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Cli {
    /// One or more files to open. Pass multiple files to browse them as a list.
    /// Use '-' to read from stdin (pipe mode).
    pub files: Vec<PathBuf>,

    /// Column delimiter (auto-detected if not specified)
    #[arg(short, long)]
    pub delimiter: Option<char>,

    /// Data format (csv, tsv, txt, parquet, arrow, xlsx, sqlite, duckdb, json, jsonl,
    /// yaml, toml, md). Required when reading from stdin, which takes csv, tsv, txt, json,
    /// jsonl, yaml and toml; for a file it overrides the extension and content detection,
    /// so `-t yaml deploy.conf` and `-t csv export.dat` work.
    #[arg(short = 't', long = "type")]
    pub data_type: Option<String>,

    /// Run as an MCP server on stdio, so a language model can query data files
    /// through tuitab's engine instead of computing over them itself.
    #[arg(long)]
    pub mcp: bool,

    /// Allow the MCP server to change things that already exist: rows in a SQLite
    /// or DuckDB table, a table replaced wholesale, or a file overwritten. Off by
    /// default — without it the write tools do not exist and the server can only
    /// create what is not there yet. Every such change is shown first and runs
    /// only when applied by name. Implies --mcp.
    #[arg(long)]
    pub mcp_write: bool,
}

pub fn run() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();

    // Before anything else: the MCP transport owns stdin and stdout, so neither
    // the terminal detection below nor the /dev/tty juggling after it may run.
    if cli.mcp || cli.mcp_write {
        // Tools name their own files, so a path here does nothing.  Say so on
        // stderr — which the transport allows for logging — rather than let it
        // vanish.
        if !cli.files.is_empty() {
            eprintln!("tuitab: --mcp ignores file arguments; the model names the files it wants.");
        }
        return mcp::serve(cli.mcp_write);
    }

    use std::io::IsTerminal;

    let is_terminal = std::io::stdin().is_terminal();
    let use_stdin = (!is_terminal && cli.files.is_empty())
        || cli
            .files
            .first()
            .map(|p| p.to_str() == Some("-"))
            .unwrap_or(false);

    let mut app = if use_stdin {
        if cli.data_type.is_none() {
            eprintln!("Error: When reading from stdin, you must specify the data type using the -t or --type argument.");
            eprintln!("Examples:");
            eprintln!("  cat data.csv | tuitab -t csv");
            eprintln!("  echo '[{{\"a\":1}}]' | tuitab -t json");
            std::process::exit(1);
        }
        app::App::from_stdin_typed(cli.data_type.unwrap().as_str(), cli.delimiter)?
    } else if cli.files.len() >= 2 {
        for p in &cli.files {
            if !p.exists() {
                if data::io::is_pattern(p) {
                    // Otherwise "no such file" for a pattern that matches plenty, which
                    // is the same lie the single-file path used to tell.
                    eprintln!(
                        "Error: '{}': a pattern reads its files as one table, so it goes \
                         on its own — not alongside other arguments.",
                        p.display()
                    );
                } else {
                    eprintln!("Error: '{}': no such file or directory", p.display());
                }
                std::process::exit(1);
            }
        }
        app::App::from_file_list(cli.files, cli.delimiter)?
    } else {
        let path = cli
            .files
            .into_iter()
            .next()
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        // A path that is not there is no longer an error: `App::new_as` opens it blank
        // so a new file can be built from nothing.  It is also the one that refuses the
        // cases that cannot work.
        app::App::new_as(&path, cli.delimiter, cli.data_type.as_deref())?
    };

    #[cfg(unix)]
    {
        use std::io::IsTerminal;
        if !std::io::stdin().is_terminal() {
            use std::os::unix::io::AsRawFd;
            unsafe {
                let mut buf = [0u8; 256];
                let mut real_tty_opened = false;
                if libc::ttyname_r(
                    libc::STDERR_FILENO,
                    buf.as_mut_ptr() as *mut libc::c_char,
                    buf.len(),
                ) == 0
                {
                    let c_str = std::ffi::CStr::from_ptr(buf.as_ptr() as *const libc::c_char);
                    if let Ok(path) = c_str.to_str() {
                        if let Ok(real_tty) = std::fs::OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(path)
                        {
                            libc::dup2(real_tty.as_raw_fd(), libc::STDIN_FILENO);
                            real_tty_opened = true;
                        }
                    }
                }

                if !real_tty_opened {
                    if let Ok(tty) = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open("/dev/tty")
                    {
                        libc::dup2(tty.as_raw_fd(), libc::STDIN_FILENO);
                    }
                }
            }
        }
    }

    // Not `ratatui::init`, which panics: run from cron, a CI step or a shell escape there
    // is no terminal to draw on, and that deserves a sentence rather than a crash report.
    let mut terminal = ratatui::try_init().map_err(|e| {
        let _ = ratatui::try_restore();
        color_eyre::eyre::eyre!("tuitab needs a terminal to show its interface: {}", e)
    })?;
    let result = app.run(&mut terminal);
    ratatui::restore();

    result
}
