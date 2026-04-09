use clap::Parser;
use color_eyre::Result;
use std::path::PathBuf;

mod app;
mod clipboard;
mod data;
mod event;
mod keymap;
mod sheet;
mod theme;
mod types;
mod ui;

/// TuiTab — Terminal tabular data explorer
#[derive(Parser, Debug)]
#[command(name = "tuitab", version, about)]
struct Cli {
    /// Path to CSV/TSV/JSON/Parquet/Excel/SQLite file to open.
    /// Use '-' to read from stdin (pipe mode).
    file: Option<PathBuf>,

    /// Column delimiter (auto-detected if not specified)
    #[arg(short, long)]
    delimiter: Option<char>,

    /// Data format for stdin (e.g. csv, json). Required when reading from stdin.
    #[arg(short = 't', long = "type")]
    data_type: Option<String>,
}

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();

    use std::io::IsTerminal;

    // Resolve path: None or "-" means stdin
    let mut path = cli.file.as_deref();

    let is_terminal = std::io::stdin().is_terminal();
    let use_stdin =
        (path.is_none() && !is_terminal) || path.map(|p| p.to_str() == Some("-")).unwrap_or(false);

    // If no path was given and not piping from stdin, open the current directory
    if path.is_none() && !use_stdin {
        path = Some(std::path::Path::new("."));
    }

    let mut app = if use_stdin {
        // Require type specification for stdin piped data (Feature F2)
        if cli.data_type.is_none() {
            eprintln!("Error: When reading from stdin, you must specify the data type using the -t or --type argument.");
            eprintln!("Examples:");
            eprintln!("  cat data.csv | tuitab -t csv");
            eprintln!("  echo '[{{\"a\":1}}]' | tuitab -t json");
            std::process::exit(1);
        }
        app::App::from_stdin_typed(cli.data_type.unwrap().as_str(), cli.delimiter)?
    } else {
        app::App::new(path.unwrap(), cli.delimiter)?
    };

    #[cfg(unix)]
    {
        use std::io::IsTerminal;
        if !std::io::stdin().is_terminal() {
            use std::os::unix::io::AsRawFd;
            // On macOS, kqueue cannot register /dev/tty directly. We must open the actual
            // terminal device. STDERR is usually still attached to the real TTY.
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

                // Fallback to /dev/tty if stderr isn't a tty or ttyname fails
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

    let mut terminal = ratatui::init();
    let result = app.run(&mut terminal);
    ratatui::restore();

    result
}
