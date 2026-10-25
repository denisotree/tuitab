use crate::data::io::{open_target, Opened};
use color_eyre::Result;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

/// Event sent from the background loading thread to the main thread.
pub enum LoadEvent {
    /// Everything [`open_target`] produced.  It travels whole: a frame on its own loses
    /// the document tree (nesting would vanish on the next save) and the container path
    /// (the drill-in would have nothing to open — #43).
    Complete(Result<Opened>),
}

/// Spawn a background thread to load a file.
/// Returns a `Receiver` that delivers a `LoadEvent::Complete` when done.
pub fn load_in_background(
    path: PathBuf,
    delimiter: Option<u8>,
    forced: Option<String>,
) -> mpsc::Receiver<LoadEvent> {
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let result = open_target(&path, delimiter, forced.as_deref());
        let _ = tx.send(LoadEvent::Complete(result));
    });

    rx
}
