use crate::data::dataframe::DataFrame;
use crate::data::io::load_file;
use color_eyre::Result;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

/// Event sent from the background loading thread to the main thread.
pub enum LoadEvent {
    Complete(Result<DataFrame>),
}

/// Spawn a background thread to load a CSV file.
/// Returns a `Receiver` that delivers a `LoadEvent::Complete` when done.
pub fn load_in_background(path: PathBuf, delimiter: Option<u8>) -> mpsc::Receiver<LoadEvent> {
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let result = load_file(&path, delimiter);
        let _ = tx.send(LoadEvent::Complete(result));
    });

    rx
}
