use std::path::PathBuf;

use log::{error, info};
use notify::event::{AccessKind, AccessMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

pub struct FileWatcher {
    _watcher: RecommendedWatcher,
}

impl FileWatcher {
    pub fn new(
        watch_dir: PathBuf,
        tx: mpsc::UnboundedSender<PathBuf>,
    ) -> Result<Self, notify::Error> {
        let tx_clone = tx.clone();

        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            match res {
                Ok(event) => {
                    // Wait for CloseWrite event - file is fully written and closed
                    if matches!(
                        event.kind,
                        EventKind::Access(AccessKind::Close(AccessMode::Write))
                    ) {
                        for path in event.paths {
                            if is_csv_file(&path) {
                                info!("New file detected: {}", path.display());
                                if let Err(e) = tx_clone.send(path) {
                                    error!("Failed to send file path: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => error!("Watch error: {}", e),
            }
        })?;

        watcher.watch(&watch_dir, RecursiveMode::NonRecursive)?;
        info!("Watching directory: {}", watch_dir.display());

        Ok(Self { _watcher: watcher })
    }
}

fn is_csv_file(path: &PathBuf) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("csv"))
        .unwrap_or(false)
}
