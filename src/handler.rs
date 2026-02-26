use std::fs;
use std::path::{Path, PathBuf};

use log::info;

use crate::cli::FileAction;

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Directory does not exist: {0}")]
    DirNotFound(PathBuf),
    #[error("Directory is not writable: {0}")]
    DirNotWritable(PathBuf),
}

pub struct FileHandler {
    success_action: FileAction,
    success_dir: Option<PathBuf>,
    error_action: FileAction,
    error_dir: Option<PathBuf>,
}

impl FileHandler {
    pub fn new(
        success_action: FileAction,
        success_dir: Option<PathBuf>,
        error_action: FileAction,
        error_dir: Option<PathBuf>,
    ) -> Result<Self, HandlerError> {
        if matches!(success_action, FileAction::Move) {
            if let Some(dir) = &success_dir {
                validate_directory(dir)?;
            }
        }

        if matches!(error_action, FileAction::Move) {
            if let Some(dir) = &error_dir {
                validate_directory(dir)?;
            }
        }

        Ok(Self {
            success_action,
            success_dir,
            error_action,
            error_dir,
        })
    }

    pub fn handle_success(&self, path: &Path) -> Result<(), HandlerError> {
        match self.success_action {
            FileAction::Remove => {
                fs::remove_file(path)?;
                info!("Removed file: {}", path.display());
            }
            FileAction::Move => {
                let dest = move_file(path, self.success_dir.as_ref().unwrap())?;
                info!("Moved file to: {}", dest.display());
            }
        }
        Ok(())
    }

    pub fn handle_error(&self, path: &Path) -> Result<(), HandlerError> {
        match self.error_action {
            FileAction::Remove => {
                fs::remove_file(path)?;
                info!("Removed failed file: {}", path.display());
            }
            FileAction::Move => {
                let dest = move_file(path, self.error_dir.as_ref().unwrap())?;
                info!("Moved failed file to: {}", dest.display());
            }
        }
        Ok(())
    }
}

fn validate_directory(dir: &Path) -> Result<(), HandlerError> {
    if !dir.exists() {
        return Err(HandlerError::DirNotFound(dir.to_path_buf()));
    }

    if !dir.is_dir() {
        return Err(HandlerError::DirNotFound(dir.to_path_buf()));
    }

    let metadata = fs::metadata(dir)?;
    if metadata.permissions().readonly() {
        return Err(HandlerError::DirNotWritable(dir.to_path_buf()));
    }

    Ok(())
}

fn move_file(src: &Path, dest_dir: &Path) -> Result<PathBuf, HandlerError> {
    let file_name = src.file_name().unwrap();
    let dest = dest_dir.join(file_name);

    fs::rename(src, &dest)?;
    Ok(dest)
}
