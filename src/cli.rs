use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum, Default)]
pub enum FileAction {
    #[default]
    Remove,
    Move,
}

#[derive(Parser, Debug)]
#[command(name = "csv-inserter")]
#[command(about = "Watch directory for CSV files and insert into ClickHouse")]
pub struct Args {
    /// Directory to watch for new files
    #[arg(long)]
    pub watch_dir: PathBuf,

    /// ClickHouse connection URL (e.g., http://localhost:8123)
    #[arg(long)]
    pub clickhouse_url: String,

    /// Target database name
    #[arg(long)]
    pub database: String,

    /// Target table name
    #[arg(long)]
    pub table: String,

    /// ClickHouse username
    #[arg(long, default_value = "default")]
    pub user: String,

    /// ClickHouse password
    #[arg(long, default_value = "")]
    pub password: String,

    /// Action after successful processing
    #[arg(long, default_value = "remove")]
    pub on_success: FileAction,

    /// Directory to move processed files (required if on-success=move)
    #[arg(long)]
    pub success_dir: Option<PathBuf>,

    /// Action on processing error
    #[arg(long, default_value = "remove")]
    pub on_error: FileAction,

    /// Directory to move failed files (required if on-error=move)
    #[arg(long)]
    pub error_dir: Option<PathBuf>,

    /// Comma-separated list of fields to load (loads all if not specified)
    #[arg(long)]
    pub fields: Option<String>,

    /// CSV files do not have a header row
    #[arg(long)]
    pub no_header: bool,

    /// Enable ClickHouse async insert (server-side batching)
    #[arg(long)]
    pub async_insert: bool,
}

impl Args {
    pub fn validate(&mut self) -> Result<(), String> {
        if matches!(self.on_success, FileAction::Move) && self.success_dir.is_none() {
            return Err("--success-dir is required when --on-success=move".to_string());
        }
        if matches!(self.on_error, FileAction::Move) && self.error_dir.is_none() {
            return Err("--error-dir is required when --on-error=move".to_string());
        }
        if !self.watch_dir.exists() {
            return Err(format!(
                "Watch directory does not exist: {:?}",
                self.watch_dir
            ));
        }

        // Resolve paths to absolute
        self.watch_dir = self
            .watch_dir
            .canonicalize()
            .map_err(|e| format!("Failed to resolve watch_dir: {}", e))?;

        if let Some(dir) = &self.success_dir {
            self.success_dir = Some(
                dir.canonicalize()
                    .map_err(|e| format!("Failed to resolve success_dir: {}", e))?,
            );
        }

        if let Some(dir) = &self.error_dir {
            self.error_dir = Some(
                dir.canonicalize()
                    .map_err(|e| format!("Failed to resolve error_dir: {}", e))?,
            );
        }

        Ok(())
    }

    pub fn selected_fields(&self) -> Option<Vec<String>> {
        self.fields
            .as_ref()
            .map(|f| f.split(',').map(|s| s.trim().to_string()).collect())
    }
}
