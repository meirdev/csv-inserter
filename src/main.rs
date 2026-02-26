mod cli;
mod handler;
mod inserter;
mod watcher;

use std::fs;

use clap::Parser;
use log::{error, info};
use tokio::sync::mpsc;

use crate::cli::Args;
use crate::handler::FileHandler;
use crate::inserter::ClickHouseInserter;
use crate::watcher::FileWatcher;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let mut args = Args::parse();

    if let Err(e) = args.validate() {
        error!("{}", e);
        std::process::exit(1);
    }

    info!("Starting csv-inserter");
    info!("Watch directory: {}", args.watch_dir.display());
    info!(
        "ClickHouse: {} / {}.{}",
        args.clickhouse_url, args.database, args.table
    );

    let (tx, mut rx) = mpsc::unbounded_channel();

    let _watcher = FileWatcher::new(args.watch_dir.clone(), tx)?;

    let inserter = ClickHouseInserter::new(
        &args.clickhouse_url,
        &args.database,
        &args.user,
        &args.password,
        args.table.clone(),
        args.selected_fields(),
        !args.no_header,
        args.async_insert,
    );

    let handler = FileHandler::new(
        args.on_success,
        args.success_dir.clone(),
        args.on_error,
        args.error_dir.clone(),
    )?;

    info!("Ready to process files");

    while let Some(path) = rx.recv().await {
        info!("Processing file: {}", path.display());

        match fs::read(&path) {
            Ok(content) => {
                info!("File size: {} bytes", content.len());
                match inserter.insert(content).await {
                    Ok(_) => {
                        if let Err(e) = handler.handle_success(&path) {
                            error!("Failed to handle successful file: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Insert failed: {}", e);
                        if let Err(e) = handler.handle_error(&path) {
                            error!("Failed to handle error file: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to read file: {}", e);
                if let Err(e) = handler.handle_error(&path) {
                    error!("Failed to handle error file: {}", e);
                }
            }
        }
    }

    Ok(())
}
