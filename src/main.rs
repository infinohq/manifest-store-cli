// Copyright 2026 Infino AI, Inc. All rights reserved.
//! CLI for the manifest object store.
//!
//! Usage:
//!   manifest-store put    --cloud-provider aws --environment prod --name app.yml-crypt --file ./app.yml-crypt
//!   manifest-store get    --cloud-provider aws --environment prod --name app.yml-crypt --output ./app.yml-crypt
//!   manifest-store delete --cloud-provider aws --environment prod --name app.yml-crypt
//!   manifest-store list   --cloud-provider aws --environment prod
mod manifest_store;

use std::path::PathBuf;
use std::process::ExitCode;

use async_walkdir::WalkDir;
use clap::{Parser, Subcommand};
use futures_lite::stream::StreamExt;
use manifest_store::{ManifestStore, ManifestStoreError};
use object_store::path::Path as ObjectStorePath;

#[derive(Parser)]
#[command(
    name = "manifest-store",
    about = "Manage encrypted K8s manifests in cloud object storage"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Upload an encrypted manifest file to the object store
    Put {
        #[arg(long)]
        cloud_provider: String,
        #[arg(long)]
        environment: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        file: PathBuf,
    },

    /// Download a manifest file from the object store
    Get {
        #[arg(long)]
        cloud_provider: String,
        #[arg(long)]
        environment: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        output: PathBuf,
    },

    /// Delete a manifest file from the object store
    Delete {
        #[arg(long)]
        cloud_provider: String,
        #[arg(long)]
        environment: String,
        #[arg(long)]
        name: String,
    },

    /// List manifest files under a cloud_provider/environment prefix
    List {
        #[arg(long)]
        cloud_provider: String,
        #[arg(long)]
        environment: String,
    },

    /// Downloads all manifest files from the object store
    Clone {
        #[arg(long)]
        output_dir: PathBuf,
    },

    /// Write all data from a local dir to output_dir
    Push {
        #[arg(long)]
        output_dir: PathBuf,
    },
}

async fn run_command(cmd: Command, store: ManifestStore) -> Result<(), ManifestStoreError> {
    match cmd {
        Command::Put {
            cloud_provider,
            environment,
            name,
            file,
        } => store.put(&cloud_provider, &environment, &name, &file).await,

        Command::Get {
            cloud_provider,
            environment,
            name,
            output,
        } => {
            store
                .get(&cloud_provider, &environment, &name, &output)
                .await
        }

        Command::Delete {
            cloud_provider,
            environment,
            name,
        } => store.delete(&cloud_provider, &environment, &name).await,

        Command::List {
            cloud_provider,
            environment,
        } => match store.list(&cloud_provider, &environment).await {
            Ok(names) => {
                for name in &names {
                    println!("{name}");
                }
                Ok(())
            }
            Err(e) => Err(e),
        },
        Command::Clone { output_dir } => {
            let children = store.list_all().await?;
            for child in children {
                let output_path = output_dir.join(child.to_string());
                store.raw_get_file(&child, &output_path).await?;
            }

            Ok(())
        }
        Command::Push { output_dir } => {
            let mut files = WalkDir::new(output_dir.clone());
            loop {
                match files.next().await {
                    Some(Ok(entry)) => {
                        let file_type = entry.file_type().await?;
                        if file_type.is_dir() {
                            continue;
                        }

                        let data = tokio::fs::read(entry.path()).await?;
                        let entry_path = entry.path();
                        let stripped_path = entry_path
                            .strip_prefix(output_dir.clone())
                            .unwrap_or(entry_path.as_path());

                        let stripped_path = stripped_path
                            .to_str()
                            .expect("all paths should be valid unicode");

                        store
                            .raw_put(&ObjectStorePath::from(stripped_path), data)
                            .await?;
                    }
                    Some(Err(e)) => {
                        eprintln!("error: {}", e);
                        break;
                    }
                    None => break,
                }
            }
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();
    let cli = Cli::parse();

    let store = match ManifestStore::from_env() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to initialize manifest store: {e}");
            return ExitCode::FAILURE;
        }
    };

    let result = run_command(cli.command, store).await;

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}
