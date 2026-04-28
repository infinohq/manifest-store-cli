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

use clap::{Parser, Subcommand};
use manifest_store::ManifestStore;

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

    let result = match cli.command {
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
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}
