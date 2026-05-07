//! Terminal adapter for Ledger store.
//!
//! This binary owns argument parsing, `.env` loading, service construction, and
//! JSON output. Object registry, R2, hydration, and local object behavior belong in
//! the `store` crate.

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use serde_json::Value;
use std::path::PathBuf;
use store::{ObjectFilter, R2Store, RegisterFileRequest, StoreObjectId, StoreObjectRole};

#[derive(Parser)]
#[command(name = "ledger")]
#[command(about = "Ledger store object registry")]
struct Cli {
    #[arg(long, env = "LEDGER_DATA_DIR", default_value = "data")]
    data_dir: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Store(StoreCommand),
}

#[derive(Args)]
struct StoreCommand {
    #[command(subcommand)]
    command: StoreSubcommand,
}

#[derive(Subcommand)]
enum StoreSubcommand {
    List(StoreListArgs),
    Show(StoreShowArgs),
    ImportFile(StoreImportFileArgs),
    Hydrate(StoreShowArgs),
    Delete(StoreShowArgs),
    LocalStatus,
    LocalPrune,
    Validate(StoreValidateArgs),
}

#[derive(Args, Clone)]
struct StoreListArgs {
    #[arg(long)]
    role: Option<String>,
    #[arg(long)]
    kind: Option<String>,
    #[arg(long)]
    id_prefix: Option<String>,
}

#[derive(Args, Clone)]
struct StoreShowArgs {
    #[arg(long)]
    id: String,
}

#[derive(Args, Clone)]
struct StoreImportFileArgs {
    #[arg(long)]
    path: PathBuf,
    #[arg(long)]
    role: String,
    #[arg(long)]
    kind: String,
    #[arg(long)]
    file_name: Option<String>,
    #[arg(long)]
    format: Option<String>,
    #[arg(long)]
    media_type: Option<String>,
    #[arg(long, default_value = "{}")]
    metadata_json: String,
}

#[derive(Args, Clone)]
struct StoreValidateArgs {
    #[arg(long)]
    id: Option<String>,
    #[arg(long)]
    role: Option<String>,
    #[arg(long)]
    kind: Option<String>,
    #[arg(long)]
    verify_remote: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Command::Store(command) => {
            run_store_command(R2Store::from_env(&cli.data_dir).await?, command).await?
        }
    }

    Ok(())
}

async fn run_store_command(ledger_store: R2Store, command: StoreCommand) -> Result<()> {
    match command.command {
        StoreSubcommand::List(args) => {
            let rows = ledger_store.list_objects(store_filter(
                args.role.as_deref(),
                args.kind,
                args.id_prefix,
            )?)?;
            print_json(&rows)?;
        }
        StoreSubcommand::Show(args) => {
            let id = StoreObjectId::new(args.id)?;
            let object = ledger_store.get_object(&id)?;
            print_json(&object)?;
        }
        StoreSubcommand::ImportFile(args) => {
            let metadata_json = serde_json::from_str::<Value>(&args.metadata_json)?;
            let role = StoreObjectRole::parse(&args.role)?;
            let descriptor = ledger_store
                .register_file(RegisterFileRequest {
                    path: &args.path,
                    role,
                    kind: args.kind,
                    file_name: args.file_name,
                    format: args.format,
                    media_type: args.media_type,
                    lineage: Vec::new(),
                    metadata_json,
                })
                .await?;
            print_json(&descriptor)?;
        }
        StoreSubcommand::Hydrate(args) => {
            let id = StoreObjectId::new(args.id)?;
            let hydrated = ledger_store.hydrate(&id).await?;
            print_json(&hydrated)?;
        }
        StoreSubcommand::Delete(args) => {
            let id = StoreObjectId::new(args.id)?;
            let report = ledger_store.delete_object(&id).await?;
            print_json(&report)?;
        }
        StoreSubcommand::LocalStatus => {
            let status = ledger_store.local_status()?;
            print_json(&status)?;
        }
        StoreSubcommand::LocalPrune => {
            let report = ledger_store.enforce_local_limit(None)?;
            print_json(&report)?;
        }
        StoreSubcommand::Validate(args) => {
            if let Some(id) = args.id {
                let id = StoreObjectId::new(id)?;
                let report = ledger_store
                    .validate_object(&id, args.verify_remote)
                    .await?;
                print_json(&report)?;
            } else {
                let report = ledger_store
                    .validate_all(
                        store_filter(args.role.as_deref(), args.kind, None)?,
                        args.verify_remote,
                    )
                    .await?;
                print_json(&report)?;
            }
        }
    }

    Ok(())
}

fn store_filter(
    role: Option<&str>,
    kind: Option<String>,
    id_prefix: Option<String>,
) -> Result<ObjectFilter> {
    Ok(ObjectFilter {
        role: role.map(StoreObjectRole::parse).transpose()?,
        kind,
        id_prefix,
    })
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
