//! Terminal adapter for Ledger store.
//!
//! This binary owns argument parsing, `.env` loading, service construction, and
//! JSON output. Object registry, R2, hydration, and local object behavior belong in
//! the `store` crate.

use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};
use ledger::feed::es_replay::{
    es_day_catalog, fetch_es_raw, prepare_es_replay_artifact, EsPrepareSummary, FetchProgress,
    PrepareProgress, RAW_DATABENTO_DBN_ZST_KIND,
};
use ledger::market::MarketDay;
use serde::Serialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use store::{ObjectFilter, R2Store, RegisterFileRequest, StoreObjectId, StoreObjectRole};
use tokio::sync::mpsc;

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
    Es(EsCommand),
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
    Sync(StoreSyncArgs),
    Validate(StoreValidateArgs),
    AbortIncompleteUploads(StoreAbortUploadsArgs),
}

#[derive(Args)]
struct EsCommand {
    #[command(subcommand)]
    command: EsSubcommand,
}

#[derive(Subcommand)]
enum EsSubcommand {
    Days,
    Prepare(EsPrepareArgs),
    Fetch(EsFetchArgs),
}

#[derive(Args, Clone)]
struct EsPrepareArgs {
    #[arg(long)]
    raw_id: Option<String>,
    #[arg(long)]
    day: Option<String>,
    #[arg(long)]
    all: bool,
    #[arg(long)]
    force: bool,
}

#[derive(Args, Clone)]
struct EsFetchArgs {
    #[arg(long)]
    day: String,
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value = "GLBX.MDP3")]
    dataset: String,
    #[arg(long)]
    force: bool,
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
struct StoreSyncArgs {
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Args, Clone)]
struct StoreAbortUploadsArgs {
    /// Only consider uploads whose key starts with this prefix.
    #[arg(long, default_value = "store/objects")]
    prefix: String,
    /// Actually abort. Without this the command only lists what it would abort.
    #[arg(long)]
    execute: bool,
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
        Command::Es(command) => {
            run_es_command(
                R2Store::from_env(&cli.data_dir).await?,
                cli.data_dir,
                command,
            )
            .await?
        }
    }

    Ok(())
}

async fn run_es_command(
    ledger_store: R2Store,
    data_dir: PathBuf,
    command: EsCommand,
) -> Result<()> {
    match command.command {
        EsSubcommand::Days => {
            let catalog = es_day_catalog(&ledger_store)?;
            print_json(&catalog)?;
        }
        EsSubcommand::Prepare(args) => {
            run_es_prepare_command(ledger_store, args).await?;
        }
        EsSubcommand::Fetch(args) => {
            let day = MarketDay::parse(&args.day)?;
            let progress = fetch_progress_channel();
            let staging_dir = data_dir
                .join("tmp")
                .join("fetch")
                .join(&args.symbol)
                .join(day.to_string());
            let summary = fetch_es_raw(
                &ledger_store,
                day,
                &args.symbol,
                &args.dataset,
                &staging_dir,
                args.force,
                Some(progress),
            )
            .await?;
            print_json(&summary)?;
        }
    }
    Ok(())
}

async fn run_es_prepare_command(ledger_store: R2Store, args: EsPrepareArgs) -> Result<()> {
    let selector_count = usize::from(args.raw_id.is_some())
        + usize::from(args.day.is_some())
        + usize::from(args.all);
    if selector_count != 1 {
        return Err(anyhow!(
            "exactly one of --raw-id, --day, or --all is required"
        ));
    }

    if let Some(raw_id) = args.raw_id {
        let summary = prepare_one(&ledger_store, StoreObjectId::new(raw_id)?, args.force).await?;
        print_json(&summary)?;
        return Ok(());
    }

    let raw_ids = if let Some(day) = args.day {
        let day = MarketDay::parse(&day)?;
        let catalog = es_day_catalog(&ledger_store)?;
        let entry = catalog
            .days
            .into_iter()
            .find(|entry| entry.market_day == day)
            .ok_or_else(|| anyhow!("unknown ES market day {day}"))?;
        entry
            .raws
            .into_iter()
            .map(|status| status.raw.id)
            .collect::<Vec<_>>()
    } else {
        ledger_store
            .list_objects(ObjectFilter {
                role: Some(StoreObjectRole::Raw),
                kind: Some(RAW_DATABENTO_DBN_ZST_KIND.to_string()),
                id_prefix: None,
            })?
            .into_iter()
            .map(|descriptor| descriptor.id)
            .collect::<Vec<_>>()
    };

    let mut aggregate = EsPrepareAggregate::default();
    for raw_id in raw_ids {
        match prepare_one(&ledger_store, raw_id.clone(), args.force).await {
            Ok(summary) => aggregate.ok.push(summary),
            Err(error) => aggregate.failed.push(EsPrepareFailure {
                raw_object_id: raw_id.to_string(),
                error: error.to_string(),
            }),
        }
    }
    let failed = !aggregate.failed.is_empty();
    print_json(&aggregate)?;
    if failed {
        std::process::exit(1);
    }
    Ok(())
}

async fn prepare_one(
    ledger_store: &R2Store,
    raw_id: StoreObjectId,
    force: bool,
) -> Result<EsPrepareSummary> {
    let progress = prepare_progress_channel();
    let artifact = prepare_es_replay_artifact(ledger_store, &raw_id, force, Some(progress)).await?;
    Ok(artifact.summary(&raw_id))
}

#[derive(Debug, Default, Serialize)]
struct EsPrepareAggregate {
    ok: Vec<EsPrepareSummary>,
    failed: Vec<EsPrepareFailure>,
}

#[derive(Debug, Serialize)]
struct EsPrepareFailure {
    raw_object_id: String,
    error: String,
}

fn prepare_progress_channel() -> mpsc::UnboundedSender<PrepareProgress> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(progress) = rx.recv().await {
            match progress {
                PrepareProgress::Hydrating => eprintln!("prepare hydrating"),
                PrepareProgress::Decoding { records } => {
                    eprintln!("prepare decoding records={records}")
                }
                PrepareProgress::Encoding { events, batches } => {
                    eprintln!("prepare encoding events={events} batches={batches}")
                }
                PrepareProgress::Registering => eprintln!("prepare registering"),
            }
        }
    });
    tx
}

fn fetch_progress_channel() -> mpsc::UnboundedSender<FetchProgress> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(progress) = rx.recv().await {
            match progress {
                FetchProgress::Requesting => eprintln!("fetch requesting"),
                FetchProgress::Downloading => eprintln!("fetch downloading"),
                FetchProgress::Registering => eprintln!("fetch registering"),
            }
        }
    });
    tx
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
        StoreSubcommand::Sync(args) => {
            let report = ledger_store
                .sync_registry(args.overwrite, args.dry_run)
                .await?;
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
        StoreSubcommand::AbortIncompleteUploads(args) => {
            let uploads = ledger_store
                .list_incomplete_multipart_uploads(&args.prefix)
                .await?;
            let mut aborted = Vec::new();
            let mut failed = Vec::new();
            if args.execute {
                for upload in &uploads {
                    match ledger_store
                        .abort_multipart_upload(&upload.key, &upload.upload_id)
                        .await
                    {
                        Ok(()) => aborted.push(upload.upload_id.clone()),
                        Err(error) => failed.push(json!({
                            "upload_id": upload.upload_id,
                            "key": upload.key,
                            "error": error.to_string(),
                        })),
                    }
                }
            }
            print_json(&json!({
                "prefix": args.prefix,
                "executed": args.execute,
                "found": uploads.len(),
                "uploads": uploads,
                "aborted": aborted,
                "failed": failed,
            }))?;
            if !failed.is_empty() {
                std::process::exit(1);
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
