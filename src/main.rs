mod api;
mod jobs;
mod pipeline;
mod types;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use crate::api::{AppState, router};
use crate::jobs::{Db, parse_webhook_headers};
use crate::pipeline::{Pipeline, PipelineError};
use crate::types::{QueryRequest, utc_now_millis};

pub const PUBLIC_BASE_URL: &str = "https://baqs.spaza.co.ke";

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub olaf: OlafConfig,
    pub worker: WorkerConfig,
    pub query: QueryConfig,
    pub paths: PathsConfig,
    pub webhook: WebhookConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind: String,
    pub public_base_url: String,
    pub api_token: String,
    pub health_public: bool,
    pub max_body_bytes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8080".to_string(),
            public_base_url: PUBLIC_BASE_URL.to_string(),
            api_token: String::new(),
            health_public: true,
            max_body_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub archive_prefix: String,
    pub force_path_style: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            bucket: String::new(),
            region: "auto".to_string(),
            access_key: String::new(),
            secret_key: String::new(),
            archive_prefix: "archives".to_string(),
            force_path_style: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OlafConfig {
    pub binary: String,
    pub map_size_bytes: usize,
    pub query_timeout_seconds: u64,
    pub threads: usize,
    pub sample_rate: u32,
    pub step_size: u32,
}

impl Default for OlafConfig {
    fn default() -> Self {
        Self {
            binary: "/usr/local/bin/olaf".to_string(),
            map_size_bytes: 64 * 1024 * 1024 * 1024,
            query_timeout_seconds: 600,
            threads: 1,
            sample_rate: 16_000,
            step_size: 128,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WorkerConfig {
    pub concurrency: usize,
    pub lease_seconds: u64,
    pub poll_interval_ms: u64,
    pub cleanup_interval_seconds: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: 1,
            lease_seconds: 300,
            poll_interval_ms: 1000,
            cleanup_interval_seconds: 3600,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    pub minimum_confidence: f64,
    pub minimum_match_count: u32,
    pub max_results: usize,
    pub merge_gap_seconds: f64,
    pub max_chunks: usize,
    pub include_recording: bool,
    pub max_recording_bytes: u64,
    pub download_timeout_seconds: u64,
    pub allow_private_urls: bool,
    pub job_ttl_seconds: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            minimum_confidence: 0.0,
            minimum_match_count: 6,
            max_results: 100,
            merge_gap_seconds: 2.0,
            max_chunks: 100_000,
            include_recording: true,
            max_recording_bytes: 2 * 1024 * 1024 * 1024,
            download_timeout_seconds: 300,
            allow_private_urls: false,
            job_ttl_seconds: 7 * 24 * 3600,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PathsConfig {
    pub data_dir: PathBuf,
    pub work_dir: PathBuf,
    pub database: PathBuf,
}

impl Default for PathsConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/baqs"),
            work_dir: PathBuf::from("/var/lib/baqs/work"),
            database: PathBuf::from("/var/lib/baqs/baqs.db"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WebhookConfig {
    pub timeout_seconds: u64,
    pub max_attempts: i64,
    pub retry_base_seconds: u64,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 30,
            max_attempts: 8,
            retry_base_seconds: 5,
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "baqs", version, about = "Broadcast Archive Query Service")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = "/etc/baqs/config.toml")]
        config: PathBuf,
        #[arg(long)]
        check_config: bool,
    },
    Query {
        #[arg(long)]
        request: PathBuf,
        #[arg(long, default_value = "/etc/baqs/config.toml")]
        config: PathBuf,
    },
    InitConfig {
        #[arg(long, default_value = "/etc/baqs/config.toml")]
        output: PathBuf,
        #[arg(long)]
        olaf_binary: Option<PathBuf>,
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        force: bool,
    },
    Version,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Serve {
            config,
            check_config,
        } => serve(config, check_config).await,
        Command::Query { request, config } => run_cli_query(config, request).await,
        Command::InitConfig {
            output,
            olaf_binary,
            data_dir,
            force,
        } => init_config(&output, olaf_binary, data_dir, force),
        Command::Version => {
            println!("baqs {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}

async fn serve(config_path: PathBuf, check_config: bool) -> Result<()> {
    let config = Arc::new(load_config(&config_path)?);
    validate_config(&config)?;
    if check_config {
        println!("configuration ok: {}", config_path.display());
        return Ok(());
    }

    tokio::fs::create_dir_all(&config.paths.data_dir)
        .await
        .with_context(|| {
            format!(
                "creating data directory {}",
                config.paths.data_dir.display()
            )
        })?;
    tokio::fs::create_dir_all(&config.paths.work_dir)
        .await
        .with_context(|| {
            format!(
                "creating work directory {}",
                config.paths.work_dir.display()
            )
        })?;

    let db = Db::open(&config.paths.database).await?;
    let pipeline = Pipeline::new(config.clone(), db.clone()).await?;
    let tokens = Arc::new(Mutex::new(HashMap::<String, CancellationToken>::new()));
    let state = AppState {
        config: config.clone(),
        db: db.clone(),
        pipeline: pipeline.clone(),
        tokens: tokens.clone(),
        started_at: Instant::now(),
        workers: config.worker.concurrency.max(1),
    };

    spawn_workers(state.clone());
    spawn_webhook_dispatcher(state.clone());

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(&config.server.bind)
        .await
        .with_context(|| format!("binding {}", config.server.bind))?;
    info!(
        bind = %config.server.bind,
        workers = config.worker.concurrency,
        database = %config.paths.database.display(),
        "Broadcast Archive Query Service listening"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serving HTTP")?;
    Ok(())
}

fn spawn_workers(state: AppState) {
    let count = state.config.worker.concurrency.max(1);
    for worker_index in 0..count {
        let state = state.clone();
        tokio::spawn(async move {
            let poll = Duration::from_millis(state.config.worker.poll_interval_ms.max(50));
            let lease = state.config.worker.lease_seconds.max(30);
            loop {
                match state.db.claim_next_job(lease).await {
                    Ok(Some(job)) => {
                        let token = state.token_for(&job.id).await;
                        let job_id = job.id.clone();
                        info!(job_id = %job_id, worker = worker_index, "Job started");
                        let heartbeat_db = state.db.clone();
                        let heartbeat_job_id = job_id.clone();
                        let heartbeat_lease = lease;
                        let heartbeat = tokio::spawn(async move {
                            let interval = Duration::from_secs((heartbeat_lease / 2).max(15));
                            loop {
                                tokio::time::sleep(interval).await;
                                if let Err(error) = heartbeat_db
                                    .renew_lease(&heartbeat_job_id, heartbeat_lease)
                                    .await
                                {
                                    warn!(
                                        job_id = %heartbeat_job_id,
                                        %error,
                                        "Failed to renew job lease"
                                    );
                                }
                            }
                        });
                        let result = state.pipeline.run_job(&job, token).await;
                        heartbeat.abort();
                        match result {
                            Ok(result) => {
                                if let Err(error) = state.db.complete_job(&job_id, &result).await {
                                    error!(job_id = %job_id, %error, "Failed to complete job");
                                } else {
                                    info!(
                                        job_id = %job_id,
                                        matches = result.matches.len(),
                                        "Job completed"
                                    );
                                }
                            }
                            Err(PipelineError::Cancelled) => {
                                if let Err(error) = state
                                    .db
                                    .finish_cancelled(&job_id, "cancelled by request")
                                    .await
                                {
                                    error!(job_id = %job_id, %error, "Failed to mark job cancelled");
                                }
                            }
                            Err(PipelineError::Other(error)) => {
                                let cancelled = state
                                    .db
                                    .get_job(&job_id)
                                    .await
                                    .ok()
                                    .flatten()
                                    .map(|job| job.cancel_requested)
                                    .unwrap_or(false);
                                if cancelled {
                                    let _ = state
                                        .db
                                        .finish_cancelled(&job_id, "cancelled by request")
                                        .await;
                                } else if let Err(error) = state
                                    .db
                                    .fail_job(&job_id, "query_failed", &error.to_string())
                                    .await
                                {
                                    error!(job_id = %job_id, %error, "Failed to record job failure");
                                } else {
                                    warn!(job_id = %job_id, %error, "Job failed");
                                }
                            }
                        }
                        state.remove_token(&job_id).await;
                    }
                    Ok(None) => tokio::time::sleep(poll).await,
                    Err(error) => {
                        error!(%error, "Failed to claim job");
                        tokio::time::sleep(poll).await;
                    }
                }
            }
        });
    }

    let state_cleanup = state.clone();
    tokio::spawn(async move {
        let interval =
            Duration::from_secs(state_cleanup.config.worker.cleanup_interval_seconds.max(60));
        loop {
            tokio::time::sleep(interval).await;
            match state_cleanup
                .db
                .cleanup(state_cleanup.config.query.job_ttl_seconds)
                .await
            {
                Ok(count) if count > 0 => info!(count, "Removed expired jobs"),
                Ok(_) => {}
                Err(error) => warn!(%error, "Job cleanup failed"),
            }
        }
    });
}

fn spawn_webhook_dispatcher(state: AppState) {
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(
                state.config.webhook.timeout_seconds.max(1),
            ))
            .build()
            .expect("webhook HTTP client");
        loop {
            match state.db.claim_webhook().await {
                Ok(Some(delivery)) => {
                    let payload: serde_json::Value =
                        serde_json::from_str(&delivery.payload_json).unwrap_or_default();
                    let mut request = client.post(&delivery.url).json(&payload);
                    for (name, value) in parse_webhook_headers(delivery.headers_json.as_deref()) {
                        request = request.header(name, value);
                    }
                    match request.send().await {
                        Ok(response) if response.status().is_success() => {
                            if let Err(error) = state
                                .db
                                .mark_webhook_delivered(delivery.id, &delivery.job_id)
                                .await
                            {
                                error!(%error, "Failed to mark webhook delivered");
                            }
                        }
                        Ok(response) => {
                            let error = format!("HTTP {}", response.status());
                            schedule_webhook_retry(&state, &delivery, &error).await;
                        }
                        Err(error) => {
                            schedule_webhook_retry(&state, &delivery, &error.to_string()).await;
                        }
                    }
                }
                Ok(None) => tokio::time::sleep(Duration::from_secs(1)).await,
                Err(error) => {
                    warn!(%error, "Failed to claim webhook");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
}

async fn schedule_webhook_retry(state: &AppState, delivery: &jobs::WebhookDelivery, error: &str) {
    let next_attempt = utc_now_millis()
        + (state.config.webhook.retry_base_seconds.max(1) as i64
            * 1000
            * (1i64 << delivery.attempts.min(8)));
    if let Err(error) = state
        .db
        .mark_webhook_failed(
            delivery.id,
            &delivery.job_id,
            error,
            Some(next_attempt),
            state.config.webhook.max_attempts.max(1),
        )
        .await
    {
        error!(%error, "Failed to schedule webhook retry");
    }
}

async fn run_cli_query(config_path: PathBuf, request_path: PathBuf) -> Result<()> {
    let config = Arc::new(load_config(&config_path)?);
    validate_config(&config)?;
    let request: QueryRequest = serde_json::from_slice(
        &tokio::fs::read(&request_path)
            .await
            .with_context(|| format!("reading request {}", request_path.display()))?,
    )
    .with_context(|| format!("parsing request {}", request_path.display()))?;
    request.validate().map_err(anyhow::Error::msg)?;

    let db = Db::open(&config.paths.database).await?;
    let pipeline = Pipeline::new(config.clone(), db.clone()).await?;
    let job_id = Uuid::new_v4().to_string();
    db.insert_job(&request, "query", &job_id).await?;
    let job = db
        .claim_job(&job_id, config.worker.lease_seconds)
        .await?
        .context("unable to claim CLI job")?;
    let token = CancellationToken::new();
    let result = pipeline.run_job(&job, token).await;
    match result {
        Ok(result) => {
            db.complete_job(&job_id, &result).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Err(PipelineError::Cancelled) => {
            db.finish_cancelled(&job_id, "cancelled").await?;
            anyhow::bail!("query cancelled");
        }
        Err(PipelineError::Other(error)) => {
            db.fail_job(&job_id, "query_failed", &error.to_string())
                .await?;
            return Err(error);
        }
    }
    Ok(())
}

fn init_config(
    output: &Path,
    olaf_binary: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    force: bool,
) -> Result<()> {
    if output.exists() && !force {
        anyhow::bail!(
            "{} already exists; use --force to replace it",
            output.display()
        );
    }
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let olaf = olaf_binary
        .map(|path| path.to_string_lossy().to_string())
        .or_else(|| {
            std::env::var_os("PATH").and_then(|paths| {
                std::env::split_paths(&paths)
                    .map(|directory| directory.join("olaf"))
                    .find(|candidate| candidate.is_file())
                    .map(|path| path.to_string_lossy().to_string())
            })
        })
        .unwrap_or_else(|| "/usr/local/bin/olaf".to_string());
    let data_dir = data_dir.unwrap_or_else(|| PathBuf::from("/var/lib/baqs"));
    let data_dir_text = data_dir.to_string_lossy();
    let work_dir_text = data_dir.join("work").to_string_lossy().to_string();
    let database_text = data_dir.join("baqs.db").to_string_lossy().to_string();

    let env_token = std::env::var("BAQS_API_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let token = env_token
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
    let api_token = if env_token.is_some() {
        "${BAQS_API_TOKEN}".to_string()
    } else {
        token.clone()
    };
    let template = CONFIG_TEMPLATE
        .replace("__OLAF_BINARY__", &olaf)
        .replace("__DATA_DIR__", data_dir_text.as_ref())
        .replace("__WORK_DIR__", &work_dir_text)
        .replace("__DATABASE__", &database_text)
        .replace("__API_TOKEN__", &api_token);
    std::fs::write(output, template).with_context(|| format!("writing {}", output.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(output, std::fs::Permissions::from_mode(0o600))?;
    }
    println!("wrote {}", output.display());
    if env_token.is_none() {
        println!("generated API token: {token}");
    }
    println!("Olaf binary: {olaf}");
    println!("Data directory: {}", data_dir.display());
    Ok(())
}

fn load_config(path: &Path) -> Result<Config> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading configuration {}", path.display()))?;
    let expanded = expand_env(&raw);
    let config: Config = toml::from_str(&expanded)
        .with_context(|| format!("parsing configuration {}", path.display()))?;
    Ok(config)
}

fn validate_config(config: &Config) -> Result<()> {
    if config.olaf.binary.trim().is_empty() {
        anyhow::bail!("olaf.binary is required");
    }
    if config.worker.concurrency == 0 {
        anyhow::bail!("worker.concurrency must be at least 1");
    }
    if config.olaf.sample_rate == 0 || config.olaf.step_size == 0 {
        anyhow::bail!("olaf.sample_rate and olaf.step_size must be positive");
    }
    Ok(())
}

fn expand_env(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        output.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        if let Some(end) = after.find('}') {
            let key = &after[..end];
            output.push_str(&std::env::var(key).unwrap_or_default());
            rest = &after[end + 1..];
        } else {
            output.push_str(&rest[start..]);
            break;
        }
    }
    output.push_str(rest);
    output
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    info!("Shutdown requested");
}

const CONFIG_TEMPLATE: &str = r#"[server]
bind = "0.0.0.0:8080"
public_base_url = "https://baqs.spaza.co.ke"
api_token = "__API_TOKEN__"
health_public = true
max_body_bytes = 1048576

[storage]
endpoint = "${BAQS_S3_ENDPOINT}"
bucket = "${BAQS_S3_BUCKET}"
region = "${BAQS_S3_REGION}"
access_key = "${BAQS_S3_ACCESS_KEY}"
secret_key = "${BAQS_S3_SECRET_KEY}"
archive_prefix = "archives"
force_path_style = true

[olaf]
binary = "__OLAF_BINARY__"
map_size_bytes = 68719476736
query_timeout_seconds = 600
threads = 1
sample_rate = 16000
step_size = 128

[worker]
concurrency = 1
lease_seconds = 300
poll_interval_ms = 1000
cleanup_interval_seconds = 3600

[query]
minimum_confidence = 0.0
minimum_match_count = 6
max_results = 100
merge_gap_seconds = 2.0
max_chunks = 100000
include_recording = true
max_recording_bytes = 2147483648
download_timeout_seconds = 300
allow_private_urls = false
job_ttl_seconds = 604800

[paths]
data_dir = "__DATA_DIR__"
work_dir = "__WORK_DIR__"
database = "__DATABASE__"

[webhook]
timeout_seconds = 30
max_attempts = 8
retry_base_seconds = 5
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_environment() {
        unsafe {
            std::env::set_var("BAQS_TEST_VALUE", "abc");
        }
        assert_eq!(expand_env("x=${BAQS_TEST_VALUE}"), "x=abc");
        assert_eq!(expand_env("x=${BAQS_TEST_MISSING}"), "x=");
    }

    #[test]
    fn parses_default_config() {
        let config: Config = toml::from_str(CONFIG_TEMPLATE).unwrap();
        assert_eq!(config.server.bind, "0.0.0.0:8080");
        assert_eq!(config.server.public_base_url, PUBLIC_BASE_URL);
        assert_eq!(config.olaf.sample_rate, 16_000);
        assert_eq!(config.worker.concurrency, 1);
    }

    #[test]
    fn allows_startup_without_storage_credentials() {
        let mut config: Config = toml::from_str(CONFIG_TEMPLATE).unwrap();
        config.storage.bucket.clear();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn job_status_is_used() {
        assert_eq!(crate::types::JobStatus::Queued.as_str(), "queued");
    }
}
