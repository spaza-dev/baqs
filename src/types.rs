use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use uuid::Uuid;

pub const QUERY_CONTRACT_VERSION: &str = "2";

fn contract_version() -> String {
    QUERY_CONTRACT_VERSION.to_string()
}

fn request_id() -> String {
    Uuid::new_v4().to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CatalogReference {
    pub catalog_item_id: String,
    #[serde(default)]
    pub name: String,
    #[serde(rename = "type", default)]
    pub content_type: String,
    #[serde(default)]
    #[schema(value_type = Object)]
    pub metadata: Value,
    #[serde(default)]
    pub fingerprint_reference: Option<String>,
    #[serde(default)]
    pub audio_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IndexObjectReference {
    pub object_key: String,
    #[serde(default)]
    pub format: String,
    #[serde(default)]
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArchiveReference {
    #[serde(default)]
    pub archive_id: String,
    pub stream_id: String,
    #[serde(default)]
    pub started_at: i64,
    #[serde(default)]
    pub ended_at: i64,
    #[serde(default)]
    pub index_objects: Vec<IndexObjectReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryOptions {
    #[serde(default)]
    pub minimum_confidence: Option<f64>,
    #[serde(default)]
    pub minimum_match_count: Option<u32>,
    #[serde(default)]
    pub max_results: Option<usize>,
    #[serde(default)]
    pub merge_gap_seconds: Option<f64>,
    #[serde(default)]
    pub execution_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub max_chunks: Option<usize>,
    #[serde(default)]
    pub include_recording: Option<bool>,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            minimum_confidence: None,
            minimum_match_count: None,
            max_results: None,
            merge_gap_seconds: None,
            execution_timeout_seconds: None,
            max_chunks: None,
            include_recording: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryRequest {
    #[serde(default = "contract_version")]
    pub contract_version: String,
    #[serde(default = "request_id")]
    pub request_id: String,
    #[serde(default)]
    pub stream_id: Option<String>,
    #[serde(default)]
    pub external_stream_id: Option<String>,
    #[serde(default)]
    pub stream_ids: Vec<String>,
    #[serde(default)]
    pub start: Option<String>,
    #[serde(default)]
    pub end: Option<String>,
    #[serde(default)]
    pub month: Option<String>,
    #[serde(default)]
    pub recording_id: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub audio_url: Option<String>,
    #[serde(default)]
    #[schema(value_type = Object)]
    pub recording_metadata: Option<Value>,
    #[serde(default)]
    pub catalog: Vec<CatalogReference>,
    #[serde(default)]
    pub archives: Vec<ArchiveReference>,
    #[serde(default)]
    pub options: QueryOptions,
    #[serde(default)]
    pub webhook_url: Option<String>,
    #[serde(default)]
    pub webhook_headers: HashMap<String, String>,
}

impl QueryRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.contract_version != QUERY_CONTRACT_VERSION {
            return Err(format!(
                "unsupported contract_version '{}', expected '{}'",
                self.contract_version, QUERY_CONTRACT_VERSION
            ));
        }
        if self.catalog.is_empty() && self.audio_url.is_none() {
            return Err("catalog[].audio_url or audio_url is required".to_string());
        }
        for reference in &self.catalog {
            if reference
                .audio_url
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return Err(format!(
                    "catalog item '{}' is missing audio_url",
                    reference.catalog_item_id
                ));
            }
        }
        if self.month.is_some() && (self.start.is_some() || self.end.is_some()) {
            return Err("month cannot be combined with start/end".to_string());
        }
        if self.month.is_none() && self.start.is_none() && self.end.is_none() {
            return Err("one of month, start, or end is required".to_string());
        }
        Ok(())
    }

    pub fn catalogs(&self) -> Vec<CatalogReference> {
        if !self.catalog.is_empty() {
            return self.catalog.clone();
        }
        self.audio_url
            .as_ref()
            .map(|url| CatalogReference {
                catalog_item_id: self
                    .recording_id
                    .clone()
                    .unwrap_or_else(|| self.request_id.clone()),
                name: self.title.clone().unwrap_or_default(),
                content_type: "audio/*".to_string(),
                metadata: self.recording_metadata.clone().unwrap_or(Value::Null),
                fingerprint_reference: None,
                audio_url: Some(url.clone()),
            })
            .into_iter()
            .collect()
    }

    pub fn effective_minimum_confidence(&self, fallback: f64) -> f64 {
        self.options.minimum_confidence.unwrap_or(fallback)
    }

    pub fn effective_minimum_match_count(&self, fallback: u32) -> u32 {
        self.options.minimum_match_count.unwrap_or(fallback)
    }

    pub fn effective_max_results(&self, fallback: usize) -> usize {
        self.options.max_results.unwrap_or(fallback)
    }

    pub fn effective_max_chunks(&self, fallback: usize) -> usize {
        self.options.max_chunks.unwrap_or(fallback)
    }

    pub fn include_recording(&self, fallback: bool) -> bool {
        self.options.include_recording.unwrap_or(fallback)
    }

    pub fn webhook_url(&self) -> Option<&str> {
        self.webhook_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn from_str(value: &str) -> Self {
        match value {
            "running" => Self::Running,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => Self::Queued,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobSubmissionResponse {
    pub contract_version: String,
    pub request_id: String,
    pub job_id: String,
    pub status: String,
    pub status_url: String,
    pub result_url: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct JobProgress {
    #[serde(default)]
    pub stage: String,
    #[serde(default)]
    pub days_total: usize,
    #[serde(default)]
    pub days_completed: usize,
    #[serde(default)]
    pub chunks_total: usize,
    #[serde(default)]
    pub chunks_completed: usize,
    #[serde(default)]
    pub recordings_total: usize,
    #[serde(default)]
    pub recordings_completed: usize,
    #[serde(default)]
    pub current_stream: Option<String>,
    #[serde(default)]
    pub current_day: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobStatusResponse {
    pub contract_version: String,
    pub job_id: String,
    pub request_id: String,
    pub kind: String,
    pub status: String,
    pub progress: JobProgress,
    pub submitted_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub cancel_requested: bool,
    pub result_available: bool,
    pub webhook_status: String,
    pub error: Option<JobError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct Recording {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    #[schema(value_type = Object)]
    pub metadata: Value,
    #[serde(default)]
    pub content_type: String,
    #[serde(default)]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub duration_seconds: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct Stream {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub external_stream_id: Option<String>,
    #[serde(default)]
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct Provenance {
    #[serde(default)]
    pub chunk_ids: Vec<String>,
    #[serde(default)]
    pub archive_ids: Vec<String>,
    #[serde(default)]
    pub node_ids: Vec<String>,
    #[serde(default)]
    pub days: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryMatch {
    pub catalog_item_id: String,
    pub recording_id: String,
    pub recording_title: String,
    pub archive_id: String,
    pub stream_id: String,
    pub matched_at: i64,
    pub matched_at_iso: String,
    pub matched_end: i64,
    pub matched_end_iso: String,
    pub date: String,
    pub time: String,
    pub timezone: String,
    pub duration_seconds: f64,
    pub confidence: f64,
    pub match_count: u32,
    pub query_start: f64,
    pub query_stop: f64,
    pub reference_start: f64,
    pub reference_stop: f64,
    pub recording: Recording,
    pub stream: Stream,
    pub provenance: Provenance,
    #[serde(default)]
    #[schema(value_type = Object)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct QuerySummary {
    #[serde(default)]
    pub archives_scanned: usize,
    #[serde(default)]
    pub chunks_merged: usize,
    #[serde(default)]
    pub matches_found: usize,
    #[serde(default)]
    pub duration_seconds: f64,
    #[serde(default)]
    pub streams: usize,
    #[serde(default)]
    pub days: usize,
    #[serde(default)]
    pub recordings: usize,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryResult {
    pub contract_version: String,
    pub request_id: String,
    pub status: String,
    pub matches: Vec<QueryMatch>,
    pub summary: QuerySummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: String,
    pub request_id: String,
    pub kind: String,
    pub contract_version: String,
    pub status: JobStatus,
    pub request_json: String,
    pub result_json: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub progress_json: Option<String>,
    pub webhook_url: Option<String>,
    pub webhook_headers_json: Option<String>,
    pub webhook_status: String,
    pub submitted_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub cancel_requested: bool,
}

impl JobRecord {
    pub fn request(&self) -> anyhow::Result<QueryRequest> {
        Ok(serde_json::from_str(&self.request_json)?)
    }

    pub fn progress(&self) -> JobProgress {
        self.progress_json
            .as_deref()
            .and_then(|value| serde_json::from_str(value).ok())
            .unwrap_or_default()
    }
}

pub fn utc_now_millis() -> i64 {
    Utc::now().timestamp_millis()
}

pub fn iso_from_millis(value: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(value)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub fn day_from_millis(value: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(value)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .format("%Y-%m-%d")
        .to_string()
}

pub fn time_from_millis(value: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(value)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .format("%H:%M:%S%.3f")
        .to_string()
}
