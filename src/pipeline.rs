use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use futures_util::StreamExt;
use lmdb::{Cursor, DatabaseFlags, Environment, Transaction, WriteFlags};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use url::Url;

use crate::Config;
use crate::jobs::Db;
use crate::types::{
    ArchiveReference, CatalogReference, JobProgress, JobRecord, Provenance, QueryMatch,
    QueryRequest, QueryResult, QuerySummary, Recording, Stream, day_from_millis, iso_from_millis,
    time_from_millis,
};

const OLAF_FINGERPRINTS: &str = "olaf_fingerprints";
const OLAF_RESOURCE_MAP: &str = "olaf_resource_map";
const RESOURCE_META_SIZE: usize = 528;
const RESOURCE_PATH_OFFSET: usize = 16;
const RESOURCE_PATH_SIZE: usize = 512;

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("query cancelled")]
    Cancelled,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl PipelineError {
    fn cancelled() -> Self {
        Self::Cancelled
    }
}

#[derive(Clone)]
pub struct Pipeline {
    config: Arc<Config>,
    db: Db,
    storage: Storage,
    http: reqwest::Client,
}

#[derive(Clone)]
struct Storage {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    prefix: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Manifest {
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub stream_id: String,
    #[serde(default)]
    pub external_stream_id: Option<String>,
    #[serde(default)]
    pub chunk_id: String,
    #[serde(default)]
    pub started_at: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub index_objects: Vec<IndexObject>,
}

#[derive(Debug, Clone, Deserialize)]
struct IndexObject {
    #[serde(default)]
    pub object_key: String,
    #[serde(default)]
    pub format: String,
}

#[derive(Debug, Clone)]
struct ChunkRef {
    archive_id: String,
    node_id: String,
    stream_id: String,
    external_stream_id: Option<String>,
    chunk_id: String,
    started_at: DateTime<Utc>,
    index_key: String,
}

#[derive(Debug, Clone)]
struct DayGroup {
    stream_id: String,
    external_stream_id: Option<String>,
    node_ids: Vec<String>,
    day: String,
    day_start: DateTime<Utc>,
    chunks: Vec<ChunkRef>,
}

#[derive(Debug, Clone)]
struct PreparedRecording {
    catalog: CatalogReference,
    recording: Recording,
    path: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct OlafQueryOutput {
    #[serde(default)]
    pub matches: Vec<OlafMatch>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct OlafMatch {
    #[serde(default)]
    pub match_count: u32,
    #[serde(default)]
    pub query_start: f64,
    #[serde(default)]
    pub query_stop: f64,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub reference_start: f64,
    #[serde(default)]
    pub reference_stop: f64,
}

#[derive(Debug, Default)]
struct MergeStats {
    fingerprints: u64,
    resources: u64,
}

impl Pipeline {
    pub async fn new(config: Arc<Config>, db: Db) -> Result<Self> {
        let storage = Storage::new(&config).await?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::limited(5))
            .timeout(Duration::from_secs(config.query.download_timeout_seconds))
            .build()
            .context("building HTTP client")?;
        Ok(Self {
            config,
            db,
            storage,
            http,
        })
    }

    pub async fn run_job(
        &self,
        job: &JobRecord,
        cancel: CancellationToken,
    ) -> std::result::Result<QueryResult, PipelineError> {
        let work_dir = self.config.paths.work_dir.join(&job.id).to_path_buf();
        if let Err(error) = tokio::fs::create_dir_all(&work_dir).await {
            return Err(PipelineError::Other(anyhow::anyhow!(
                "creating work directory {}: {error}",
                work_dir.display()
            )));
        }

        let result = self.run_inner(job, &work_dir, cancel).await;
        if let Err(error) = tokio::fs::remove_dir_all(&work_dir).await {
            warn!(job_id = %job.id, path = %work_dir.display(), %error, "Failed to clean job work directory");
        }
        result
    }

    async fn run_inner(
        &self,
        job: &JobRecord,
        work_dir: &Path,
        cancel: CancellationToken,
    ) -> std::result::Result<QueryResult, PipelineError> {
        let started = std::time::Instant::now();
        let request = job.request().map_err(PipelineError::Other)?;
        request
            .validate()
            .map_err(|error| PipelineError::Other(anyhow::anyhow!(error)))?;

        let (range_start, range_end) = parse_range(&request)?;
        let catalogs = request.catalogs();
        if catalogs.is_empty() {
            return Err(PipelineError::Other(anyhow::anyhow!(
                "no catalog recordings were supplied"
            )));
        }

        let mut progress = JobProgress {
            stage: "preparing".to_string(),
            recordings_total: catalogs.len(),
            ..JobProgress::default()
        };
        self.progress(job, &progress).await;

        let recordings = self
            .prepare_recordings(&catalogs, work_dir, &mut progress, job, &cancel)
            .await?;
        check_cancel(&cancel)?;

        progress.stage = "discovering".to_string();
        progress.recordings_completed = recordings.len();
        self.progress(job, &progress).await;

        let chunks = self
            .discover_chunks(&request, range_start, range_end, &cancel)
            .await?;
        let max_chunks = request.effective_max_chunks(self.config.query.max_chunks);
        if chunks.len() > max_chunks {
            return Err(PipelineError::Other(anyhow::anyhow!(
                "query selected {} chunks, above max_chunks {}",
                chunks.len(),
                max_chunks
            )));
        }

        let groups = group_chunks(chunks);
        progress.stage = "merging".to_string();
        progress.days_total = groups.len();
        progress.days_completed = 0;
        progress.chunks_total = groups.iter().map(|group| group.chunks.len()).sum();
        progress.chunks_completed = 0;
        self.progress(job, &progress).await;

        let minimum_confidence =
            request.effective_minimum_confidence(self.config.query.minimum_confidence);
        let minimum_match_count =
            request.effective_minimum_match_count(self.config.query.minimum_match_count);
        let max_results = request.effective_max_results(self.config.query.max_results);
        let merge_gap = request
            .options
            .merge_gap_seconds
            .unwrap_or(self.config.query.merge_gap_seconds);

        let mut all_matches = Vec::new();
        let mut warnings = Vec::new();
        let mut archives_scanned = 0usize;
        let mut chunks_merged = 0usize;
        let mut streams = HashSet::new();
        let mut days = HashSet::new();

        for group in &groups {
            check_cancel(&cancel)?;
            progress.current_stream = Some(group.stream_id.clone());
            progress.current_day = Some(group.day.clone());
            progress.stage = "merging".to_string();
            self.progress(job, &progress).await;

            let merged_dir = work_dir
                .join("merged")
                .join(safe_path_component(&group.stream_id))
                .join(&group.day);
            let merged_chunks = self
                .merge_group(group, &merged_dir, &mut progress, job, &cancel)
                .await?;
            chunks_merged += merged_chunks;
            archives_scanned += merged_chunks;

            progress.stage = "querying".to_string();
            self.progress(job, &progress).await;

            let olaf_home = work_dir.join("olaf-home");
            let matches = self
                .query_group(
                    group,
                    &merged_dir,
                    &olaf_home,
                    &recordings,
                    minimum_match_count,
                    &cancel,
                )
                .await?;
            all_matches.extend(matches);
            streams.insert(group.stream_id.clone());
            days.insert(group.day.clone());

            progress.days_completed += 1;
            self.progress(job, &progress).await;

            if let Err(error) = tokio::fs::remove_dir_all(&merged_dir).await {
                debug!(path = %merged_dir.display(), %error, "Failed to remove merged index");
            }
        }

        let mut matches = merge_adjacent_matches(all_matches, merge_gap);
        matches.retain(|item| {
            item.confidence >= minimum_confidence && item.match_count >= minimum_match_count
        });
        matches.sort_by(|left, right| {
            left.recording_id
                .cmp(&right.recording_id)
                .then(left.stream_id.cmp(&right.stream_id))
                .then(left.matched_at.cmp(&right.matched_at))
        });
        let total_found = matches.len();
        matches.truncate(max_results);
        if total_found > matches.len() {
            warnings.push(format!(
                "result truncated from {} to {} matches",
                total_found,
                matches.len()
            ));
        }

        let include_recording = request.include_recording(self.config.query.include_recording);
        if !include_recording {
            for item in &mut matches {
                item.recording.url.clear();
                item.recording.metadata = serde_json::Value::Null;
            }
        }

        let summary = QuerySummary {
            archives_scanned,
            chunks_merged,
            matches_found: matches.len(),
            duration_seconds: started.elapsed().as_secs_f64(),
            streams: streams.len(),
            days: days.len(),
            recordings: recordings.len(),
            warnings,
        };

        Ok(QueryResult {
            contract_version: request.contract_version,
            request_id: request.request_id,
            status: "completed".to_string(),
            matches,
            summary,
        })
    }

    async fn progress(&self, job: &JobRecord, progress: &JobProgress) {
        if let Err(error) = self.db.update_progress(&job.id, progress).await {
            warn!(job_id = %job.id, %error, "Failed to update job progress");
        }
    }

    async fn prepare_recordings(
        &self,
        catalogs: &[CatalogReference],
        work_dir: &Path,
        progress: &mut JobProgress,
        job: &JobRecord,
        cancel: &CancellationToken,
    ) -> std::result::Result<Vec<PreparedRecording>, PipelineError> {
        let recordings_dir = work_dir.join("recordings");
        tokio::fs::create_dir_all(&recordings_dir)
            .await
            .map_err(|error| {
                PipelineError::Other(anyhow::anyhow!("creating recordings directory: {error}"))
            })?;
        let mut prepared = Vec::with_capacity(catalogs.len());
        for (index, catalog) in catalogs.iter().enumerate() {
            check_cancel(cancel)?;
            progress.stage = format!("downloading recording {}/{}", index + 1, catalogs.len());
            self.progress(job, progress).await;
            let url = catalog
                .audio_url
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    PipelineError::Other(anyhow::anyhow!(
                        "catalog item '{}' is missing audio_url",
                        catalog.catalog_item_id
                    ))
                })?;
            validate_recording_url(url, self.config.query.allow_private_urls)?;
            let extension = audio_extension(url, &catalog.content_type);
            let path = recordings_dir.join(format!("recording-{index}{extension}"));
            let (size, sha256) = self
                .download_recording(url, &path, cancel)
                .await
                .map_err(PipelineError::Other)?;
            let duration = probe_duration(&path).await.ok().flatten();
            let title = if catalog.name.trim().is_empty() {
                catalog
                    .metadata
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            } else {
                catalog.name.clone()
            };
            let recording = Recording {
                id: catalog.catalog_item_id.clone(),
                title: title.clone(),
                name: title,
                url: url.to_string(),
                metadata: catalog.metadata.clone(),
                content_type: catalog.content_type.clone(),
                size_bytes: Some(size),
                sha256: Some(sha256),
                duration_seconds: duration,
            };
            prepared.push(PreparedRecording {
                catalog: catalog.clone(),
                recording,
                path,
            });
            progress.recordings_completed += 1;
            self.progress(job, progress).await;
        }
        progress.stage = "recordings ready".to_string();
        self.progress(job, progress).await;
        Ok(prepared)
    }

    async fn download_recording(
        &self,
        url: &str,
        path: &Path,
        cancel: &CancellationToken,
    ) -> Result<(u64, String)> {
        let response = self
            .http
            .get(url)
            .send()
            .await
            .with_context(|| format!("downloading recording {url}"))?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("recording download returned HTTP {status}");
        }
        if let Some(length) = response.content_length() {
            if length > self.config.query.max_recording_bytes {
                anyhow::bail!(
                    "recording is {} bytes, above limit {}",
                    length,
                    self.config.query.max_recording_bytes
                );
            }
        }
        let mut file = tokio::fs::File::create(path)
            .await
            .with_context(|| format!("creating recording file {}", path.display()))?;
        let mut stream = response.bytes_stream();
        use futures_util::StreamExt;
        let mut size = 0u64;
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream.next().await {
            check_cancel(cancel).map_err(|error| anyhow::anyhow!(error.to_string()))?;
            let chunk = chunk.context("reading recording body")?;
            size += chunk.len() as u64;
            if size > self.config.query.max_recording_bytes {
                anyhow::bail!(
                    "recording exceeded maximum size {}",
                    self.config.query.max_recording_bytes
                );
            }
            hasher.update(&chunk);
            file.write_all(&chunk)
                .await
                .context("writing recording body")?;
        }
        file.flush().await?;
        Ok((size, hex::encode(hasher.finalize())))
    }

    async fn discover_chunks(
        &self,
        request: &QueryRequest,
        range_start: DateTime<Utc>,
        range_end: DateTime<Utc>,
        cancel: &CancellationToken,
    ) -> std::result::Result<Vec<ChunkRef>, PipelineError> {
        let mut chunks = Vec::new();
        for archive in &request.archives {
            chunks.extend(chunks_from_archive(archive)?);
        }

        if chunks.is_empty() {
            chunks = self
                .storage
                .discover(range_start, range_end, request, cancel)
                .await
                .map_err(PipelineError::Other)?;
        }

        let filters = stream_filters(request);
        if !filters.is_empty() {
            chunks.retain(|chunk| {
                filters.contains(&chunk.stream_id)
                    || chunk
                        .external_stream_id
                        .as_deref()
                        .map(|value| filters.contains(value))
                        .unwrap_or(false)
            });
        }
        chunks.retain(|chunk| chunk.started_at >= range_start && chunk.started_at < range_end);
        chunks.sort_by_key(|chunk| chunk.started_at);
        chunks.dedup_by(|left, right| {
            left.stream_id == right.stream_id && left.index_key == right.index_key
        });
        if chunks.is_empty() {
            return Err(PipelineError::Other(anyhow::anyhow!(
                "no archive chunks matched the request"
            )));
        }
        Ok(chunks)
    }

    async fn merge_group(
        &self,
        group: &DayGroup,
        merged_dir: &Path,
        progress: &mut JobProgress,
        job: &JobRecord,
        cancel: &CancellationToken,
    ) -> std::result::Result<usize, PipelineError> {
        let chunks_dir = merged_dir.join("chunks");
        let db_dir = merged_dir.join("db");
        tokio::fs::create_dir_all(&chunks_dir)
            .await
            .map_err(|error| {
                PipelineError::Other(anyhow::anyhow!("creating chunk directory: {error}"))
            })?;
        tokio::fs::create_dir_all(&db_dir).await.map_err(|error| {
            PipelineError::Other(anyhow::anyhow!(
                "creating merged database directory: {error}"
            ))
        })?;

        let mut chunk_paths = Vec::new();
        for (index, chunk) in group.chunks.iter().enumerate() {
            check_cancel(cancel)?;
            let chunk_path = chunks_dir.join(format!("chunk-{index}.mdb"));
            self.storage
                .download_object(&chunk.index_key, &chunk_path)
                .await
                .map_err(PipelineError::Other)?;
            chunk_paths.push((chunk.clone(), chunk_path));
            progress.chunks_completed += 1;
            self.progress(job, progress).await;
        }

        let sample_rate = self.config.olaf.sample_rate;
        let step_size = self.config.olaf.step_size;
        let step_seconds = step_size as f64 / sample_rate.max(1) as f64;
        let day_start = group.day_start;
        let map_size = self.config.olaf.map_size_bytes;
        let chunks_for_merge: Vec<(ChunkRef, PathBuf)> = chunk_paths.clone();
        let db_dir_for_merge = db_dir.clone();
        let map_size_for_merge = map_size;
        let cancel_for_merge = cancel.clone();
        let stats = tokio::task::spawn_blocking(move || -> Result<MergeStats> {
            merge_chunks(
                &chunks_for_merge,
                &db_dir_for_merge,
                map_size_for_merge,
                day_start,
                step_seconds,
                &cancel_for_merge,
            )
        })
        .await
        .map_err(|error| PipelineError::Other(anyhow::anyhow!("merge task failed: {error}")))?
        .map_err(|error| {
            if error.to_string().contains("query cancelled") {
                PipelineError::Cancelled
            } else {
                PipelineError::Other(error)
            }
        })?;

        for (_, path) in &chunk_paths {
            let _ = tokio::fs::remove_file(path).await;
        }

        info!(
            stream_id = %group.stream_id,
            day = %group.day,
            chunks = group.chunks.len(),
            fingerprints = stats.fingerprints,
            resources = stats.resources,
            "Merged archive chunks"
        );
        Ok(group.chunks.len())
    }

    async fn query_group(
        &self,
        group: &DayGroup,
        merged_dir: &Path,
        olaf_home: &Path,
        recordings: &[PreparedRecording],
        minimum_match_count: u32,
        cancel: &CancellationToken,
    ) -> std::result::Result<Vec<QueryMatch>, PipelineError> {
        let config_dir = olaf_home.join(".olaf");
        tokio::fs::create_dir_all(&config_dir)
            .await
            .map_err(|error| {
                PipelineError::Other(anyhow::anyhow!("creating Olaf HOME: {error}"))
            })?;
        let db_dir = merged_dir.join("db");
        let cache_dir = merged_dir.join("cache");
        tokio::fs::create_dir_all(&cache_dir)
            .await
            .map_err(|error| {
                PipelineError::Other(anyhow::anyhow!("creating Olaf cache: {error}"))
            })?;
        let olaf_config = serde_json::json!({
            "db_folder": format!("{}/", db_dir.display()),
            "cache_folder": format!("{}/", cache_dir.display()),
            "check_incoming_audio": false,
            "target_sample_rate": self.config.olaf.sample_rate,
            "audio_step_size": self.config.olaf.step_size,
            "min_match_count": minimum_match_count.max(1),
            "max_results": self.config.query.max_results,
            "verbose": false,
        });
        tokio::fs::write(
            config_dir.join("olaf_config.json"),
            serde_json::to_vec_pretty(&olaf_config)
                .map_err(|error| PipelineError::Other(error.into()))?,
        )
        .await
        .map_err(|error| PipelineError::Other(anyhow::anyhow!("writing Olaf config: {error}")))?;

        let mut matches = Vec::new();
        for recording in recordings {
            check_cancel(cancel)?;
            let output = self.run_olaf_query(recording, olaf_home, cancel).await?;
            for query in output {
                for matched in query.matches {
                    let matched_at = group.day_start.timestamp_millis()
                        + (matched.reference_start * 1000.0).round() as i64;
                    let matched_end = group.day_start.timestamp_millis()
                        + (matched.reference_stop * 1000.0).round() as i64;
                    let duration = (matched.reference_stop - matched.reference_start).max(0.0);
                    let confidence = 1.0
                        - (-(matched.match_count as f64) / minimum_match_count.max(1) as f64).exp();
                    let chunk = group
                        .chunks
                        .iter()
                        .find(|chunk| canonical_chunk_path(chunk) == matched.path)
                        .cloned();
                    let provenance = Provenance {
                        chunk_ids: chunk
                            .as_ref()
                            .map(|value| vec![value.chunk_id.clone()])
                            .unwrap_or_default(),
                        archive_ids: chunk
                            .as_ref()
                            .map(|value| vec![value.archive_id.clone()])
                            .unwrap_or_default(),
                        node_ids: chunk
                            .as_ref()
                            .map(|value| vec![value.node_id.clone()])
                            .unwrap_or_else(|| group.node_ids.clone()),
                        days: vec![group.day.clone()],
                    };
                    let stream = Stream {
                        id: group.stream_id.clone(),
                        external_stream_id: group.external_stream_id.clone(),
                        node_ids: group.node_ids.clone(),
                    };
                    let recording_data = recording.recording.clone();
                    let metadata = recording.catalog.metadata.clone();
                    matches.push(QueryMatch {
                        catalog_item_id: recording.catalog.catalog_item_id.clone(),
                        recording_id: recording.recording.id.clone(),
                        recording_title: recording.recording.title.clone(),
                        archive_id: provenance.archive_ids.first().cloned().unwrap_or_default(),
                        stream_id: group.stream_id.clone(),
                        matched_at,
                        matched_at_iso: iso_from_millis(matched_at),
                        matched_end,
                        matched_end_iso: iso_from_millis(matched_end),
                        date: day_from_millis(matched_at),
                        time: time_from_millis(matched_at),
                        timezone: "UTC".to_string(),
                        duration_seconds: duration,
                        confidence,
                        match_count: matched.match_count,
                        query_start: matched.query_start,
                        query_stop: matched.query_stop,
                        reference_start: matched.reference_start,
                        reference_stop: matched.reference_stop,
                        recording: recording_data,
                        stream,
                        provenance,
                        metadata,
                    });
                }
            }
        }
        Ok(matches)
    }

    async fn run_olaf_query(
        &self,
        recording: &PreparedRecording,
        olaf_home: &Path,
        cancel: &CancellationToken,
    ) -> std::result::Result<Vec<OlafQueryOutput>, PipelineError> {
        let mut command = tokio::process::Command::new(&self.config.olaf.binary);
        command
            .arg("query")
            .arg("--format")
            .arg("json")
            .arg("--threads")
            .arg(self.config.olaf.threads.to_string())
            .arg(&recording.path)
            .env("HOME", olaf_home)
            .env("XDG_CACHE_HOME", olaf_home.join(".cache"))
            .current_dir(olaf_home)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let output = tokio::time::timeout(
            Duration::from_secs(self.config.olaf.query_timeout_seconds),
            command.output(),
        )
        .await
        .map_err(|_| {
            PipelineError::Other(anyhow::anyhow!(
                "Olaf query timed out after {} seconds",
                self.config.olaf.query_timeout_seconds
            ))
        })?
        .with_context(|| format!("running Olaf query for {}", recording.path.display()))
        .map_err(PipelineError::Other)?;
        if !output.status.success() {
            return Err(PipelineError::Other(anyhow::anyhow!(
                "Olaf query failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        if cancel.is_cancelled() {
            return Err(PipelineError::cancelled());
        }
        let values: Vec<OlafQueryOutput> = serde_json::Deserializer::from_slice(&output.stdout)
            .into_iter::<OlafQueryOutput>()
            .filter_map(|value| value.ok())
            .collect();
        if values.is_empty() && !output.stdout.iter().all(u8::is_ascii_whitespace) {
            return Err(PipelineError::Other(anyhow::anyhow!(
                "could not parse Olaf JSON output: {}",
                String::from_utf8_lossy(&output.stdout)
            )));
        }
        Ok(values)
    }
}

fn check_cancel(cancel: &CancellationToken) -> std::result::Result<(), PipelineError> {
    if cancel.is_cancelled() {
        Err(PipelineError::cancelled())
    } else {
        Ok(())
    }
}

fn safe_path_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn canonical_chunk_path(chunk: &ChunkRef) -> String {
    format!(
        "{}/{}/{}",
        chunk.stream_id,
        chunk.started_at.format("%Y-%m-%d"),
        chunk.chunk_id
    )
}

fn stream_filters(request: &QueryRequest) -> HashSet<String> {
    let mut values = HashSet::new();
    if let Some(value) = request.stream_id.as_ref().filter(|value| !value.is_empty()) {
        values.insert(value.clone());
    }
    if let Some(value) = request
        .external_stream_id
        .as_ref()
        .filter(|value| !value.is_empty())
    {
        values.insert(value.clone());
    }
    for value in &request.stream_ids {
        if !value.is_empty() {
            values.insert(value.clone());
        }
    }
    for archive in &request.archives {
        if !archive.stream_id.is_empty() {
            values.insert(archive.stream_id.clone());
        }
    }
    values
}

fn chunks_from_archive(
    archive: &ArchiveReference,
) -> std::result::Result<Vec<ChunkRef>, PipelineError> {
    let started = if archive.started_at > 0 {
        Utc.timestamp_millis_opt(archive.started_at)
            .single()
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
    } else {
        DateTime::<Utc>::UNIX_EPOCH
    };
    let mut chunks = Vec::new();
    for object in &archive.index_objects {
        if !object.object_key.ends_with("data.mdb") {
            continue;
        }
        chunks.push(ChunkRef {
            archive_id: archive.archive_id.clone(),
            node_id: archive
                .archive_id
                .split('/')
                .next()
                .unwrap_or("archive")
                .to_string(),
            stream_id: archive.stream_id.clone(),
            external_stream_id: None,
            chunk_id: archive
                .archive_id
                .rsplit('/')
                .next()
                .unwrap_or(&archive.archive_id)
                .to_string(),
            started_at: started,
            index_key: object.object_key.clone(),
        });
    }
    if chunks.is_empty() {
        return Err(PipelineError::Other(anyhow::anyhow!(
            "archive '{}' has no data.mdb index object",
            archive.archive_id
        )));
    }
    Ok(chunks)
}

fn group_chunks(chunks: Vec<ChunkRef>) -> Vec<DayGroup> {
    let mut groups: BTreeMap<(String, String), (DateTime<Utc>, Vec<ChunkRef>)> = BTreeMap::new();
    for chunk in chunks {
        let day = chunk.started_at.format("%Y-%m-%d").to_string();
        let day_start = Utc.from_utc_datetime(
            &chunk
                .started_at
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .expect("valid midnight"),
        );
        groups
            .entry((chunk.stream_id.clone(), day))
            .or_insert_with(|| (day_start, Vec::new()))
            .1
            .push(chunk);
    }
    groups
        .into_iter()
        .map(|((stream_id, day), (day_start, mut chunks))| {
            chunks.sort_by_key(|chunk| chunk.started_at);
            let external_stream_id = chunks
                .iter()
                .find_map(|chunk| chunk.external_stream_id.clone());
            let mut node_ids = chunks
                .iter()
                .map(|chunk| chunk.node_id.clone())
                .collect::<Vec<_>>();
            node_ids.sort();
            node_ids.dedup();
            DayGroup {
                stream_id,
                external_stream_id,
                node_ids,
                day,
                day_start,
                chunks,
            }
        })
        .collect()
}

fn merge_chunks(
    chunks: &[(ChunkRef, PathBuf)],
    target_dir: &Path,
    map_size: usize,
    day_start: DateTime<Utc>,
    step_seconds: f64,
    cancel: &CancellationToken,
) -> Result<MergeStats> {
    let target = Environment::new()
        .set_max_dbs(2)
        .set_map_size(map_size)
        .open(target_dir)
        .with_context(|| format!("opening merged LMDB {}", target_dir.display()))?;
    let target_fps = target
        .create_db(
            Some(OLAF_FINGERPRINTS),
            DatabaseFlags::INTEGER_KEY
                | DatabaseFlags::DUP_SORT
                | DatabaseFlags::DUP_FIXED
                | DatabaseFlags::INTEGER_DUP,
        )
        .context("creating merged fingerprint database")?;
    let target_resources = target
        .create_db(Some(OLAF_RESOURCE_MAP), DatabaseFlags::INTEGER_KEY)
        .context("creating merged resource database")?;

    let mut stats = MergeStats::default();
    for (chunk, chunk_path) in chunks {
        if cancel.is_cancelled() {
            anyhow::bail!("query cancelled");
        }
        let source = Environment::new()
            .set_max_dbs(2)
            .set_map_size(map_size)
            .set_flags(lmdb::EnvironmentFlags::READ_ONLY | lmdb::EnvironmentFlags::NO_TLS)
            .open(chunk_path)
            .with_context(|| format!("opening chunk LMDB {}", chunk_path.display()))?;
        let source_fps = source.open_db(Some(OLAF_FINGERPRINTS)).with_context(|| {
            format!("opening {} in {}", OLAF_FINGERPRINTS, chunk_path.display())
        })?;
        let source_resources = source.open_db(Some(OLAF_RESOURCE_MAP)).with_context(|| {
            format!("opening {} in {}", OLAF_RESOURCE_MAP, chunk_path.display())
        })?;

        let day_start_millis = day_start.timestamp_millis();
        let offset_steps = ((chunk.started_at.timestamp_millis() - day_start_millis) as f64
            / 1000.0
            / step_seconds)
            .round() as i64;
        let canonical_path = canonical_chunk_path(chunk);

        let read_txn = source
            .begin_ro_txn()
            .context("starting chunk read transaction")?;
        let mut cursor = read_txn
            .open_ro_cursor(source_fps)
            .context("opening chunk fingerprint cursor")?;
        let mut write_txn = target
            .begin_rw_txn()
            .context("starting merged write transaction")?;
        for (key, value) in cursor.iter() {
            if key.len() != 8 || value.len() != 8 {
                continue;
            }
            let old_value = u64::from_ne_bytes(value.try_into().expect("8-byte value"));
            let old_time = (old_value >> 32) as i64;
            let new_time = old_time + offset_steps;
            if !(0..=u32::MAX as i64).contains(&new_time) {
                continue;
            }
            let identifier = old_value as u32;
            let new_value = ((new_time as u64) << 32) | identifier as u64;
            match write_txn.put(
                target_fps,
                &key,
                &new_value.to_ne_bytes(),
                WriteFlags::NO_DUP_DATA,
            ) {
                Ok(()) => stats.fingerprints += 1,
                Err(lmdb::Error::KeyExist) => {}
                Err(error) => return Err(error).context("writing merged fingerprint"),
            }
        }

        let mut resource_cursor = read_txn
            .open_ro_cursor(source_resources)
            .context("opening chunk resource cursor")?;
        for (key, value) in resource_cursor.iter() {
            if key.len() != 4 || value.len() < RESOURCE_META_SIZE {
                continue;
            }
            let mut metadata = vec![0u8; RESOURCE_META_SIZE];
            metadata.copy_from_slice(&value[..RESOURCE_META_SIZE]);
            let path_bytes = canonical_path.as_bytes();
            let path_len = path_bytes.len().min(RESOURCE_PATH_SIZE - 1);
            metadata[RESOURCE_PATH_OFFSET..RESOURCE_PATH_OFFSET + RESOURCE_PATH_SIZE].fill(0);
            metadata[RESOURCE_PATH_OFFSET..RESOURCE_PATH_OFFSET + path_len]
                .copy_from_slice(&path_bytes[..path_len]);
            match write_txn.put(target_resources, &key, &metadata, WriteFlags::empty()) {
                Ok(()) => stats.resources += 1,
                Err(lmdb::Error::KeyExist) => {}
                Err(error) => return Err(error).context("writing merged resource metadata"),
            }
        }
        drop(cursor);
        drop(resource_cursor);
        read_txn.abort();
        write_txn.commit().context("committing merged chunk")?;
    }
    target.sync(true).context("syncing merged database")?;
    Ok(stats)
}

fn parse_range(
    request: &QueryRequest,
) -> std::result::Result<(DateTime<Utc>, DateTime<Utc>), PipelineError> {
    if let Some(month) = request.month.as_deref() {
        let start_date =
            NaiveDate::parse_from_str(&format!("{month}-01"), "%Y-%m-%d").map_err(|error| {
                PipelineError::Other(anyhow::anyhow!("invalid month '{month}': {error}"))
            })?;
        let start = Utc.from_utc_datetime(
            &start_date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| PipelineError::Other(anyhow::anyhow!("invalid month start")))?,
        );
        let next_month = if start_date.month() == 12 {
            NaiveDate::from_ymd_opt(start_date.year() + 1, 1, 1)
        } else {
            NaiveDate::from_ymd_opt(start_date.year(), start_date.month() + 1, 1)
        }
        .ok_or_else(|| PipelineError::Other(anyhow::anyhow!("invalid next month")))?;
        let end = Utc.from_utc_datetime(
            &next_month
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| PipelineError::Other(anyhow::anyhow!("invalid month end")))?,
        );
        return Ok((start, end));
    }

    let now = Utc::now();
    let start = match request.start.as_deref() {
        Some(value) => parse_time_bound(value, false)?,
        None => now - chrono::Duration::days(31),
    };
    let end = match request.end.as_deref() {
        Some(value) => parse_time_bound(value, true)?,
        None => now,
    };
    if end <= start {
        return Err(PipelineError::Other(anyhow::anyhow!(
            "end must be after start"
        )));
    }
    Ok((start, end))
}

fn parse_time_bound(
    value: &str,
    is_end: bool,
) -> std::result::Result<DateTime<Utc>, PipelineError> {
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let time = if is_end {
            date.and_hms_milli_opt(23, 59, 59, 999)
        } else {
            date.and_hms_opt(0, 0, 0)
        }
        .ok_or_else(|| PipelineError::Other(anyhow::anyhow!("invalid date '{value}'")))?;
        return Ok(Utc.from_utc_datetime(&time));
    }
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return Ok(value.with_timezone(&Utc));
    }
    if let Ok(millis) = value.parse::<i64>() {
        return Ok(Utc
            .timestamp_millis_opt(millis)
            .single()
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH));
    }
    Err(PipelineError::Other(anyhow::anyhow!(
        "invalid time '{value}', expected RFC3339, YYYY-MM-DD, or epoch milliseconds"
    )))
}

fn validate_recording_url(
    value: &str,
    allow_private: bool,
) -> std::result::Result<(), PipelineError> {
    let url = Url::parse(value)
        .map_err(|error| PipelineError::Other(anyhow::anyhow!("invalid audio_url: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(PipelineError::Other(anyhow::anyhow!(
            "audio_url must use http or https"
        )));
    }
    if allow_private {
        return Ok(());
    }
    let host = url
        .host_str()
        .ok_or_else(|| PipelineError::Other(anyhow::anyhow!("audio_url has no host")))?;
    let lower = host.to_ascii_lowercase();
    if lower == "localhost" || lower.ends_with(".localhost") {
        return Err(PipelineError::Other(anyhow::anyhow!(
            "audio_url host is not allowed"
        )));
    }
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        if private_address(ip) {
            return Err(PipelineError::Other(anyhow::anyhow!(
                "audio_url host is not allowed"
            )));
        }
    }
    Ok(())
}

fn private_address(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(value) => {
            value.is_loopback()
                || value.is_private()
                || value.is_link_local()
                || value.is_unspecified()
                || value.is_multicast()
                || value.is_broadcast()
        }
        std::net::IpAddr::V6(value) => {
            value.is_loopback()
                || value.is_unspecified()
                || value.is_multicast()
                || value.is_unique_local()
                || value.is_unicast_link_local()
        }
    }
}

fn audio_extension(url: &str, content_type: &str) -> String {
    const ALLOWED: &[&str] = &[
        "m4a", "wav", "mp4", "wv", "ape", "ogg", "mp3", "raw", "flac", "wma",
    ];
    if let Ok(url) = Url::parse(url) {
        if let Some(extension) = Path::new(url.path())
            .extension()
            .and_then(|value| value.to_str())
        {
            let extension = extension.to_ascii_lowercase();
            if ALLOWED.contains(&extension.as_str()) {
                return format!(".{extension}");
            }
        }
    }
    let content_type = content_type.to_ascii_lowercase();
    let extension = if content_type.contains("mpeg") {
        "mp3"
    } else if content_type.contains("mp4") || content_type.contains("m4a") {
        "m4a"
    } else if content_type.contains("ogg") || content_type.contains("opus") {
        "ogg"
    } else if content_type.contains("flac") {
        "flac"
    } else {
        "wav"
    };
    format!(".{extension}")
}

async fn probe_duration(path: &Path) -> Result<Option<f64>> {
    let output = tokio::process::Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("format=duration")
        .arg("-of")
        .arg("default=noprint_wrappers=1:nokey=1")
        .arg(path)
        .output()
        .await
        .context("running ffprobe")?;
    if !output.status.success() {
        return Ok(None);
    }
    let value = String::from_utf8_lossy(&output.stdout);
    Ok(value.trim().parse::<f64>().ok())
}

fn merge_adjacent_matches(mut matches: Vec<QueryMatch>, gap_seconds: f64) -> Vec<QueryMatch> {
    matches.sort_by(|left, right| {
        left.recording_id
            .cmp(&right.recording_id)
            .then(left.stream_id.cmp(&right.stream_id))
            .then(left.matched_at.cmp(&right.matched_at))
    });
    let gap_millis = (gap_seconds.max(0.0) * 1000.0).round() as i64;
    let mut result: Vec<QueryMatch> = Vec::with_capacity(matches.len());
    for item in matches {
        if let Some(previous) = result.last_mut() {
            if previous.recording_id == item.recording_id
                && previous.stream_id == item.stream_id
                && item.matched_at <= previous.matched_end + gap_millis
            {
                previous.matched_end = previous.matched_end.max(item.matched_end);
                previous.matched_end_iso = iso_from_millis(previous.matched_end);
                previous.duration_seconds =
                    ((previous.matched_end - previous.matched_at) as f64 / 1000.0).max(0.0);
                previous.match_count = previous.match_count.saturating_add(item.match_count);
                previous.reference_stop = previous.reference_stop.max(item.reference_stop);
                previous.query_stop = previous.query_stop.max(item.query_stop);
                merge_unique(
                    &mut previous.provenance.chunk_ids,
                    item.provenance.chunk_ids,
                );
                merge_unique(
                    &mut previous.provenance.archive_ids,
                    item.provenance.archive_ids,
                );
                merge_unique(&mut previous.provenance.node_ids, item.provenance.node_ids);
                merge_unique(&mut previous.provenance.days, item.provenance.days);
                continue;
            }
        }
        result.push(item);
    }
    result
}

fn merge_unique(target: &mut Vec<String>, values: Vec<String>) {
    for value in values {
        if !target.contains(&value) {
            target.push(value);
        }
    }
}

fn object_path(prefix: &str) -> Option<ObjectPath> {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(ObjectPath::from(trimmed))
    }
}

impl Storage {
    async fn new(config: &Config) -> Result<Self> {
        let storage = &config.storage;
        let bucket = storage.bucket.trim();
        let region = if storage.region.trim().is_empty() {
            "auto".to_string()
        } else {
            storage.region.trim().to_string()
        };
        let builder_bucket = if bucket.is_empty() {
            "unconfigured"
        } else {
            bucket
        };
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(builder_bucket)
            .with_region(region)
            .with_access_key_id(storage.access_key.trim())
            .with_secret_access_key(storage.secret_key.trim())
            .with_virtual_hosted_style_request(!storage.force_path_style);
        let endpoint = storage.endpoint.trim();
        if !endpoint.is_empty() {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"));
        }
        let store = builder.build().context("configuring S3 storage")?;
        Ok(Self {
            store: Arc::new(store),
            bucket: bucket.to_string(),
            prefix: storage.archive_prefix.trim().trim_matches('/').to_string(),
        })
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let path = object_path(prefix);
        let result = self
            .store
            .list_with_delimiter(path.as_ref())
            .await
            .with_context(|| format!("listing prefixes under {prefix}"))?;
        Ok(result
            .common_prefixes
            .into_iter()
            .map(|value| format!("{value}/"))
            .collect())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let path = object_path(prefix);
        let mut objects = self.store.list(path.as_ref());
        let mut result = Vec::new();
        while let Some(item) = objects.next().await {
            let meta = item.with_context(|| format!("listing objects under {prefix}"))?;
            result.push(meta.location.to_string());
        }
        Ok(result)
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        let path = ObjectPath::from(key.trim_start_matches('/'));
        let result = self
            .store
            .get_opts(&path, Default::default())
            .await
            .with_context(|| format!("downloading s3://{}/{}", self.bucket, key))?;
        let bytes = result
            .bytes()
            .await
            .with_context(|| format!("reading s3://{}/{}", self.bucket, key))?;
        Ok(bytes.to_vec())
    }

    async fn download_object(&self, key: &str, path: &Path) -> Result<()> {
        let object = ObjectPath::from(key.trim_start_matches('/'));
        let result = self
            .store
            .get_opts(&object, Default::default())
            .await
            .with_context(|| format!("downloading s3://{}/{}", self.bucket, key))?;
        let mut stream = result.into_stream();
        let mut file = tokio::fs::File::create(path)
            .await
            .with_context(|| format!("creating {}", path.display()))?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.with_context(|| format!("reading s3://{}/{}", self.bucket, key))?;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        Ok(())
    }

    async fn discover(
        &self,
        range_start: DateTime<Utc>,
        range_end: DateTime<Utc>,
        request: &QueryRequest,
        cancel: &CancellationToken,
    ) -> Result<Vec<ChunkRef>> {
        let filters = stream_filters(request);
        let base = self.prefix.clone();
        let node_prefix = if base.is_empty() {
            String::new()
        } else {
            format!("{base}/")
        };
        let mut nodes = self
            .list_prefixes(&node_prefix)
            .await?
            .into_iter()
            .filter_map(|prefix| {
                prefix
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .map(str::to_string)
            })
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        nodes.sort();
        nodes.dedup();

        if nodes.is_empty() {
            return self
                .discover_from_objects(&node_prefix, range_start, range_end, &filters, cancel)
                .await;
        }

        let mut chunks = Vec::new();
        for node in nodes {
            if cancel.is_cancelled() {
                anyhow::bail!("query cancelled");
            }
            let manifest_prefix = if base.is_empty() {
                format!("{node}/manifests/")
            } else {
                format!("{base}/{node}/manifests/")
            };
            let days = self.list_prefixes(&manifest_prefix).await?;
            if days.is_empty() {
                chunks.extend(
                    self.discover_from_objects(
                        &manifest_prefix,
                        range_start,
                        range_end,
                        &filters,
                        cancel,
                    )
                    .await?,
                );
                continue;
            }
            for day_prefix in days {
                if cancel.is_cancelled() {
                    anyhow::bail!("query cancelled");
                }
                let day = day_prefix
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or_default();
                if !day_in_range(day, range_start, range_end) {
                    continue;
                }
                for key in self.list_objects(&day_prefix).await? {
                    if !key.ends_with(".json") {
                        continue;
                    }
                    if let Some(chunk) = self
                        .load_manifest(&key, range_start, range_end, &filters)
                        .await?
                    {
                        chunks.push(chunk);
                    }
                }
            }
        }
        Ok(chunks)
    }

    async fn discover_from_objects(
        &self,
        prefix: &str,
        range_start: DateTime<Utc>,
        range_end: DateTime<Utc>,
        filters: &HashSet<String>,
        cancel: &CancellationToken,
    ) -> Result<Vec<ChunkRef>> {
        let mut chunks = Vec::new();
        for key in self.list_objects(prefix).await? {
            if cancel.is_cancelled() {
                anyhow::bail!("query cancelled");
            }
            if !key.ends_with(".json") || !key.contains("/manifests/") {
                continue;
            }
            if let Some(chunk) = self
                .load_manifest(&key, range_start, range_end, filters)
                .await?
            {
                chunks.push(chunk);
            }
        }
        Ok(chunks)
    }

    async fn load_manifest(
        &self,
        key: &str,
        range_start: DateTime<Utc>,
        range_end: DateTime<Utc>,
        filters: &HashSet<String>,
    ) -> Result<Option<ChunkRef>> {
        let Some(day) = manifest_day_from_key(key) else {
            return Ok(None);
        };
        if !day_in_range(&day, range_start, range_end) {
            return Ok(None);
        }
        let bytes = self.get_bytes(key).await?;
        let manifest: Manifest =
            serde_json::from_slice(&bytes).with_context(|| format!("parsing manifest {key}"))?;
        if manifest.status != "complete" && !manifest.status.is_empty() {
            return Ok(None);
        }
        if !filters.is_empty()
            && !filters.contains(&manifest.stream_id)
            && !manifest
                .external_stream_id
                .as_deref()
                .map(|value| filters.contains(value))
                .unwrap_or(false)
        {
            return Ok(None);
        }
        let started_at = DateTime::parse_from_rfc3339(&manifest.started_at)
            .map(|value| value.with_timezone(&Utc))
            .with_context(|| format!("invalid started_at in manifest {key}"))?;
        if started_at < range_start || started_at >= range_end {
            return Ok(None);
        }
        let index_key = manifest
            .index_objects
            .iter()
            .find(|object| object.format == "olaf-lmdb" && object.object_key.ends_with("data.mdb"))
            .or_else(|| {
                manifest
                    .index_objects
                    .iter()
                    .find(|object| object.object_key.ends_with("data.mdb"))
            })
            .map(|object| object.object_key.clone());
        let Some(index_key) = index_key else {
            warn!(manifest = %key, "Manifest has no Olaf data.mdb index object");
            return Ok(None);
        };
        Ok(Some(ChunkRef {
            archive_id: key.to_string(),
            node_id: manifest.node_id,
            stream_id: manifest.stream_id,
            external_stream_id: manifest.external_stream_id,
            chunk_id: manifest.chunk_id,
            started_at,
            index_key,
        }))
    }
}

fn manifest_day_from_key(key: &str) -> Option<String> {
    let parts: Vec<&str> = key.split('/').collect();
    let index = parts.iter().position(|part| *part == "manifests")?;
    parts.get(index + 1).map(|value| value.to_string())
}

fn day_in_range(day: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
    let Ok(date) = NaiveDate::parse_from_str(day, "%Y-%m-%d") else {
        return false;
    };
    let day_start = Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap());
    let next_day = day_start + chrono::Duration::days(1);
    day_start < end && next_day > start
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::JobStatus;
    use lmdb::EnvironmentFlags;

    #[test]
    fn parses_month_range() {
        let request = QueryRequest {
            contract_version: "2".to_string(),
            request_id: "test".to_string(),
            stream_id: Some("stream".to_string()),
            external_stream_id: None,
            stream_ids: Vec::new(),
            start: None,
            end: None,
            month: Some("2026-11".to_string()),
            recording_id: None,
            title: None,
            audio_url: Some("https://example.com/a.mp3".to_string()),
            recording_metadata: None,
            catalog: Vec::new(),
            archives: Vec::new(),
            options: Default::default(),
            webhook_url: None,
            webhook_headers: Default::default(),
        };
        let (start, end) = parse_range(&request).unwrap();
        assert_eq!(start.to_rfc3339(), "2026-11-01T00:00:00+00:00");
        assert_eq!(end.to_rfc3339(), "2026-12-01T00:00:00+00:00");
    }

    #[test]
    fn groups_and_orders_chunks() {
        let chunk = ChunkRef {
            archive_id: "a".into(),
            node_id: "n".into(),
            stream_id: "s".into(),
            external_stream_id: None,
            chunk_id: "c".into(),
            started_at: Utc.with_ymd_and_hms(2026, 11, 11, 10, 0, 0).unwrap(),
            index_key: "a/index/db/data.mdb".into(),
        };
        let groups = group_chunks(vec![chunk]);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].day, "2026-11-11");
    }

    #[test]
    fn merges_overlapping_matches() {
        let mut first = test_match(1000, 5000, 10);
        first.provenance.chunk_ids = vec!["a".into()];
        let mut second = test_match(4500, 8000, 20);
        second.provenance.chunk_ids = vec!["b".into()];
        let merged = merge_adjacent_matches(vec![first, second], 2.0);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].matched_end, 8000);
        assert_eq!(merged[0].match_count, 30);
        assert_eq!(merged[0].provenance.chunk_ids, vec!["a", "b"]);
    }

    #[test]
    fn merges_lmdb_chunks_with_day_offset_and_canonical_paths() {
        let temp = tempfile::tempdir().unwrap();
        let chunk_dir = temp.path().join("chunk");
        std::fs::create_dir_all(&chunk_dir).unwrap();
        let day_start = Utc.with_ymd_and_hms(2026, 11, 11, 0, 0, 0).unwrap();
        let chunk_start = Utc.with_ymd_and_hms(2026, 11, 11, 0, 0, 10).unwrap();

        let chunk_env = Environment::new()
            .set_max_dbs(2)
            .set_map_size(1024 * 1024)
            .open(&chunk_dir)
            .unwrap();
        let fps = chunk_env
            .create_db(
                Some(OLAF_FINGERPRINTS),
                DatabaseFlags::INTEGER_KEY
                    | DatabaseFlags::DUP_SORT
                    | DatabaseFlags::DUP_FIXED
                    | DatabaseFlags::INTEGER_DUP,
            )
            .unwrap();
        let resources = chunk_env
            .create_db(Some(OLAF_RESOURCE_MAP), DatabaseFlags::INTEGER_KEY)
            .unwrap();

        let old_time: u32 = 500;
        let identifier: u32 = 42;
        let old_value = ((old_time as u64) << 32) | identifier as u64;
        let key = 7u64.to_ne_bytes();
        {
            let mut txn = chunk_env.begin_rw_txn().unwrap();
            txn.put(fps, &key, &old_value.to_ne_bytes(), WriteFlags::empty())
                .unwrap();
            let mut metadata = vec![0u8; RESOURCE_META_SIZE];
            metadata[0..4].copy_from_slice(&60.0f32.to_ne_bytes());
            metadata[4..12].copy_from_slice(&123i64.to_ne_bytes());
            let path = b"/old/chunk/audio.ogg";
            metadata[RESOURCE_PATH_OFFSET..RESOURCE_PATH_OFFSET + path.len()].copy_from_slice(path);
            txn.put(
                resources,
                &identifier.to_ne_bytes(),
                &metadata,
                WriteFlags::empty(),
            )
            .unwrap();
            txn.commit().unwrap();
        }
        drop(chunk_env);

        let chunk = ChunkRef {
            archive_id: "node/2026-11-11/chunk-a".into(),
            node_id: "node".into(),
            stream_id: "stream".into(),
            external_stream_id: None,
            chunk_id: "chunk-a".into(),
            started_at: chunk_start,
            index_key: "node/2026-11-11/chunk-a/index/db/data.mdb".into(),
        };
        let target_dir = temp.path().join("merged");
        std::fs::create_dir_all(&target_dir).unwrap();
        let stats = merge_chunks(
            &[(chunk.clone(), chunk_dir.clone())],
            &target_dir,
            1024 * 1024,
            day_start,
            128.0 / 16_000.0,
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(stats.fingerprints, 1);
        assert_eq!(stats.resources, 1);

        let merged = Environment::new()
            .set_max_dbs(2)
            .set_map_size(1024 * 1024)
            .set_flags(EnvironmentFlags::READ_ONLY | EnvironmentFlags::NO_TLS)
            .open(&target_dir)
            .unwrap();
        let merged_fps = merged.open_db(Some(OLAF_FINGERPRINTS)).unwrap();
        let merged_resources = merged.open_db(Some(OLAF_RESOURCE_MAP)).unwrap();
        let txn = merged.begin_ro_txn().unwrap();
        let value = txn.get(merged_fps, &key).unwrap();
        let merged_value = u64::from_ne_bytes(value.try_into().unwrap());
        assert_eq!((merged_value >> 32) as u32, old_time + 1250);
        assert_eq!(merged_value as u32, identifier);

        let metadata = txn
            .get(merged_resources, &identifier.to_ne_bytes())
            .unwrap();
        let path_bytes = &metadata[RESOURCE_PATH_OFFSET..RESOURCE_PATH_OFFSET + RESOURCE_PATH_SIZE];
        let path_end = path_bytes
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(path_bytes.len());
        let path = std::str::from_utf8(&path_bytes[..path_end]).unwrap();
        assert_eq!(path, canonical_chunk_path(&chunk));
    }

    fn test_match(start: i64, end: i64, count: u32) -> QueryMatch {
        QueryMatch {
            catalog_item_id: "cat".into(),
            recording_id: "rec".into(),
            recording_title: "Title".into(),
            archive_id: "archive".into(),
            stream_id: "stream".into(),
            matched_at: start,
            matched_at_iso: iso_from_millis(start),
            matched_end: end,
            matched_end_iso: iso_from_millis(end),
            date: day_from_millis(start),
            time: time_from_millis(start),
            timezone: "UTC".into(),
            duration_seconds: (end - start) as f64 / 1000.0,
            confidence: 0.5,
            match_count: count,
            query_start: 0.0,
            query_stop: 1.0,
            reference_start: 0.0,
            reference_stop: 1.0,
            recording: Recording::default(),
            stream: Stream::default(),
            provenance: Provenance::default(),
            metadata: serde_json::Value::Null,
        }
    }

    #[test]
    fn rejects_private_urls() {
        assert!(validate_recording_url("http://127.0.0.1/a.mp3", false).is_err());
        assert!(validate_recording_url("http://localhost/a.mp3", false).is_err());
        assert!(validate_recording_url("https://example.com/a.mp3", false).is_ok());
    }

    #[test]
    fn job_status_round_trip() {
        assert_eq!(JobStatus::from_str("running"), JobStatus::Running);
        assert_eq!(JobStatus::Cancelled.as_str(), "cancelled");
    }
}
