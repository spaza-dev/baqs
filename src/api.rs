use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use utoipa::openapi::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use crate::Config;
use crate::jobs::Db;
use crate::pipeline::Pipeline;
use crate::types::{
    JobError, JobProgress, JobStatusResponse, JobSubmissionResponse, QUERY_CONTRACT_VERSION,
    QueryRequest, QueryResult, utc_now_millis,
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: Db,
    pub pipeline: Pipeline,
    pub tokens: Arc<Mutex<HashMap<String, CancellationToken>>>,
    pub started_at: Instant,
    pub workers: usize,
}

impl AppState {
    pub async fn token_for(&self, job_id: &str) -> CancellationToken {
        let mut tokens = self.tokens.lock().await;
        tokens
            .entry(job_id.to_string())
            .or_insert_with(CancellationToken::new)
            .clone()
    }

    pub async fn insert_token(&self, job_id: &str, token: CancellationToken) {
        self.tokens.lock().await.insert(job_id.to_string(), token);
    }

    pub async fn remove_token(&self, job_id: &str) {
        self.tokens.lock().await.remove(job_id);
    }

    async fn cancel_token(&self, job_id: &str) {
        if let Some(token) = self.tokens.lock().await.get(job_id).cloned() {
            token.cancel();
        }
    }
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "bad_request",
            message: message.into(),
        }
    }

    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: "missing or invalid bearer token".to_string(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: message.into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "conflict",
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": {
                    "code": self.code,
                    "message": self.message,
                }
            })),
        )
            .into_response()
    }
}

pub fn router(state: AppState) -> Router {
    let document = openapi_document(&state.config.server.public_base_url);
    let swagger = SwaggerUi::new("/docs").url("/openapi.json", document);
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/version", get(version))
        .route("/v1/search", post(submit_search))
        .route("/v1/query", post(submit_query))
        .route("/v1/jobs/{id}", get(get_job))
        .route("/v1/jobs/{id}/result", get(get_result))
        .route("/v1/jobs/{id}/cancel", post(cancel_job))
        .merge(swagger)
        .layer(axum::extract::DefaultBodyLimit::max(
            state.config.server.max_body_bytes,
        ))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn healthz(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize_health(&state, &headers)?;
    Ok(Json(json!({
        "status": "ok",
        "service": "Broadcast Archive Query Service",
        "short_name": "BAQS",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": state.started_at.elapsed().as_secs(),
    })))
}

async fn readyz(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(error) = authorize_health(&state, &headers) {
        return error.into_response();
    }
    let mut checks = serde_json::Map::new();
    let mut healthy = true;

    match state.db.pending_job_count().await {
        Ok(_) => {
            checks.insert("sqlite".to_string(), json!("ok"));
        }
        Err(error) => {
            healthy = false;
            checks.insert("sqlite".to_string(), json!(format!("error: {error}")));
        }
    }

    let data_dir = state.config.paths.data_dir.clone();
    match tokio::fs::create_dir_all(&data_dir).await {
        Ok(()) => {
            checks.insert("data_dir_writable".to_string(), json!("ok"));
        }
        Err(error) => {
            healthy = false;
            checks.insert(
                "data_dir_writable".to_string(),
                json!(format!("error: {error}")),
            );
        }
    }

    if Path::new(&state.config.olaf.binary).is_file() {
        checks.insert("olaf_binary".to_string(), json!("ok"));
    } else {
        healthy = false;
        checks.insert("olaf_binary".to_string(), json!("missing"));
    }

    for binary in ["ffmpeg", "ffprobe"] {
        if binary_exists(binary) {
            checks.insert(binary.to_string(), json!("ok"));
        } else {
            healthy = false;
            checks.insert(binary.to_string(), json!("missing"));
        }
    }

    let storage_ok = !state.config.storage.bucket.trim().is_empty()
        && !state.config.storage.access_key.trim().is_empty()
        && !state.config.storage.secret_key.trim().is_empty();
    checks.insert(
        "storage".to_string(),
        json!(if storage_ok { "ok" } else { "not configured" }),
    );
    healthy &= storage_ok;
    checks.insert("workers".to_string(), json!(state.workers));
    healthy &= state.workers > 0;

    let body = json!({
        "status": if healthy { "ok" } else { "error" },
        "checks": checks,
    });
    if healthy {
        (StatusCode::OK, Json(body)).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
    }
}

async fn version() -> Json<Value> {
    Json(json!({
        "service": "Broadcast Archive Query Service",
        "short_name": "BAQS",
        "version": env!("CARGO_PKG_VERSION"),
        "contract_version": QUERY_CONTRACT_VERSION,
    }))
}

async fn submit_search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut request): Json<QueryRequest>,
) -> Result<(StatusCode, Json<JobSubmissionResponse>), ApiError> {
    authorize(&state, &headers)?;
    if request.contract_version.trim().is_empty() {
        request.contract_version = QUERY_CONTRACT_VERSION.to_string();
    }
    submit(state, request, "search").await
}

async fn submit_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut request): Json<QueryRequest>,
) -> Result<(StatusCode, Json<JobSubmissionResponse>), ApiError> {
    authorize(&state, &headers)?;
    if request.contract_version.trim().is_empty() {
        request.contract_version = QUERY_CONTRACT_VERSION.to_string();
    }
    submit(state, request, "query").await
}

async fn submit(
    state: AppState,
    request: QueryRequest,
    kind: &str,
) -> Result<(StatusCode, Json<JobSubmissionResponse>), ApiError> {
    request.validate().map_err(ApiError::bad_request)?;
    let job_id = Uuid::new_v4().to_string();
    state
        .db
        .insert_job(&request, kind, &job_id)
        .await
        .map_err(|error| ApiError::internal(format!("persisting job: {error}")))?;
    state.insert_token(&job_id, CancellationToken::new()).await;
    let base = state
        .config
        .server
        .public_base_url
        .trim_end_matches('/')
        .to_string();
    Ok((
        StatusCode::ACCEPTED,
        Json(JobSubmissionResponse {
            contract_version: request.contract_version,
            request_id: request.request_id,
            job_id: job_id.clone(),
            status: "accepted".to_string(),
            status_url: format!("{base}/v1/jobs/{job_id}"),
            result_url: format!("{base}/v1/jobs/{job_id}/result"),
        }),
    ))
}

async fn get_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<JobStatusResponse>, ApiError> {
    authorize(&state, &headers)?;
    let job = state
        .db
        .get_job(&id)
        .await
        .map_err(|error| ApiError::internal(format!("loading job: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("job '{id}' not found")))?;
    let error = job.error_code.as_ref().map(|code| JobError {
        code: code.clone(),
        message: job.error_message.clone().unwrap_or_default(),
    });
    let progress = job.progress();
    Ok(Json(JobStatusResponse {
        contract_version: job.contract_version,
        job_id: job.id.clone(),
        request_id: job.request_id.clone(),
        kind: job.kind.clone(),
        status: job.status.as_str().to_string(),
        progress,
        submitted_at: job.submitted_at,
        started_at: job.started_at,
        completed_at: job.completed_at,
        cancel_requested: job.cancel_requested,
        result_available: job.result_json.is_some(),
        webhook_status: job.webhook_status,
        error,
    }))
}

async fn get_result(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Response, ApiError> {
    authorize(&state, &headers)?;
    let (status, result, error) = state
        .db
        .get_result(&id)
        .await
        .map_err(|error| ApiError::internal(format!("loading result: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("job '{id}' not found")))?;
    if status == "completed" {
        let value: Value = result
            .as_deref()
            .and_then(|value| serde_json::from_str(value).ok())
            .ok_or_else(|| ApiError::internal("completed job has no valid result"))?;
        return Ok(Json(value).into_response());
    }
    if status == "failed" {
        let error = error.unwrap_or(JobError {
            code: "failed".to_string(),
            message: "job failed".to_string(),
        });
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            code: "job_failed",
            message: format!("{}: {}", error.code, error.message),
        });
    }
    if status == "cancelled" {
        return Err(ApiError::conflict("job was cancelled"));
    }
    Err(ApiError::conflict(format!(
        "job is not complete; current status is {status}"
    )))
}

async fn cancel_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let job = state
        .db
        .get_job(&id)
        .await
        .map_err(|error| ApiError::internal(format!("loading job: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("job '{id}' not found")))?;
    if matches!(
        job.status,
        crate::types::JobStatus::Completed
            | crate::types::JobStatus::Failed
            | crate::types::JobStatus::Cancelled
    ) {
        return Err(ApiError::conflict(format!(
            "job is already {}",
            job.status.as_str()
        )));
    }
    state
        .db
        .cancel_job(&id, "cancelled by API request")
        .await
        .map_err(|error| ApiError::internal(format!("cancelling job: {error}")))?;
    state.cancel_token(&id).await;
    Ok(Json(json!({
        "job_id": id,
        "status": "cancelling",
        "cancel_requested": true,
    })))
}

fn openapi_document(base_url: &str) -> OpenApi {
    let value = json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Broadcast Archive Query Service (BAQS)",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Asynchronous broadcast archive fingerprint search and query service."
        },
        "servers": [{ "url": base_url.trim_end_matches('/') }],
        "paths": {
            "/v1/search": {
                "post": {
                    "summary": "Submit an asynchronous archive search",
                    "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/QueryRequest" } } } },
                    "responses": { "202": { "description": "Job accepted" }, "400": { "description": "Invalid request" }, "401": { "description": "Unauthorized" } }
                }
            },
            "/v1/query": {
                "post": {
                    "summary": "Submit an asynchronous platform query (contract v2)",
                    "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/QueryRequest" } } } },
                    "responses": { "202": { "description": "Job accepted" }, "400": { "description": "Invalid request" }, "401": { "description": "Unauthorized" } }
                }
            },
            "/v1/jobs/{id}": {
                "get": {
                    "summary": "Get job status",
                    "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Job status" }, "404": { "description": "Not found" } }
                }
            },
            "/v1/jobs/{id}/result": {
                "get": {
                    "summary": "Get completed query result",
                    "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Query result" }, "409": { "description": "Not ready or failed" } }
                }
            },
            "/v1/jobs/{id}/cancel": {
                "post": {
                    "summary": "Cancel a job",
                    "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Cancellation requested" }, "409": { "description": "Already finished" } }
                }
            },
            "/healthz": { "get": { "summary": "Liveness", "responses": { "200": { "description": "OK" } } } },
            "/readyz": { "get": { "summary": "Readiness", "responses": { "200": { "description": "Ready" }, "503": { "description": "Not ready" } } } }
        },
        "components": {
            "schemas": {
                "QueryRequest": { "type": "object" },
                "CatalogReference": { "type": "object" },
                "ArchiveReference": { "type": "object" },
                "QueryOptions": { "type": "object" }
            }
        }
    });
    serde_json::from_value(value).expect("valid OpenAPI document")
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let expected = state.config.server.api_token.trim();
    if expected.is_empty() {
        return Ok(());
    }
    let provided = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .or_else(|| {
            headers
                .get("x-api-key")
                .and_then(|value| value.to_str().ok())
        });
    match provided {
        Some(value) if constant_time_eq(value.as_bytes(), expected.as_bytes()) => Ok(()),
        _ => Err(ApiError::unauthorized()),
    }
}

fn authorize_health(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    if state.config.server.health_public {
        Ok(())
    } else {
        authorize(state, headers)
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0u8;
    for (left, right) in left.iter().zip(right) {
        difference |= left ^ right;
    }
    difference == 0
}

fn binary_exists(name: &str) -> bool {
    if Path::new(name).is_file() {
        return true;
    }
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|directory| directory.join(name).is_file()))
        .unwrap_or(false)
}

#[allow(dead_code)]
fn now() -> i64 {
    utc_now_millis()
}

#[allow(dead_code)]
fn empty_progress() -> JobProgress {
    JobProgress::default()
}

#[allow(dead_code)]
fn empty_result() -> Option<QueryResult> {
    None
}
