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
use utoipa::openapi::OpenApi as OpenApiDocument;
use utoipa::openapi::Server;
use utoipa::openapi::security::{ApiKey, ApiKeyValue, HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use crate::jobs::Db;
use crate::pipeline::Pipeline;
use crate::types::{
    ArchiveReference, CatalogReference, IndexObjectReference, JobError, JobProgress,
    JobStatusResponse, JobSubmissionResponse, Provenance, QUERY_CONTRACT_VERSION, QueryMatch,
    QueryOptions, QueryRequest, QueryResult, QuerySummary, Recording, Stream, utc_now_millis,
};
use crate::{Config, PUBLIC_BASE_URL};

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

#[derive(ToSchema)]
#[allow(dead_code)]
struct ErrorDetail {
    code: String,
    message: String,
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct ErrorResponse {
    error: ErrorDetail,
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct HealthResponse {
    status: String,
    service: String,
    short_name: String,
    version: String,
    uptime_seconds: u64,
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct ReadinessResponse {
    status: String,
    #[schema(value_type = Object)]
    checks: HashMap<String, Value>,
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct VersionResponse {
    service: String,
    short_name: String,
    version: String,
    contract_version: String,
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct CancelResponse {
    job_id: String,
    status: String,
    cancel_requested: bool,
}

struct SecuritySchemes;

impl Modify for SecuritySchemes {
    fn modify(&self, openapi: &mut OpenApiDocument) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer_auth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("API token")
                    .build(),
            ),
        );
        components.add_security_scheme(
            "api_key",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "x-api-key",
                "BAQS API token supplied as an x-api-key header",
            ))),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Broadcast Archive Query Service (BAQS)",
        version = env!("CARGO_PKG_VERSION"),
        description = "Asynchronous broadcast archive fingerprint search and query service.\n\nAuthentication: send `Authorization: Bearer <token>` or `x-api-key: <token>` on all `/v1/*` endpoints. Health endpoints are public by default. `GET /docs` and `GET /openapi.json` are public."
    ),
    paths(
        submit_query,
        submit_search,
        get_job,
        get_result,
        cancel_job,
        healthz,
        readyz,
        version
    ),
    components(schemas(
        QueryRequest,
        CatalogReference,
        IndexObjectReference,
        ArchiveReference,
        QueryOptions,
        JobSubmissionResponse,
        JobStatusResponse,
        JobProgress,
        JobError,
        QueryResult,
        QueryMatch,
        QuerySummary,
        Recording,
        Stream,
        Provenance,
        ErrorDetail,
        ErrorResponse,
        HealthResponse,
        ReadinessResponse,
        VersionResponse,
        CancelResponse
    )),
    modifiers(&SecuritySchemes)
)]
struct ApiDoc;

pub fn router(state: AppState) -> Router {
    let document = openapi_document();

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

#[utoipa::path(
    get,
    path = "/healthz",
    tag = "health",
    summary = "Process liveness",
    description = "Public by default. Requires the API token when `server.health_public` is false.",
    responses(
        (status = 200, description = "Service is alive", body = HealthResponse),
        (status = 401, description = "Health checks are protected and the token is missing or invalid", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/readyz",
    tag = "health",
    summary = "Readiness checks",
    description = "Public by default. Requires the API token when `server.health_public` is false.",
    responses(
        (status = 200, description = "All readiness checks passed", body = ReadinessResponse),
        (status = 401, description = "Health checks are protected and the token is missing or invalid", body = ErrorResponse),
        (status = 503, description = "One or more readiness checks failed", body = ReadinessResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/version",
    tag = "health",
    summary = "Service and contract version",
    responses(
        (status = 200, description = "Version information", body = VersionResponse)
    )
)]
async fn version() -> Json<Value> {
    Json(json!({
        "service": "Broadcast Archive Query Service",
        "short_name": "BAQS",
        "version": env!("CARGO_PKG_VERSION"),
        "contract_version": QUERY_CONTRACT_VERSION,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/search",
    tag = "query",
    summary = "Submit an asynchronous archive search",
    description = "Alias of `POST /v1/query`. Creates a background job with kind `search` and returns immediately.",
    request_body(
        content = QueryRequest,
        description = "Search request. Supply a stream selector plus either `month` or `start`/`end`, and either `catalog[].audio_url` or a top-level `audio_url`.",
        example = json!({
            "contract_version": "2",
            "request_id": "sample-search-001",
            "stream_id": "stream-1",
            "month": "2026-11",
            "recording_id": "rec-1",
            "title": "Example recording",
            "audio_url": "https://example.com/recording.mp3",
            "options": {
                "minimum_confidence": 0.5,
                "minimum_match_count": 6,
                "max_results": 100,
                "include_recording": true
            }
        })
    ),
    responses(
        (status = 202, description = "Job accepted", body = JobSubmissionResponse,
            example = json!({
                "contract_version": "2",
                "request_id": "sample-search-001",
                "job_id": "8f2b2f0e-4c8b-4d46-9f1c-4f4f1a5f2a10",
                "status": "accepted",
                "status_url": "https://baqs.spaza.co.ke/v1/jobs/8f2b2f0e-4c8b-4d46-9f1c-4f4f1a5f2a10",
                "result_url": "https://baqs.spaza.co.ke/v1/jobs/8f2b2f0e-4c8b-4d46-9f1c-4f4f1a5f2a10/result"
            })),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Missing or invalid API token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
    security(("bearer_auth" = []), ("api_key" = []))
)]
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

#[utoipa::path(
    post,
    path = "/v1/query",
    tag = "query",
    summary = "Submit an asynchronous platform query (contract v2)",
    description = "Creates a background job that discovers archive chunks, merges Olaf indexes, and queries them. Poll `status_url` or fetch `result_url` when complete.",
    request_body(
        content = QueryRequest,
        description = "Query request. `contract_version` must be `2`. Supply a stream selector plus either `month` or `start`/`end`, and either `catalog[].audio_url` or a top-level `audio_url`.",
        example = json!({
            "contract_version": "2",
            "request_id": "sample-query-001",
            "stream_ids": ["stream-1"],
            "start": "2026-11-01T00:00:00Z",
            "end": "2026-11-02T00:00:00Z",
            "catalog": [
                {
                    "catalog_item_id": "cat-1",
                    "name": "Example track",
                    "type": "music",
                    "metadata": {},
                    "fingerprint_reference": null,
                    "audio_url": "https://example.com/track.mp3"
                }
            ],
            "options": {
                "minimum_confidence": 0.5,
                "minimum_match_count": 6,
                "max_results": 100,
                "merge_gap_seconds": 2.0,
                "max_chunks": 100000,
                "include_recording": true
            }
        })
    ),
    responses(
        (status = 202, description = "Job accepted", body = JobSubmissionResponse,
            example = json!({
                "contract_version": "2",
                "request_id": "sample-query-001",
                "job_id": "8f2b2f0e-4c8b-4d46-9f1c-4f4f1a5f2a10",
                "status": "accepted",
                "status_url": "https://baqs.spaza.co.ke/v1/jobs/8f2b2f0e-4c8b-4d46-9f1c-4f4f1a5f2a10",
                "result_url": "https://baqs.spaza.co.ke/v1/jobs/8f2b2f0e-4c8b-4d46-9f1c-4f4f1a5f2a10/result"
            })),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Missing or invalid API token", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
    security(("bearer_auth" = []), ("api_key" = []))
)]
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

#[utoipa::path(
    get,
    path = "/v1/jobs/{id}",
    tag = "jobs",
    summary = "Get job status",
    params(("id" = String, Path, description = "Job identifier returned by the submit endpoints")),
    responses(
        (status = 200, description = "Current job status", body = JobStatusResponse),
        (status = 401, description = "Missing or invalid API token", body = ErrorResponse),
        (status = 404, description = "Job not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
    security(("bearer_auth" = []), ("api_key" = []))
)]
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

#[utoipa::path(
    get,
    path = "/v1/jobs/{id}/result",
    tag = "jobs",
    summary = "Get completed query result",
    params(("id" = String, Path, description = "Job identifier returned by the submit endpoints")),
    responses(
        (status = 200, description = "Completed query result", body = QueryResult),
        (status = 401, description = "Missing or invalid API token", body = ErrorResponse),
        (status = 404, description = "Job not found", body = ErrorResponse),
        (status = 409, description = "Job is not complete, failed, or cancelled", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
    security(("bearer_auth" = []), ("api_key" = []))
)]
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

#[utoipa::path(
    post,
    path = "/v1/jobs/{id}/cancel",
    tag = "jobs",
    summary = "Cancel a job",
    params(("id" = String, Path, description = "Job identifier returned by the submit endpoints")),
    responses(
        (status = 200, description = "Cancellation requested", body = CancelResponse),
        (status = 401, description = "Missing or invalid API token", body = ErrorResponse),
        (status = 404, description = "Job not found", body = ErrorResponse),
        (status = 409, description = "Job already finished", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
    security(("bearer_auth" = []), ("api_key" = []))
)]
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

fn openapi_document() -> OpenApiDocument {
    let mut document = ApiDoc::openapi();
    document.servers = Some(vec![Server::new(PUBLIC_BASE_URL)]);
    document
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
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_document_is_aligned_with_api() {
        let document = openapi_document();
        let value = serde_json::to_value(&document).expect("serializable OpenAPI document");

        assert_eq!(value["servers"][0]["url"], PUBLIC_BASE_URL);

        for path in [
            "/v1/search",
            "/v1/query",
            "/v1/jobs/{id}",
            "/v1/jobs/{id}/result",
            "/v1/jobs/{id}/cancel",
            "/healthz",
            "/readyz",
            "/version",
        ] {
            assert!(value["paths"].get(path).is_some(), "missing path {path}");
        }

        let schemas = &value["components"]["schemas"];
        for schema in [
            "QueryRequest",
            "CatalogReference",
            "ArchiveReference",
            "IndexObjectReference",
            "QueryOptions",
            "JobSubmissionResponse",
            "JobStatusResponse",
            "JobProgress",
            "JobError",
            "QueryResult",
            "QueryMatch",
            "QuerySummary",
            "Recording",
            "Stream",
            "Provenance",
            "ErrorResponse",
        ] {
            assert!(
                schemas.get(schema).is_some(),
                "missing component schema {schema}"
            );
        }
        assert!(
            schemas["QueryRequest"]["properties"]
                .get("contract_version")
                .is_some()
        );
        assert!(
            schemas["CatalogReference"]["properties"]
                .get("type")
                .is_some()
        );
        assert!(
            schemas["QueryOptions"]["properties"]
                .get("minimum_confidence")
                .is_some()
        );

        let schemes = &value["components"]["securitySchemes"];
        assert!(schemes.get("bearer_auth").is_some());
        assert!(schemes.get("api_key").is_some());

        for path in [
            "/v1/search",
            "/v1/query",
            "/v1/jobs/{id}",
            "/v1/jobs/{id}/result",
            "/v1/jobs/{id}/cancel",
        ] {
            let operation = value["paths"][path]
                .as_object()
                .and_then(|value| value.values().next())
                .expect("path operation");
            assert!(
                operation["security"]
                    .as_array()
                    .is_some_and(|value| value.len() == 2),
                "{path} must document bearer and api-key auth"
            );
        }

        for path in ["/healthz", "/readyz", "/version"] {
            let operation = value["paths"][path]
                .as_object()
                .and_then(|value| value.values().next())
                .expect("health operation");
            assert!(
                operation.get("security").is_none(),
                "{path} must remain public in the documentation"
            );
        }

        for path in ["/v1/query", "/v1/search"] {
            let content =
                &value["paths"][path]["post"]["requestBody"]["content"]["application/json"];
            assert!(
                content.get("example").is_some() || content.get("examples").is_some(),
                "{path} must include sample POST data"
            );
            let example = content
                .get("example")
                .expect("single request example")
                .clone();
            let request: QueryRequest =
                serde_json::from_value(example).expect("sample request must parse");
            request.validate().expect("sample request must be valid");
        }
    }
}
