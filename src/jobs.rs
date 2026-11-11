use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::json;

use crate::types::{
    JobError, JobProgress, JobRecord, JobStatus, QueryRequest, QueryResult, utc_now_millis,
};

#[derive(Clone)]
pub struct Db {
    inner: Arc<Mutex<Connection>>,
}

pub struct WebhookDelivery {
    pub id: i64,
    pub job_id: String,
    pub url: String,
    pub headers_json: Option<String>,
    pub payload_json: String,
    pub attempts: i64,
}

impl Db {
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Creating database directory {}", parent.display()))?;
        }
        let path = path.to_path_buf();
        let conn = tokio::task::spawn_blocking(move || -> anyhow::Result<Connection> {
            let conn = Connection::open(&path)
                .with_context(|| format!("Opening SQLite database {}", path.display()))?;
            conn.busy_timeout(std::time::Duration::from_secs(30))?;
            conn.execute_batch(
                r#"
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    contract_version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    result_json TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    progress_json TEXT,
                    webhook_url TEXT,
                    webhook_headers_json TEXT,
                    webhook_status TEXT NOT NULL DEFAULT 'none',
                    submitted_at INTEGER NOT NULL,
                    started_at INTEGER,
                    completed_at INTEGER,
                    lease_expires_at INTEGER,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS jobs_queue_idx
                    ON jobs(status, submitted_at);
                CREATE INDEX IF NOT EXISTS jobs_lease_idx
                    ON jobs(status, lease_expires_at);

                CREATE TABLE IF NOT EXISTS webhook_deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    url TEXT NOT NULL,
                    headers_json TEXT,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at INTEGER,
                    last_error TEXT,
                    created_at INTEGER NOT NULL,
                    delivered_at INTEGER,
                    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS webhook_due_idx
                    ON webhook_deliveries(status, next_attempt_at);
                "#,
            )?;
            Ok(conn)
        })
        .await??;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    async fn with_conn<F, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(&mut Connection) -> anyhow::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = inner
                .lock()
                .map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
            f(&mut conn)
        })
        .await?
    }

    pub async fn insert_job(
        &self,
        request: &QueryRequest,
        kind: &str,
        job_id: &str,
    ) -> anyhow::Result<()> {
        let request = request.clone();
        let kind = kind.to_string();
        let job_id = job_id.to_string();
        self.with_conn(move |conn| {
            let now = utc_now_millis();
            let request_json = serde_json::to_string(&request)?;
            let headers_json = if request.webhook_headers.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&request.webhook_headers)?)
            };
            conn.execute(
                r#"
                INSERT INTO jobs (
                    id, request_id, kind, contract_version, status, request_json,
                    webhook_url, webhook_headers_json, webhook_status,
                    submitted_at, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, 'queued', ?5, ?6, ?7, ?8, ?9, ?9, ?9)
                "#,
                params![
                    job_id,
                    request.request_id,
                    kind,
                    request.contract_version,
                    request_json,
                    request.webhook_url(),
                    headers_json,
                    if request.webhook_url().is_some() {
                        "pending"
                    } else {
                        "none"
                    },
                    now,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_job(&self, id: &str) -> anyhow::Result<Option<JobRecord>> {
        let id = id.to_string();
        self.with_conn(move |conn| {
            let job = conn
                .query_row(
                    r#"
                    SELECT id, request_id, kind, contract_version, status, request_json,
                           result_json, error_code, error_message, progress_json,
                           webhook_url, webhook_headers_json, webhook_status,
                           submitted_at, started_at, completed_at, cancel_requested
                    FROM jobs WHERE id = ?1
                    "#,
                    params![id],
                    row_to_job,
                )
                .optional()?;
            Ok(job)
        })
        .await
    }

    pub async fn claim_next_job(&self, lease_seconds: u64) -> anyhow::Result<Option<JobRecord>> {
        self.with_conn(move |conn| {
            let tx = conn.transaction()?;
            let now = utc_now_millis();
            let lease_until = now + (lease_seconds as i64 * 1000);
            let id: Option<String> = tx
                .query_row(
                    r#"
                    SELECT id FROM jobs
                    WHERE (status = 'queued' AND cancel_requested = 0)
                       OR (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?1)
                    ORDER BY submitted_at ASC
                    LIMIT 1
                    "#,
                    params![now],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(id) = id else {
                tx.commit()?;
                return Ok(None);
            };
            tx.execute(
                r#"
                UPDATE jobs
                SET status = 'running',
                    started_at = COALESCE(started_at, ?1),
                    lease_expires_at = ?2,
                    updated_at = ?1
                WHERE id = ?3
                "#,
                params![now, lease_until, id],
            )?;
            let job = tx.query_row(
                r#"
                SELECT id, request_id, kind, contract_version, status, request_json,
                       result_json, error_code, error_message, progress_json,
                       webhook_url, webhook_headers_json, webhook_status,
                       submitted_at, started_at, completed_at, cancel_requested
                FROM jobs WHERE id = ?1
                "#,
                params![id],
                row_to_job,
            )?;
            tx.commit()?;
            Ok(Some(job))
        })
        .await
    }

    pub async fn claim_job(
        &self,
        id: &str,
        lease_seconds: u64,
    ) -> anyhow::Result<Option<JobRecord>> {
        let id = id.to_string();
        self.with_conn(move |conn| {
            let tx = conn.transaction()?;
            let now = utc_now_millis();
            let lease_until = now + (lease_seconds as i64 * 1000);
            let claimed = tx.execute(
                r#"
                UPDATE jobs
                SET status = 'running',
                    started_at = COALESCE(started_at, ?1),
                    lease_expires_at = ?2,
                    updated_at = ?1
                WHERE id = ?3
                  AND ((status = 'queued' AND cancel_requested = 0)
                    OR (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?1))
                "#,
                params![now, lease_until, id],
            )?;
            if claimed == 0 {
                tx.commit()?;
                return Ok(None);
            }
            let job = tx
                .query_row(
                    r#"
                    SELECT id, request_id, kind, contract_version, status, request_json,
                           result_json, error_code, error_message, progress_json,
                           webhook_url, webhook_headers_json, webhook_status,
                           submitted_at, started_at, completed_at, cancel_requested
                    FROM jobs WHERE id = ?1
                    "#,
                    params![id],
                    row_to_job,
                )
                .optional()?;
            tx.commit()?;
            Ok(job)
        })
        .await
    }

    pub async fn renew_lease(&self, id: &str, lease_seconds: u64) -> anyhow::Result<()> {
        let id = id.to_string();
        self.with_conn(move |conn| {
            let now = utc_now_millis();
            conn.execute(
                "UPDATE jobs SET lease_expires_at = ?1, updated_at = ?2 WHERE id = ?3",
                params![now + lease_seconds as i64 * 1000, now, id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn update_progress(&self, id: &str, progress: &JobProgress) -> anyhow::Result<()> {
        let id = id.to_string();
        let json = serde_json::to_string(progress)?;
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE jobs SET progress_json = ?1, updated_at = ?2 WHERE id = ?3",
                params![json, utc_now_millis(), id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn complete_job(&self, id: &str, result: &QueryResult) -> anyhow::Result<()> {
        let id = id.to_string();
        let result_json = serde_json::to_string(result)?;
        self.with_conn(move |conn| {
            let tx = conn.transaction()?;
            let now = utc_now_millis();
            tx.execute(
                r#"
                UPDATE jobs
                SET status = 'completed', result_json = ?1, completed_at = ?2,
                    updated_at = ?2, lease_expires_at = NULL, progress_json = ?3
                WHERE id = ?4
                "#,
                params![
                    result_json,
                    now,
                    serde_json::to_string(&JobProgress {
                        stage: "complete".to_string(),
                        ..JobProgress::default()
                    })?,
                    id
                ],
            )?;
            enqueue_webhook(
                &tx,
                &id,
                "query.completed",
                &json!({
                    "event": "query.completed",
                    "job_id": id,
                    "result": serde_json::from_str::<serde_json::Value>(&result_json)?,
                }),
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn fail_job(&self, id: &str, code: &str, message: &str) -> anyhow::Result<()> {
        let id = id.to_string();
        let code = code.to_string();
        let message = message.to_string();
        self.with_conn(move |conn| {
            let tx = conn.transaction()?;
            let now = utc_now_millis();
            tx.execute(
                r#"
                UPDATE jobs
                SET status = 'failed', error_code = ?1, error_message = ?2,
                    completed_at = ?3, updated_at = ?3, lease_expires_at = NULL
                WHERE id = ?4
                "#,
                params![code, message, now, id],
            )?;
            enqueue_webhook(
                &tx,
                &id,
                "query.failed",
                &json!({
                    "event": "query.failed",
                    "job_id": id,
                    "error": { "code": code, "message": message },
                }),
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn cancel_job(&self, id: &str, message: &str) -> anyhow::Result<JobStatus> {
        let id = id.to_string();
        let message = message.to_string();
        self.with_conn(move |conn| {
            let tx = conn.transaction()?;
            let now = utc_now_millis();
            let status: Option<String> = tx
                .query_row(
                    "SELECT status FROM jobs WHERE id = ?1",
                    params![id],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(status) = status else {
                return Ok(JobStatus::Failed);
            };
            if status == "queued" {
                tx.execute(
                    r#"
                    UPDATE jobs
                    SET status = 'cancelled', cancel_requested = 1, completed_at = ?1,
                        updated_at = ?1, error_code = 'cancelled', error_message = ?2
                    WHERE id = ?3
                    "#,
                    params![now, message, id],
                )?;
                enqueue_webhook(
                    &tx,
                    &id,
                    "query.cancelled",
                    &json!({
                        "event": "query.cancelled",
                        "job_id": id,
                        "error": { "code": "cancelled", "message": message },
                    }),
                )?;
            } else if status == "running" {
                tx.execute(
                    "UPDATE jobs SET cancel_requested = 1, updated_at = ?1 WHERE id = ?2",
                    params![now, id],
                )?;
            }
            tx.commit()?;
            Ok(JobStatus::from_str(&status))
        })
        .await
    }

    pub async fn finish_cancelled(&self, id: &str, message: &str) -> anyhow::Result<()> {
        let id = id.to_string();
        let message = message.to_string();
        self.with_conn(move |conn| {
            let tx = conn.transaction()?;
            let now = utc_now_millis();
            tx.execute(
                r#"
                UPDATE jobs
                SET status = 'cancelled', completed_at = ?1, updated_at = ?1,
                    lease_expires_at = NULL, error_code = 'cancelled', error_message = ?2
                WHERE id = ?3
                "#,
                params![now, message, id],
            )?;
            enqueue_webhook(
                &tx,
                &id,
                "query.cancelled",
                &json!({
                    "event": "query.cancelled",
                    "job_id": id,
                    "error": { "code": "cancelled", "message": message },
                }),
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn get_result(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<(String, Option<String>, Option<JobError>)>> {
        let id = id.to_string();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    r#"
                    SELECT status, result_json, error_code, error_message
                    FROM jobs WHERE id = ?1
                    "#,
                    params![id],
                    |row| {
                        let status: String = row.get(0)?;
                        let result: Option<String> = row.get(1)?;
                        let code: Option<String> = row.get(2)?;
                        let message: Option<String> = row.get(3)?;
                        let error = code.map(|code| JobError {
                            code,
                            message: message.unwrap_or_default(),
                        });
                        Ok((status, result, error))
                    },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn cleanup(&self, ttl_seconds: u64) -> anyhow::Result<usize> {
        self.with_conn(move |conn| {
            let cutoff = utc_now_millis() - ttl_seconds as i64 * 1000;
            let tx = conn.transaction()?;
            let ids: Vec<String> = {
                let mut stmt = tx.prepare(
                    r#"
                    SELECT id FROM jobs
                    WHERE status IN ('completed', 'failed', 'cancelled')
                      AND COALESCE(completed_at, updated_at) < ?1
                    "#,
                )?;
                let rows = stmt.query_map(params![cutoff], |row| row.get(0))?;
                rows.collect::<rusqlite::Result<Vec<String>>>()?
            };
            let count = ids.len();
            for id in ids {
                tx.execute("DELETE FROM jobs WHERE id = ?1", params![id])?;
            }
            tx.commit()?;
            Ok(count)
        })
        .await
    }

    pub async fn claim_webhook(&self) -> anyhow::Result<Option<WebhookDelivery>> {
        self.with_conn(move |conn| {
            let tx = conn.transaction()?;
            let now = utc_now_millis();
            let id: Option<i64> = tx
                .query_row(
                    r#"
                    SELECT id FROM webhook_deliveries
                    WHERE status IN ('pending', 'retry') AND next_attempt_at <= ?1
                    ORDER BY id ASC LIMIT 1
                    "#,
                    params![now],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(id) = id else {
                tx.commit()?;
                return Ok(None);
            };
            tx.execute(
                "UPDATE webhook_deliveries SET status = 'sending' WHERE id = ?1",
                params![id],
            )?;
            let delivery = tx.query_row(
                r#"
                SELECT id, job_id, url, headers_json, payload_json, attempts
                FROM webhook_deliveries WHERE id = ?1
                "#,
                params![id],
                |row| {
                    Ok(WebhookDelivery {
                        id: row.get(0)?,
                        job_id: row.get(1)?,
                        url: row.get(2)?,
                        headers_json: row.get(3)?,
                        payload_json: row.get(4)?,
                        attempts: row.get(5)?,
                    })
                },
            )?;
            tx.commit()?;
            Ok(Some(delivery))
        })
        .await
    }

    pub async fn mark_webhook_delivered(&self, id: i64, job_id: &str) -> anyhow::Result<()> {
        let job_id = job_id.to_string();
        self.with_conn(move |conn| {
            let tx = conn.transaction()?;
            let now = utc_now_millis();
            tx.execute(
                r#"
                UPDATE webhook_deliveries
                SET status = 'delivered', delivered_at = ?1, last_error = NULL
                WHERE id = ?2
                "#,
                params![now, id],
            )?;
            tx.execute(
                "UPDATE jobs SET webhook_status = 'delivered', updated_at = ?1 WHERE id = ?2",
                params![now, job_id],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn mark_webhook_failed(
        &self,
        id: i64,
        job_id: &str,
        error: &str,
        next_attempt_at: Option<i64>,
        max_attempts: i64,
    ) -> anyhow::Result<()> {
        let job_id = job_id.to_string();
        let error = error.to_string();
        self.with_conn(move |conn| {
            let tx = conn.transaction()?;
            let now = utc_now_millis();
            let attempts: i64 = tx.query_row(
                "SELECT attempts FROM webhook_deliveries WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )?;
            let attempts = attempts + 1;
            if attempts >= max_attempts {
                tx.execute(
                    r#"
                    UPDATE webhook_deliveries
                    SET status = 'failed', attempts = ?1, last_error = ?2, next_attempt_at = NULL
                    WHERE id = ?3
                    "#,
                    params![attempts, error, id],
                )?;
                tx.execute(
                    "UPDATE jobs SET webhook_status = 'failed', updated_at = ?1 WHERE id = ?2",
                    params![now, job_id],
                )?;
            } else {
                tx.execute(
                    r#"
                    UPDATE webhook_deliveries
                    SET status = 'retry', attempts = ?1, last_error = ?2, next_attempt_at = ?3
                    WHERE id = ?4
                    "#,
                    params![attempts, error, next_attempt_at, id],
                )?;
                tx.execute(
                    "UPDATE jobs SET webhook_status = 'retrying', updated_at = ?1 WHERE id = ?2",
                    params![now, job_id],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn pending_job_count(&self) -> anyhow::Result<i64> {
        self.with_conn(|conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')",
                [],
                |row| row.get(0),
            )?)
        })
        .await
    }
}

fn row_to_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobRecord> {
    let status: String = row.get(4)?;
    Ok(JobRecord {
        id: row.get(0)?,
        request_id: row.get(1)?,
        kind: row.get(2)?,
        contract_version: row.get(3)?,
        status: JobStatus::from_str(&status),
        request_json: row.get(5)?,
        result_json: row.get(6)?,
        error_code: row.get(7)?,
        error_message: row.get(8)?,
        progress_json: row.get(9)?,
        webhook_url: row.get(10)?,
        webhook_headers_json: row.get(11)?,
        webhook_status: row.get(12)?,
        submitted_at: row.get(13)?,
        started_at: row.get(14)?,
        completed_at: row.get(15)?,
        cancel_requested: row.get::<_, i64>(16)? != 0,
    })
}

fn enqueue_webhook(
    tx: &rusqlite::Transaction<'_>,
    job_id: &str,
    _event: &str,
    payload: &serde_json::Value,
) -> anyhow::Result<()> {
    let row: Option<(Option<String>, Option<String>)> = tx
        .query_row(
            "SELECT webhook_url, webhook_headers_json FROM jobs WHERE id = ?1",
            params![job_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((Some(url), headers)) = row else {
        return Ok(());
    };
    if url.trim().is_empty() {
        return Ok(());
    }
    tx.execute(
        r#"
        INSERT INTO webhook_deliveries (
            job_id, url, headers_json, payload_json, status, attempts,
            next_attempt_at, created_at
        ) VALUES (?1, ?2, ?3, ?4, 'pending', 0, ?5, ?5)
        "#,
        params![
            job_id,
            url,
            headers,
            serde_json::to_string(payload)?,
            utc_now_millis()
        ],
    )?;
    tx.execute(
        "UPDATE jobs SET webhook_status = 'pending' WHERE id = ?1",
        params![job_id],
    )?;
    Ok(())
}

pub fn parse_webhook_headers(headers_json: Option<&str>) -> HashMap<String, String> {
    headers_json
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or_default()
}
