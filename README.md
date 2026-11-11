# Broadcast Archive Query Service (BAQS)

BAQS is an asynchronous Rust service that fingerprints catalog audio with
[Olaf](https://github.com/JorenSix/Olaf), queries archived Olaf indexes from
R2/S3, and returns broadcast matches with recording metadata.

It discovers archive indexes from BMAS manifests, downloads every matching
Olaf LMDB chunk for the requested range, merges chunks by **stream + UTC day**,
then queries each merged index. Query work runs in background SQLite-backed
jobs; HTTP callers submit, poll, cancel, and fetch results.

## Stack

- Rust, axum, rusqlite, lmdb, aws-sdk-s3, reqwest
- Olaf CLI for fingerprinting/querying
- ffmpeg/ffprobe for recording duration probing
- systemd for deployment

## Layout

```text
/opt/baqs/baqs                  binary
/etc/baqs/config.toml           configuration
/etc/baqs/baqs.env              secrets/environment
/var/lib/baqs/baqs.db           jobs, results, webhook outbox
/var/lib/baqs/work/             temporary job work directories
```

## Install

```bash
sudo bash scripts/install.sh
```

The installer:

1. Installs system dependencies, Rust, and Olaf (release binary with SHA-256
   verification, or pinned Zig source build as fallback).
2. Builds BAQS in release mode.
3. Creates the `baqs` system user and directories.
4. Generates `/etc/baqs/config.toml`, `/etc/baqs/baqs.env`, and an API token.
5. Installs and starts only `baqs.service`, waiting for `/healthz`.

Provision storage credentials in `/etc/baqs/baqs.env`, then restart:

```bash
sudo systemctl restart baqs
sudo systemctl status baqs
sudo journalctl -u baqs -f
```

CLI-only install:

```bash
sudo bash scripts/install.sh --cli
baqs init-config --output ~/.config/baqs/config.toml
baqs query --config ~/.config/baqs/config.toml --request request.json
```

## Update

After `git pull`:

```bash
sudo bash scripts/update.sh
```

The updater rebuilds, backs up the previous binary/config, installs, restarts,
checks `/healthz`, and rolls back automatically on failure.

## Configuration

Environment variables referenced as `${NAME}` are expanded at startup. Typical
storage values:

```env
BAQS_API_TOKEN=...
BAQS_S3_ENDPOINT=https://<account>.r2.cloudflarestorage.com
BAQS_S3_BUCKET=my-bucket
BAQS_S3_REGION=auto
BAQS_S3_ACCESS_KEY=...
BAQS_S3_SECRET_KEY=...
```

Important config sections:

- `[server]`: bind address, public base URL, API token, health visibility.
- `[storage]`: R2/S3 endpoint/bucket/credentials and `archive_prefix`.
- `[olaf]`: binary path, map size, timeout, sample rate, step size.
- `[worker]`: concurrency, job lease, polling, cleanup.
- `[query]`: confidence/match-count thresholds, result limit, merge gap,
  chunk cap, recording size/timeouts, TTL, private-URL policy.
- `[paths]`: data/work/database paths.
- `[webhook]`: timeout, attempts, retry backoff.

`[storage]` credentials may be empty at startup so the service can boot before
provisioning. `/readyz` reports storage as `not configured`, and queries fail
until credentials are supplied.

## HTTP API

Authentication: `Authorization: Bearer ${BAQS_API_TOKEN}`. Health endpoints are
public by default.

### Submit a query

```bash
curl -X POST http://127.0.0.1:8080/v1/query \
  -H "authorization: Bearer $BAQS_API_TOKEN" \
  -H "content-type: application/json" \
  -d '{
    "contract_version": "2",
    "request_id": "req-1",
    "start": "2026-11-01T00:00:00Z",
    "end": "2026-12-01T00:00:00Z",
    "stream_ids": ["stream-1"],
    "catalog": [{
      "catalog_item_id": "cat-1",
      "name": "Example track",
      "type": "music",
      "metadata": {},
      "fingerprint_reference": null,
      "audio_url": "https://example.com/track.mp3"
    }],
    "options": { "minimum_confidence": 0.5 }
  }'
```

Response (`202`):

```json
{
  "contract_version": "2",
  "request_id": "req-1",
  "job_id": "...",
  "status": "queued",
  "status_url": "/v1/jobs/...",
  "result_url": "/v1/jobs/.../result"
}
```

### Poll, fetch, cancel

```bash
curl -H "authorization: Bearer $BAQS_API_TOKEN" \
  http://127.0.0.1:8080/v1/jobs/<job_id>

curl -H "authorization: Bearer $BAQS_API_TOKEN" \
  http://127.0.0.1:8080/v1/jobs/<job_id>/result

curl -X POST -H "authorization: Bearer $BAQS_API_TOKEN" \
  http://127.0.0.1:8080/v1/jobs/<job_id>/cancel
```

### Other endpoints

- `POST /v1/search` — alias of `/v1/query`.
- `GET /healthz` — process liveness.
- `GET /readyz` — SQLite, data directory, Olaf, ffmpeg/ffprobe, storage, workers.
- `GET /version` — service/contract version.
- `GET /openapi.json` — OpenAPI document.
- `GET /docs` — Swagger UI.

## Query ranges

`start`/`end` are RFC 3339 timestamps. Alternatively pass `month: "YYYY-MM"`
for a full UTC month. Archive manifests are discovered by listing
`archives/<node>/manifests/<YYYY-MM-DD>/` prefixes and downloading their JSON
manifests. Only index objects ending in `data.mdb` are used.

Chunks are grouped by stream and UTC day. Each time index is shifted by
`round((chunk.started_at - day_start) / step_seconds)` before writing into the
merged LMDB, and `olaf_resource_map` paths are rewritten to canonical chunk
paths. Matches are mapped back to absolute timestamps using the day start.

Request `stream_ids`, `sources.stream_ids`, and `stream_id` are matched against
both manifest `stream_id` and `external_stream_id`. A stream selector is
required so archive discovery stays bounded.

## Results

Completed results include flat fields plus nested recording/stream/provenance
data. `recording.audio_url` is supplied by the API service as a short-lived
presigned URL; BAQS uses it for downloading and returns the recording metadata
alongside matches.

Webhooks are unsigned POSTs with configurable headers. Delivery is retried from
the SQLite outbox.

## Development

```bash
cargo fmt --all
cargo check
cargo test --all-targets
```

Tests use synthetic LMDB chunks and do not require R2 credentials. End-to-end
queries require a real archive prefix and valid storage credentials.
