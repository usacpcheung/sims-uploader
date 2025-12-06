# Web UI requirements

This document captures the current contract between the browser-based uploader and
the SIMS Upload API. It maps each screen to the supporting endpoint(s), describes
the payloads that must be exchanged, and calls out how polling and error handling
should behave so the front end remains aligned with the backend. It also reflects
the latest UI behavior (manual path entry, auto-filled metadata, queued notices,
and revised polling cadence).

## Upload form

**Backed by:**
* `POST /uploads/files` for streaming the workbook binary into the staging
  directory configured by `UPLOAD_STORAGE_DIR` (default `./uploads`).
* `POST /uploads` for enqueueing the job with the persisted file path.
* Manual path option: when operators already have the workbook on-disk beside the
  API, the UI lets them skip the binary upload and supply a path directly.

The upload form is responsible for collecting the metadata required to enqueue a
new workbook. Flow:

1. Call `POST /uploads/files` with `multipart/form-data`. The API enforces
   extension checks and the file-size limit derived from
   `UPLOAD_MAX_FILE_SIZE_BYTES`/`max_file_size`.
2. Use the returned `stored_path` as `workbook_path` when submitting the JSON
   body defined by `EnqueueUploadRequest` to `POST /uploads`. Include the
   `file_size` reported by the staging response when available so the queue
   helper can reuse it for limit checks.
3. If no file is uploaded, populate `workbook_path` directly via the “manual path”
   input and submit `POST /uploads` without calling `/uploads/files`.

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `workbook_path` | string | ✅ | `stored_path` returned by `POST /uploads/files` **or** the path entered manually. The backend trims this to populate `original_filename`. |
| `sheet` | string | ✅ | Worksheet tab to import. The worker defaults to `prep_excel.DEFAULT_SHEET` if omitted, but the API layer requires it. |
| `source_year` | string | ✅ | Stored with the job payload so downstream normalization can enforce year-specific rules. Defaults to the current calendar year in the UI. |
| `workbook_type` | string | ❌ | Defaults to `"default"`; expose a select when alternative pipelines exist. |
| `batch_id` | string | ❌ | Associates multiple uploads with a batch. |
| `workbook_name` | string | ❌ | Human-friendly workbook label surfaced in job listings. Auto-filled from the uploaded filename when blank. |
| `worksheet_name` | string | ❌ | Friendly sheet label surfaced in job listings. |
| `file_size` | integer | ❌ | Bytes. Populate with the size returned by `POST /uploads/files` (or the HTML file metadata) so limit enforcement can happen synchronously and surface as `400` errors. |
| `row_count` | integer | ❌ | Used by the queue helper for preflight limit checks. |
| `max_file_size` | integer | ❌ | Optional override for instance-wide limit. Leave blank to rely on defaults. |
| `max_rows` | integer | ❌ | Optional override for maximum row count. |
| `conflict_resolution` | string | ❌ | One of `append` (default), `replace`, or `skip`. Set to resolve detected overlaps before queueing. |

### Client-side validation and defaults

* Users must either select a workbook file **or** provide a manual path; submitting
  without one of these options shows inline guidance near the manual path field.
* `source_year` defaults to the current year when empty; `workbook_type` defaults
  to `default`.
* When a file is chosen, the UI mirrors metadata into the form (`workbook_name`
  and `file_size`) using the HTML `File` object or the `/uploads/files` response.

**Successful submission** returns `202 Accepted` and an `EnqueueUploadResponse`
containing the created job model. Use `job.job_id` to navigate to the job detail
view.

### Error handling

* `UploadLimitExceeded` → `400` with `detail` explaining the file size/row
  violation. Display this inline above the form.
* Overlap detection → `409` with `overlap_detected` that includes a human summary
  and an array of overlapping ranges (`target_table`, `time_range_column`,
  `requested_*`, `existing_*`, `record_id`). Surface this block as an inline
  warning and provide buttons to resubmit with `conflict_resolution` = `append`,
  `replace`, or `skip` so the preflight can succeed on retry.
* Validation or lookup failures during job creation raise `ValueError`.
  - Messages containing “not found/does not exist” map to `404`; surface them as
    a dismissible alert.
  - All other `ValueError`s return `400`; display the `detail` copy from the API.
* Pipeline execution errors are surfaced after a job starts processing. The API
  wraps them in an `ErrorResponse` with both `detail` and `validation_summary`
  (when available). Store these strings with the job detail state so they can be
  shown on the job detail view once the job transitions to `Errors`.

## Job list view

**Backed by:** `GET /uploads`

The list presents recently created jobs sorted newest first. The endpoint accepts
an optional `limit` query parameter (default 20, maximum 100) and returns an array
of `UploadJobModel` objects:

* `job_id` – stable identifier used for routing to detail pages.
* `status` – possible values today include `Queued`, `Parsing`, `Validating`,
  `Loaded`, and `Errors` (from `job_store.mark_*`).
* `created_at`, `updated_at` – rendered as local timestamps. The current UI table
  focuses on these columns and the job ID; friendly names and sizes are available
  in the API payload for richer tables later.

### Refresh cadence

Poll `GET /uploads` every **12 seconds** while the list view is mounted. A manual
“Refresh now” button forces an immediate fetch using the same endpoint and limit
value.

### Queued notice

After a successful job enqueue, the UI redirects back to the list with
`?notice=<message>&job_id=<id>`. The list page surfaces this banner until the
user dismisses it, then removes the query parameters via `replaceState`.

### Empty and error states

* When no jobs are returned, show an empty state encouraging the user to upload a
  workbook.
* For non-`404` errors (network issues, 5xx, malformed responses), surface an
  inline error block and keep the current jobs rendered; the next poll attempts to
  recover automatically.

## Job detail & event timeline

**Backed by:**
* `GET /uploads/{job_id}` for job metadata and aggregate results.
* `GET /uploads/{job_id}/events` for the chronological status log. Support the
  optional `limit` query when you only need the latest `n` entries.

`GET /uploads/{job_id}` returns an `UploadJobDetailModel` with:

* `job` – same shape as the list view row. Always present.
* `result` – optional. When present, display:
  - `total_rows`, `processed_rows`, `successful_rows`, `rejected_rows`.
  - `normalized_table_name`.
  - `rejected_rows_path` – render as a download link if the path is web-accessible.
  - `coverage_metadata` – JSON object; render as a collapsible inspector for
    advanced users.

`GET /uploads/{job_id}/events` yields an array of `UploadJobEventModel` entries:
`event_id`, `status`, `message`, `event_at`. Render them in ascending order to
form the timeline. Highlight the most recent status to match the header badge.

### Refresh cadence

* Poll job metadata (`GET /uploads/{job_id}`) every **10 seconds** until the job
  reaches a terminal state (`Loaded` or `Errors`). Once terminal, stop polling.
* Poll the event timeline endpoint every **5 seconds** while the job is not
  terminal to surface progress messages quickly.

### Error handling

* `404` from either endpoint → show a “job not found” message and offer a link
  back to the job list.
* `400` responses (e.g., invalid `limit`) should only occur because of a UI bug;
  log to the console and show a generic error banner.
* When the detail endpoint includes `validation_summary`, display it prominently
  in the results/error panel to explain validation failures.

## Follow-up issues

* Job list pagination – `GET /uploads` only exposes a `limit` parameter. Create a
  follow-up issue to add cursor or offset/limit support so the UI can browse jobs
  beyond the newest 100.
* Event timeline deltas – fetching the full timeline on every poll is workable
  now, but we should track an enhancement to support “since event_id” filtering if
  latency becomes a concern.

### Operational considerations for browser uploads

* `UPLOAD_STORAGE_DIR` controls where uploaded binaries land. Default is
  `./uploads`; choose a persistent volume so staged files survive restarts until
  they are processed.
* Plan disk space around `UPLOAD_MAX_FILE_SIZE_BYTES`, the number of concurrent
  workers reading from `UPLOAD_QUEUE_NAME`, and any stricter `max_rows`/`UPLOAD_MAX_ROWS`
  limits set at runtime.
* Implement a periodic cleanup job to delete staged files once ingestion has
  completed to prevent the storage volume from filling over time.
