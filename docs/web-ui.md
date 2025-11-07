# Web UI requirements

This document captures the current contract between the browser-based uploader and
the SIMS Upload API. It maps each screen to the supporting endpoint(s), describes
the payloads that must be exchanged, and calls out how polling and error handling
should behave so the front end remains aligned with the backend.

## Upload form

**Backed by:** `POST /uploads`

The upload form is responsible for collecting the metadata required to enqueue a
new workbook. Submit the JSON body defined by `EnqueueUploadRequest`:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `workbook_path` | string | ✅ | Absolute or repo-relative path where the API server can read the workbook. The backend trims this to populate `original_filename`. |
| `sheet` | string | ✅ | Worksheet tab to import. The worker defaults to `prep_excel.DEFAULT_SHEET` if omitted, but the API layer requires it. |
| `source_year` | string | ✅ | Stored with the job payload so downstream normalization can enforce year-specific rules. |
| `workbook_type` | string | ❌ | Defaults to `"default"`; expose a select when alternative pipelines exist. |
| `batch_id` | string | ❌ | Associates multiple uploads with a batch. |
| `workbook_name` | string | ❌ | Human-friendly workbook label surfaced in job listings. |
| `worksheet_name` | string | ❌ | Friendly sheet label surfaced in job listings. |
| `file_size` | integer | ❌ | Bytes. When provided, limit enforcement happens synchronously and failures surface as `400` errors. |
| `row_count` | integer | ❌ | Used by the queue helper for preflight limit checks. |
| `max_file_size` | integer | ❌ | Optional override for instance-wide limit. Leave blank to rely on defaults. |
| `max_rows` | integer | ❌ | Optional override for maximum row count. |

**Successful submission** returns `202 Accepted` and an `EnqueueUploadResponse`
containing the created job model. Use `job.job_id` to navigate to the job detail
view.

### Error handling

* `UploadLimitExceeded` → `400` with `detail` explaining the file size/row
  violation. Display this inline above the form.
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
* `original_filename` – displayed in the table and used when no friendly name is
  provided.
* `workbook_name`, `worksheet_name` – optional friendly labels; fall back to
  filename when missing.
* `file_size` – bytes; render using a human-readable formatter when present.
* `status` – possible values today include `Queued`, `Parsing`, `Validating`,
  `Loaded`, and `Errors` (from `job_store.mark_*`).
* `created_at`, `updated_at` – use `updated_at` to drive the “last updated” column
  and `created_at` for initial ordering.

### Refresh cadence

Poll `GET /uploads` every **30 seconds** while the list view is mounted. If the
user manually refreshes, reuse the same endpoint with the current `limit` value.

### Empty and error states

* When no jobs are returned, show an empty state encouraging the user to upload a
  workbook.
* For non-`404` errors (network issues, 5xx, malformed responses), display a toast
  and pause polling for one interval before retrying.

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
