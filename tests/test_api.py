from __future__ import annotations

import os
import sys
from datetime import date, datetime, timezone
from http import HTTPStatus
from types import SimpleNamespace

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_USER", "user")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "database")
os.environ.setdefault("DB_CHARSET", "utf8mb4")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fastapi.testclient import TestClient
import pymysql
import pytest

from pathlib import Path

from app import api, config, job_runner, job_store
from app import storage
from tests import test_job_runner


def _make_job(
    job_id: str = "job-123",
    *,
    latest_message: str | None = None,
    processed_rows: int | None = None,
    successful_rows: int | None = None,
    rejected_rows: int | None = None,
    normalized_table_name: str | None = None,
) -> job_store.UploadJob:
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    return job_store.UploadJob(
        job_id=job_id,
        original_filename="example.xlsx",
        workbook_name="Workbook",
        worksheet_name="Sheet1",
        file_size=1024,
        status="Queued",
        created_at=now,
        updated_at=now,
        latest_message=latest_message,
        processed_rows=processed_rows,
        successful_rows=successful_rows,
        rejected_rows=rejected_rows,
        normalized_table_name=normalized_table_name,
    )


def _make_result(job_id: str = "job-123") -> job_store.UploadJobResult:
    now = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
    return job_store.UploadJobResult(
        job_id=job_id,
        total_rows=100,
        processed_rows=90,
        successful_rows=90,
        rejected_rows=10,
        normalized_table_name="teach_record_norm",
        rejected_rows_path="/tmp/rejected.csv",
        coverage_metadata={"columns": ["a", "b"]},
        created_at=now,
        updated_at=now,
    )


def _make_events(job_id: str = "job-123") -> list[job_store.UploadJobEvent]:
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    return [
        job_store.UploadJobEvent(
            event_id=1,
            job_id=job_id,
            status="Queued",
            message="Queued for processing",
            event_at=now,
        ),
        job_store.UploadJobEvent(
            event_id=2,
            job_id=job_id,
            status="Parsing",
            message="Parsing worksheet",
            event_at=now,
        ),
    ]


@pytest.fixture
def upload_storage_dir(tmp_path, monkeypatch):
    storage_dir = tmp_path / "uploads"
    monkeypatch.setenv(config.UPLOAD_STORAGE_DIR_ENV, str(storage_dir))
    monkeypatch.setattr(config, "load_environment", lambda dotenv_path=None: None)
    return storage_dir


@pytest.fixture
def staged_upload(upload_storage_dir):
    client = TestClient(api.app)
    content = b"sample content"
    response = client.post(
        "/uploads/files",
        files={"file": ("example.xlsx", content, "application/vnd.ms-excel")},
    )
    assert response.status_code == 201
    data = response.json()
    stored_path = Path(data["stored_path"])
    return data, stored_path, content


@pytest.fixture(autouse=True)
def stub_table_config(monkeypatch):
    monkeypatch.setattr(
        api.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default": {
            "overlap_target_table": None,
            "time_range_column": None,
        },
    )


def test_create_upload_success(monkeypatch, staged_upload):
    staging_payload, stored_path, _ = staged_upload
    job = _make_job()

    def fake_enqueue_job(**kwargs):
        incoming_path = Path(kwargs["workbook_path"])
        assert incoming_path.exists()
        assert incoming_path == stored_path
        assert (
            storage.get_original_filename(incoming_path)
            == staging_payload["original_filename"]
        )
        assert kwargs["sheet"] == "SHEET"
        assert kwargs["source_year"] == "2024"
        assert kwargs["file_size"] == staging_payload["file_size"]
        assert kwargs["row_count"] == 123
        assert kwargs["workbook_name"] == staging_payload["original_filename"]
        return job.job_id, object()

    monkeypatch.setattr(job_runner, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(job_store, "get_job", lambda job_id: job)

    client = TestClient(api.app)
    response = client.post(
        "/uploads",
        json={
            "workbook_path": staging_payload["stored_path"],
            "workbook_name": staging_payload["original_filename"],
            "sheet": "SHEET",
            "source_year": "2024",
            "file_size": staging_payload["file_size"],
            "row_count": 123,
        },
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["job"]["job_id"] == job.job_id
    assert payload["job"]["original_filename"] == job.original_filename


def test_upload_file_success(staged_upload):
    staging_payload, stored_path, content = staged_upload
    assert stored_path.exists()
    assert stored_path.read_bytes() == content
    assert staging_payload["original_filename"] == "example.xlsx"
    assert staging_payload["file_size"] == len(content)
    assert storage.get_original_filename(stored_path) == "example.xlsx"


def test_upload_file_rejects_bad_extension(monkeypatch, upload_storage_dir):
    client = TestClient(api.app)
    response = client.post(
        "/uploads/files",
        files={"file": ("example.txt", b"irrelevant", "text/plain")},
    )

    assert response.status_code == 400
    assert "Unsupported" in response.json()["detail"]


def test_upload_file_respects_size_limit(monkeypatch, upload_storage_dir):
    monkeypatch.setenv(job_runner.FILE_SIZE_LIMIT_ENV, "5")

    client = TestClient(api.app)
    response = client.post(
        "/uploads/files",
        files={"file": ("example.xlsx", b"0123456", "application/octet-stream")},
    )

    assert response.status_code == 400
    payload = response.json()
    assert "exceeds" in payload["detail"]


def test_upload_flow_stages_then_enqueues(monkeypatch, staged_upload):
    staging_payload, stored_path, _ = staged_upload
    job = _make_job("staged-job")

    captured = {}

    def fake_enqueue_job(**kwargs):
        captured.update(kwargs)
        return job.job_id, object()

    monkeypatch.setattr(job_runner, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(job_store, "get_job", lambda job_id: job)

    client = TestClient(api.app)
    response = client.post(
        "/uploads",
        json={
            "workbook_path": staging_payload["stored_path"],
            "workbook_name": staging_payload["original_filename"],
            "sheet": "Sheet A",
            "source_year": "2023",
            "file_size": staging_payload["file_size"],
        },
    )

    assert response.status_code == 202
    assert captured["workbook_path"] == str(stored_path)
    assert captured["file_size"] == staging_payload["file_size"]
    assert captured["workbook_name"] == staging_payload["original_filename"]
    assert captured["sheet"] == "Sheet A"
    assert captured["conflict_resolution"] == "append"
    assert captured.get("time_ranges") is None


def test_create_upload_limit_violation(monkeypatch):
    def fake_enqueue_job(**kwargs):
        raise job_runner.UploadLimitExceeded("File too large")

    monkeypatch.setattr(job_runner, "enqueue_job", fake_enqueue_job)

    client = TestClient(api.app)
    response = client.post(
        "/uploads",
        json={
            "workbook_path": "uploads/example.xlsx",
            "sheet": "SHEET",
            "source_year": "2024",
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "File too large"


def test_create_upload_returns_overlap(monkeypatch):
    overlap_payload = [
        {
            "target_table": "calendar_table",
            "time_range_column": "period",
            "requested_start": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "requested_end": datetime(2024, 1, 31, tzinfo=timezone.utc),
            "existing_start": datetime(2024, 1, 5, tzinfo=timezone.utc),
            "existing_end": datetime(2024, 1, 20, tzinfo=timezone.utc),
            "record_id": 99,
        }
    ]

    monkeypatch.setattr(
        api.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default": {
            "overlap_target_table": "calendar_table",
            "time_range_column": "period",
        },
    )
    monkeypatch.setattr(
        job_runner,
        "check_time_overlap",
        lambda **kwargs: overlap_payload,
    )

    enqueue_called = False

    def _fail_enqueue(**kwargs):
        nonlocal enqueue_called
        enqueue_called = True
        raise AssertionError("enqueue_job should not be called when overlaps exist")

    monkeypatch.setattr(job_runner, "enqueue_job", _fail_enqueue)

    client = TestClient(api.app)
    response = client.post(
        "/uploads",
        json={
            "workbook_path": "uploads/example.xlsx",
            "sheet": "SHEET",
            "source_year": "2024",
            "time_ranges": [
                {
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-01-31T00:00:00Z",
                }
            ],
        },
    )

    assert response.status_code == 409
    assert not enqueue_called
    payload = response.json()
    assert payload["job"] is None
    assert payload["overlap_detected"]["overlaps"][0]["target_table"] == "calendar_table"
    assert "overlap" in payload["overlap_detected"].get("summary", "").lower()


def test_create_upload_returns_overlap_for_date_column(monkeypatch):
    executed: list[tuple[str, tuple[object, ...]]] = []
    min_max = {"range_start": date(2024, 1, 5), "range_end": date(2024, 1, 9)}

    connection = test_job_runner._FakeConnection([], executed, min_max=min_max)
    monkeypatch.setattr(job_runner.pymysql, "connect", lambda **kwargs: connection)
    monkeypatch.setattr(
        job_runner.ingest_excel, "_get_db_settings", lambda overrides=None: {"database": "db"}
    )
    monkeypatch.setattr(
        api.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default": {
            "overlap_target_table": "calendar_table",
            "time_range_column": "period",
        },
    )
    monkeypatch.setattr(api.time_range_utils, "derive_ranges_from_workbook", lambda *_, **__: None)

    enqueue_called = False

    def _fail_enqueue(**kwargs):
        nonlocal enqueue_called
        enqueue_called = True
        raise AssertionError("enqueue_job should not be called when overlaps exist")

    monkeypatch.setattr(job_runner, "enqueue_job", _fail_enqueue)

    client = TestClient(api.app)
    response = client.post(
        "/uploads",
        json={
            "workbook_path": "uploads/example.xlsx",
            "sheet": "SHEET",
            "source_year": "2024",
            "time_ranges": [{"start": "2024-01-01", "end": "2024-01-10"}],
        },
    )

    assert response.status_code == 409
    assert not enqueue_called
    payload = response.json()
    overlap = payload["overlap_detected"]["overlaps"][0]
    assert overlap["target_table"] == "calendar_table"
    assert overlap["existing_start"].startswith("2024-01-05")
    assert overlap["existing_end"].startswith("2024-01-09")


def test_create_upload_returns_overlap_for_date_column_with_datetime_input(monkeypatch):
    executed: list[tuple[str, tuple[object, ...]]] = []
    min_max = {"range_start": date(2024, 1, 5), "range_end": date(2024, 1, 9)}

    connection = test_job_runner._FakeConnection([], executed, min_max=min_max)
    monkeypatch.setattr(job_runner.pymysql, "connect", lambda **kwargs: connection)
    monkeypatch.setattr(
        job_runner.ingest_excel, "_get_db_settings", lambda overrides=None: {"database": "db"}
    )
    monkeypatch.setattr(
        api.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default": {
            "overlap_target_table": "calendar_table",
            "time_range_column": "period",
        },
    )
    monkeypatch.setattr(api.time_range_utils, "derive_ranges_from_workbook", lambda *_, **__: None)

    enqueue_called = False

    def _fail_enqueue(**kwargs):
        nonlocal enqueue_called
        enqueue_called = True
        raise AssertionError("enqueue_job should not be called when overlaps exist")

    monkeypatch.setattr(job_runner, "enqueue_job", _fail_enqueue)

    client = TestClient(api.app)
    response = client.post(
        "/uploads",
        json={
            "workbook_path": "uploads/example.xlsx",
            "sheet": "SHEET",
            "source_year": "2024",
            "time_ranges": [
                {
                    "start": "2024-01-01T12:00:00",
                    "end": "2024-01-10T18:00:00",
                }
            ],
        },
    )

    assert response.status_code == 409
    assert not enqueue_called
    payload = response.json()
    overlap = payload["overlap_detected"]["overlaps"][0]
    assert overlap["target_table"] == "calendar_table"
    assert overlap["existing_start"].startswith("2024-01-05")
    assert overlap["existing_end"].startswith("2024-01-09")


def test_create_upload_allows_acknowledged_append(monkeypatch):
    overlaps = [
        {
            "target_table": "calendar_table",
            "time_range_column": "period",
            "requested_start": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "requested_end": datetime(2024, 1, 31, tzinfo=timezone.utc),
            "existing_start": datetime(2024, 1, 5, tzinfo=timezone.utc),
            "existing_end": datetime(2024, 1, 20, tzinfo=timezone.utc),
            "record_id": 99,
        }
    ]
    overlap_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        api.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default": {
            "overlap_target_table": "calendar_table",
            "time_range_column": "period",
        },
    )

    def _capture_overlaps(**kwargs):
        overlap_calls.append(kwargs)
        return overlaps

    monkeypatch.setattr(job_runner, "check_time_overlap", _capture_overlaps)

    enqueue_called = False

    def _enqueue_job(**kwargs):
        nonlocal enqueue_called
        enqueue_called = True
        return "job-ack", SimpleNamespace(id="rq-job-ack")

    monkeypatch.setattr(job_runner, "enqueue_job", _enqueue_job)
    job = _make_job("job-ack")
    monkeypatch.setattr(job_store, "get_job", lambda job_id: job)

    client = TestClient(api.app)
    payload = {
        "workbook_path": "uploads/example.xlsx",
        "sheet": "SHEET",
        "source_year": "2024",
        "time_ranges": [
            {"start": "2024-01-01T00:00:00Z", "end": "2024-01-31T00:00:00Z"}
        ],
    }

    first_response = client.post("/uploads", json=payload)
    assert first_response.status_code == HTTPStatus.CONFLICT
    assert first_response.json()["overlap_detected"]["overlaps"]
    assert len(overlap_calls) == 1
    assert not enqueue_called

    second_response = client.post(
        "/uploads", json={**payload, "overlap_acknowledged": True}
    )
    assert second_response.status_code == HTTPStatus.ACCEPTED
    assert second_response.json()["job"]["job_id"] == "job-ack"
    assert enqueue_called
    assert len(overlap_calls) == 2


def test_create_upload_passes_time_range_format(monkeypatch):
    captured_kwargs: dict[str, object] = {}

    def _capture_check_time_overlap(**kwargs):
        nonlocal captured_kwargs
        captured_kwargs = kwargs
        return [
            {
                "target_table": "calendar_table",
                "time_range_column": "period",
                "requested_start": datetime(2024, 1, 1),
                "requested_end": datetime(2024, 1, 2),
                "existing_start": datetime(2024, 1, 1),
                "existing_end": datetime(2024, 1, 2),
                "record_id": None,
            }
        ]

    monkeypatch.setattr(
        api.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default": {
            "overlap_target_table": "calendar_table",
            "time_range_column": "period",
            "time_range_format": "%m/%d/%Y",
        },
    )
    monkeypatch.setattr(api.time_range_utils, "derive_ranges_from_workbook", lambda *_, **__: None)
    monkeypatch.setattr(job_runner, "enqueue_job", lambda **kwargs: ("job-1", object()))
    monkeypatch.setattr(job_runner, "check_time_overlap", _capture_check_time_overlap)

    client = TestClient(api.app)
    response = client.post(
        "/uploads",
        json={
            "workbook_path": "uploads/example.xlsx",
            "sheet": "SHEET",
            "source_year": "2024",
            "time_ranges": [
                {"start": "2024-01-01T00:00:00", "end": "2024-01-02T00:00:00"}
            ],
        },
    )

    assert response.status_code == 409
    assert captured_kwargs.get("time_range_format") == "%m/%d/%Y"


def test_create_upload_accepts_missing_overlap_table(monkeypatch):
    executed: list[tuple[str, tuple[object, ...]]] = []
    connection = test_job_runner._FakeConnection([], executed, table_exists=False)

    monkeypatch.setattr(job_runner.pymysql, "connect", lambda **kwargs: connection)
    monkeypatch.setattr(
        job_runner.ingest_excel, "_get_db_settings", lambda overrides=None: {"database": "db"}
    )
    monkeypatch.setattr(
        api.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default": {
            "overlap_target_table": "missing_table",
            "time_range_column": "period",
        },
    )
    monkeypatch.setattr(
        api.time_range_utils,
        "derive_ranges_from_workbook",
        lambda *args, **kwargs: None,
    )

    def fake_enqueue_job(**kwargs):
        return "job-123", SimpleNamespace(id="rq-job-123")

    monkeypatch.setattr(job_runner, "enqueue_job", fake_enqueue_job)
    job = _make_job("job-123")
    monkeypatch.setattr(job_store, "get_job", lambda job_id: job)

    client = TestClient(api.app)
    response = client.post(
        "/uploads",
        json={
            "workbook_path": "uploads/example.xlsx",
            "sheet": "SHEET",
            "source_year": "2024",
            "time_ranges": [
                {"start": "2024-01-01T00:00:00Z", "end": "2024-01-31T00:00:00Z"}
            ],
        },
    )

    assert response.status_code == HTTPStatus.ACCEPTED
    payload = response.json()
    assert payload["job"]["job_id"] == "job-123"
    assert payload["overlaps"] in (None, [])


def test_create_upload_handles_base_column_overlap(monkeypatch):
    executed: list[tuple[str, tuple[object, ...]]] = []
    programming_error = pymysql.err.ProgrammingError(1054, "Unknown column '日期_start'")
    connection = test_job_runner._FakeConnection(
        [], executed, programming_error=programming_error, min_max={}
    )

    monkeypatch.setattr(job_runner.pymysql, "connect", lambda **kwargs: connection)
    monkeypatch.setattr(
        job_runner.ingest_excel, "_get_db_settings", lambda overrides=None: {"database": "db"}
    )
    monkeypatch.setattr(
        api.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default": {
            "overlap_target_table": "calendar",
            "time_range_column": "日期",
        },
    )
    monkeypatch.setattr(
        api.time_range_utils,
        "derive_ranges_from_workbook",
        lambda *args, **kwargs: [{"start": "2024-01-01", "end": "2024-01-31"}],
    )

    def fake_enqueue_job(**kwargs):
        return "job-456", SimpleNamespace(id="rq-job-456")

    monkeypatch.setattr(job_runner, "enqueue_job", fake_enqueue_job)
    job = _make_job("job-456")
    monkeypatch.setattr(job_store, "get_job", lambda job_id: job)

    client = TestClient(api.app)
    response = client.post(
        "/uploads",
        json={
            "workbook_path": "uploads/example.xlsx",
            "sheet": "SHEET",
            "source_year": "2024",
        },
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["job"]["job_id"] == "job-456"


def test_get_upload_detail_with_results(monkeypatch):
    job = _make_job()
    result = _make_result()

    monkeypatch.setattr(job_store, "get_job", lambda job_id: job)
    monkeypatch.setattr(job_store, "get_job_result", lambda job_id: result)

    client = TestClient(api.app)
    response = client.get(f"/uploads/{job.job_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["job"]["status"] == job.status
    assert payload["result"]["total_rows"] == result.total_rows
    assert payload["result"]["coverage_metadata"] == result.coverage_metadata


def test_get_upload_detail_missing_job(monkeypatch):
    monkeypatch.setattr(
        job_store,
        "get_job",
        lambda job_id: (_ for _ in ()).throw(
            ValueError(f"Upload job {job_id} not found")
        ),
    )

    client = TestClient(api.app)
    response = client.get("/uploads/missing")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_list_upload_events(monkeypatch):
    events = _make_events()
    monkeypatch.setattr(job_store, "list_job_events", lambda job_id, limit=None: events)

    client = TestClient(api.app)
    response = client.get("/uploads/job-123/events")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == len(events)
    assert payload[1]["status"] == "Parsing"


def test_list_recent_uploads(monkeypatch):
    jobs = [
        _make_job(
            "job-1",
            latest_message="Upload skipped due to overlapping records",
            processed_rows=0,
            rejected_rows=3,
        ),
        _make_job("job-2"),
    ]
    monkeypatch.setattr(job_store, "list_recent_jobs", lambda limit=20: jobs)

    client = TestClient(api.app)
    response = client.get("/uploads")

    assert response.status_code == 200
    payload = response.json()
    assert [job["job_id"] for job in payload] == ["job-1", "job-2"]
    assert payload[0]["latest_message"] == "Upload skipped due to overlapping records"
    assert payload[0]["rejected_rows"] == 3
