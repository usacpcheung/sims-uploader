from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_USER", "user")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "database")
os.environ.setdefault("DB_CHARSET", "utf8mb4")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fastapi.testclient import TestClient
import pandas as pd
import pytest

from io import BytesIO
from pathlib import Path

from app import api, config, job_runner, job_store
from app import storage


def _make_job(job_id: str = "job-123") -> job_store.UploadJob:
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


def test_config_preview_detects_headers(monkeypatch):
    client = TestClient(api.app)
    dataframe = pd.DataFrame(
        [
            {"raw_id": 1, "姓名": "Alice", "日期": "2024-01-01"},
            {"raw_id": 2, "姓名": "Bob", "日期": "2024-01-02"},
        ]
    )

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        dataframe.to_excel(writer, index=False, sheet_name="SheetA")
    buffer.seek(0)

    response = client.post(
        "/config/preview",
        data={"sheet": "SheetA", "row_limit": "5"},
        files={
            "workbook": (
                "sample.xlsx",
                buffer.getvalue(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["sheet"] == "SheetA"
    assert payload["headers"] == ["raw_id", "姓名", "日期"]
    assert payload["metadata_collisions"] == ["raw_id"]
    assert payload["suggested_required_columns"] == ["姓名", "日期"]
    assert payload["suggested_column_mappings"] == {"姓名": "姓名", "日期": "日期"}


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
    jobs = [_make_job("job-1"), _make_job("job-2")]
    monkeypatch.setattr(job_store, "list_recent_jobs", lambda limit=20: jobs)

    client = TestClient(api.app)
    response = client.get("/uploads")

    assert response.status_code == 200
    payload = response.json()
    assert [job["job_id"] for job in payload] == ["job-1", "job-2"]
