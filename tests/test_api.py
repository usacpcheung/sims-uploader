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

from app import api, job_runner, job_store


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


def test_create_upload_success(monkeypatch):
    job = _make_job()

    def fake_enqueue_job(**kwargs):
        assert kwargs["workbook_path"] == "uploads/example.xlsx"
        assert kwargs["sheet"] == "SHEET"
        assert kwargs["source_year"] == "2024"
        assert kwargs["file_size"] == 2048
        assert kwargs["row_count"] == 123
        return job.job_id, object()

    monkeypatch.setattr(job_runner, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(job_store, "get_job", lambda job_id: job)

    client = TestClient(api.app)
    response = client.post(
        "/uploads",
        json={
            "workbook_path": "uploads/example.xlsx",
            "sheet": "SHEET",
            "source_year": "2024",
            "file_size": 2048,
            "row_count": 123,
        },
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["job"]["job_id"] == job.job_id
    assert payload["job"]["original_filename"] == job.original_filename


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
        lambda job_id: (_ for _ in ()).throw(ValueError(f"Upload job {job_id} not found")),
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
