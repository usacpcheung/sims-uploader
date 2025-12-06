import datetime as dt
import logging
import os
from datetime import datetime
from types import SimpleNamespace

import pytest

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_USER", "user")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "database")
os.environ.setdefault("DB_CHARSET", "utf8mb4")

from app import job_runner, pipeline


class QueueStub:
    def __init__(self):
        self.enqueued = []

    def enqueue(self, func, job_id_arg, payload, **kwargs):
        rq_job_id = kwargs.get("job_id")
        self.enqueued.append(
            SimpleNamespace(func=func, args=(job_id_arg, payload), job_id=rq_job_id)
        )
        return SimpleNamespace(id=rq_job_id)


@pytest.fixture(autouse=True)
def load_env(monkeypatch):
    monkeypatch.setattr(job_runner, "_load_environment", lambda: None)


def test_enqueue_job_enqueues_when_within_limits(monkeypatch):
    queue = QueueStub()
    monkeypatch.setattr(job_runner, "get_queue", lambda: queue)

    created_job = SimpleNamespace(job_id="job-1")
    captured_kwargs = {}

    def fake_create_job(**kwargs):
        captured_kwargs.update(kwargs)
        return created_job

    monkeypatch.setattr(job_runner.job_store, "create_job", fake_create_job)
    mark_error_calls = []
    monkeypatch.setattr(job_runner.job_store, "mark_error", lambda *args, **kwargs: mark_error_calls.append((args, kwargs)))

    job_id, rq_job = job_runner.enqueue_job(
        workbook_path="/tmp/workbook.xlsx",
        sheet="SheetA",
        source_year="2024",
        file_size=1024,
        row_count=10,
        queue=queue,
    )

    assert job_id == "job-1"
    assert rq_job.id == "job-1"
    assert queue.enqueued and queue.enqueued[0].func is job_runner.process_job
    assert not mark_error_calls
    assert captured_kwargs["original_filename"] == "workbook.xlsx"


def test_enqueue_job_records_normalized_table(monkeypatch):
    queue = QueueStub()
    monkeypatch.setattr(job_runner, "get_queue", lambda: queue)

    created_job = SimpleNamespace(job_id="job-nt")
    monkeypatch.setattr(job_runner.job_store, "create_job", lambda **kwargs: created_job)
    monkeypatch.setattr(job_runner.job_store, "mark_error", lambda *args, **kwargs: None)

    recorded_tables: list[tuple[str, str | None]] = []

    def fake_record_results(job_id, normalized_table_name=None, **_):
        recorded_tables.append((job_id, normalized_table_name))

    monkeypatch.setattr(job_runner.job_store, "record_results", fake_record_results)

    job_runner.enqueue_job(
        workbook_path="/tmp/workbook.xlsx",
        sheet="SheetA",
        source_year="2024",
        queue=queue,
        normalized_table="normalized_table",
        overlap_target_table="target_table",
        time_range_column="period",
        time_ranges=[{"start": datetime(2024, 1, 1), "end": datetime(2024, 1, 2)}],
    )

    assert recorded_tables == [("job-nt", "normalized_table")]
    payload = queue.enqueued[0].args[1]
    assert payload["normalized_table"] == "normalized_table"
    assert payload["overlap_target_table"] == "target_table"
    assert payload["time_range_column"] == "period"


def test_enqueue_job_marks_error_when_limit_exceeded(monkeypatch):
    queue = QueueStub()
    monkeypatch.setattr(job_runner, "get_queue", lambda: queue)

    created_job = SimpleNamespace(job_id="job-2")
    monkeypatch.setattr(job_runner.job_store, "create_job", lambda **_: created_job)
    errors = []
    monkeypatch.setattr(
        job_runner.job_store,
        "mark_error",
        lambda job_id, message=None, db_settings=None: errors.append((job_id, message)),
    )

    with pytest.raises(job_runner.UploadLimitExceeded):
        job_runner.enqueue_job(
            workbook_path="/tmp/workbook.xlsx",
            sheet="SheetA",
            source_year="2024",
            file_size=10,
            queue=queue,
            max_file_size=5,
        )

    assert not queue.enqueued
    assert errors and errors[0][0] == "job-2"
    assert "exceeds" in errors[0][1]


def test_enqueue_job_extracts_original_from_stored_path(monkeypatch):
    queue = QueueStub()
    monkeypatch.setattr(job_runner, "get_queue", lambda: queue)

    created_job = SimpleNamespace(job_id="job-3")
    captured_kwargs = {}

    def fake_create_job(**kwargs):
        captured_kwargs.update(kwargs)
        return created_job

    monkeypatch.setattr(job_runner.job_store, "create_job", fake_create_job)
    monkeypatch.setattr(job_runner.job_store, "mark_error", lambda *args, **kwargs: None)

    stored_path = "uploads/abcdef1234567890__example.xlsx"

    job_runner.enqueue_job(
        workbook_path=stored_path,
        sheet="SheetA",
        source_year="2024",
        queue=queue,
    )

    assert captured_kwargs["original_filename"] == "example.xlsx"


def _setup_job_store_spies(monkeypatch):
    calls = {"parsing": [], "validating": [], "record_results": [], "mark_loaded": [], "mark_error": [], "save_rejected": []}

    monkeypatch.setattr(
        job_runner.job_store,
        "get_job",
        lambda job_id, db_settings=None: SimpleNamespace(
            job_id=job_id,
            created_at=datetime(2024, 1, 1),
        ),
    )

    monkeypatch.setattr(
        job_runner.job_store,
        "mark_parsing",
        lambda job_id, message=None, db_settings=None: calls["parsing"].append((job_id, message)),
    )
    monkeypatch.setattr(
        job_runner.job_store,
        "mark_validating",
        lambda job_id, message=None, db_settings=None: calls["validating"].append((job_id, message)),
    )
    monkeypatch.setattr(
        job_runner.job_store,
        "record_results",
        lambda *args, **kwargs: calls["record_results"].append((args, kwargs)),
    )
    monkeypatch.setattr(
        job_runner.job_store,
        "mark_loaded",
        lambda job_id, message=None, db_settings=None: calls["mark_loaded"].append((job_id, message)),
    )
    monkeypatch.setattr(
        job_runner.job_store,
        "mark_error",
        lambda job_id, message=None, db_settings=None: calls["mark_error"].append((job_id, message)),
    )
    monkeypatch.setattr(
        job_runner.job_store,
        "save_rejected_rows_path",
        lambda job_id, path, db_settings=None: calls["save_rejected"].append((job_id, path)),
    )

    return calls


def _make_result(**overrides):
    base = dict(
        file_hash="hash",
        staging_table="staging",
        normalized_table="normalized",
        staged_rows=10,
        normalized_rows=8,
        rejected_rows=2,
        batch_id="batch",  # noqa: A003 - keep descriptive name
        ingested_at=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        processed_at=dt.datetime(2024, 1, 1, 1, tzinfo=dt.timezone.utc),
        column_coverage={"col": ["col"]},
        inserted_count=8,
        updated_count=0,
        rejected_rows_path="/tmp/rejected.csv",
        validation_errors=[],
        skipped=False,
    )
    base.update(overrides)
    return pipeline.PipelineResult(**base)


def test_process_job_success(monkeypatch):
    calls = _setup_job_store_spies(monkeypatch)

    def fake_run(*args, **kwargs):
        notifier = kwargs["status_notifier"]
        notifier("Parsing", None)
        notifier("Validating", None)
        return _make_result()

    monkeypatch.setattr(job_runner.pipeline, "run_pipeline", fake_run)

    result = job_runner.process_job(
        "job-1",
        {"workbook_path": "workbook.xlsx", "sheet": "SheetA", "source_year": "2024"},
    )

    assert isinstance(result, pipeline.PipelineResult)
    assert calls["parsing"] == [("job-1", None)]
    assert calls["validating"] == [("job-1", None)]
    assert calls["mark_loaded"] == [("job-1", None)]
    assert not calls["mark_error"]
    assert len(calls["record_results"]) == 1
    assert calls["save_rejected"] == [("job-1", "/tmp/rejected.csv")]


def test_process_job_validation_errors(monkeypatch):
    calls = _setup_job_store_spies(monkeypatch)

    def fake_run(*args, **kwargs):
        notifier = kwargs["status_notifier"]
        notifier("Parsing", None)
        notifier("Validating", None)
        return _make_result(validation_errors=["Row 1 missing id"], rejected_rows_path=None)

    monkeypatch.setattr(job_runner.pipeline, "run_pipeline", fake_run)

    job_runner.process_job(
        "job-2",
        {"workbook_path": "workbook.xlsx", "sheet": "SheetA", "source_year": "2024"},
    )

    assert calls["parsing"] == [("job-2", None)]
    assert calls["validating"] == [("job-2", None)]
    assert not calls["mark_loaded"]
    assert calls["mark_error"] and "Row 1 missing id" in calls["mark_error"][0][1]
    assert len(calls["record_results"]) == 1


def test_process_job_hard_failure(monkeypatch):
    calls = _setup_job_store_spies(monkeypatch)

    partial_result = _make_result(validation_errors=["boom"], rejected_rows_path=None)

    def fake_run(*args, **kwargs):
        notifier = kwargs["status_notifier"]
        notifier("Parsing", None)
        raise pipeline.PipelineExecutionError("pipeline failed", result=partial_result)

    monkeypatch.setattr(job_runner.pipeline, "run_pipeline", fake_run)

    with pytest.raises(pipeline.PipelineExecutionError):
        job_runner.process_job(
            "job-3",
            {"workbook_path": "workbook.xlsx", "sheet": "SheetA", "source_year": "2024"},
        )

    assert calls["parsing"] == [("job-3", None)]
    assert not calls["validating"]
    assert not calls["mark_loaded"]
    assert calls["mark_error"] and "boom" in calls["mark_error"][0][1]
    assert len(calls["record_results"]) == 1


class _FakeCursor:
    def __init__(self, rows, executed, *, table_exists=True, programming_error=None):
        self.rows = rows
        self.executed = executed
        self.table_exists = table_exists
        self.programming_error = programming_error
        self._fetchone_result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def execute(self, query, params):
        if self.programming_error:
            raise self.programming_error
        self.executed.append((query, params))
        if "information_schema.tables" in query:
            self._fetchone_result = (1,) if self.table_exists else None
        else:
            self._fetchone_result = None

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self._fetchone_result


class _FakeConnection:
    def __init__(self, rows, executed, *, table_exists=True, programming_error=None):
        self.rows = rows
        self.executed = executed
        self.closed = False
        self.table_exists = table_exists
        self.programming_error = programming_error

    def cursor(self, cursor_class=None):
        return _FakeCursor(
            self.rows,
            self.executed,
            table_exists=self.table_exists,
            programming_error=self.programming_error,
        )

    def close(self):
        self.closed = True


def test_check_time_overlap_returns_matches(monkeypatch):
    executed: list[tuple[str, tuple[object, ...]]] = []
    rows = [
        {
            "id": 1,
            "range_start": datetime(2024, 1, 5),
            "range_end": datetime(2024, 1, 9),
        }
    ]

    connection = _FakeConnection(rows, executed)
    monkeypatch.setattr(job_runner.pymysql, "connect", lambda **kwargs: connection)
    monkeypatch.setattr(
        job_runner.ingest_excel, "_get_db_settings", lambda overrides=None: {"database": "db"}
    )

    overlaps = job_runner.check_time_overlap(
        workbook_type="demo",
        target_table="calendar",
        time_range_column="period",
        time_ranges=[{"start": datetime(2024, 1, 1), "end": datetime(2024, 1, 10)}],
    )

    assert connection.closed
    assert executed and "`period_start`" in executed[0][0]
    assert overlaps and overlaps[0]["record_id"] == 1
    assert overlaps[0]["existing_end"] == rows[0]["range_end"]


def test_check_time_overlap_allows_unicode_identifier(monkeypatch):
    executed: list[tuple[str, tuple[object, ...]]] = []
    connection = _FakeConnection([], executed)
    monkeypatch.setattr(job_runner.pymysql, "connect", lambda **kwargs: connection)
    monkeypatch.setattr(
        job_runner.ingest_excel, "_get_db_settings", lambda overrides=None: {"database": "db"}
    )

    overlaps = job_runner.check_time_overlap(
        workbook_type="demo",
        target_table="计划表",
        time_range_column="日期",
        time_ranges=[{"start": datetime(2024, 1, 1), "end": datetime(2024, 1, 2)}],
    )

    assert connection.closed
    assert executed and "`日期_start`" in executed[0][0]
    assert overlaps == []


def test_check_time_overlap_rejects_unsafe_identifier():
    with pytest.raises(ValueError):
        job_runner.check_time_overlap(
            workbook_type="demo",
            target_table="calendar;DROP",
            time_range_column="period",
            time_ranges=[{"start": datetime(2024, 1, 1), "end": datetime(2024, 1, 10)}],
        )


def test_check_time_overlap_handles_missing_table(monkeypatch, caplog):
    executed: list[tuple[str, tuple[object, ...]]] = []
    connection = _FakeConnection([], executed, table_exists=False)
    monkeypatch.setattr(job_runner.pymysql, "connect", lambda **kwargs: connection)
    monkeypatch.setattr(
        job_runner.ingest_excel, "_get_db_settings", lambda overrides=None: {"database": "db"}
    )

    with caplog.at_level(logging.INFO):
        overlaps = job_runner.check_time_overlap(
            workbook_type="demo",
            target_table="calendar",
            time_range_column="period",
            time_ranges=[{"start": datetime(2024, 1, 1), "end": datetime(2024, 1, 10)}],
        )

    assert connection.closed
    assert overlaps == []
    assert any("overlap check" in message.lower() for message in caplog.messages)
