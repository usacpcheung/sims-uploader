import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest

from app import job_store


class CursorStub:
    def __init__(
        self,
        executed,
        *,
        rowcount=1,
        fetchone_results=None,
        fetchall_results=None,
        lastrowid=0,
        exception=None,
    ):
        self.executed = executed
        self.rowcount = rowcount
        self.lastrowid = lastrowid
        self._fetchone_results = list(fetchone_results or [])
        self._fetchall_results = list(fetchall_results or [])
        self._exception = exception

    def __enter__(self):
        if self._exception:
            raise self._exception
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        if self._exception:
            raise self._exception
        self.executed.append((query, tuple(params or ())))

    def fetchone(self):
        if self._exception:
            raise self._exception
        if self._fetchone_results:
            return self._fetchone_results.pop(0)
        return None

    def fetchall(self):
        if self._exception:
            raise self._exception
        results = list(self._fetchall_results)
        self._fetchall_results.clear()
        return results


class ConnectionStub:
    def __init__(self, cursors):
        self._cursors = {key: list(value) for key, value in cursors.items()}
        self.begun = False
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self, cursor_class=None):
        key = cursor_class
        cursor_list = self._cursors.get(key)
        if not cursor_list:
            raise AssertionError(f"No cursor prepared for {cursor_class!r}")
        return cursor_list.pop(0)

    def begin(self):
        self.begun = True

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def patch_db_settings(monkeypatch):
    monkeypatch.setattr(job_store, "get_db_settings", lambda: {})


def _set_connection(monkeypatch, connection):
    monkeypatch.setattr(job_store, "_connect", lambda overrides=None: connection)


def test_get_job_fetches_single_job(monkeypatch):
    job_id = "job"
    executed_dict = []
    job_row = {
        "job_id": job_id,
        "original_filename": "file.csv",
        "workbook_name": None,
        "worksheet_name": None,
        "file_size": None,
        "status": "pending",
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "updated_at": datetime(2024, 1, 1, 12, 1, 0),
    }
    connection = ConnectionStub(
        {
            job_store.pymysql.cursors.DictCursor: [
                CursorStub(executed_dict, fetchone_results=[job_row])
            ]
        }
    )
    _set_connection(monkeypatch, connection)

    job = job_store.get_job(job_id)

    assert executed_dict == [("SELECT * FROM `upload_jobs` WHERE job_id = %s", (job_id,))]
    assert job == job_store.UploadJob(**job_row)
    assert connection.closed is True


def test_get_job_missing_raises(monkeypatch):
    executed_dict = []
    connection = ConnectionStub(
        {job_store.pymysql.cursors.DictCursor: [CursorStub(executed_dict)]}
    )
    _set_connection(monkeypatch, connection)

    with pytest.raises(ValueError) as excinfo:
        job_store.get_job("missing")

    assert "Upload job missing not found" in str(excinfo.value)
    assert executed_dict == [("SELECT * FROM `upload_jobs` WHERE job_id = %s", ("missing",))]


def test_list_job_events_orders_and_limits(monkeypatch):
    job_id = "job"
    executed_dict = []
    event_rows = [
        {
            "event_id": 1,
            "job_id": job_id,
            "status": "Pending",
            "message": "queued",
            "event_at": datetime(2024, 1, 1, 12, 0, 0),
        },
        {
            "event_id": 2,
            "job_id": job_id,
            "status": "Parsing",
            "message": "started",
            "event_at": datetime(2024, 1, 1, 12, 5, 0),
        },
    ]
    connection = ConnectionStub(
        {
            job_store.pymysql.cursors.DictCursor: [
                CursorStub(
                    executed_dict,
                    fetchone_results=[{"exists": 1}],
                    fetchall_results=event_rows,
                )
            ]
        }
    )
    _set_connection(monkeypatch, connection)

    events = job_store.list_job_events(job_id, limit=5)

    assert executed_dict == [
        ("SELECT 1 FROM `upload_jobs` WHERE job_id = %s", (job_id,)),
        (
            "SELECT * FROM `upload_job_events` WHERE job_id = %s ORDER BY `event_at` ASC, `event_id` ASC LIMIT %s",
            (job_id, 5),
        ),
    ]
    assert events == [job_store.UploadJobEvent(**row) for row in event_rows]


def test_list_job_events_missing_job(monkeypatch):
    executed_dict = []
    connection = ConnectionStub(
        {job_store.pymysql.cursors.DictCursor: [CursorStub(executed_dict)]}
    )
    _set_connection(monkeypatch, connection)

    with pytest.raises(ValueError) as excinfo:
        job_store.list_job_events("missing")

    assert "Upload job missing not found" in str(excinfo.value)
    assert executed_dict == [("SELECT 1 FROM `upload_jobs` WHERE job_id = %s", ("missing",))]


def test_get_job_result_fetches(monkeypatch):
    job_id = "job"
    executed_dict = []
    result_row = {
        "job_id": job_id,
        "total_rows": 10,
        "processed_rows": 9,
        "successful_rows": 8,
        "rejected_rows": 1,
        "normalized_table_name": "table",
        "rejected_rows_path": None,
        "coverage_metadata": None,
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "updated_at": datetime(2024, 1, 1, 12, 5, 0),
    }
    connection = ConnectionStub(
        {
            job_store.pymysql.cursors.DictCursor: [
                CursorStub(executed_dict, fetchone_results=[result_row])
            ]
        }
    )
    _set_connection(monkeypatch, connection)

    result = job_store.get_job_result(job_id)

    assert executed_dict == [
        ("SELECT * FROM `upload_job_results` WHERE job_id = %s", (job_id,))
    ]
    assert result == job_store.UploadJobResult(**result_row)


def test_get_job_result_missing(monkeypatch):
    executed_dict = []
    connection = ConnectionStub(
        {job_store.pymysql.cursors.DictCursor: [CursorStub(executed_dict)]}
    )
    _set_connection(monkeypatch, connection)

    with pytest.raises(ValueError) as excinfo:
        job_store.get_job_result("missing")

    assert "Upload job results for missing not found" in str(excinfo.value)
    assert executed_dict == [
        ("SELECT * FROM `upload_job_results` WHERE job_id = %s", ("missing",))
    ]


def test_list_recent_jobs_returns_latest(monkeypatch):
    executed_dict = []
    job_rows = [
        {
            "job_id": "b",
            "original_filename": "b.csv",
            "workbook_name": None,
            "worksheet_name": None,
            "file_size": None,
            "status": "pending",
            "created_at": datetime(2024, 1, 1, 12, 5, 0),
            "updated_at": datetime(2024, 1, 1, 12, 6, 0),
        },
        {
            "job_id": "a",
            "original_filename": "a.csv",
            "workbook_name": None,
            "worksheet_name": None,
            "file_size": None,
            "status": "pending",
            "created_at": datetime(2024, 1, 1, 12, 0, 0),
            "updated_at": datetime(2024, 1, 1, 12, 1, 0),
        },
    ]
    connection = ConnectionStub(
        {
            job_store.pymysql.cursors.DictCursor: [
                CursorStub(executed_dict, fetchall_results=job_rows)
            ]
        }
    )
    _set_connection(monkeypatch, connection)

    jobs = job_store.list_recent_jobs(limit=2)

    assert executed_dict == [
        (
            "SELECT * FROM `upload_jobs` ORDER BY `created_at` DESC, `job_id` DESC LIMIT %s",
            (2,),
        )
    ]
    assert jobs == [job_store.UploadJob(**row) for row in job_rows]


def test_list_recent_jobs_requires_positive_limit(monkeypatch):
    _set_connection(monkeypatch, ConnectionStub({}))

    with pytest.raises(ValueError) as excinfo:
        job_store.list_recent_jobs(limit=0)

    assert "limit must be positive" in str(excinfo.value)


def test_create_job_inserts_and_returns_job(monkeypatch):
    job_id = "12345678-1234-5678-1234-567812345678"
    executed_default = []
    executed_dict = []
    job_row = {
        "job_id": job_id,
        "original_filename": "file.csv",
        "workbook_name": None,
        "worksheet_name": None,
        "file_size": None,
        "status": "pending",
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "updated_at": datetime(2024, 1, 1, 12, 5, 0),
    }
    connection = ConnectionStub(
        {
            None: [CursorStub(executed_default)],
            job_store.pymysql.cursors.DictCursor: [CursorStub(executed_dict, fetchone_results=[job_row])],
        }
    )
    _set_connection(monkeypatch, connection)
    monkeypatch.setattr(job_store.uuid, "uuid4", lambda: job_id)

    result = job_store.create_job(
        original_filename="file.csv",
        status="pending",
        status_message="queued",
    )

    assert connection.begun is True
    assert connection.committed is True
    assert connection.rolled_back is False
    assert connection.closed is True

    assert executed_default == [
        (
            "INSERT INTO `upload_jobs` (job_id, original_filename, workbook_name, worksheet_name, file_size, status) VALUES (%s, %s, %s, %s, %s, %s)",
            (job_id, "file.csv", None, None, None, "pending"),
        ),
        (
            "INSERT INTO `upload_job_events` (job_id, status, message) VALUES (%s, %s, %s)",
            (job_id, "pending", "queued"),
        ),
    ]
    assert executed_dict == [
        ("SELECT * FROM `upload_jobs` WHERE job_id = %s", (job_id,)),
    ]
    assert result == job_store.UploadJob(**job_row)


def test_create_job_duplicate_rolls_back(monkeypatch):
    executed_default = []
    integrity_error = job_store.pymysql_err.IntegrityError(1062, "Duplicate entry")
    connection = ConnectionStub({None: [CursorStub(executed_default, exception=integrity_error)]})
    _set_connection(monkeypatch, connection)

    with pytest.raises(ValueError) as excinfo:
        job_store.create_job(original_filename="file.csv", status="pending")

    assert "Failed to create upload job" in str(excinfo.value)
    assert connection.committed is False
    assert connection.rolled_back is True
    assert executed_default == []


def test_set_status_updates_and_logs_event(monkeypatch):
    job_id = "abc"
    executed_default = []
    executed_dict = []
    job_row = {
        "job_id": job_id,
        "original_filename": "file.csv",
        "workbook_name": None,
        "worksheet_name": None,
        "file_size": None,
        "status": "done",
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "updated_at": datetime(2024, 1, 1, 12, 10, 0),
    }
    event_row = {
        "event_id": 5,
        "job_id": job_id,
        "status": "done",
        "message": "complete",
        "event_at": datetime(2024, 1, 1, 12, 10, 0),
    }
    connection = ConnectionStub(
        {
            None: [CursorStub(executed_default, rowcount=1, lastrowid=5)],
            job_store.pymysql.cursors.DictCursor: [
                CursorStub(executed_dict, fetchone_results=[job_row, event_row])
            ],
        }
    )
    _set_connection(monkeypatch, connection)

    job, event = job_store.set_status(job_id, "done", message="complete")

    assert executed_default == [
        ("UPDATE `upload_jobs` SET status = %s WHERE job_id = %s", ("done", job_id)),
        (
            "INSERT INTO `upload_job_events` (job_id, status, message) VALUES (%s, %s, %s)",
            (job_id, "done", "complete"),
        ),
    ]
    assert executed_dict == [
        ("SELECT * FROM `upload_jobs` WHERE job_id = %s", (job_id,)),
        ("SELECT * FROM `upload_job_events` WHERE event_id = %s", (5,)),
    ]
    assert job == job_store.UploadJob(**job_row)
    assert event == job_store.UploadJobEvent(**event_row)
    assert connection.committed is True
    assert connection.rolled_back is False


def test_set_status_missing_job_rolls_back(monkeypatch):
    executed_default = []
    connection = ConnectionStub({None: [CursorStub(executed_default, rowcount=0)]})
    _set_connection(monkeypatch, connection)

    with pytest.raises(ValueError) as excinfo:
        job_store.set_status("missing", "done")

    assert "does not exist" in str(excinfo.value)
    assert executed_default == [
        ("UPDATE `upload_jobs` SET status = %s WHERE job_id = %s", ("done", "missing")),
        ("SELECT 1 FROM `upload_jobs` WHERE job_id = %s", ("missing",)),
    ]
    assert connection.committed is False
    assert connection.rolled_back is True


def test_set_status_noop_update_succeeds(monkeypatch):
    job_id = "job"
    executed_default = []
    executed_dict = []
    job_row = {
        "job_id": job_id,
        "original_filename": "file.csv",
        "workbook_name": None,
        "worksheet_name": None,
        "file_size": None,
        "status": "pending",
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "updated_at": datetime(2024, 1, 1, 12, 5, 0),
    }
    event_row = {
        "event_id": 5,
        "job_id": job_id,
        "status": "pending",
        "message": "still pending",
        "event_at": datetime(2024, 1, 1, 12, 10, 0),
    }
    connection = ConnectionStub(
        {
            None: [
                CursorStub(
                    executed_default,
                    rowcount=0,
                    fetchone_results=[(1,)],
                    lastrowid=5,
                )
            ],
            job_store.pymysql.cursors.DictCursor: [
                CursorStub(
                    executed_dict,
                    fetchone_results=[job_row, event_row],
                )
            ],
        }
    )
    _set_connection(monkeypatch, connection)

    job, event = job_store.set_status(job_id, "pending", message="still pending")

    assert executed_default == [
        ("UPDATE `upload_jobs` SET status = %s WHERE job_id = %s", ("pending", job_id)),
        ("SELECT 1 FROM `upload_jobs` WHERE job_id = %s", (job_id,)),
        (
            "INSERT INTO `upload_job_events` (job_id, status, message) VALUES (%s, %s, %s)",
            (job_id, "pending", "still pending"),
        ),
    ]
    assert executed_dict == [
        ("SELECT * FROM `upload_jobs` WHERE job_id = %s", (job_id,)),
        ("SELECT * FROM `upload_job_events` WHERE event_id = %s", (5,)),
    ]
    assert job == job_store.UploadJob(**job_row)
    assert event == job_store.UploadJobEvent(**event_row)
    assert connection.committed is True
    assert connection.rolled_back is False


def test_record_results_upserts_and_serializes(monkeypatch):
    job_id = "job"
    executed_default = []
    executed_dict = []
    result_row = {
        "job_id": job_id,
        "total_rows": 10,
        "processed_rows": 9,
        "successful_rows": 8,
        "rejected_rows": 1,
        "normalized_table_name": "table",
        "rejected_rows_path": "path.csv",
        "coverage_metadata": {"a": 1},
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "updated_at": datetime(2024, 1, 1, 12, 5, 0),
    }
    connection = ConnectionStub(
        {
            None: [CursorStub(executed_default)],
            job_store.pymysql.cursors.DictCursor: [CursorStub(executed_dict, fetchone_results=[result_row])],
        }
    )
    _set_connection(monkeypatch, connection)

    result = job_store.record_results(
        job_id,
        total_rows=10,
        processed_rows=9,
        successful_rows=8,
        rejected_rows=1,
        normalized_table_name="table",
        coverage_metadata={"a": 1},
    )

    expected_query = (
        "INSERT INTO `upload_job_results` (job_id, `total_rows`, `processed_rows`, `successful_rows`, "
        "`rejected_rows`, `normalized_table_name`, `coverage_metadata`) VALUES (%s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE `total_rows` = VALUES(`total_rows`), `processed_rows` = VALUES(`processed_rows`), "
        "`successful_rows` = VALUES(`successful_rows`), `rejected_rows` = VALUES(`rejected_rows`), "
        "`normalized_table_name` = VALUES(`normalized_table_name`), `coverage_metadata` = VALUES(`coverage_metadata`), "
        "`updated_at` = CURRENT_TIMESTAMP"
    )
    assert executed_default == [
        (
            expected_query,
            (
                job_id,
                10,
                9,
                8,
                1,
                "table",
                json.dumps({"a": 1}),
            ),
        )
    ]
    assert executed_dict == [
        ("SELECT * FROM `upload_job_results` WHERE job_id = %s", (job_id,)),
    ]
    assert result == job_store.UploadJobResult(**result_row)
    assert connection.committed is True
    assert connection.rolled_back is False


def test_record_results_integrity_error(monkeypatch):
    executed_default = []
    integrity_error = job_store.pymysql_err.IntegrityError(1062, "Duplicate entry")
    connection = ConnectionStub({None: [CursorStub(executed_default, exception=integrity_error)]})
    _set_connection(monkeypatch, connection)

    with pytest.raises(ValueError) as excinfo:
        job_store.record_results("job")

    assert "Failed to record results" in str(excinfo.value)
    assert connection.committed is False
    assert connection.rolled_back is True


def test_save_rejected_rows_path_updates(monkeypatch):
    job_id = "job"
    executed_default = []
    executed_dict = []
    result_row = {
        "job_id": job_id,
        "total_rows": None,
        "processed_rows": None,
        "successful_rows": None,
        "rejected_rows": None,
        "normalized_table_name": None,
        "rejected_rows_path": "rejected.csv",
        "coverage_metadata": None,
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "updated_at": datetime(2024, 1, 1, 12, 5, 0),
    }
    connection = ConnectionStub(
        {
            None: [CursorStub(executed_default, rowcount=1)],
            job_store.pymysql.cursors.DictCursor: [CursorStub(executed_dict, fetchone_results=[result_row])],
        }
    )
    _set_connection(monkeypatch, connection)

    result = job_store.save_rejected_rows_path(job_id, "rejected.csv")

    assert executed_default == [
        (
            "UPDATE `upload_job_results` SET rejected_rows_path = %s, updated_at = CURRENT_TIMESTAMP WHERE job_id = %s",
            ("rejected.csv", job_id),
        )
    ]
    assert executed_dict == [
        ("SELECT * FROM `upload_job_results` WHERE job_id = %s", (job_id,)),
    ]
    assert result == job_store.UploadJobResult(**result_row)
    assert connection.committed is True
    assert connection.rolled_back is False


def test_save_rejected_rows_path_without_results(monkeypatch):
    executed_default = []
    connection = ConnectionStub({None: [CursorStub(executed_default, rowcount=0)]})
    _set_connection(monkeypatch, connection)

    with pytest.raises(ValueError) as excinfo:
        job_store.save_rejected_rows_path("job", "rejected.csv")

    assert "Cannot save rejected rows path" in str(excinfo.value)
    assert executed_default == [
        (
            "UPDATE `upload_job_results` SET rejected_rows_path = %s, updated_at = CURRENT_TIMESTAMP WHERE job_id = %s",
            ("rejected.csv", "job"),
        )
    ]
    assert connection.committed is False
    assert connection.rolled_back is True
