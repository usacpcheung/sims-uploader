from __future__ import annotations

from collections import OrderedDict
import datetime as dt
from collections import OrderedDict
from types import SimpleNamespace

import pymysql
import pytest

from app import ingest_excel, pipeline


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows):
        self._rows = rows
        self.cursor_calls = []
        self.begun = False
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self, cursor_class=None):
        self.cursor_calls.append(cursor_class)
        return _Cursor(self._rows)

    def begin(self):
        self.begun = True

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def test_run_pipeline_threads_file_hash(monkeypatch):
    csv_path = "/tmp/fake.csv"
    file_hash = "hash-123"
    staged_rows = [
        {
            "id": 1,
            "file_hash": file_hash,
            "batch_id": "batch-1",
            "source_year": "2024",
        },
        {
            "id": 2,
            "file_hash": file_hash,
            "batch_id": "batch-1",
            "source_year": "2024",
        },
    ]
    staging_result = ingest_excel.StagingLoadResult(
        staging_table="teach_record_raw",
        file_hash=file_hash,
        batch_id="batch-1",
        source_year="2024",
        ingested_at=dt.datetime(2024, 5, 1, 12, 0, tzinfo=dt.timezone.utc),
        rowcount=len(staged_rows),
    )

    captured = SimpleNamespace(prep_call=None, mark_call=None)

    def fake_prep_main(workbook, sheet, *, workbook_type, emit_stdout, db_settings):
        captured.prep_call = {
            "workbook": workbook,
            "sheet": sheet,
            "workbook_type": workbook_type,
            "emit_stdout": emit_stdout,
            "db_settings": db_settings,
        }
        return csv_path, file_hash

    monkeypatch.setattr(pipeline.prep_excel, "main", fake_prep_main)
    monkeypatch.setattr(
        pipeline.ingest_excel,
        "load_csv_into_staging",
        lambda *args, **kwargs: staging_result,
    )
    monkeypatch.setattr(
        pipeline.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default", db_settings=None: {
            "table": "teach_record_raw",
            "normalized_table": "teach_record_normalized",
            "column_mappings": {"姓名": "姓名"},
        },
    )

    connection = _Connection(staged_rows)
    monkeypatch.setattr(pipeline.pymysql, "connect", lambda **kwargs: connection)

    inserted = {}

    def fake_insert(
        connection_obj,
        table,
        rows,
        column_mappings=None,
        *,
        prepared,
    ):
        inserted["table"] = table
        inserted["prepared"] = prepared
        return pipeline.normalize_staging.InsertNormalizedRowsResult(
            inserted_count=len(prepared.normalized_rows),
            prepared=prepared,
        )

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "insert_normalized_rows",
        fake_insert,
    )
    monkeypatch.setattr(
        pipeline.normalize_staging,
        "ensure_normalized_schema",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        pipeline.validation,
        "validate_rows",
        lambda job_id, prepared, **kwargs: pipeline.validation.ValidationResult(
            prepared=prepared,
            rejected_rows_path=None,
            errors=[],
        ),
    )

    def fake_mark(connection_obj, table, row_ids, *, file_hash):
        captured.mark_call = {
            "table": table,
            "row_ids": tuple(row_ids),
            "file_hash": file_hash,
        }
        return dt.datetime(2024, 5, 1, 12, 30, tzinfo=dt.timezone.utc)

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "mark_staging_rows_processed",
        fake_mark,
    )

    result = pipeline.run_pipeline(
        "workbook.xlsx", source_year="2024", batch_id="batch-1"
    )

    assert captured.prep_call["emit_stdout"] is False
    assert captured.prep_call["workbook"] == "workbook.xlsx"
    assert captured.prep_call["workbook_type"] == "default"
    assert inserted["table"] == "teach_record_normalized"
    assert len(inserted["prepared"].normalized_rows) == len(staged_rows)
    assert captured.mark_call["file_hash"] == file_hash
    assert captured.mark_call["row_ids"] == (1, 2)
    assert result.file_hash == file_hash
    assert result.staged_rows == len(staged_rows)
    assert result.normalized_rows == len(staged_rows)
    assert result.inserted_count == len(staged_rows)
    assert result.batch_id == "batch-1"
    assert result.staging_table == "teach_record_raw"
    assert result.normalized_table == "teach_record_normalized"
    assert result.rejected_rows_path is None
    assert result.validation_errors == []
    assert not result.skipped
    assert connection.committed
    assert not connection.rolled_back
    assert connection.closed


def test_run_pipeline_passes_normalization_overrides(monkeypatch):
    csv_path = "/tmp/fake.csv"
    file_hash = "hash-override"
    staged_rows = [
        {
            "id": 1,
            "file_hash": file_hash,
            "batch_id": "batch-override",
            "source_year": "2024",
            "custom_meta": "meta",
        }
    ]
    staging_result = ingest_excel.StagingLoadResult(
        staging_table="teach_record_raw",
        file_hash=file_hash,
        batch_id="batch-override",
        source_year="2024",
        ingested_at=dt.datetime(2024, 7, 1, 9, tzinfo=dt.timezone.utc),
        rowcount=len(staged_rows),
    )

    metadata_override = ("custom_meta", "raw_id", "file_hash")
    reserved_override = frozenset({"id", "processed_at", "skip_me"})
    type_overrides = {"Custom": "JSON NULL"}

    monkeypatch.setattr(
        pipeline.prep_excel,
        "main",
        lambda *args, **kwargs: (csv_path, file_hash),
    )
    monkeypatch.setattr(
        pipeline.ingest_excel,
        "load_csv_into_staging",
        lambda *args, **kwargs: staging_result,
    )
    monkeypatch.setattr(
        pipeline.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default", db_settings=None: {
            "table": "teach_record_raw",
            "normalized_table": "teach_record_normalized",
            "column_mappings": {"Custom": "Custom"},
            "column_types": {},
            "normalized_metadata_columns": metadata_override,
            "reserved_source_columns": reserved_override,
            "normalized_column_type_overrides": type_overrides,
        },
    )

    connection = _Connection(staged_rows)
    monkeypatch.setattr(pipeline.pymysql, "connect", lambda **kwargs: connection)

    captured = {}

    prepared = pipeline.normalize_staging.PreparedNormalization(
        normalized_rows=[("meta", 1, "hash", "value")],
        rejected_rows=[],
        resolved_mappings=OrderedDict({"Custom": "Custom"}),
        metadata_columns=metadata_override,
        ordered_columns=metadata_override + ("Custom",),
    )

    def fake_prepare(rows, column_mappings, **kwargs):
        captured["prepare"] = kwargs
        return prepared

    def fake_ensure(connection_obj, table, mappings, column_types, **kwargs):
        captured["ensure"] = kwargs
        return True

    def fake_insert(
        connection_obj,
        table,
        rows,
        column_mappings=None,
        *,
        prepared: pipeline.normalize_staging.PreparedNormalization,
    ):
        captured["insert"] = {"prepared": prepared}
        return pipeline.normalize_staging.InsertNormalizedRowsResult(
            inserted_count=len(prepared.normalized_rows),
            prepared=prepared,
        )

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "prepare_normalization",
        fake_prepare,
    )
    monkeypatch.setattr(
        pipeline.normalize_staging,
        "ensure_normalized_schema",
        fake_ensure,
    )
    monkeypatch.setattr(
        pipeline.normalize_staging,
        "insert_normalized_rows",
        fake_insert,
    )
    monkeypatch.setattr(
        pipeline.normalize_staging,
        "mark_staging_rows_processed",
        lambda *args, **kwargs: dt.datetime(2024, 7, 1, 10, tzinfo=dt.timezone.utc),
    )
    monkeypatch.setattr(
        pipeline.validation,
        "validate_rows",
        lambda job_id, prep, **kwargs: pipeline.validation.ValidationResult(
            prepared=prep,
            rejected_rows_path=None,
            errors=[],
        ),
    )

    result = pipeline.run_pipeline(
        "workbook.xlsx", source_year="2024", batch_id="batch-override"
    )

    assert captured["prepare"]["metadata_columns"] == metadata_override
    assert captured["prepare"]["reserved_source_columns"] == reserved_override
    assert captured["ensure"]["metadata_columns"] == metadata_override
    assert captured["ensure"]["column_type_overrides"] == type_overrides
    assert captured["insert"]["prepared"] is prepared
    assert result.normalized_rows == len(staged_rows)
    assert connection.committed


def test_run_pipeline_normalizes_zero_date_rows(monkeypatch):
    csv_path = "/tmp/fake.csv"
    file_hash = "hash-zero"
    staged_rows = [
        {
            "id": 1,
            "file_hash": file_hash,
            "batch_id": "batch-zero",
            "source_year": "2024",
            "processed_at": "0000-00-00 00:00:00",
        },
        {
            "id": 2,
            "file_hash": file_hash,
            "batch_id": "batch-zero",
            "source_year": "2024",
            "processed_at": "0000-00-00 00:00:00",
        },
    ]
    staging_result = ingest_excel.StagingLoadResult(
        staging_table="teach_record_raw",
        file_hash=file_hash,
        batch_id="batch-zero",
        source_year="2024",
        ingested_at=dt.datetime(2024, 8, 1, 8, tzinfo=dt.timezone.utc),
        rowcount=len(staged_rows),
    )

    monkeypatch.setattr(
        pipeline.prep_excel,
        "main",
        lambda *args, **kwargs: (csv_path, file_hash),
    )
    monkeypatch.setattr(
        pipeline.ingest_excel,
        "load_csv_into_staging",
        lambda *args, **kwargs: staging_result,
    )
    monkeypatch.setattr(
        pipeline.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default", db_settings=None: {
            "table": "teach_record_raw",
            "normalized_table": "teach_record_normalized",
            "column_mappings": {"姓名": "姓名"},
        },
    )

    class _ZeroDateCursor(_Cursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if sql.strip().upper().startswith("SELECT"):
                assert (
                    "processed_at IS NULL OR processed_at = '0000-00-00 00:00:00'"
                    in sql
                )

    class _ZeroDateConnection(_Connection):
        def cursor(self, cursor_class=None):
            self.cursor_calls.append(cursor_class)
            return _ZeroDateCursor(self._rows)

    connection = _ZeroDateConnection(staged_rows)
    monkeypatch.setattr(pipeline.pymysql, "connect", lambda **kwargs: connection)

    inserted = {}

    def fake_insert(
        conn,
        table,
        rows,
        column_mappings=None,
        *,
        prepared,
    ):
        inserted["table"] = table
        inserted["prepared"] = prepared
        return pipeline.normalize_staging.InsertNormalizedRowsResult(
            inserted_count=len(prepared.normalized_rows),
            prepared=prepared,
        )

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "insert_normalized_rows",
        fake_insert,
    )
    monkeypatch.setattr(
        pipeline.normalize_staging,
        "ensure_normalized_schema",
        lambda *args, **kwargs: None,
    )

    marked = {}

    def fake_mark(connection_obj, table, row_ids, *, file_hash):
        marked.update(
            {
                "table": table,
                "row_ids": tuple(row_ids),
                "file_hash": file_hash,
            }
        )
        return dt.datetime(2024, 8, 1, 9, tzinfo=dt.timezone.utc)

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "mark_staging_rows_processed",
        fake_mark,
    )
    monkeypatch.setattr(
        pipeline.validation,
        "validate_rows",
        lambda job_id, prepared, **kwargs: pipeline.validation.ValidationResult(
            prepared=prepared,
            rejected_rows_path=None,
            errors=[],
        ),
    )

    result = pipeline.run_pipeline("workbook.xlsx", source_year="2024")

    assert len(inserted["prepared"].normalized_rows) == len(staged_rows)
    assert result.normalized_rows == len(staged_rows)
    assert marked["row_ids"] == (1, 2)
    assert marked["file_hash"] == file_hash
    assert marked["table"] == "teach_record_raw"
    assert not connection.rolled_back
    assert connection.committed


@pytest.mark.parametrize(
    "error_cls",
    [pymysql.err.ProgrammingError, pymysql.err.InternalError],
)
def test_run_pipeline_falls_back_when_config_lacks_column_mappings(
    monkeypatch, error_cls
):
    csv_path = "/tmp/fake.csv"
    file_hash = "hash-abc"
    staged_rows = [
        {"id": 10, "file_hash": file_hash},
        {"id": 11, "file_hash": file_hash},
    ]
    staging_result = ingest_excel.StagingLoadResult(
        staging_table="teach_record_raw",
        file_hash=file_hash,
        batch_id="batch-xyz",
        source_year="2024",
        ingested_at=dt.datetime(2024, 6, 1, 12, tzinfo=dt.timezone.utc),
        rowcount=len(staged_rows),
    )

    def fake_prep_main(*args, **kwargs):
        return csv_path, file_hash

    monkeypatch.setattr(pipeline.prep_excel, "main", fake_prep_main)
    monkeypatch.setattr(
        pipeline.ingest_excel,
        "load_csv_into_staging",
        lambda *args, **kwargs: staging_result,
    )

    class ConfigCursor:
        def __init__(self, rows):
            self.rows = rows
            self.queries = []
            self._raise_missing = True

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            self.queries.append(sql)
            if self._raise_missing and "column_mappings" in sql:
                self._raise_missing = False
                raise error_cls(1054, "Unknown column 'column_mappings' in 'SELECT'")

        def fetchall(self):
            return self.rows

    class ConfigConnection:
        def __init__(self, rows):
            self.cursor_obj = ConfigCursor(rows)
            self.closed = False

        def cursor(self, cursor_class=None):
            return self.cursor_obj

        def close(self):
            self.closed = True

    config_rows = [
        {
            "sheet_name": "TEACH_RECORD",
            "staging_table": "teach_record_raw",
            "metadata_columns": ["file_hash", "processed_at"],
            "required_columns": [],
            "options": {"normalized_table": "teach_record_normalized"},
        }
    ]
    config_connection = ConfigConnection(config_rows)
    connection = _Connection(staged_rows)

    def fake_connect(**kwargs):
        if kwargs.get("local_infile"):
            return connection
        return config_connection

    monkeypatch.setattr(pipeline.pymysql, "connect", fake_connect)
    monkeypatch.setattr(pipeline.prep_excel.pymysql, "connect", fake_connect)
    pipeline.prep_excel._get_sheet_config.cache_clear()
    inserted = {}

    def fake_insert(
        connection_obj,
        table,
        rows,
        column_mappings=None,
        *,
        prepared,
    ):
        inserted["table"] = table
        inserted["prepared"] = prepared
        return pipeline.normalize_staging.InsertNormalizedRowsResult(
            inserted_count=len(prepared.normalized_rows),
            prepared=prepared,
        )

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "insert_normalized_rows",
        fake_insert,
    )

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "mark_staging_rows_processed",
        lambda *args, **kwargs: dt.datetime(2024, 6, 1, 13, tzinfo=dt.timezone.utc),
    )
    monkeypatch.setattr(
        pipeline.normalize_staging,
        "ensure_normalized_schema",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        pipeline.validation,
        "validate_rows",
        lambda job_id, prepared, **kwargs: pipeline.validation.ValidationResult(
            prepared=prepared,
            rejected_rows_path=None,
            errors=[],
        ),
    )

    result = pipeline.run_pipeline("workbook.xlsx", source_year="2024")

    assert len(config_connection.cursor_obj.queries) == 2
    assert "column_mappings" in config_connection.cursor_obj.queries[0]
    assert "column_mappings" not in config_connection.cursor_obj.queries[1]
    assert inserted["prepared"].resolved_mappings == OrderedDict()
    assert inserted["table"] == "teach_record_normalized"
    assert result.normalized_rows == len(staged_rows)
    assert result.normalized_table == "teach_record_normalized"
    assert connection.committed
    pipeline.prep_excel._get_sheet_config.cache_clear()


def test_run_pipeline_uses_derived_normalized_table(monkeypatch):
    csv_path = "/tmp/fake.csv"
    file_hash = "hash-derived"
    staged_rows = [
        {"id": 100, "file_hash": file_hash},
        {"id": 101, "file_hash": file_hash},
    ]
    staging_result = ingest_excel.StagingLoadResult(
        staging_table="teach_record_raw",
        file_hash=file_hash,
        batch_id="batch-derived",
        source_year="2024",
        ingested_at=dt.datetime(2024, 7, 1, 8, tzinfo=dt.timezone.utc),
        rowcount=len(staged_rows),
    )

    monkeypatch.setattr(
        pipeline.prep_excel,
        "main",
        lambda *args, **kwargs: (csv_path, file_hash),
    )
    monkeypatch.setattr(
        pipeline.ingest_excel,
        "load_csv_into_staging",
        lambda *args, **kwargs: staging_result,
    )

    config_rows = [
        {
            "sheet_name": "TEACH_RECORD",
            "staging_table": "teach_record_raw",
            "metadata_columns": ["file_hash", "processed_at"],
            "required_columns": [],
            "options": {},
            "column_mappings": None,
        }
    ]
    config_connection = _Connection(config_rows)
    connection = _Connection(staged_rows)
    pipeline.prep_excel._get_sheet_config.cache_clear()

    def fake_connect(**kwargs):
        if kwargs.get("local_infile"):
            return connection
        return config_connection

    monkeypatch.setattr(pipeline.pymysql, "connect", fake_connect)
    monkeypatch.setattr(pipeline.prep_excel.pymysql, "connect", fake_connect)

    inserted = {}

    def fake_insert(
        connection_obj,
        table,
        rows,
        column_mappings=None,
        *,
        prepared,
    ):
        inserted["table"] = table
        inserted["prepared"] = prepared
        return pipeline.normalize_staging.InsertNormalizedRowsResult(
            inserted_count=len(prepared.normalized_rows),
            prepared=prepared,
        )

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "insert_normalized_rows",
        fake_insert,
    )

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "mark_staging_rows_processed",
        lambda *args, **kwargs: dt.datetime(2024, 7, 1, 9, tzinfo=dt.timezone.utc),
    )
    monkeypatch.setattr(
        pipeline.normalize_staging,
        "ensure_normalized_schema",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        pipeline.validation,
        "validate_rows",
        lambda job_id, prepared, **kwargs: pipeline.validation.ValidationResult(
            prepared=prepared,
            rejected_rows_path=None,
            errors=[],
        ),
    )

    result = pipeline.run_pipeline("workbook.xlsx", source_year="2024")

    assert inserted["table"] == "teach_record_normalized"
    assert len(inserted["prepared"].normalized_rows) == len(staged_rows)
    assert result.normalized_table == "teach_record_normalized"
    assert result.staging_table == "teach_record_raw"
    assert result.normalized_rows == len(staged_rows)
    assert connection.committed
    assert config_connection.closed
    pipeline.prep_excel._get_sheet_config.cache_clear()


def test_run_pipeline_expands_normalized_schema(monkeypatch):
    csv_path = "/tmp/fake.csv"
    file_hash = "hash-extra"
    staged_rows = [
        {
            "id": 1,
            "file_hash": file_hash,
            "batch_id": "batch-extra",
            "source_year": "2024",
            "ingested_at": "2024-09-01T00:00:00",
            "姓名": "Student",
            "教學跟進/回饋": "Long feedback text",
        }
    ]
    staging_result = ingest_excel.StagingLoadResult(
        staging_table="teach_record_raw",
        file_hash=file_hash,
        batch_id="batch-extra",
        source_year="2024",
        ingested_at=dt.datetime(2024, 9, 1, tzinfo=dt.timezone.utc),
        rowcount=len(staged_rows),
    )

    monkeypatch.setattr(
        pipeline.prep_excel,
        "main",
        lambda *args, **kwargs: (csv_path, file_hash),
    )
    monkeypatch.setattr(
        pipeline.ingest_excel,
        "load_csv_into_staging",
        lambda *args, **kwargs: staging_result,
    )
    monkeypatch.setattr(
        pipeline.prep_excel,
        "_get_table_config",
        lambda sheet, workbook_type="default", db_settings=None: {
            "table": "teach_record_raw",
            "normalized_table": "teach_record_normalized",
            "column_mappings": None,
            "column_types": {"教學跟進/回饋": "TEXT NULL"},
        },
    )

    connection = _Connection(staged_rows)
    monkeypatch.setattr(pipeline.pymysql, "connect", lambda **kwargs: connection)

    ensured = {}

    def fake_ensure(conn, table, mappings, column_types, **kwargs):
        ensured["table"] = table
        ensured["columns"] = tuple(mappings.keys())
        ensured["column_types"] = dict(column_types)
        ensured["kwargs"] = kwargs

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "ensure_normalized_schema",
        fake_ensure,
    )

    inserted = {}

    def fake_insert(
        connection_obj,
        table,
        rows,
        column_mappings=None,
        *,
        prepared,
    ):
        inserted["table"] = table
        inserted["prepared"] = prepared
        return pipeline.normalize_staging.InsertNormalizedRowsResult(
            inserted_count=len(prepared.normalized_rows),
            prepared=prepared,
        )

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "insert_normalized_rows",
        fake_insert,
    )

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "mark_staging_rows_processed",
        lambda *args, **kwargs: dt.datetime(2024, 9, 1, 1, tzinfo=dt.timezone.utc),
    )
    monkeypatch.setattr(
        pipeline.validation,
        "validate_rows",
        lambda job_id, prepared, **kwargs: pipeline.validation.ValidationResult(
            prepared=prepared,
            rejected_rows_path=None,
            errors=[],
        ),
    )

    result = pipeline.run_pipeline("workbook.xlsx", source_year="2024")

    assert ensured["table"] == "teach_record_normalized"
    assert "教學跟進/回饋" in ensured["columns"]
    assert ensured["column_types"]["教學跟進/回饋"] == "TEXT NULL"
    assert inserted["table"] == "teach_record_normalized"
    assert (
        inserted["prepared"].resolved_mappings["教學跟進/回饋"]
        == "教學跟進/回饋"
    )
    row_dict = dict(
        zip(
            inserted["prepared"].ordered_columns,
            inserted["prepared"].normalized_rows[0],
        )
    )
    assert row_dict["教學跟進/回饋"] == "Long feedback text"
    assert result.normalized_rows == 1
    assert connection.committed


def test_cli_enqueues_job(monkeypatch, capsys, tmp_path):
    workbook = tmp_path / "workbook.xlsx"
    workbook.write_bytes(b"data")

    captured_args = {}

    def fake_enqueue_job(**kwargs):
        captured_args.update(kwargs)
        return "job-123", object()

    monkeypatch.setattr("app.job_runner.enqueue_job", fake_enqueue_job)

    returned = pipeline.cli(
        [
            str(workbook),
            "SheetA",
            "--source-year",
            "2024",
            "--batch-id",
            "batch-9",
        ]
    )

    out = capsys.readouterr().out
    assert "Queued upload job job-123" in out
    assert returned == "job-123"
    assert captured_args == {
        "workbook_path": str(workbook),
        "sheet": "SheetA",
        "workbook_type": "default",
        "source_year": "2024",
        "batch_id": "batch-9",
        "file_size": workbook.stat().st_size,
    }

