from __future__ import annotations

import datetime as dt
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

    def fake_prep_main(workbook, sheet, *, emit_stdout, db_settings):
        captured.prep_call = {
            "workbook": workbook,
            "sheet": sheet,
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
        lambda sheet, db_settings=None: {
            "table": "teach_record_raw",
            "normalized_table": "teach_record_normalized",
            "column_mappings": {"姓名": "姓名"},
        },
    )

    connection = _Connection(staged_rows)
    monkeypatch.setattr(pipeline.pymysql, "connect", lambda **kwargs: connection)

    inserted = {}

    def fake_insert(connection_obj, table, rows, column_mappings):
        inserted["table"] = table
        inserted["rows"] = rows
        inserted["column_mappings"] = column_mappings
        return len(rows)

    monkeypatch.setattr(
        pipeline.normalize_staging,
        "insert_normalized_rows",
        fake_insert,
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
    assert inserted["table"] == "teach_record_normalized"
    assert inserted["column_mappings"] == {"姓名": "姓名"}
    assert captured.mark_call["file_hash"] == file_hash
    assert captured.mark_call["row_ids"] == (1, 2)
    assert result.file_hash == file_hash
    assert result.staged_rows == len(staged_rows)
    assert result.normalized_rows == len(staged_rows)
    assert result.batch_id == "batch-1"
    assert result.staging_table == "teach_record_raw"
    assert result.normalized_table == "teach_record_normalized"
    assert not result.skipped
    assert connection.committed
    assert not connection.rolled_back
    assert connection.closed


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
            "metadata_columns": ["file_hash"],
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

    def fake_insert(connection_obj, table, rows, column_mappings):
        inserted["table"] = table
        inserted["rows"] = rows
        inserted["column_mappings"] = column_mappings
        return len(rows)

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

    result = pipeline.run_pipeline("workbook.xlsx", source_year="2024")

    assert len(config_connection.cursor_obj.queries) == 2
    assert "column_mappings" in config_connection.cursor_obj.queries[0]
    assert "column_mappings" not in config_connection.cursor_obj.queries[1]
    assert inserted["column_mappings"] is None
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
            "metadata_columns": ["file_hash"],
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

    def fake_insert(connection_obj, table, rows, column_mappings):
        inserted["table"] = table
        inserted["rows"] = rows
        inserted["column_mappings"] = column_mappings
        return len(rows)

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

    result = pipeline.run_pipeline("workbook.xlsx", source_year="2024")

    assert inserted["table"] == "teach_record_normalized"
    assert result.normalized_table == "teach_record_normalized"
    assert result.staging_table == "teach_record_raw"
    assert result.normalized_rows == len(staged_rows)
    assert connection.committed
    assert config_connection.closed
    pipeline.prep_excel._get_sheet_config.cache_clear()


def test_cli_invokes_pipeline(monkeypatch, capsys):
    result = pipeline.PipelineResult(
        file_hash="hash-xyz",
        staging_table="teach_record_raw",
        normalized_table="teach_record_normalized",
        staged_rows=2,
        normalized_rows=2,
        batch_id="batch-9",
        ingested_at=dt.datetime(2024, 5, 1, tzinfo=dt.timezone.utc),
        processed_at=dt.datetime(2024, 5, 1, 12, tzinfo=dt.timezone.utc),
    )

    captured_args = {}

    def fake_run(workbook, sheet, *, source_year, batch_id, db_settings=None):
        captured_args.update(
            {
                "workbook": workbook,
                "sheet": sheet,
                "source_year": source_year,
                "batch_id": batch_id,
                "db_settings": db_settings,
            }
        )
        return result

    monkeypatch.setattr(pipeline, "run_pipeline", fake_run)

    returned = pipeline.cli(
        ["workbook.xlsx", "SheetA", "--source-year", "2024", "--batch-id", "batch-9"]
    )

    out = capsys.readouterr().out
    assert "Staged" in out
    assert captured_args == {
        "workbook": "workbook.xlsx",
        "sheet": "SheetA",
        "source_year": "2024",
        "batch_id": "batch-9",
        "db_settings": None,
    }
    assert returned is result

