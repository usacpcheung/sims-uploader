import datetime as dt
import os
import sys
from decimal import Decimal

import pytest

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_USER", "user")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "database")
os.environ.setdefault("DB_CHARSET", "utf8mb4")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app import normalize_staging
from app.prep_excel import TableMissingError


class _Cursor:
    def __init__(self):
        self.executed: list[tuple[str, list[tuple[object, ...]]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.execute_calls.append((sql, tuple(params or ())))

    def executemany(self, sql, params):
        self.executed.append((sql, params))
        self.rowcount += len(params)


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


@pytest.fixture
def sample_row():
    return {
        "id": 42,
        "file_hash": "abc123",
        "batch_id": "batch-9",
        "source_year": "2024",
        "ingested_at": "2024-05-02T12:30:45",
        "記錄狀態": "Completed",
        "日期": "2024/05/01",
        "任教老師": "Ms. Chan",
        "學生編號": "S001",
        "姓名": "陳大文",
        "英文姓名": "Chan Tai Man",
        "性別": "M",
        "學生級別": "P3",
        "病房": "A1",
        "病床": "12",
        "出勤 (來自出勤記錄輸入)": "Y",
        "出勤": "Y",
        "教學組別": "Group A",
        "科目": "Math",
        "取代科目": "",
        "教授科目": "Algebra",
        "課程級別": "Advanced",
        "教材": "Workbook",
        "課題": "Fractions",
        "教學重點1": "Concept",
        "教學重點2": "Practice",
        "教學重點3": "Assessment",
        "教學重點4": "Feedback",
        "自定課題": "Extension",
        "自定教學重點": "Real world",
        "練習": "Worksheet",
        "上課時數": "1.5",
        "備註": "N/A",
        "教學跟進/回饋": "Call parents",
    }


@pytest.fixture
def column_mappings(sample_row):
    return normalize_staging.resolve_column_mappings([sample_row], None)


def test_build_insert_statement_uses_chinese_headers(column_mappings):
    sql, columns = normalize_staging.build_insert_statement(
        "teach_record_normalized", column_mappings
    )
    assert "`記錄狀態`" in sql
    assert "`教學跟進/回饋`" in sql
    assert sql.startswith("INSERT INTO `teach_record_normalized`")
    # Metadata columns should lead the list for join friendliness.
    assert columns[:5] == [
        "raw_id",
        "file_hash",
        "batch_id",
        "source_year",
        "ingested_at",
    ]


def test_prepare_rows_coerces_date_and_decimal(sample_row, column_mappings):
    _, ordered_columns = normalize_staging.build_insert_statement(
        "teach_record_normalized", column_mappings
    )
    rows = normalize_staging.prepare_rows([sample_row], column_mappings)
    prepared = rows[0]
    # Metadata columns remain accessible for joins.
    assert prepared[0] == sample_row["id"]
    assert prepared[1] == sample_row["file_hash"]
    # 日期 coerces to a ``date`` object and retains Chinese column naming.
    date_index = ordered_columns.index("日期")
    date_value = prepared[date_index]
    assert isinstance(date_value, dt.date)
    assert date_value == dt.date(2024, 5, 1)
    # 上課時數 coerces to Decimal to support numeric analytics.
    practice_hours = prepared[ordered_columns.index("上課時數")]
    assert isinstance(practice_hours, Decimal)
    assert practice_hours == Decimal("1.5")


def test_insert_normalized_rows_uses_identity_columns(sample_row, column_mappings):
    cursor = _Cursor()
    connection = _Connection(cursor)

    result = normalize_staging.insert_normalized_rows(
        connection,
        "teach_record_normalized",
        [sample_row],
        column_mappings,
    )

    assert result.inserted_count == 1
    assert len(cursor.executed) == 1
    sql, params = cursor.executed[0]
    assert "`日期`" in sql
    assert "`上課時數`" in sql
    _, ordered_columns = normalize_staging.build_insert_statement(
        "teach_record_normalized", column_mappings
    )
    row_values = params[0]
    assert row_values[0] == sample_row["id"]
    assert row_values[1] == sample_row["file_hash"]
    assert row_values[ordered_columns.index("日期")] == dt.date(2024, 5, 1)
    assert row_values[ordered_columns.index("上課時數")] == Decimal("1.5")


def test_prepare_normalization_collects_rejections(sample_row):
    bad_row = dict(sample_row)
    bad_row["上課時數"] = "not-a-number"

    prepared = normalize_staging.prepare_normalization([bad_row], None)

    assert prepared.normalized_rows == []
    assert len(prepared.rejected_rows) == 1
    rejection = prepared.rejected_rows[0]
    assert "上課時數" in rejection.errors[0]


def test_build_column_coverage_maps_sources():
    rows = [
        {
            "id": 1,
            "file_hash": "hash",
            "batch_id": "batch",
            "source_year": "2024",
            "ingested_at": "2024-05-01T00:00:00",
            "姓名": "Student",
            "別名": "Alias",
        }
    ]
    resolved = normalize_staging.resolve_column_mappings(rows, {"姓名": "姓名", "名字": "別名"})
    coverage = normalize_staging.build_column_coverage(resolved)
    assert coverage["姓名"] == ["姓名"]
    assert coverage["別名"] == ["名字"]


def test_resolve_column_mappings_adds_new_columns():
    rows = [
        {
            "id": 1,
            "file_hash": "hash",
            "batch_id": "batch",
            "source_year": "2024",
            "ingested_at": "2024-01-01T00:00:00",
            "姓名": "Student",
            "新欄位": "value",
        }
    ]
    resolved = normalize_staging.resolve_column_mappings(rows, {"姓名": "姓名"})
    assert list(resolved.keys())[-1] == "新欄位"
    assert resolved["新欄位"] == "新欄位"


def test_resolve_column_mappings_honours_custom_metadata_and_reserved():
    rows = [
        {
            "id": 7,
            "file_hash": "hash",
            "batch_id": "batch-7",
            "source_year": "2024",
            "ingested_at": "2024-05-01T00:00:00",
            "custom_meta": "meta",
            "skip_me": "value",
            "姓名": "Student",
        }
    ]
    metadata_override = ["custom_meta", "raw_id", "file_hash"]
    reserved_override = {"skip_me"}

    resolved = normalize_staging.resolve_column_mappings(
        rows,
        None,
        metadata_columns=metadata_override,
        reserved_source_columns=reserved_override,
    )

    assert "skip_me" not in resolved
    sql, columns = normalize_staging.build_insert_statement(
        "teach_record_normalized",
        resolved,
        metadata_columns=metadata_override,
    )
    assert sql.startswith("INSERT INTO `teach_record_normalized`")
    assert columns[:3] == ["custom_meta", "raw_id", "file_hash"]

    prepared = normalize_staging.prepare_rows(
        rows,
        resolved,
        metadata_columns=metadata_override,
    )
    assert prepared[0][0] == "meta"
    assert prepared[0][1] == 7


def test_ensure_normalized_schema_alters_missing_columns(monkeypatch, column_mappings):
    cursor = _Cursor()
    connection = _Connection(cursor)

    monkeypatch.setattr(
        normalize_staging,
        "_fetch_existing_columns",
        lambda conn, table: [
            {"name": "raw_id", "type": "int(11)", "is_nullable": False},
            {"name": "file_hash", "type": "varchar(64)", "is_nullable": False},
            {"name": "batch_id", "type": "varchar(64)", "is_nullable": True},
            {"name": "source_year", "type": "int(11)", "is_nullable": True},
            {"name": "ingested_at", "type": "datetime", "is_nullable": True},
        ],
    )

    changed = normalize_staging.ensure_normalized_schema(
        connection, "teach_record_normalized", column_mappings, {}
    )

    assert changed is True
    assert cursor.execute_calls
    alter_statements = [sql for sql, _ in cursor.execute_calls if sql.startswith("ALTER TABLE")]
    assert alter_statements


def test_ensure_normalized_schema_creates_table_when_missing(
    monkeypatch, column_mappings
):
    cursor = _Cursor()
    connection = _Connection(cursor)

    def _missing(*_, **__):
        raise TableMissingError("teach_record_normalized")

    monkeypatch.setattr(normalize_staging, "_fetch_existing_columns", _missing)

    changed = normalize_staging.ensure_normalized_schema(
        connection,
        "teach_record_normalized",
        column_mappings,
        {"教學跟進/回饋": "TEXT NULL"},
    )

    assert changed is True
    create_statements = [
        sql for sql, _ in cursor.execute_calls if sql.startswith("CREATE TABLE")
    ]
    assert len(create_statements) == 1
    create_sql = create_statements[0]
    assert "`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY" in create_sql
    assert "`raw_id` BIGINT UNSIGNED NOT NULL" in create_sql
    assert "`file_hash` CHAR(64) NOT NULL" in create_sql
    assert "`ingested_at` DATETIME NOT NULL" in create_sql
    assert "`上課時數` DECIMAL(6,2) NULL" in create_sql
    assert "`教學跟進/回饋` TEXT NULL" in create_sql


def test_ensure_normalized_schema_uses_configured_overrides(monkeypatch):
    cursor = _Cursor()
    connection = _Connection(cursor)

    monkeypatch.setattr(
        normalize_staging,
        "_fetch_existing_columns",
        lambda conn, table: [
            {"name": "raw_id", "type": "int(11)", "is_nullable": False},
            {"name": "file_hash", "type": "varchar(64)", "is_nullable": False},
        ],
    )

    mappings = {"特別欄": "特別欄"}
    overrides = {"特別欄": "JSON NULL"}

    changed = normalize_staging.ensure_normalized_schema(
        connection,
        "teach_record_normalized",
        mappings,
        {},
        metadata_columns=["raw_id", "file_hash"],
        column_type_overrides=overrides,
    )

    assert changed is True
    assert any("JSON NULL" in sql for sql, _ in cursor.execute_calls)


def test_ensure_normalized_schema_uses_column_type_overrides(monkeypatch):
    cursor = _Cursor()
    connection = _Connection(cursor)

    monkeypatch.setattr(
        normalize_staging,
        "_fetch_existing_columns",
        lambda conn, table: [
            {"name": "raw_id", "type": "int(11)", "is_nullable": False},
            {"name": "file_hash", "type": "varchar(64)", "is_nullable": False},
            {"name": "batch_id", "type": "varchar(64)", "is_nullable": True},
            {"name": "source_year", "type": "int(11)", "is_nullable": True},
            {"name": "ingested_at", "type": "datetime", "is_nullable": True},
        ],
    )

    mappings = {"教學跟進/回饋": "教學跟進/回饋"}
    changed = normalize_staging.ensure_normalized_schema(
        connection,
        "teach_record_normalized",
        mappings,
        {"教學跟進/回饋": "TEXT NULL"},
    )

    assert changed is True
    alter_statements = [sql for sql, _ in cursor.execute_calls if sql.startswith("ALTER TABLE")]
    assert alter_statements
    assert "TEXT NULL" in alter_statements[0]


def test_ensure_normalized_schema_modifies_existing_column_type(monkeypatch):
    cursor = _Cursor()
    connection = _Connection(cursor)

    monkeypatch.setattr(
        normalize_staging,
        "_fetch_existing_columns",
        lambda conn, table: [
            {"name": "raw_id", "type": "int(11)", "is_nullable": False},
            {"name": "file_hash", "type": "varchar(64)", "is_nullable": False},
            {"name": "教學跟進/回饋", "type": "varchar(255)", "is_nullable": True},
        ],
    )

    mappings = {"教學跟進/回饋": "教學跟進/回饋"}
    changed = normalize_staging.ensure_normalized_schema(
        connection,
        "teach_record_normalized",
        mappings,
        {"教學跟進/回饋": "TEXT NULL"},
    )

    assert changed is True
    assert (
        "ALTER TABLE `teach_record_normalized` MODIFY COLUMN `教學跟進/回饋` TEXT NULL",
        (),
    ) in cursor.execute_calls


def test_ensure_normalized_schema_skips_modify_when_type_matches(monkeypatch):
    cursor = _Cursor()
    connection = _Connection(cursor)

    monkeypatch.setattr(
        normalize_staging,
        "_fetch_existing_columns",
        lambda conn, table: [
            {"name": "raw_id", "type": "int(11)", "is_nullable": False},
            {"name": "file_hash", "type": "varchar(64)", "is_nullable": False},
            {"name": "教學跟進/回饋", "type": "text", "is_nullable": True},
        ],
    )

    mappings = {"教學跟進/回饋": "教學跟進/回饋"}
    changed = normalize_staging.ensure_normalized_schema(
        connection,
        "teach_record_normalized",
        mappings,
        {"教學跟進/回饋": "TEXT NULL"},
    )

    assert changed is False
    assert all(
        not sql.startswith("ALTER TABLE `teach_record_normalized` MODIFY")
        for sql, _ in cursor.execute_calls
    )


def test_mark_staging_rows_processed_updates_by_file_hash(monkeypatch):
    cursor = _Cursor()
    connection = _Connection(cursor)
    processed_at = dt.datetime(2024, 5, 3, 8, 30, tzinfo=dt.timezone.utc)

    class _FrozenDateTime(dt.datetime):
        @classmethod
        def now(cls, tz=None):  # pragma: no cover - exercised via monkeypatch
            assert tz == dt.timezone.utc
            return processed_at

    monkeypatch.setattr(normalize_staging._dt, "datetime", _FrozenDateTime)

    timestamp = normalize_staging.mark_staging_rows_processed(
        connection,
        "teach_record_raw",
        [101, 102],
        file_hash="hash-123",
    )

    assert timestamp == processed_at
    assert cursor.execute_calls == [
        (
            "UPDATE `teach_record_raw` SET processed_at = %s, status = %s "
            "WHERE file_hash = %s AND (processed_at IS NULL OR processed_at = '0000-00-00 00:00:00')",
            (processed_at, "processed", "hash-123"),
        )
    ]
