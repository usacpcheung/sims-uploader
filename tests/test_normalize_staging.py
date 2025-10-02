import datetime as dt
import os
import sys
from decimal import Decimal

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app import normalize_staging


class _Cursor:
    def __init__(self):
        self.executed: list[tuple[str, list[tuple[object, ...]]]] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

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
def column_mappings():
    return {column: column for column in normalize_staging.BUSINESS_COLUMNS}


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

    affected = normalize_staging.insert_normalized_rows(
        connection,
        "teach_record_normalized",
        [sample_row],
        column_mappings,
    )

    assert affected == 1
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
