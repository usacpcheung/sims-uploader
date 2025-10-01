import os
import tempfile
import unittest
from unittest import mock

import pandas as pd

# Ensure database configuration is available before importing the module under test.
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_USER", "user")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "test_db")
os.environ.setdefault("DB_CHARSET", "utf8mb4")

from app import prep_excel


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self.executed = (query, params)

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, rows):
        self._cursor = _FakeCursor(rows)
        self.closed = False

    def cursor(self, *args, **kwargs):
        return self._cursor

    def close(self):
        self.closed = True


class PrepExcelSchemaTests(unittest.TestCase):
    def test_get_schema_details_uses_information_schema(self):
        rows = [
            {"COLUMN_NAME": "id", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "日期", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "任教老師", "IS_NULLABLE": "YES", "COLUMN_DEFAULT": ""},
            {"COLUMN_NAME": "file_hash", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "學生編號", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
        ]

        fake_connection = _FakeConnection(rows)

        with mock.patch.object(prep_excel.pymysql, "connect", return_value=fake_connection):
            schema = prep_excel.get_schema_details()

        self.assertEqual(
            schema,
            {
                "order": ["日期", "任教老師", "學生編號"],
                "required": ["日期", "學生編號"],
            },
        )
        self.assertTrue(fake_connection.closed)
        self.assertEqual(
            fake_connection._cursor.executed[1],
            (prep_excel.DB["database"], prep_excel.TABLE_CONFIG[prep_excel.DEFAULT_SHEET]["table"]),
        )

    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_raises_missing_columns_error(self, mock_read_excel, mock_get_schema_details):
        mock_get_schema_details.return_value = {
            "order": ["日期", "任教老師"],
            "required": ["日期", "任教老師"],
        }
        mock_read_excel.return_value = pd.DataFrame({"任教老師": ["Ms. Chan"]})

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(b"dummy")
            excel_path = tmp.name

        try:
            with mock.patch("pandas.DataFrame.to_csv", return_value=None):
                with self.assertRaises(prep_excel.MissingColumnsError) as ctx:
                    prep_excel.main(excel_path)
        finally:
            os.remove(excel_path)

        self.assertEqual(ctx.exception.missing_columns, ("日期",))


if __name__ == "__main__":
    unittest.main()
