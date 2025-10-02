import json
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd

# Ensure database configuration is available before importing the module under test.
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_USER", "user")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "test_db")
os.environ.setdefault("DB_CHARSET", "utf8mb4")

from app import prep_excel


class _FakeCursor:
    def __init__(self, rows, fetchone_result=None):
        self._rows = rows
        self._fetchone_result = fetchone_result
        self.executed: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._fetchone_result


class _FakeConnection:
    def __init__(self, rows, fetchone_result=None):
        self._cursor = _FakeCursor(rows, fetchone_result)
        self.closed = False

    def cursor(self, *args, **kwargs):
        return self._cursor

    def close(self):
        self.closed = True


class _SequenceConnection:
    def __init__(self, cursor_payloads):
        self._payloads = list(cursor_payloads)
        self.closed = False
        self.cursors: list[_FakeCursor] = []

    def cursor(self, *args, **kwargs):
        if not self._payloads:
            payload = {"rows": [], "fetchone": None}
        else:
            payload = self._payloads.pop(0)
        cursor = _FakeCursor(payload.get("rows", []), payload.get("fetchone"))
        self.cursors.append(cursor)
        return cursor

    def close(self):
        self.closed = True


class PrepExcelSchemaTests(unittest.TestCase):
    def setUp(self):
        prep_excel._get_sheet_config.cache_clear()

    def tearDown(self):
        prep_excel._get_sheet_config.cache_clear()

    def test_normalize_headers_drops_blank_header_with_no_data(self):
        df = pd.DataFrame({" ": [pd.NA, None], "Keep": ["A", "B"]})

        result = prep_excel.normalize_headers_and_subject(df, rename_last_subject=False)

        self.assertListEqual(list(result.columns), ["Keep"])

    def test_normalize_headers_keeps_blank_header_with_data(self):
        df = pd.DataFrame({" ": ["Value", ""], "Keep": ["A", "B"]})

        result = prep_excel.normalize_headers_and_subject(df, rename_last_subject=False)

        self.assertListEqual(list(result.columns), ["", "Keep"])

    def test_get_schema_details_uses_information_schema(self):
        config_rows = [
            {
                "sheet_name": prep_excel.DEFAULT_SHEET,
                "staging_table": "teach_record_raw",
                "metadata_columns": json.dumps(
                    ["id", "file_hash", "batch_id", "source_year", "ingested_at"]
                ),
                "required_columns": json.dumps([]),
                "options": None,
            }
        ]
        rows = [
            {"COLUMN_NAME": "id", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "日期", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "任教老師", "IS_NULLABLE": "YES", "COLUMN_DEFAULT": ""},
            {"COLUMN_NAME": "file_hash", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "學生編號", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
        ]

        config_connection = _FakeConnection(config_rows)
        schema_connection = _FakeConnection(rows)

        with mock.patch.object(
            prep_excel.pymysql,
            "connect",
            side_effect=[config_connection, schema_connection],
        ):
            schema = prep_excel.get_schema_details()

        self.assertEqual(
            schema,
            {
                "order": ["日期", "任教老師", "學生編號"],
                "required": ["日期", "任教老師", "學生編號"],
            },
        )
        self.assertTrue(config_connection.closed)
        self.assertTrue(schema_connection.closed)
        self.assertEqual(len(config_connection._cursor.executed), 1)
        self.assertIn("FROM sheet_ingest_config", config_connection._cursor.executed[0][0])
        self.assertEqual(
            schema_connection._cursor.executed[-1][1],
            (prep_excel.DB["database"], "teach_record_raw"),
        )

    def test_get_schema_details_missing_sheet_raises(self):
        config_rows = [
            {
                "sheet_name": "SOMETHING_ELSE",
                "staging_table": "other_table",
                "metadata_columns": json.dumps([]),
                "required_columns": json.dumps([]),
                "options": None,
            }
        ]
        config_connection = _FakeConnection(config_rows)

        with mock.patch.object(prep_excel.pymysql, "connect", return_value=config_connection):
            with self.assertRaises(ValueError):
                prep_excel.get_schema_details("UNKNOWN_SHEET")

    def test_get_schema_details_respects_required_columns_from_config(self):
        config_rows = [
            {
                "sheet_name": prep_excel.DEFAULT_SHEET,
                "staging_table": "teach_record_raw",
                "metadata_columns": json.dumps(["id", "file_hash"]),
                "required_columns": json.dumps(["日期"]),
                "options": None,
            }
        ]
        rows = [
            {"COLUMN_NAME": "id", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "日期", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "任教老師", "IS_NULLABLE": "YES", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "file_hash", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
        ]

        config_connection = _FakeConnection(config_rows)
        schema_connection = _FakeConnection(rows)

        with mock.patch.object(
            prep_excel.pymysql,
            "connect",
            side_effect=[config_connection, schema_connection],
        ):
            schema = prep_excel.get_schema_details()

        self.assertEqual(
            schema,
            {
                "order": ["日期", "任教老師"],
                "required": ["日期"],
            },
        )

    def test_get_schema_details_with_injected_connection(self):
        config_rows = [
            {
                "sheet_name": prep_excel.DEFAULT_SHEET,
                "staging_table": "teach_record_raw",
                "metadata_columns": json.dumps(["id", "file_hash"]),
                "required_columns": json.dumps([]),
                "options": None,
            }
        ]
        column_rows = [
            {"COLUMN_NAME": "id", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "日期", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "任教老師", "IS_NULLABLE": "YES", "COLUMN_DEFAULT": None},
            {"COLUMN_NAME": "file_hash", "IS_NULLABLE": "NO", "COLUMN_DEFAULT": None},
        ]

        connection = _SequenceConnection(
            [
                {"rows": config_rows},
                {"rows": column_rows},
            ]
        )

        with mock.patch.object(prep_excel.pymysql, "connect") as mock_connect:
            schema = prep_excel.get_schema_details(connection=connection)

        self.assertFalse(mock_connect.called)
        self.assertFalse(connection.closed)
        self.assertEqual(
            schema,
            {
                "order": ["日期", "任教老師"],
                "required": ["日期", "任教老師"],
            },
        )
        self.assertGreaterEqual(len(connection.cursors), 2)

    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_raises_missing_columns_error(
        self,
        mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
    ):
        mock_get_schema_details.return_value = {
            "order": ["日期", "任教老師"],
            "required": ["日期", "任教老師"],
        }
        mock_get_table_config.return_value = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset(),
            "options": {"rename_last_subject": True},
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

    def test_staging_file_hash_exists_with_injected_connection(self):
        connection = _SequenceConnection([
            {"rows": [], "fetchone": (1,)},
        ])

        with mock.patch.object(prep_excel.pymysql, "connect") as mock_connect:
            exists = prep_excel._staging_file_hash_exists(
                "teach_record_raw", "deadbeef", connection=connection
            )

        self.assertTrue(exists)
        self.assertFalse(connection.closed)
        self.assertFalse(mock_connect.called)
        self.assertEqual(len(connection.cursors), 1)
        self.assertEqual(connection.cursors[0].executed[0][1], ("deadbeef",))

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=False)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_normalizes_subject_column_when_option_enabled(
        self,
        mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
        _mock_hash_exists,
    ):
        mock_get_table_config.return_value = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset(),
            "options": {"rename_last_subject": True},
        }
        mock_get_schema_details.return_value = {
            "order": ["教授科目", "教師"],
            "required": [],
        }
        mock_read_excel.return_value = pd.DataFrame(
            {
                "Unnamed: 0": ["", ""],
                "Unnamed: 1": ["Math", ""],
                "教師": ["Ms. Chan", "Mr. Lee"],
            }
        )

        captured: dict[str, pd.DataFrame] = {}

        def fake_to_csv(self, path, *_args, **_kwargs):
            captured["df"] = self.copy()
            captured["path"] = path
            return None

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(b"dummy")
            excel_path = tmp.name

        try:
            with mock.patch("pandas.DataFrame.to_csv", new=fake_to_csv):
                csv_path, _ = prep_excel.main(excel_path)
        finally:
            os.remove(excel_path)

        self.assertIn("df", captured)
        result = captured["df"]
        self.assertIn("教授科目", result.columns)
        self.assertNotIn("Unnamed: 1", result.columns)
        self.assertEqual(result["教授科目"].tolist(), ["Math", ""])
        self.assertEqual(result["教師"].tolist(), ["Ms. Chan", "Mr. Lee"])
        self.assertEqual(csv_path, captured["path"])

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=False)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_skips_subject_normalization_when_option_disabled(
        self,
        mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
        _mock_hash_exists,
    ):
        mock_get_table_config.return_value = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset(),
            "options": {"rename_last_subject": False},
        }
        mock_get_schema_details.return_value = {
            "order": ["Unnamed: 0", "Unnamed: 1", "教師"],
            "required": [],
        }
        mock_read_excel.return_value = pd.DataFrame(
            {
                "Unnamed: 0": ["", ""],
                "Unnamed: 1": ["Math", ""],
                "教師": ["Ms. Chan", "Mr. Lee"],
            }
        )

        captured: dict[str, pd.DataFrame] = {}

        def fake_to_csv(self, path, *_args, **_kwargs):
            captured["df"] = self.copy()
            captured["path"] = path
            return None

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(b"dummy")
            excel_path = tmp.name

        try:
            with mock.patch("pandas.DataFrame.to_csv", new=fake_to_csv):
                csv_path, _ = prep_excel.main(excel_path)
        finally:
            os.remove(excel_path)

        self.assertIn("df", captured)
        result = captured["df"]
        self.assertIn("Unnamed: 1", result.columns)
        self.assertNotIn("教授科目", result.columns)
        self.assertEqual(result["Unnamed: 1"].tolist(), ["Math", ""])
        self.assertEqual(csv_path, captured["path"])

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=False)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_threads_connection_parameter(
        self,
        mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
        mock_hash_exists,
    ):
        connection = object()

        mock_get_table_config.return_value = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset(),
            "options": {},
        }
        mock_get_schema_details.return_value = {
            "order": ["日期"],
            "required": ["日期"],
        }
        mock_read_excel.return_value = pd.DataFrame({"日期": ["2024-01-01"]})

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(b"dummy")
            excel_path = tmp.name

        try:
            with mock.patch("pandas.DataFrame.to_csv", return_value=None):
                prep_excel.main(excel_path, connection=connection)
        finally:
            os.remove(excel_path)

        mock_get_table_config.assert_called_once_with(
            prep_excel.DEFAULT_SHEET, connection=connection, db_settings=None
        )
        mock_get_schema_details.assert_called_once_with(
            prep_excel.DEFAULT_SHEET, connection=connection, db_settings=None
        )
        self.assertTrue(mock_hash_exists.called)
        _, hash_kwargs = mock_hash_exists.call_args
        self.assertEqual(hash_kwargs["connection"], connection)
        self.assertIsNone(hash_kwargs["db_settings"])

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=False)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_uses_unique_output_path_per_file(
        self,
        mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
        _mock_hash_exists,
    ):
        mock_get_table_config.return_value = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset(),
            "options": {},
        }
        mock_get_schema_details.return_value = {
            "order": ["日期", "任教老師"],
            "required": ["日期", "任教老師"],
        }
        mock_read_excel.return_value = pd.DataFrame(
            {
                "日期": ["2024-01-01"],
                "任教老師": ["Ms. Chan"],
            }
        )

        written_paths = []

        def fake_to_csv(_self, path, *_args, **_kwargs):
            written_paths.append(path)
            return None

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp_a, tempfile.NamedTemporaryFile(
            suffix=".xlsx", delete=False
        ) as tmp_b:
            tmp_a.write(b"file-a")
            tmp_a.flush()
            tmp_b.write(b"file-b")
            tmp_b.flush()
            excel_a = tmp_a.name
            excel_b = tmp_b.name

        try:
            with mock.patch("pandas.DataFrame.to_csv", new=fake_to_csv):
                csv_a, _ = prep_excel.main(excel_a)
                csv_b, _ = prep_excel.main(excel_b)
        finally:
            os.remove(excel_a)
            os.remove(excel_b)

        self.assertEqual(len(written_paths), 2)
        self.assertEqual(csv_a, written_paths[0])
        self.assertEqual(csv_b, written_paths[1])
        self.assertNotEqual(csv_a, csv_b)

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=True)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_skips_when_file_hash_already_exists(
        self,
        mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
        mock_hash_exists,
    ):
        mock_get_table_config.return_value = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset(),
            "options": {},
        }
        mock_get_schema_details.return_value = {
            "order": ["日期", "任教老師"],
            "required": ["日期", "任教老師"],
        }
        mock_read_excel.return_value = pd.DataFrame(
            {
                "日期": ["2024-01-01"],
                "任教老師": ["Ms. Chan"],
            }
        )

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(b"duplicate")
            tmp.flush()
            excel_path = tmp.name

        try:
            with mock.patch("pandas.DataFrame.to_csv") as fake_to_csv:
                csv_path, file_hash = prep_excel.main(excel_path)
        finally:
            os.remove(excel_path)

        fake_to_csv.assert_not_called()
        self.assertIsNone(csv_path)
        self.assertIsInstance(file_hash, str)
        mock_hash_exists.assert_called_once()


class PrepExcelMainTests(unittest.TestCase):
    def setUp(self):
        prep_excel._get_sheet_config.cache_clear()

    def tearDown(self):
        prep_excel._get_sheet_config.cache_clear()

    def test_main_sanitizes_extra_columns(self):
        df = pd.DataFrame(
            {
                "日期": ["2024-01-01"],
                "任教老師": ["Teacher"],
                "教學跟進/回饋": ["Feedback"],
                "2023 Amount": ["10"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            xlsx_path = os.path.join(tmpdir, "input.xlsx")
            with open(xlsx_path, "wb") as handle:
                handle.write(b"dummy")

            with mock.patch.object(
                prep_excel.pd, "read_excel", return_value=df
            ), mock.patch.object(
                prep_excel, "_get_table_config", return_value={"table": "teach_record_raw", "options": {}},
            ), mock.patch.object(
                prep_excel,
                "get_schema_details",
                return_value={"order": ["日期", "任教老師"], "required": ["日期"]},
            ), mock.patch.object(
                prep_excel, "_staging_file_hash_exists", return_value=False
            ):
                csv_path, file_hash = prep_excel.main(
                    xlsx_path, emit_stdout=False
                )

            self.assertIsNotNone(csv_path)
            self.assertIsNotNone(file_hash)

            with open(csv_path, encoding="utf-8") as csv_file:
                header_line = csv_file.readline().strip()

        self.assertEqual(header_line, "日期,任教老師,教學跟進_回饋,_2023_amount")


if __name__ == "__main__":
    unittest.main()
