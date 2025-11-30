import json
import os
import re
import sys
import tempfile
import unittest
import warnings
from collections import OrderedDict
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import pymysql

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


class _AlteringCursor:
    def __init__(self, log):
        self._log = log

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self._log.append((query, params))


class _AlteringConnection:
    def __init__(self):
        self.commands: list[tuple[str, object]] = []
        self.commits = 0

    def cursor(self, *args, **kwargs):
        return _AlteringCursor(self.commands)

    def commit(self):
        self.commits += 1

    def close(self):
        pass


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


class _DDLTrackingCursor:
    def __init__(self, connection):
        self._connection = connection
        self._rows: list[dict[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def _build_column_details(self, column_type: str) -> dict[str, object]:
        text = column_type.strip()
        nullable = "NOT NULL" not in text.upper()
        return {"type": text, "nullable": nullable, "default": None}

    def _parse_table_name(self, query: str) -> str:
        match = re.search(r"`([^`]+)`", query)
        if not match:
            raise AssertionError(f"Unable to parse table name from query: {query}")
        return match.group(1)

    def _split_column_definitions(self, body: str) -> list[str]:
        parts: list[str] = []
        current: list[str] = []
        depth = 0
        for char in body:
            if char == "(":
                depth += 1
            elif char == ")" and depth > 0:
                depth -= 1
            if char == "," and depth == 0:
                part = "".join(current).strip()
                if part:
                    parts.append(part)
                current = []
            else:
                current.append(char)
        tail = "".join(current).strip()
        if tail:
            parts.append(tail)
        return parts

    def _parse_create_definition(
        self, query: str
    ) -> tuple[str, "OrderedDict[str, dict[str, object]]"]:
        table_name = self._parse_table_name(query)
        start = query.find("(")
        end = query.rfind(")")
        body = query[start + 1 : end]
        columns: "OrderedDict[str, dict[str, object]]" = OrderedDict()
        for definition in self._split_column_definitions(body):
            definition = definition.strip()
            if not definition or not definition.startswith("`"):
                continue
            closing = definition.find("`", 1)
            if closing == -1:
                continue
            name = definition[1:closing]
            column_type = definition[closing + 1 :].strip()
            if not column_type:
                continue
            columns[name] = self._build_column_details(column_type)
        return table_name, columns

    def _parse_alter_clause(self, query: str, clause: str) -> tuple[str, str]:
        upper_query = query.upper()
        index = upper_query.index(clause)
        segment = query[index + len(clause) :].strip()
        segment = segment.rstrip(";")
        if segment.startswith("`"):
            closing = segment.find("`", 1)
            name = segment[1:closing]
            column_type = segment[closing + 1 :].strip()
        else:
            parts = segment.split(None, 1)
            name = parts[0].strip("`")
            column_type = parts[1] if len(parts) > 1 else ""
        return name, column_type.strip()

    def execute(self, query, params=None):
        self._connection.commands.append((query, params))
        statement = query.strip()
        upper = statement.upper()
        if upper.startswith("SELECT COLUMN_NAME"):
            schema, table = params
            if table not in self._connection.tables:
                raise pymysql.err.ProgrammingError(
                    1146, f"Table '{schema}.{table}' doesn't exist"
                )
            self._rows = [
                {
                    "COLUMN_NAME": name,
                    "IS_NULLABLE": "YES" if details["nullable"] else "NO",
                    "COLUMN_DEFAULT": details.get("default"),
                    "COLUMN_TYPE": details["type"],
                }
                for name, details in self._connection.tables[table].items()
            ]
        elif upper.startswith("CREATE TABLE"):
            table_name, columns = self._parse_create_definition(statement)
            self._connection.tables[table_name] = columns
            self._rows = []
        elif upper.startswith("ALTER TABLE"):
            table_name = self._parse_table_name(statement)
            if table_name not in self._connection.tables:
                self._connection.tables[table_name] = OrderedDict()
            if "ADD COLUMN" in upper:
                name, column_type = self._parse_alter_clause(statement, "ADD COLUMN")
                self._connection.tables[table_name][name] = self._build_column_details(
                    column_type
                )
            if "MODIFY COLUMN" in upper:
                name, column_type = self._parse_alter_clause(statement, "MODIFY COLUMN")
                self._connection.tables[table_name][name] = self._build_column_details(
                    column_type
                )
            self._rows = []
        elif upper.startswith("SELECT 1 FROM"):
            self._rows = []
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _DDLTrackingConnection:
    def __init__(self):
        self.tables: dict[str, "OrderedDict[str, dict[str, object]]"] = {}
        self.commands: list[tuple[str, object]] = []
        self.commits = 0
        self.closed = False

    def cursor(self, *args, **kwargs):
        return _DDLTrackingCursor(self)

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


class _MissingInfoSchemaCursor:
    def __init__(self, connection):
        self._connection = connection
        self._rows: list[dict[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self._connection.commands.append((query, params))
        statement = query.strip()
        upper = statement.upper()
        if upper.startswith("SELECT COLUMN_NAME"):
            if not self._connection.table_created:
                self._rows = []
            else:
                self._rows = [
                    {
                        "COLUMN_NAME": "id",
                        "IS_NULLABLE": "NO",
                        "COLUMN_DEFAULT": None,
                        "COLUMN_TYPE": "bigint(20) unsigned",
                    },
                    {
                        "COLUMN_NAME": "file_hash",
                        "IS_NULLABLE": "NO",
                        "COLUMN_DEFAULT": None,
                        "COLUMN_TYPE": "char(64)",
                    },
                    {
                        "COLUMN_NAME": "日期",
                        "IS_NULLABLE": "NO",
                        "COLUMN_DEFAULT": None,
                        "COLUMN_TYPE": "date",
                    },
                ]
        elif upper.startswith("CREATE TABLE"):
            self._connection.table_created = True
            self._rows = []
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _MissingInfoSchemaConnection:
    def __init__(self):
        self.commands: list[tuple[str, object]] = []
        self.table_created = False
        self.commits = 0
        self.closed = False

    def cursor(self, *args, **kwargs):
        return _MissingInfoSchemaCursor(self)

    def commit(self):
        self.commits += 1

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
                "workbook_type": "default",
                "sheet_name": prep_excel.DEFAULT_SHEET,
                "staging_table": "teach_record_raw",
                "metadata_columns": json.dumps(
                    [
                        "id",
                        "file_hash",
                        "batch_id",
                        "source_year",
                        "ingested_at",
                        "processed_at",
                    ]
                ),
                "required_columns": json.dumps([]),
                "options": None,
            }
        ]
        rows = [
            {
                "COLUMN_NAME": "id",
                "IS_NULLABLE": "NO",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "int(11)",
            },
            {
                "COLUMN_NAME": "日期",
                "IS_NULLABLE": "NO",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "date",
            },
            {
                "COLUMN_NAME": "任教老師",
                "IS_NULLABLE": "YES",
                "COLUMN_DEFAULT": "",
                "COLUMN_TYPE": "varchar(255)",
            },
            {
                "COLUMN_NAME": "file_hash",
                "IS_NULLABLE": "NO",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "varchar(64)",
            },
            {
                "COLUMN_NAME": "學生編號",
                "IS_NULLABLE": "NO",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "varchar(32)",
            },
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

    def test_ensure_staging_columns_creates_table_when_information_schema_empty(self):
        connection = _MissingInfoSchemaConnection()
        headers = ["日期"]
        config = {
            "table": "teach_record_raw",
            "metadata_columns": ["id", "file_hash"],
            "metadata_column_order": ["id", "file_hash"],
            "required_columns": ["日期"],
            "required_column_order": ["日期"],
            "column_types": {"日期": "DATE NOT NULL"},
        }

        schema_changed = prep_excel._ensure_staging_columns(
            headers=headers,
            config=config,
            connection=connection,
        )

        self.assertTrue(schema_changed)
        self.assertTrue(connection.table_created)
        self.assertEqual(connection.commits, 1)
        self.assertGreaterEqual(len(connection.commands), 2)
        select_query, _ = connection.commands[0]
        create_query, _ = connection.commands[1]
        self.assertIn("SELECT COLUMN_NAME", select_query)
        self.assertIn("CREATE TABLE", create_query)

    def test_get_table_config_supports_multiple_workbook_types(self):
        rows = [
            {
                "workbook_type": "default",
                "sheet_name": "Shared",
                "staging_table": "shared_default_raw",
                "metadata_columns": json.dumps([]),
                "required_columns": json.dumps([]),
                "column_mappings": None,
                "options": None,
            },
            {
                "workbook_type": "alt",
                "sheet_name": "Shared",
                "staging_table": "shared_alt_raw",
                "metadata_columns": json.dumps([]),
                "required_columns": json.dumps([]),
                "column_mappings": None,
                "options": None,
            },
        ]

        connection = _FakeConnection(rows)

        config_alt = prep_excel._get_table_config(
            "Shared", workbook_type="alt", connection=connection
        )
        self.assertEqual(config_alt["table"], "shared_alt_raw")

        config_default = prep_excel._get_table_config(
            "Shared", workbook_type="default", connection=connection
        )
        self.assertEqual(config_default["table"], "shared_default_raw")

    def test_get_table_config_falls_back_to_default_workbook_type(self):
        rows = [
            {
                "workbook_type": "default",
                "sheet_name": "OnlyDefault",
                "staging_table": "default_raw",
                "metadata_columns": json.dumps([]),
                "required_columns": json.dumps([]),
                "column_mappings": None,
                "options": None,
            }
        ]

        connection = _FakeConnection(rows)

        config = prep_excel._get_table_config(
            "OnlyDefault", workbook_type="custom", connection=connection
        )
        self.assertEqual(config["table"], "default_raw")

    def test_get_table_config_raises_for_unknown_workbook_type(self):
        rows = []
        connection = _FakeConnection(rows)

        with self.assertRaisesRegex(ValueError, "Unsupported workbook type"):
            prep_excel._get_table_config(
                "Missing", workbook_type="nonexistent", connection=connection
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
                "metadata_columns": json.dumps(["id", "file_hash", "processed_at"]),
                "required_columns": json.dumps(["日期"]),
                "options": None,
            }
        ]
        rows = [
            {
                "COLUMN_NAME": "id",
                "IS_NULLABLE": "NO",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "int(11)",
            },
            {
                "COLUMN_NAME": "日期",
                "IS_NULLABLE": "NO",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "date",
            },
            {
                "COLUMN_NAME": "任教老師",
                "IS_NULLABLE": "YES",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "varchar(255)",
            },
            {
                "COLUMN_NAME": "file_hash",
                "IS_NULLABLE": "NO",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "varchar(64)",
            },
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

    def test_sheet_config_prefers_row_with_normalized_table(self):
        generic_row = {
            "sheet_name": prep_excel.DEFAULT_SHEET,
            "staging_table": "teach_record_raw",
            "metadata_columns": json.dumps(["id", "file_hash", "processed_at"]),
            "required_columns": json.dumps([]),
            "options": json.dumps({}),
        }
        specific_row = {
            "sheet_name": prep_excel.DEFAULT_SHEET,
            "staging_table": "teach_record_raw",
            "metadata_columns": json.dumps(["id", "file_hash", "processed_at"]),
            "required_columns": json.dumps([]),
            "options": json.dumps({"normalized_table": "teach_record_normalized"}),
        }

        connection = _FakeConnection([generic_row, specific_row])

        config = prep_excel._get_table_config(
            prep_excel.DEFAULT_SHEET, connection=connection
        )

        self.assertEqual(config["normalized_table"], "teach_record_normalized")

    def test_sheet_config_derives_normalized_table_when_missing(self):
        row = {
            "sheet_name": prep_excel.DEFAULT_SHEET,
            "staging_table": "teach_record_raw",
            "metadata_columns": json.dumps(["id", "file_hash", "processed_at"]),
            "required_columns": json.dumps([]),
            "options": json.dumps({}),
            "column_mappings": None,
        }

        connection = _FakeConnection([row])

        config = prep_excel._get_table_config(
            prep_excel.DEFAULT_SHEET, connection=connection
        )

        self.assertEqual(config["normalized_table"], "teach_record_normalized")

    def test_parse_sheet_config_rows_includes_normalization_overrides(self):
        row = {
            "workbook_type": "default",
            "sheet_name": prep_excel.DEFAULT_SHEET,
            "staging_table": "teach_record_raw",
            "metadata_columns": json.dumps(["id", "file_hash", "processed_at"]),
            "required_columns": json.dumps([]),
            "column_mappings": None,
            "options": json.dumps(
                {
                    "normalized_metadata_columns": [
                        "file_hash",
                        "raw_id",
                        "custom_meta",
                        "raw_id",
                    ],
                    "reserved_source_columns": ["id", "custom_reserved", "id"],
                    "normalized_column_type_overrides": {
                        "日期": "DATETIME NULL",
                        "上課時數": None,
                        "": "ignored",
                    },
                }
            ),
        }

        config = prep_excel._parse_sheet_config_rows([row])
        entry = config["default"][prep_excel.DEFAULT_SHEET]

        self.assertEqual(
            entry["normalized_metadata_columns"],
            ("file_hash", "raw_id", "custom_meta"),
        )
        self.assertEqual(
            entry["reserved_source_columns"],
            frozenset({"id", "custom_reserved"}),
        )
        self.assertEqual(
            entry["normalized_column_type_overrides"],
            {"日期": "DATETIME NULL"},
        )

    def test_get_schema_details_with_injected_connection(self):
        config_rows = [
            {
                "sheet_name": prep_excel.DEFAULT_SHEET,
                "staging_table": "teach_record_raw",
                "metadata_columns": json.dumps(["id", "file_hash", "processed_at"]),
                "required_columns": json.dumps([]),
                "options": None,
            }
        ]
        column_rows = [
            {
                "COLUMN_NAME": "id",
                "IS_NULLABLE": "NO",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "int(11)",
            },
            {
                "COLUMN_NAME": "日期",
                "IS_NULLABLE": "NO",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "date",
            },
            {
                "COLUMN_NAME": "任教老師",
                "IS_NULLABLE": "YES",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "varchar(255)",
            },
            {
                "COLUMN_NAME": "file_hash",
                "IS_NULLABLE": "NO",
                "COLUMN_DEFAULT": None,
                "COLUMN_TYPE": "varchar(64)",
            },
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

    @mock.patch.object(prep_excel, "_ensure_staging_columns", return_value=False)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_raises_missing_columns_error(
        self,
        _mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
        _mock_ensure,
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
        _mock_read_excel.return_value = pd.DataFrame({"任教老師": ["Ms. Chan"]})

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

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=False)
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch.object(prep_excel, "_fetch_table_columns")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_adds_missing_staging_columns(
        self,
        mock_read_excel,
        mock_fetch_columns,
        mock_get_table_config,
        _mock_hash_exists,
    ):
        df = pd.DataFrame(
            {
                "日期": ["2024-01-01"],
                "任教老師": ["Ms. Chan"],
                "新欄位": ["optional"],
            }
        )
        mock_read_excel.return_value = df

        config = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset({"file_hash", "batch_id", "source_year", "ingested_at"}),
            "options": {},
            "column_types": {},
        }
        mock_get_table_config.return_value = config

        existing = [
            {
                "name": "file_hash",
                "is_nullable": False,
                "default": None,
                "type": "char(64)",
            },
            {
                "name": "batch_id",
                "is_nullable": True,
                "default": None,
                "type": "char(36)",
            },
            {
                "name": "source_year",
                "is_nullable": True,
                "default": None,
                "type": "int",
            },
            {
                "name": "ingested_at",
                "is_nullable": False,
                "default": None,
                "type": "datetime",
            },
            {
                "name": "日期",
                "is_nullable": True,
                "default": None,
                "type": "date",
            },
            {
                "name": "任教老師",
                "is_nullable": True,
                "default": None,
                "type": "varchar(255)",
            },
        ]
        expanded = existing + [
            {
                "name": "新欄位",
                "is_nullable": True,
                "default": None,
                "type": "varchar(255)",
            },
        ]
        mock_fetch_columns.side_effect = [existing, expanded]

        captured: dict[str, object] = {}

        def fake_to_csv(self, path, *_args, **_kwargs):
            captured["columns"] = list(self.columns)
            captured["path"] = path
            return None

        connection = _AlteringConnection()

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(b"dummy")
            excel_path = tmp.name

        try:
            with mock.patch("pandas.DataFrame.to_csv", new=fake_to_csv):
                csv_path, _ = prep_excel.main(excel_path, connection=connection, emit_stdout=False)
        finally:
            os.remove(excel_path)

        self.assertIn(
            (
                "ALTER TABLE `teach_record_raw` ADD COLUMN `新欄位` VARCHAR(255) NULL",
                None,
            ),
            connection.commands,
        )
        self.assertGreaterEqual(connection.commits, 1)
        self.assertEqual(mock_fetch_columns.call_count, 2)
        self.assertEqual(captured["columns"], ["日期", "任教老師", "新欄位"])
        self.assertEqual(csv_path, captured["path"])

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=False)
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch.object(prep_excel, "_fetch_table_columns")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_adds_missing_staging_columns_with_type_override(
        self,
        mock_read_excel,
        mock_fetch_columns,
        mock_get_table_config,
        _mock_hash_exists,
    ):
        df = pd.DataFrame(
            {
                "日期": ["2024-01-01"],
                "教學跟進/回饋": ["feedback"],
            }
        )
        mock_read_excel.return_value = df

        config = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset({"file_hash", "batch_id", "source_year", "ingested_at"}),
            "options": {},
            "column_types": {"教學跟進/回饋": "TEXT NULL"},
        }
        mock_get_table_config.return_value = config

        existing = [
            {
                "name": "日期",
                "is_nullable": True,
                "default": None,
                "type": "date",
            },
        ]
        expanded = existing + [
            {
                "name": "教學跟進/回饋",
                "is_nullable": True,
                "default": None,
                "type": "text",
            },
        ]
        mock_fetch_columns.side_effect = [existing, expanded]

        connection = _AlteringConnection()

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(b"dummy")
            excel_path = tmp.name

        try:
            with mock.patch("pandas.DataFrame.to_csv", return_value=None):
                prep_excel.main(excel_path, connection=connection, emit_stdout=False)
        finally:
            os.remove(excel_path)

        assert (
            "ALTER TABLE `teach_record_raw` ADD COLUMN `教學跟進/回饋` TEXT NULL",
            None,
        ) in connection.commands

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=False)
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch.object(prep_excel, "_fetch_table_columns")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_modifies_existing_staging_column_type_when_override_differs(
        self,
        mock_read_excel,
        mock_fetch_columns,
        mock_get_table_config,
        _mock_hash_exists,
    ):
        df = pd.DataFrame(
            {
                "日期": ["2024-01-01"],
                "教學跟進/回饋": ["feedback"],
            }
        )
        mock_read_excel.return_value = df

        config = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset(
                {"file_hash", "batch_id", "source_year", "ingested_at"}
            ),
            "options": {},
            "column_types": {"教學跟進/回饋": "TEXT NULL"},
        }
        mock_get_table_config.return_value = config

        existing = [
            {
                "name": "日期",
                "is_nullable": True,
                "default": None,
                "type": "date",
            },
            {
                "name": "教學跟進/回饋",
                "is_nullable": True,
                "default": None,
                "type": "varchar(255)",
            },
        ]
        refreshed = [
            {
                "name": "日期",
                "is_nullable": True,
                "default": None,
                "type": "date",
            },
            {
                "name": "教學跟進/回饋",
                "is_nullable": True,
                "default": None,
                "type": "text",
            },
        ]
        mock_fetch_columns.side_effect = [existing, refreshed]

        connection = _AlteringConnection()

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(b"dummy")
            excel_path = tmp.name

        try:
            with mock.patch("pandas.DataFrame.to_csv", return_value=None):
                prep_excel.main(excel_path, connection=connection, emit_stdout=False)
        finally:
            os.remove(excel_path)

        assert (
            "ALTER TABLE `teach_record_raw` MODIFY COLUMN `教學跟進/回饋` TEXT NULL",
            None,
        ) in connection.commands
        self.assertEqual(connection.commits, 1)

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=False)
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch.object(prep_excel, "_fetch_table_columns")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_skips_modify_when_staging_column_matches_override(
        self,
        mock_read_excel,
        mock_fetch_columns,
        mock_get_table_config,
        _mock_hash_exists,
    ):
        df = pd.DataFrame(
            {
                "日期": ["2024-01-01"],
                "教學跟進/回饋": ["feedback"],
            }
        )
        mock_read_excel.return_value = df

        config = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset(
                {"file_hash", "batch_id", "source_year", "ingested_at"}
            ),
            "options": {},
            "column_types": {"教學跟進/回饋": "TEXT NULL"},
        }
        mock_get_table_config.return_value = config

        existing = [
            {
                "name": "file_hash",
                "is_nullable": False,
                "default": None,
                "type": "char(64)",
            },
            {
                "name": "batch_id",
                "is_nullable": True,
                "default": None,
                "type": "char(36)",
            },
            {
                "name": "source_year",
                "is_nullable": True,
                "default": None,
                "type": "int",
            },
            {
                "name": "ingested_at",
                "is_nullable": False,
                "default": None,
                "type": "datetime",
            },
            {
                "name": "日期",
                "is_nullable": True,
                "default": None,
                "type": "date",
            },
            {
                "name": "教學跟進/回饋",
                "is_nullable": True,
                "default": None,
                "type": "text",
            },
        ]
        mock_fetch_columns.side_effect = [existing, existing]

        connection = _AlteringConnection()

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(b"dummy")
            excel_path = tmp.name

        try:
            with mock.patch("pandas.DataFrame.to_csv", return_value=None):
                prep_excel.main(excel_path, connection=connection, emit_stdout=False)
        finally:
            os.remove(excel_path)

        self.assertEqual(connection.commands, [])
        self.assertEqual(connection.commits, 0)

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
    @mock.patch.object(prep_excel, "_ensure_staging_columns", return_value=False)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_normalizes_subject_column_when_option_enabled(
        self,
        mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
        _mock_ensure,
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
    @mock.patch.object(prep_excel, "_ensure_staging_columns", return_value=False)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("pandas.DataFrame.to_csv", return_value=None)
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_suppresses_openpyxl_default_style_warning(
        self,
        mock_read_excel,
        mock_to_csv,
        mock_get_table_config,
        mock_get_schema_details,
        _mock_ensure,
        _mock_hash_exists,
    ):
        mock_get_table_config.return_value = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset(),
            "options": {},
        }
        mock_get_schema_details.return_value = {
            "order": ["日期"],
            "required": ["日期"],
        }

        def emit_warning(*_args, **_kwargs):
            warnings.warn_explicit(
                "Workbook contains no default style, apply openpyxl's default",
                UserWarning,
                filename="stylesheet.py",
                lineno=1,
                module="openpyxl.styles.stylesheet",
            )
            return pd.DataFrame({"日期": ["2024-01-01"]})

        mock_read_excel.side_effect = emit_warning

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(b"dummy")
            excel_path = tmp.name

        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                prep_excel.main(excel_path)
        finally:
            os.remove(excel_path)

        self.assertEqual(caught, [])
        mock_to_csv.assert_called_once()

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=False)
    @mock.patch.object(prep_excel, "_ensure_staging_columns", return_value=False)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_skips_subject_normalization_when_option_disabled(
        self,
        mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
        _mock_ensure,
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
    @mock.patch.object(prep_excel, "_ensure_staging_columns", return_value=False)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_threads_connection_parameter(
        self,
        mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
        _mock_ensure,
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
            prep_excel.DEFAULT_SHEET,
            workbook_type="default",
            connection=connection,
            db_settings=None,
        )
        mock_get_schema_details.assert_called_once_with(
            prep_excel.DEFAULT_SHEET,
            workbook_type="default",
            connection=connection,
            db_settings=None,
        )
        self.assertTrue(mock_hash_exists.called)
        _, hash_kwargs = mock_hash_exists.call_args
        self.assertEqual(hash_kwargs["connection"], connection)
        self.assertIsNone(hash_kwargs["db_settings"])

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=False)
    @mock.patch.object(prep_excel, "_ensure_staging_columns", return_value=False)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_uses_unique_output_path_per_file(
        self,
        mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
        _mock_ensure,
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
    @mock.patch.object(prep_excel, "_ensure_staging_columns", return_value=False)
    @mock.patch.object(prep_excel, "get_schema_details")
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_skips_when_file_hash_already_exists(
        self,
        mock_read_excel,
        mock_get_table_config,
        mock_get_schema_details,
        _mock_ensure,
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

    @mock.patch.object(prep_excel, "_staging_file_hash_exists", return_value=False)
    @mock.patch.object(prep_excel, "_get_table_config")
    @mock.patch("app.prep_excel.pd.read_excel")
    def test_main_recreates_missing_staging_table_with_metadata_defaults(
        self,
        mock_read_excel,
        mock_get_table_config,
        _mock_hash_exists,
    ):
        connection = _DDLTrackingConnection()
        metadata_columns = (
            "id",
            "file_hash",
            "batch_id",
            "source_year",
            "ingested_at",
            "processed_at",
        )
        required_columns = ("日期", "任教老師")
        config = {
            "table": "teach_record_raw",
            "metadata_columns": frozenset(metadata_columns),
            "metadata_column_order": metadata_columns,
            "required_columns": frozenset(required_columns),
            "required_column_order": required_columns,
            "options": {},
            "column_types": {"教學跟進/回饋": "TEXT NULL"},
        }
        mock_get_table_config.return_value = config
        mock_read_excel.return_value = pd.DataFrame(
            {
                "日期": ["2024-01-01"],
                "任教老師": ["Ms. Chan"],
                "教學跟進/回饋": ["notes"],
            }
        )

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(b"missing-table")
            tmp.flush()
            excel_path = tmp.name

        try:
            with mock.patch("pandas.DataFrame.to_csv", return_value=None):
                prep_excel.main(excel_path, connection=connection, emit_stdout=False)
        finally:
            os.remove(excel_path)

        create_statements = [
            sql for sql, _params in connection.commands if sql.strip().upper().startswith("CREATE TABLE")
        ]
        self.assertEqual(len(create_statements), 1)

        schema = connection.tables.get("teach_record_raw")
        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(
            schema["id"]["type"], "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"
        )
        self.assertEqual(schema["file_hash"]["type"], "CHAR(64) NOT NULL")
        self.assertEqual(schema["batch_id"]["type"], "CHAR(36) NULL")
        self.assertEqual(schema["ingested_at"]["type"], "DATETIME NOT NULL")
        self.assertEqual(schema["processed_at"]["type"], "DATETIME NULL DEFAULT NULL")
        self.assertEqual(schema["日期"]["type"], "VARCHAR(255) NULL")
        self.assertEqual(schema["任教老師"]["type"], "VARCHAR(255) NULL")
        self.assertEqual(schema["教學跟進/回饋"]["type"], "TEXT NULL")
        self.assertGreaterEqual(connection.commits, 1)


if __name__ == "__main__":
    unittest.main()
