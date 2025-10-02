import os
import runpy
import sys
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_USER", "user")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "test_db")
os.environ.setdefault("DB_CHARSET", "utf8mb4")

from app import ingest_excel


class _Cursor:
    def __init__(
        self,
        *,
        rowcount: int = 0,
        exc: Exception | None = None,
        fetchone_results: list[object] | None = None,
    ):
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.rowcount = rowcount
        self._exc = exc
        self._fetchone_results = list(fetchone_results or [])

    def __enter__(self):
        if self._exc:
            raise self._exc
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        if self._exc:
            raise self._exc
        self.executed.append((query, tuple(params or ())))

    def fetchone(self):
        if self._exc:
            raise self._exc
        if self._fetchone_results:
            return self._fetchone_results.pop(0)
        return None


class _Connection:
    def __init__(self, cursor: _Cursor):
        self._cursor = cursor
        self.closed = False
        self.begun = False
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self._cursor

    def begin(self):
        self.begun = True

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class IngestExcelTests(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch("app.ingest_excel.datetime")
        self.addCleanup(patcher.stop)
        self.mock_datetime = patcher.start()
        self.now = datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc)
        self.mock_datetime.now.return_value = self.now
        self.mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        self.hash_exists_patcher = mock.patch.object(
            ingest_excel.prep_excel, "_staging_file_hash_exists", return_value=False
        )
        self.addCleanup(self.hash_exists_patcher.stop)
        self.mock_hash_exists = self.hash_exists_patcher.start()

    def _create_csv(self, header, row=None):
        temp = tempfile.NamedTemporaryFile("w", delete=False, newline="", encoding="utf-8")
        temp.write(",".join(header) + "\n")
        if row is None:
            row = [f"value{i+1}" for i in range(len(header))]
        temp.write(",".join(row) + "\n")
        temp.flush()
        temp.close()
        self.addCleanup(lambda: os.remove(temp.name))
        return temp.name

    def test_main_loads_csv_into_table(self):
        header = ["日期", "任教老師"]
        csv_path = self._create_csv(header)

        fake_cursor = _Cursor(rowcount=2, fetchone_results=[(1,)])
        connection = _Connection(fake_cursor)

        with mock.patch.object(ingest_excel.prep_excel, "main", return_value=(csv_path, "abc")) as prep_main, mock.patch.object(
            ingest_excel.prep_excel, "_get_table_config", return_value={"table": "teach_record_raw"}
        ) as get_config, mock.patch.object(
            ingest_excel.prep_excel, "get_schema_details", return_value={"order": header}
        ) as get_schema, mock.patch.object(
            ingest_excel.pymysql, "connect", return_value=connection
        ) as connect, mock.patch.object(
            ingest_excel.uuid, "uuid4", return_value=uuid.UUID("12345678123456781234567812345678")
        ) as mock_uuid:
            ingest_excel.main("workbook.xlsx", source_year="2024")

        prep_main.assert_called_once_with(
            "workbook.xlsx", ingest_excel.prep_excel.DEFAULT_SHEET, emit_stdout=False, db_settings=None
        )
        get_config.assert_called_once()
        get_schema.assert_called_once()
        connect.assert_called_once()
        mock_uuid.assert_called_once()
        args, kwargs = connect.call_args
        self.assertIn("local_infile", kwargs)
        self.assertTrue(kwargs["local_infile"])
        self.assertIn("client_flag", kwargs)
        self.assertEqual(kwargs["client_flag"], ingest_excel.pymysql.constants.CLIENT.LOCAL_FILES)

        self.assertTrue(connection.begun)
        self.assertTrue(connection.committed)
        self.assertFalse(connection.rolled_back)
        self.mock_hash_exists.assert_called_once_with(
            "teach_record_raw", "abc", connection=connection, db_settings=None
        )

        self.assertEqual(len(fake_cursor.executed), 2)
        query, params = fake_cursor.executed[1]
        column_list = ", ".join(f"`{name}`" for name in header)
        expected_query = (
            "LOAD DATA LOCAL INFILE %s INTO TABLE `teach_record_raw` "
            "FIELDS TERMINATED BY ',' ENCLOSED BY '\"' "
            "LINES TERMINATED BY '\n' IGNORE 1 LINES "
            f"({column_list}) "
            "SET file_hash = %s, batch_id = %s, source_year = %s, ingested_at = %s"
        )
        self.assertEqual(query, expected_query)
        self.assertEqual(params[0], csv_path)
        self.assertEqual(
            params[1:],
            ("abc", "12345678-1234-5678-1234-567812345678", "2024", self.now),
        )

    def test_main_ignores_trailing_csv_columns(self):
        schema_header = ["日期", "任教老師"]
        full_header = schema_header + ["Extra A", "Extra B"]
        csv_path = self._create_csv(full_header)

        fake_cursor = _Cursor(rowcount=2, fetchone_results=[(1,)])
        connection = _Connection(fake_cursor)

        with mock.patch.object(
            ingest_excel.prep_excel, "main", return_value=(csv_path, "abc")
        ) as prep_main, mock.patch.object(
            ingest_excel.prep_excel, "_get_table_config", return_value={"table": "teach_record_raw"}
        ) as get_config, mock.patch.object(
            ingest_excel.prep_excel, "get_schema_details", return_value={"order": schema_header}
        ) as get_schema, mock.patch.object(
            ingest_excel.pymysql, "connect", return_value=connection
        ) as connect:
            ingest_excel.main("workbook.xlsx", source_year="2024", batch_id="batch-1")

        prep_main.assert_called_once()
        get_config.assert_called_once()
        get_schema.assert_called_once()
        connect.assert_called_once()

        self.assertTrue(connection.begun)
        self.assertTrue(connection.committed)
        self.assertFalse(connection.rolled_back)

        self.assertEqual(len(fake_cursor.executed), 2)
        query, params = fake_cursor.executed[1]
        column_list = ", ".join(["`日期`", "`任教老師`", "@unused_0", "@unused_1"])
        expected_query = (
            "LOAD DATA LOCAL INFILE %s INTO TABLE `teach_record_raw` "
            "FIELDS TERMINATED BY ',' ENCLOSED BY '\"' "
            "LINES TERMINATED BY '\n' IGNORE 1 LINES "
            f"({column_list}) "
            "SET file_hash = %s, batch_id = %s, source_year = %s, ingested_at = %s"
        )
        self.assertEqual(query, expected_query)
        self.assertEqual(params[0], csv_path)
        self.assertEqual(params[1:4], ("abc", "batch-1", "2024"))
        self.assertEqual(params[4], self.now)

    def test_main_skips_when_duplicate(self):
        with mock.patch.object(
            ingest_excel.prep_excel, "main", return_value=(None, "abc")
        ), mock.patch.object(
            ingest_excel.pymysql, "connect"
        ) as connect:
            ingest_excel.main("workbook.xlsx", source_year="2024")
        connect.assert_not_called()

    def test_main_header_mismatch_raises(self):
        csv_path = self._create_csv(["日期", "任教老師"])
        with mock.patch.object(
            ingest_excel.prep_excel, "main", return_value=(csv_path, "abc")
        ), mock.patch.object(
            ingest_excel.prep_excel, "_get_table_config", return_value={"table": "teach_record_raw"}
        ), mock.patch.object(
            ingest_excel.prep_excel, "get_schema_details", return_value={"order": ["A", "B"]}
        ), mock.patch.object(
            ingest_excel.pymysql, "connect"
        ) as connect:
            with self.assertRaises(ValueError):
                ingest_excel.main("workbook.xlsx", source_year="2024")
        connect.assert_not_called()

    def test_main_rolls_back_on_error(self):
        header = ["日期", "任教老師"]
        csv_path = self._create_csv(header)
        cursor_exc = RuntimeError("boom")
        fake_cursor = _Cursor(exc=cursor_exc)
        connection = _Connection(fake_cursor)

        with mock.patch.object(ingest_excel.prep_excel, "main", return_value=(csv_path, "abc")), mock.patch.object(
            ingest_excel.prep_excel, "_get_table_config", return_value={"table": "teach_record_raw"}
        ), mock.patch.object(
            ingest_excel.prep_excel, "get_schema_details", return_value={"order": header}
        ), mock.patch.object(
            ingest_excel.pymysql, "connect", return_value=connection
        ):
            with self.assertRaises(RuntimeError):
                ingest_excel.main("workbook.xlsx", source_year="2024")

        self.assertTrue(connection.begun)
        self.assertTrue(connection.rolled_back)
        self.assertFalse(connection.committed)

    def test_main_requires_source_year(self):
        with mock.patch.object(ingest_excel.prep_excel, "main") as prep_main:
            with self.assertRaisesRegex(ValueError, "source_year"):
                ingest_excel.main("workbook.xlsx", source_year="")
        prep_main.assert_not_called()
        self.mock_hash_exists.assert_not_called()

    def test_main_rejects_empty_csv(self):
        header = ["日期", "任教老師"]
        temp = tempfile.NamedTemporaryFile("w", delete=False, newline="", encoding="utf-8")
        temp.write(",".join(header) + "\n")
        temp.flush()
        temp.close()
        self.addCleanup(lambda: os.remove(temp.name))

        with mock.patch.object(ingest_excel.prep_excel, "main", return_value=(temp.name, "abc")), mock.patch.object(
            ingest_excel.prep_excel, "_get_table_config", return_value={"table": "teach_record_raw"}
        ), mock.patch.object(
            ingest_excel.prep_excel, "get_schema_details", return_value={"order": header}
        ), mock.patch.object(
            ingest_excel.pymysql, "connect"
        ) as connect:
            with self.assertRaisesRegex(ValueError, "no data rows"):
                ingest_excel.main("workbook.xlsx", source_year="2024")
        connect.assert_not_called()
        self.mock_hash_exists.assert_not_called()

    def test_main_checks_local_infile_setting(self):
        header = ["日期", "任教老師"]
        csv_path = self._create_csv(header)

        fake_cursor = _Cursor(fetchone_results=[(0,)])
        connection = _Connection(fake_cursor)

        with mock.patch.object(ingest_excel.prep_excel, "main", return_value=(csv_path, "abc")), mock.patch.object(
            ingest_excel.prep_excel, "_get_table_config", return_value={"table": "teach_record_raw"}
        ), mock.patch.object(
            ingest_excel.prep_excel, "get_schema_details", return_value={"order": header}
        ), mock.patch.object(
            ingest_excel.pymysql, "connect", return_value=connection
        ):
            with self.assertRaisesRegex(RuntimeError, "LOCAL INFILE"):
                ingest_excel.main("workbook.xlsx", source_year="2024")

        self.assertTrue(connection.begun)
        self.assertTrue(connection.rolled_back)
        self.assertFalse(connection.committed)
        self.assertEqual(len(fake_cursor.executed), 1)
        self.mock_hash_exists.assert_not_called()

    def test_main_duplicate_hash_after_schema_changes(self):
        header = ["日期", "任教老師"]
        csv_path = self._create_csv(header)

        fake_cursor = _Cursor(rowcount=2, fetchone_results=[(1,)])
        connection = _Connection(fake_cursor)

        self.hash_exists_patcher.stop()
        with mock.patch.object(ingest_excel.prep_excel, "_staging_file_hash_exists", return_value=True), mock.patch.object(
            ingest_excel.prep_excel, "main", return_value=(csv_path, "abc")), mock.patch.object(
            ingest_excel.prep_excel, "_get_table_config", return_value={"table": "teach_record_raw"}
        ), mock.patch.object(
            ingest_excel.prep_excel, "get_schema_details", return_value={"order": header}
        ), mock.patch.object(
            ingest_excel.pymysql, "connect", return_value=connection
        ):
            with self.assertRaisesRegex(RuntimeError, "File hash"):
                ingest_excel.main("workbook.xlsx", source_year="2024")

        self.assertTrue(connection.begun)
        self.assertTrue(connection.rolled_back)
        self.assertFalse(connection.committed)

    def test_script_mode_bootstrap_adds_project_root(self):
        module_path = ingest_excel.__file__
        project_root = os.path.dirname(os.path.dirname(module_path))
        original_sys_path = list(sys.path)

        saved_modules = {
            name: sys.modules[name]
            for name in list(sys.modules)
            if name == "app" or name.startswith("app.")
        }
        for name in list(saved_modules):
            sys.modules.pop(name, None)

        try:
            runpy.run_path(
                module_path,
                init_globals={"__name__": "ingest_excel_script_test", "__package__": None},
            )
            self.assertIn(project_root, sys.path)
        finally:
            sys.path[:] = original_sys_path
            for name in list(sys.modules):
                if name == "app" or name.startswith("app."):
                    del sys.modules[name]
            sys.modules.update(saved_modules)


if __name__ == "__main__":
    unittest.main()
