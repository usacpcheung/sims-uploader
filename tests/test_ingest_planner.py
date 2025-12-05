import unittest

import pandas as pd

from app import ingest_planner


class NormalizeHeaderTests(unittest.TestCase):
    def test_preserves_unicode_headers(self):
        self.assertEqual(ingest_planner._normalize_header("學生編號", 0), "學生編號")

    def test_replaces_separators_with_underscores(self):
        self.assertEqual(
            ingest_planner._normalize_header("學生 編號-1", 1), "學生_編號_1"
        )


class SummarizeSheetTests(unittest.TestCase):
    def test_suggests_unicode_staging_columns(self):
        df = pd.DataFrame(
            [
                ["學生編號", "姓名", "出生日期", ""],
                ["123", "張三", "2024-01-01", ""],
            ]
        )
        summary = ingest_planner.summarize_sheet(
            "學生資料",
            df,
            workbook_component="workbook",
            staging_template=ingest_planner.DEFAULT_STAGING_TABLE_TEMPLATE,
            normalized_template=ingest_planner.DEFAULT_NORMALIZED_TABLE_TEMPLATE,
            sample_rows=ingest_planner.DEFAULT_SAMPLE_ROWS,
            overrides={},
        )

        self.assertEqual(summary["header_row_index"], 0)
        self.assertEqual(summary["clean_headers"], ["學生編號", "姓名", "出生日期"])
        self.assertEqual(
            summary["suggested_staging_columns"], ["學生編號", "姓名", "出生日期"]
        )
        self.assertEqual(summary["metadata_columns"], ingest_planner.METADATA_COLUMNS)
        self.assertEqual(summary["staging_table"], "workbook_學生資料_raw")
        self.assertEqual(
            summary["options"],
            {
                "normalized_table": "workbook_學生資料",
                "column_types": {
                    "學生編號": "INTEGER NULL",
                    "姓名": "VARCHAR(255) NULL",
                    "出生日期": "DATE NULL",
                },
            },
        )


class TableNamingTests(unittest.TestCase):
    def test_default_table_naming_uses_workbook_and_sheet(self):
        df = pd.DataFrame([["col1"]])
        summary = ingest_planner.summarize_sheet(
            "My Sheet",
            df,
            workbook_component="ExampleWorkbook",
            staging_template=ingest_planner.DEFAULT_STAGING_TABLE_TEMPLATE,
            normalized_template=ingest_planner.DEFAULT_NORMALIZED_TABLE_TEMPLATE,
            sample_rows=ingest_planner.DEFAULT_SAMPLE_ROWS,
            overrides={},
        )

        self.assertEqual(summary["staging_table"], "exampleworkbook_my_sheet_raw")
        self.assertEqual(
            summary["options"],
            {
                "normalized_table": "exampleworkbook_my_sheet",
                "column_types": {"col1": "VARCHAR(255) NULL"},
            },
        )

    def test_custom_templates_override_defaults(self):
        df = pd.DataFrame([["col1"]])
        summary = ingest_planner.summarize_sheet(
            "Data Tab",
            df,
            workbook_component="book",
            staging_template="custom_{sheet}_stage",
            normalized_template="norm_{workbook}_{sheet}",
            sample_rows=ingest_planner.DEFAULT_SAMPLE_ROWS,
            overrides={},
        )

        self.assertEqual(summary["staging_table"], "custom_data_tab_stage")
        self.assertEqual(
            summary["options"],
            {
                "normalized_table": "norm_book_data_tab",
                "column_types": {"col1": "VARCHAR(255) NULL"},
            },
        )


class ColumnTypeInferenceTests(unittest.TestCase):
    def test_infers_date_and_long_text_and_numeric(self):
        df = pd.DataFrame(
            [
                ["日期", "描述", "數量"],
                ["2024-01-01", "短", 1],
                ["2024/02/02", "a" * 300, 2.5],
            ]
        )

        summary = ingest_planner.summarize_sheet(
            "資料", df, "book", "{workbook}_{sheet}_raw", "{workbook}_{sheet}", 10, {}
        )

        self.assertEqual(
            summary["options"]["column_types"],
            {
                "日期": "DATE NULL",
                "描述": "TEXT NULL",
                "數量": "NUMERIC NULL",
            },
        )

    def test_overrides_replace_inference(self):
        df = pd.DataFrame([["日期", "描述"], ["2024-01-01", "text"]])
        summary = ingest_planner.summarize_sheet(
            "資料", df, "book", "{workbook}_{sheet}_raw", "{workbook}_{sheet}", 10, {"描述": "VARCHAR(100) NULL"}
        )

        self.assertEqual(summary["options"]["column_types"]["描述"], "VARCHAR(100) NULL")


class SqlGenerationTests(unittest.TestCase):
    def test_builds_insert_for_multiple_sheets(self):
        plan = {
            "workbook": "Book.xlsx",
            "workbook_path": "/tmp/Book.xlsx",
            "sheets": [
                {
                    "sheet_name": "SheetA",
                    "staging_table": "book_sheeta_raw",
                    "metadata_columns": ingest_planner.METADATA_COLUMNS,
                    "clean_headers": ["學生編號", "姓名"],
                    "suggested_staging_columns": ["學生編號", "姓名"],
                    "options": {
                        "normalized_table": "book_sheeta",
                        "column_types": {"學生編號": "INTEGER NULL", "姓名": "VARCHAR(255) NULL"},
                        "time_range_column": "日期",
                        "time_range_format": "%Y/%m/%d",
                        "overlap_target_table": "calendar_table",
                    },
                },
                {
                    "sheet_name": "SheetB",
                    "staging_table": "book_sheetb_raw",
                    "metadata_columns": ingest_planner.METADATA_COLUMNS,
                    "clean_headers": ["日期"],
                    "suggested_staging_columns": ["日期"],
                    "options": {
                        "normalized_table": "book_sheetb",
                        "column_types": {"日期": "DATE NULL"},
                    },
                },
            ],
        }

        sql = ingest_planner.build_ingest_config_sql(plan, workbook_type="custom_type")

        self.assertIn("INSERT INTO sheet_ingest_config", sql)
        self.assertIn("custom_type", sql)
        self.assertIn("book_sheeta_raw", sql)
        self.assertIn("book_sheetb_raw", sql)
        self.assertIn("JSON_ARRAY('學生編號', '姓名')", sql)
        self.assertIn("JSON_OBJECT('學生編號', '學生編號', '姓名', '姓名')", sql)
        self.assertIn("JSON_OBJECT('normalized_table', 'book_sheetb'", sql)
        self.assertIn("JSON_OBJECT('日期', 'DATE NULL')", sql)
        self.assertIn("%Y/%m/%d", sql)
        self.assertIn("calendar_table", sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", sql)


if __name__ == "__main__":
    unittest.main()
