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
        )

        self.assertEqual(summary["header_row_index"], 0)
        self.assertEqual(summary["clean_headers"], ["學生編號", "姓名", "出生日期"])
        self.assertEqual(
            summary["suggested_staging_columns"], ["學生編號", "姓名", "出生日期"]
        )
        self.assertEqual(summary["metadata_columns"], ingest_planner.METADATA_COLUMNS)
        self.assertEqual(summary["staging_table"], "workbook_學生資料_raw")
        self.assertEqual(summary["options"], {"normalized_table": "workbook_學生資料"})


class TableNamingTests(unittest.TestCase):
    def test_default_table_naming_uses_workbook_and_sheet(self):
        df = pd.DataFrame([["col1"]])
        summary = ingest_planner.summarize_sheet(
            "My Sheet",
            df,
            workbook_component="ExampleWorkbook",
            staging_template=ingest_planner.DEFAULT_STAGING_TABLE_TEMPLATE,
            normalized_template=ingest_planner.DEFAULT_NORMALIZED_TABLE_TEMPLATE,
        )

        self.assertEqual(summary["staging_table"], "exampleworkbook_my_sheet_raw")
        self.assertEqual(
            summary["options"], {"normalized_table": "exampleworkbook_my_sheet"}
        )

    def test_custom_templates_override_defaults(self):
        df = pd.DataFrame([["col1"]])
        summary = ingest_planner.summarize_sheet(
            "Data Tab",
            df,
            workbook_component="book",
            staging_template="custom_{sheet}_stage",
            normalized_template="norm_{workbook}_{sheet}",
        )

        self.assertEqual(summary["staging_table"], "custom_data_tab_stage")
        self.assertEqual(summary["options"], {"normalized_table": "norm_book_data_tab"})


if __name__ == "__main__":
    unittest.main()
