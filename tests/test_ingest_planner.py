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
        summary = ingest_planner.summarize_sheet("學生資料", df)

        self.assertEqual(summary["header_row_index"], 0)
        self.assertEqual(summary["clean_headers"], ["學生編號", "姓名", "出生日期"])
        self.assertEqual(
            summary["suggested_staging_columns"], ["學生編號", "姓名", "出生日期"]
        )


if __name__ == "__main__":
    unittest.main()
