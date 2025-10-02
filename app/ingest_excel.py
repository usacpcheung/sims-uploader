"""Load preprocessed Excel data into the staging database."""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

if __package__ in {None, ""}:  # pragma: no cover - exercised via dedicated unit test
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

import pymysql

from app.config import get_db_settings
from app import prep_excel
from app.identifiers import sanitize_identifier

LOGGER = logging.getLogger(__name__)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", help="Path to the Excel workbook to ingest")
    parser.add_argument(
        "sheet",
        nargs="?",
        default=prep_excel.DEFAULT_SHEET,
        help="Worksheet name inside the workbook (default: %(default)s)",
    )
    parser.add_argument(
        "--source-year",
        required=True,
        help="Source year associated with the uploaded data",
    )
    parser.add_argument(
        "--batch-id",
        help="Optional batch identifier to associate with the upload",
    )
    return parser.parse_args(argv)


def _read_csv_header(csv_path: str) -> list[str]:
    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        try:
            return next(reader)
        except StopIteration:
            return []


def _get_db_settings(overrides: Mapping[str, object] | None = None) -> dict[str, object]:
    settings = dict(get_db_settings())
    if overrides:
        settings.update(overrides)
    settings.update(
        {
            "local_infile": True,
            "client_flag": pymysql.constants.CLIENT.LOCAL_FILES,
        }
    )
    return settings


def _quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def main(
    workbook_path: str,
    sheet: str = prep_excel.DEFAULT_SHEET,
    *,
    source_year: str,
    batch_id: str | None = None,
    db_settings: Mapping[str, object] | None = None,
) -> None:
    csv_path, file_hash = prep_excel.main(
        workbook_path,
        sheet,
        emit_stdout=False,
        db_settings=db_settings,
    )

    if csv_path is None:
        LOGGER.info("Skipping load for %s; duplicate hash %s", workbook_path, file_hash)
        return

    config = prep_excel._get_table_config(
        sheet, db_settings=db_settings
    )
    table_name = config["table"]
    schema = prep_excel.get_schema_details(
        sheet, db_settings=db_settings
    )
    ordered_columns = schema["order"]

    header = _read_csv_header(csv_path)
    existing_identifiers = set(ordered_columns)
    sanitized_header: list[str] = []
    header_mapping: dict[str, str] = {}
    for column in header:
        if column in existing_identifiers:
            sanitized_header.append(column)
            continue
        sanitized = sanitize_identifier(column, existing=existing_identifiers)
        sanitized_header.append(sanitized)
        header_mapping[column] = sanitized

    settings = _get_db_settings(db_settings)
    connection = pymysql.connect(**settings)
    try:
        if header_mapping:
            with connection.cursor() as cursor:
                for original, sanitized in header_mapping.items():
                    if sanitized in ordered_columns:
                        continue
                    LOGGER.info(
                        "Adding staging column %s for header %s", sanitized, original
                    )
                    cursor.execute(
                        f"ALTER TABLE {_quote_identifier(table_name)} "
                        f"ADD COLUMN {_quote_identifier(sanitized)} TEXT NULL"
                    )
            connection.commit()

        schema = prep_excel.get_schema_details(
            sheet, connection=connection, db_settings=db_settings
        )
        ordered_columns = schema["order"]
        required_columns = schema["required"]

        missing_required = [
            column for column in required_columns if column not in sanitized_header
        ]
        if missing_required:
            raise ValueError(
                "CSV missing required column(s) after sanitization: "
                + ", ".join(missing_required)
            )

        unknown_columns = [
            column for column in sanitized_header if column not in ordered_columns
        ]
        if unknown_columns:
            raise ValueError(
                "CSV contains column(s) not present in staging schema: "
                + ", ".join(unknown_columns)
            )

        column_list = ", ".join(_quote_identifier(column) for column in sanitized_header)
        load_sql = (
            f"LOAD DATA LOCAL INFILE %s INTO TABLE {_quote_identifier(table_name)} "
            "FIELDS TERMINATED BY ',' ENCLOSED BY '\"' "
            "LINES TERMINATED BY '\n' IGNORE 1 LINES "
            f"({column_list}) "
            "SET file_hash = %s, batch_id = COALESCE(%s, UUID()), "
            "source_year = %s, ingested_at = %s"
        )

        ingested_at = datetime.now(timezone.utc)
        connection.begin()
        with connection.cursor() as cursor:
            cursor.execute(
                load_sql,
                (
                    os.fspath(csv_path),
                    file_hash,
                    batch_id,
                    source_year,
                    ingested_at,
                ),
            )
            rowcount = cursor.rowcount
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    else:
        LOGGER.info(
            "Loaded %s rows into %s (hash=%s, source_year=%s)",
            rowcount,
            table_name,
            file_hash,
            source_year,
        )
    finally:
        connection.close()


def cli(argv: Iterable[str] | None = None) -> None:
    args = _parse_args(argv)
    main(
        args.workbook,
        args.sheet,
        source_year=args.source_year,
        batch_id=args.batch_id,
    )


if __name__ == "__main__":
    cli()
