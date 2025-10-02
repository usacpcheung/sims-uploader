"""Load preprocessed Excel data into the staging database."""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

if __package__ in {None, ""}:  # pragma: no cover - exercised via dedicated unit test
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

import pymysql
from pymysql import err as pymysql_err

from pymysql.constants import ER

from app.config import get_db_settings
from app import prep_excel

LOGGER = logging.getLogger(__name__)


def _extract_scalar(result: object) -> object | None:
    if result is None:
        return None
    if isinstance(result, Mapping):
        try:
            return next(iter(result.values()))
        except StopIteration:
            return None
    if isinstance(result, (tuple, list)):
        return result[0] if result else None
    return result


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


def _read_csv_header(csv_path: str) -> tuple[list[str], bool]:
    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        try:
            header = next(reader)
        except StopIteration:
            return [], False
        try:
            next(reader)
        except StopIteration:
            return header, False
        return header, True


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


def main(
    workbook_path: str,
    sheet: str = prep_excel.DEFAULT_SHEET,
    *,
    source_year: str,
    batch_id: str | None = None,
    db_settings: Mapping[str, object] | None = None,
) -> None:
    if not source_year or str(source_year).strip() == "":
        raise ValueError("source_year is required")

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

    header, has_data_rows = _read_csv_header(csv_path)
    if header[: len(ordered_columns)] != ordered_columns:
        raise ValueError(
            "CSV header does not match expected column order"
            f" for table {table_name}: {header!r} does not begin with {ordered_columns!r}"
        )
    if not has_data_rows:
        raise ValueError("CSV contains no data rows to load")

    column_targets: list[str]
    if len(header) > len(ordered_columns):
        extra_count = len(header) - len(ordered_columns)
        column_targets = [f"`{column}`" for column in ordered_columns]
        column_targets.extend(f"@unused_{index}" for index in range(extra_count))
    else:
        column_targets = [f"`{column}`" for column in ordered_columns]

    column_list = ", ".join(column_targets)
    load_sql = (
        f"LOAD DATA LOCAL INFILE %s INTO TABLE `{table_name}` "
        "FIELDS TERMINATED BY ',' ENCLOSED BY '\"' "
        "LINES TERMINATED BY '\n' IGNORE 1 LINES "
        f"({column_list}) "
        "SET file_hash = %s, batch_id = %s, "
        "source_year = %s, ingested_at = %s"
    )

    if batch_id is None:
        batch_id = str(uuid.uuid4())

    settings = _get_db_settings(db_settings)
    connection = pymysql.connect(**settings)
    try:
        ingested_at = datetime.now(timezone.utc)
        connection.begin()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT @@local_infile")
                local_infile_result = cursor.fetchone()
        except pymysql_err.OperationalError as exc:  # pragma: no cover - defensive
            if exc.args and exc.args[0] == ER.OPTION_PREVENTS_STATEMENT:
                raise RuntimeError(
                    "MySQL server has disabled LOCAL INFILE; enable it before loading"
                ) from exc
            raise

        local_infile_enabled = bool(_extract_scalar(local_infile_result))
        if not local_infile_enabled:
            raise RuntimeError(
                "MySQL server has disabled LOCAL INFILE; enable it before loading"
            )

        if prep_excel._staging_file_hash_exists(
            table_name,
            file_hash,
            connection=connection,
            db_settings=db_settings,
        ):
            raise RuntimeError(
                f"File hash {file_hash} already exists in staging table {table_name}; skipping."
            )

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
            if rowcount == -1:
                cursor.execute("SELECT ROW_COUNT()")
                rowcount_result = cursor.fetchone()
                rowcount = int(_extract_scalar(rowcount_result) or 0)
            if not rowcount:
                raise RuntimeError("LOAD DATA did not insert any rows")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    else:
        LOGGER.info(
            "Loaded %s rows into %s (hash=%s, source_year=%s, batch_id=%s)",
            rowcount,
            table_name,
            file_hash,
            source_year,
            batch_id,
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
