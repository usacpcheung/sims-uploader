"""Load preprocessed Excel data into the staging database."""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import uuid
from dataclasses import dataclass
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
from app.identifiers import sanitize_identifier

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class StagingLoadResult:
    """Metadata describing a successful staging-table load."""

    staging_table: str
    file_hash: str
    batch_id: str
    source_year: str
    ingested_at: datetime
    rowcount: int


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
    parser.add_argument(
        "--workbook-type",
        default="default",
        help="Workbook type used to select configuration overrides (default: %(default)s)",
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


def _quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def main(
    workbook_path: str,
    sheet: str = prep_excel.DEFAULT_SHEET,
    *,
    sheet: str,
    workbook_type: str = "default",
    source_year: str,
    file_hash: str,
    batch_id: str | None = None,
    db_settings: Mapping[str, object] | None = None,
) -> StagingLoadResult:
    """Load a prepared CSV file into the staging table and return metadata."""

    config = prep_excel._get_table_config(
        sheet,
        workbook_type=workbook_type,
        db_settings=db_settings,
    )
    table_name = config["table"]
    schema = prep_excel.get_schema_details(
        sheet,
        workbook_type=workbook_type,
        db_settings=db_settings,
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

    if batch_id is None:
        batch_id = str(uuid.uuid4())

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
    finally:
        connection.close()

    return StagingLoadResult(
        staging_table=table_name,
        file_hash=file_hash,
        batch_id=batch_id,
        source_year=str(source_year),
        ingested_at=ingested_at,
        rowcount=rowcount,
    )


def main(
    workbook_path: str,
    sheet: str = prep_excel.DEFAULT_SHEET,
    *,
    workbook_type: str = "default",
    source_year: str,
    batch_id: str | None = None,
    db_settings: Mapping[str, object] | None = None,
) -> None:
    if not source_year or str(source_year).strip() == "":
        raise ValueError("source_year is required")

    csv_path, file_hash = prep_excel.main(
        workbook_path,
        sheet,
        workbook_type=workbook_type,
        emit_stdout=False,
        db_settings=db_settings,
    )

    if csv_path is None:
        LOGGER.info("Skipping load for %s; duplicate hash %s", workbook_path, file_hash)
        return

    result = load_csv_into_staging(
        csv_path,
        sheet=sheet,
        workbook_type=workbook_type,
        source_year=source_year,
        file_hash=file_hash,
        batch_id=batch_id,
        db_settings=db_settings,
    )

    LOGGER.info(
        "Loaded %s rows into %s (hash=%s, source_year=%s, batch_id=%s)",
        result.rowcount,
        result.staging_table,
        result.file_hash,
        result.source_year,
        result.batch_id,
    )


def cli(argv: Iterable[str] | None = None) -> None:
    args = _parse_args(argv)
    main(
        args.workbook,
        args.sheet,
        workbook_type=args.workbook_type,
        source_year=args.source_year,
        batch_id=args.batch_id,
    )


if __name__ == "__main__":
    cli()
