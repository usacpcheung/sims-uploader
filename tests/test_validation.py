from collections import OrderedDict

from app import normalize_staging, validation


def _require_name(row: tuple[object, ...], columns: tuple[str, ...]) -> str | None:
    name_index = columns.index("name")
    if row[name_index] is None:
        return "name missing"
    return None


def test_validate_rows_partitions_and_writes_csv(tmp_path):
    prepared = normalize_staging.PreparedNormalization(
        normalized_rows=[(1, "Alice"), (2, None)],
        rejected_rows=[
            normalize_staging.RejectedRow(
                data={"id": 3, "name": "bad"},
                errors=("existing error",),
            )
        ],
        resolved_mappings=OrderedDict({"name": "source_name"}),
        metadata_columns=("id",),
        ordered_columns=("id", "name"),
    )

    result = validation.validate_rows(
        "job-123",
        prepared,
        checks=[_require_name],
        output_dir=tmp_path,
    )

    assert len(result.prepared.normalized_rows) == 1
    assert len(result.prepared.rejected_rows) == 2
    assert result.rejected_rows_path == str(tmp_path / "job-123.csv")
    with open(result.rejected_rows_path, encoding="utf-8") as csv_file:
        content = csv_file.read()
    assert "existing error" in content
    assert "name missing" in content


def test_validate_rows_without_job_id(tmp_path):
    prepared = normalize_staging.PreparedNormalization(
        normalized_rows=[("Alice",)],
        rejected_rows=[],
        resolved_mappings=OrderedDict({"name": "source_name"}),
        metadata_columns=(),
        ordered_columns=("name",),
    )

    result = validation.validate_rows(
        None,
        prepared,
        checks=[_require_name],
        output_dir=tmp_path,
    )

    assert result.prepared.normalized_rows == prepared.normalized_rows
    assert result.rejected_rows_path is None
    assert "name missing" not in result.errors
