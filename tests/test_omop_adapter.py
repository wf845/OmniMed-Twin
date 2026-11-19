from datetime import datetime, timezone

import duckdb
import pyarrow as pa

from omnimed.adapters.omop import stage_omop_table
from omnimed.merge.kway_arrow import merge_subject_shards


def test_duckdb_adapter_writes_deterministic_shards(tmp_path) -> None:
    connection = duckdb.connect(":memory:")
    connection.register(
        "measurement",
        pa.table(
            {
                "subject_id": [2, 1],
                "measurement_datetime": [
                    datetime(2026, 1, 2),
                    datetime(2026, 1, 1),
                ],
                "measurement_concept_id": [222, 111],
                "value_as_number": [2.0, 1.0],
            }
        ),
    )

    paths = stage_omop_table(
        connection,
        "measurement",
        "measurement_datetime",
        "measurement_concept_id",
        tmp_path,
        value_column="value_as_number",
        shard_count=2,
    )
    merged = merge_subject_shards(
        paths,
        datetime(2026, 1, 3, tzinfo=timezone.utc),
    )

    assert merged.column("subject_id").to_pylist() == [1, 2]
    assert merged.column("code").to_pylist() == ["111", "222"]
