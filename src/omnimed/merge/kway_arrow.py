from datetime import datetime
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from omnimed.core.schema import MEDS_EVENT_SCHEMA, validate_prediction_index


def merge_subject_shards(
    paths: Iterable[Path],
    prediction_index_time: datetime,
) -> pa.Table:
    tables: list[pa.Table] = []
    for path in paths:
        table = pq.read_table(path, schema=MEDS_EVENT_SCHEMA)
        for row in table.to_pylist():
            validate_prediction_index(
                row["subject_id"],
                row["time"],
                prediction_index_time,
            )
        tables.append(table)

    if not tables:
        return MEDS_EVENT_SCHEMA.empty_table()

    merged = pa.concat_tables(tables)
    rows = sorted(
        merged.to_pylist(),
        key=lambda row: (row["subject_id"], row["time"]),
    )
    return pa.Table.from_pylist(rows, schema=MEDS_EVENT_SCHEMA)
