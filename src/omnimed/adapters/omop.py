from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from omnimed.core.schema import MEDS_EVENT_SCHEMA


def subject_shard(subject_id: int, shard_count: int = 64) -> int:
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    return subject_id % shard_count


def stage_events(
    events: Iterable[tuple[int, datetime, str, float | None]],
    output_dir: Path,
    shard_count: int = 64,
) -> list[Path]:
    staged: dict[int, list[tuple[int, datetime, str, float | None]]] = {
        shard: [] for shard in range(shard_count)
    }
    for event in events:
        staged[subject_shard(event[0], shard_count)].append(event)

    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for shard, rows in staged.items():
        rows.sort(key=lambda row: (row[0], row[1]))
        table = pa.Table.from_pylist(
            [
                {
                    "subject_id": subject_id,
                    "time": event_time,
                    "code": code,
                    "value": value,
                }
                for subject_id, event_time, code, value in rows
            ],
            schema=MEDS_EVENT_SCHEMA,
        )
        path = output_dir / f"shard_{shard}.parquet"
        pq.write_table(table, path)
        paths.append(path)
    return paths
