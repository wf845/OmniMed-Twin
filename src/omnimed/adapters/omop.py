from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
import re

import duckdb
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


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _identifier(name: str) -> str:
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return f'"{name}"'


def stage_omop_table(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    time_column: str,
    code_column: str,
    output_dir: Path,
    *,
    value_column: str | None = None,
    shard_count: int = 64,
) -> list[Path]:
    """Stage one registered OMOP table using an isolated DuckDB connection.

    The registered relation must contain ``subject_id`` and the supplied
    timestamp/code columns. Each shard is written independently and sorted
    before leaving DuckDB, so downstream merges do not need a global shuffle.
    """
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    table = _identifier(table_name)
    time = _identifier(time_column)
    code = _identifier(code_column)
    value = f"CAST({_identifier(value_column)} AS DOUBLE)" if value_column else "NULL::DOUBLE"
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for shard in range(shard_count):
        path = output_dir / f"shard_{shard}.parquet"
        target = str(path).replace("'", "''")
        query = f"""
            COPY (
                SELECT
                    CAST(subject_id AS BIGINT) AS subject_id,
                    CAST({time} AS TIMESTAMP) AS time,
                    CAST({code} AS VARCHAR) AS code,
                    {value} AS value
                FROM {table}
                WHERE MOD(CAST(subject_id AS BIGINT), {shard_count}) = {shard}
                ORDER BY subject_id, time
            ) TO '{target}' (FORMAT PARQUET)
        """
        connection.execute(query)
        paths.append(path)
    return paths
