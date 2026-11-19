from datetime import datetime
import heapq
from pathlib import Path
from collections.abc import Iterator, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from omnimed.core.schema import MEDS_EVENT_SCHEMA, validate_prediction_index


def merge_subject_shards(
    paths: Iterable[Path],
    prediction_index_time: datetime,
) -> pa.Table:
    rows = list(iter_merged_events(paths, prediction_index_time))
    return pa.Table.from_pylist(
        rows,
        schema=MEDS_EVENT_SCHEMA,
    )


def iter_merged_events(
    paths: Iterable[Path],
    prediction_index_time: datetime,
) -> Iterator[dict[str, object]]:
    """Yield sorted rows with one in-memory batch per input shard."""
    iterators: list[Iterator[dict[str, object]]] = []
    heap: list[tuple[int, object, int, dict[str, object]]] = []
    for source, path in enumerate(paths):
        iterator = _rows(path)
        iterators.append(iterator)
        try:
            row = next(iterator)
        except StopIteration:
            continue
        heapq.heappush(heap, (int(row["subject_id"]), row["time"], source, row))

    while heap:
        _, _, source, row = heapq.heappop(heap)
        validate_prediction_index(
            int(row["subject_id"]),
            row["time"],
            prediction_index_time,
        )
        yield row
        try:
            next_row = next(iterators[source])
        except StopIteration:
            continue
        heapq.heappush(
            heap,
            (int(next_row["subject_id"]), next_row["time"], source, next_row),
        )


def _rows(path: Path) -> Iterator[dict[str, object]]:
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(columns=MEDS_EVENT_SCHEMA.names):
        yield from batch.to_pylist()


def write_merged_shards(
    paths: Iterable[Path],
    output_path: Path,
    prediction_index_time: datetime,
) -> None:
    """Write the k-way merge without materializing the complete event stream."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    writer: pq.ParquetWriter | None = None
    try:
        for row in iter_merged_events(paths, prediction_index_time):
            batch = pa.Table.from_pylist([row], schema=MEDS_EVENT_SCHEMA)
            if writer is None:
                writer = pq.ParquetWriter(output_path, MEDS_EVENT_SCHEMA)
            writer.write_table(batch)
    finally:
        if writer is not None:
            writer.close()
