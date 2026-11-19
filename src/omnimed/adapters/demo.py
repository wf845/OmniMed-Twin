from datetime import datetime, timedelta, timezone
from pathlib import Path

from omnimed.adapters.omop import stage_events
from omnimed.core.schema import validate_prediction_index
from omnimed.merge.kway_arrow import merge_subject_shards


def main() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    events = [
        (101, now - timedelta(days=10), "SNOMED:44054006", None),
        (205, now - timedelta(days=5), "LOINC:4548-4", 6.2),
    ]
    staging = Path("staging/demo")
    paths = stage_events(events, staging)
    prediction_index = now
    for path in paths:
        if path.stat().st_size:
            merge_subject_shards([path], prediction_index)
    print(f"Staged {len(paths)} shard files in {staging}")


if __name__ == "__main__":
    main()
