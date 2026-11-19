from datetime import datetime, timezone

import pyarrow as pa


MEDS_EVENT_SCHEMA = pa.schema([
    pa.field("subject_id", pa.int64(), nullable=False),
    pa.field("time", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("code", pa.string(), nullable=False),
    pa.field("value", pa.float64(), nullable=True),
])


def validate_prediction_index(
    subject_id: int,
    event_time: datetime,
    prediction_index_time: datetime,
) -> None:
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    if prediction_index_time.tzinfo is None:
        prediction_index_time = prediction_index_time.replace(tzinfo=timezone.utc)
    if event_time > prediction_index_time:
        raise ValueError(
            f"future event blocked for subject {subject_id}: "
            f"{event_time.isoformat()} > {prediction_index_time.isoformat()}"
        )
