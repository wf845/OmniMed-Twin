from datetime import datetime, timedelta, timezone

import pytest

from omnimed.core.schema import validate_prediction_index


def test_future_event_is_blocked() -> None:
    prediction = datetime(2026, 1, 1, tzinfo=timezone.utc)
    future = prediction + timedelta(seconds=1)
    with pytest.raises(ValueError, match="future event blocked"):
        validate_prediction_index(101, future, prediction)


def test_event_at_prediction_index_is_allowed() -> None:
    prediction = datetime(2026, 1, 1, tzinfo=timezone.utc)
    validate_prediction_index(101, prediction, prediction)
