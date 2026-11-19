from omnimed.adapters.omop import subject_shard


def test_subject_partition_is_deterministic() -> None:
    assert subject_shard(101, 64) == subject_shard(101, 64)
    assert 0 <= subject_shard(101, 64) < 64


def test_subjects_never_cross_shards() -> None:
    shard = subject_shard(205, 64)
    assert subject_shard(205, 64) == shard
