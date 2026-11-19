# OmniMed-Twin

**Autonomous ETL, Semantic Quality Assurance & Trajectory Benchmarking Infrastructure Bridging OHDSI OMOP CDM v5.4 to MEDS v0.3**

OmniMed-Twin converts fragmented hospital relational EHR tables (OMOP CDM v5.4) into event-stream Parquet tensors compliant with the Medical Event Data Standard (MEDS v0.3).

## Architecture

- `subject_id % N_shards` provides deterministic subject-level partitioning.
- Each OMOP adapter stages an isolated, pre-sorted `(subject_id, time)` stream.
- The k-way Arrow merge combines shards without subject records crossing shard boundaries.
- Temporal guards reject measurement and specimen timestamps beyond the prediction index.
- Future-perturbation tests verify that events after `prediction_index_time` cannot alter retained features.

## Layout

```text
src/omnimed/
├── core/schema.py
├── adapters/
└── merge/kway_arrow.py
tests/
pyproject.toml
```

## Quick start

```bash
python -m pip install -e .
python -m pytest
python -m omnimed.adapters.demo
```

## Key References

- McDermott et al. (2026). *MEDS - An Emerging Data Standard and Ecosystem for Health AI Research*. NEJM AI.
- Wornow et al. (2023). *EHRSHOT: An EHR Benchmark for Few-Shot Evaluation of Foundation Models*. NeurIPS.
