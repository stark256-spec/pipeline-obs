"""Tests for PipelineRun schema — validation, serialization, otel attributes."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from pipeline_obs.schema import (
    CloudProvider,
    PipelineCost,
    PipelineEngine,
    PipelineIO,
    PipelineLineage,
    PipelineQuality,
    PipelineRun,
    PipelineStatus,
    PipelineType,
)


def _run(**kwargs) -> PipelineRun:
    defaults = dict(
        name="test.orders",
        run_id="run-001",
        engine=PipelineEngine.DATABRICKS,
        status=PipelineStatus.SUCCEEDED,
        started_at=datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2024, 6, 1, 11, 0, 0, tzinfo=timezone.utc),
    )
    defaults.update(kwargs)
    return PipelineRun(**defaults)


def test_required_fields_only():
    run = _run()
    assert run.name == "test.orders"
    assert run.engine == PipelineEngine.DATABRICKS
    assert run.status == PipelineStatus.SUCCEEDED


def test_duration_seconds():
    run = _run()
    assert run.duration_seconds == 3600.0


def test_duration_none_when_not_finished():
    run = _run(finished_at=None)
    assert run.duration_seconds is None


def test_missing_required_fields_raises():
    with pytest.raises(ValidationError):
        PipelineRun(name="x", run_id="y")  # missing engine, status, started_at


def test_null_rate_validates_range():
    with pytest.raises(ValidationError):
        PipelineQuality(null_rate=1.5)
    with pytest.raises(ValidationError):
        PipelineQuality(null_rate=-0.1)
    q = PipelineQuality(null_rate=0.05)
    assert q.null_rate == 0.05


def test_to_otel_attributes_contains_required_keys():
    run = _run()
    attrs = run.to_otel_attributes()
    assert attrs["pipeline.name"] == "test.orders"
    assert attrs["pipeline.run_id"] == "run-001"
    assert attrs["pipeline.engine"] == "databricks"
    assert attrs["pipeline.status"] == "succeeded"
    assert "pipeline.duration_seconds" in attrs


def test_to_otel_attributes_includes_io():
    run = _run(io=PipelineIO(rows_read=1000, rows_written=999))
    attrs = run.to_otel_attributes()
    assert attrs["pipeline.rows_read"] == 1000
    assert attrs["pipeline.rows_written"] == 999


def test_to_otel_attributes_includes_lineage():
    run = _run(
        lineage=PipelineLineage(
            sources=["catalog.bronze.orders"], destinations=["catalog.silver.orders"]
        )
    )
    attrs = run.to_otel_attributes()
    assert attrs["pipeline.lineage.sources"] == ["catalog.bronze.orders"]
    assert attrs["pipeline.lineage.destinations"] == ["catalog.silver.orders"]


def test_to_otel_attributes_includes_cost():
    run = _run(cost=PipelineCost(cost_usd=2.45, dbu_consumed=4.1))
    attrs = run.to_otel_attributes()
    assert attrs["pipeline.cost_usd"] == 2.45
    assert attrs["pipeline.dbu_consumed"] == 4.1


def test_to_otel_attributes_includes_quality():
    run = _run(quality=PipelineQuality(rules_passed=5, rules_failed=1, null_rate=0.02))
    attrs = run.to_otel_attributes()
    assert attrs["pipeline.quality.rules_passed"] == 5
    assert attrs["pipeline.quality.rules_failed"] == 1
    assert attrs["pipeline.quality.null_rate"] == 0.02


def test_to_otel_attributes_omits_none_values():
    run = _run()
    attrs = run.to_otel_attributes()
    assert "pipeline.cost_usd" not in attrs
    assert "pipeline.lineage.sources" not in attrs or attrs.get("pipeline.lineage.sources") == []


def test_json_roundtrip():
    run = _run(
        cloud=CloudProvider.AZURE,
        pipeline_type=PipelineType.INCREMENTAL,
        io=PipelineIO(rows_written=500),
        lineage=PipelineLineage(sources=["bronze.orders"]),
    )
    json_str = run.model_dump_json(exclude={"raw"})
    loaded = PipelineRun.model_validate_json(json_str)
    assert loaded.name == run.name
    assert loaded.io.rows_written == 500
    assert loaded.lineage.sources == ["bronze.orders"]


def test_tags_field():
    run = _run(tags=["finance", "critical"])
    attrs = run.to_otel_attributes()
    assert attrs["pipeline.tags"] == ["finance", "critical"]
