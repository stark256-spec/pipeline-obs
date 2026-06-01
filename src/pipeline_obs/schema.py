"""Pydantic models for the pipeline observability schema.

These models are the canonical Python representation of the semantic conventions
defined in schema/semantic-conventions.yaml.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class PipelineEngine(str, Enum):
    DATABRICKS = "databricks"
    DBT = "dbt"
    SPARK = "spark"
    DREMIO = "dremio"
    PINOT = "pinot"
    DELTA_LIVE_TABLES = "delta_live_tables"
    GLUE = "glue"
    ADF = "adf"
    AIRFLOW = "airflow"
    PREFECT = "prefect"


class PipelineType(str, Enum):
    BATCH = "batch"
    STREAMING = "streaming"
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"


class PipelineStatus(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class CloudProvider(str, Enum):
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"
    ON_PREM = "on_prem"


class PipelineIO(BaseModel):
    rows_read: int | None = Field(None, description="Rows read from all sources")
    rows_written: int | None = Field(None, description="Rows written to all destinations")
    rows_failed: int | None = Field(None, description="Rows rejected by quality checks")
    bytes_read: int | None = Field(None, description="Bytes read (uncompressed)")
    bytes_written: int | None = Field(None, description="Bytes written (uncompressed)")
    files_read: int | None = Field(None, description="Files or partitions read")
    files_written: int | None = Field(None, description="Files or partitions written")
    partitions_read: int | None = Field(None, description="Partitions scanned")
    shuffle_bytes: int | None = Field(None, description="Network shuffle bytes (Spark)")


class PipelineCost(BaseModel):
    cost_usd: float | None = Field(None, description="Estimated cost in USD")
    dbu_consumed: float | None = Field(None, description="Databricks Units consumed")
    compute_seconds: int | None = Field(None, description="Total VM-seconds of compute")
    slot_ms: int | None = Field(None, description="Slot-milliseconds (BigQuery)")


class PipelineLineage(BaseModel):
    sources: list[str] = Field(
        default_factory=list,
        description="Input dataset URIs (catalog.schema.table or cloud paths)",
    )
    destinations: list[str] = Field(
        default_factory=list,
        description="Output dataset URIs",
    )
    schema_version: str | None = Field(None, description="Output schema version")
    transformation: str | None = Field(None, description="Transformation description or hash")


class PipelineQuality(BaseModel):
    rules_passed: int | None = Field(None, description="Quality rules that passed")
    rules_failed: int | None = Field(None, description="Quality rules that failed")
    null_rate: float | None = Field(None, ge=0.0, le=1.0, description="Fraction of null rows")
    duplicate_rate: float | None = Field(None, ge=0.0, le=1.0, description="Fraction of duplicates")
    freshness_lag_seconds: int | None = Field(
        None, description="Lag between data and completion time"
    )  # noqa: E501
    anomaly_score: float | None = Field(None, ge=0.0, le=1.0, description="Composite anomaly score")


class PipelineRun(BaseModel):
    """A single execution of a data pipeline.

    This is the root schema object. Every collector produces PipelineRun
    instances; every exporter consumes them.
    """

    # Required identifiers
    name: str = Field(..., description="Logical pipeline name, stable across runs")
    run_id: str = Field(..., description="Unique execution identifier")
    engine: PipelineEngine = Field(..., description="Pipeline engine")
    status: PipelineStatus = Field(..., description="Run status")

    # Timing
    started_at: datetime = Field(..., description="Run start time (UTC)")
    finished_at: datetime | None = Field(None, description="Run end time (UTC)")

    @property
    def duration_seconds(self) -> float | None:
        if self.finished_at and self.started_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None

    # Context
    pipeline_type: PipelineType | None = None
    cloud: CloudProvider | None = None
    region: str | None = None
    environment: str = Field("production", description="Deployment environment")
    owner: str | None = None
    tags: list[str] = Field(default_factory=list)

    # Error
    error_message: str | None = None
    error_type: str | None = None

    # Sub-schemas
    io: PipelineIO = Field(default_factory=PipelineIO)
    cost: PipelineCost = Field(default_factory=PipelineCost)
    lineage: PipelineLineage = Field(default_factory=PipelineLineage)
    quality: PipelineQuality = Field(default_factory=PipelineQuality)

    # Engine-specific extras preserved verbatim
    raw: dict[str, Any] = Field(default_factory=dict, description="Raw engine payload")

    def to_otel_attributes(self) -> dict[str, Any]:
        """Flatten into OpenTelemetry span attribute key-value pairs."""
        attrs: dict[str, Any] = {
            "pipeline.name": self.name,
            "pipeline.run_id": self.run_id,
            "pipeline.engine": self.engine.value,
            "pipeline.status": self.status.value,
            "pipeline.environment": self.environment,
        }
        if self.pipeline_type:
            attrs["pipeline.type"] = self.pipeline_type.value
        if self.cloud:
            attrs["pipeline.cloud"] = self.cloud.value
        if self.region:
            attrs["pipeline.region"] = self.region
        if self.owner:
            attrs["pipeline.owner"] = self.owner
        if self.tags:
            attrs["pipeline.tags"] = self.tags
        if self.duration_seconds is not None:
            attrs["pipeline.duration_seconds"] = self.duration_seconds
        if self.error_message:
            attrs["pipeline.error_message"] = self.error_message
        if self.error_type:
            attrs["pipeline.error_type"] = self.error_type

        for field_name, value in self.io.model_dump(exclude_none=True).items():
            attrs[f"pipeline.{field_name}"] = value
        for field_name, value in self.cost.model_dump(exclude_none=True).items():
            attrs[f"pipeline.{field_name}"] = value
        for field_name, value in self.lineage.model_dump(exclude_none=True).items():
            attrs[f"pipeline.lineage.{field_name}"] = value
        for field_name, value in self.quality.model_dump(exclude_none=True).items():
            attrs[f"pipeline.quality.{field_name}"] = value

        return attrs
