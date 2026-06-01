"""JSONL exporter — newline-delimited JSON for files, S3, Kafka, etc."""

from __future__ import annotations

import sys
from pathlib import Path

from pipeline_obs.schema import PipelineRun


def export_jsonl(runs: list[PipelineRun], output: Path | None = None) -> None:
    """Write runs as JSONL to a file or stdout."""
    lines = [run.model_dump_json(exclude={"raw"}) for run in runs]
    content = "\n".join(lines) + "\n"
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("a", encoding="utf-8") as f:
            f.write(content)
    else:
        sys.stdout.write(content)
