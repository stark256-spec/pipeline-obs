"""dbt collector — reads dbt Cloud API or local run_results.json artifacts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from pipeline_obs.collectors.base import BaseCollector
from pipeline_obs.schema import (
    PipelineEngine,
    PipelineIO,
    PipelineLineage,
    PipelineQuality,
    PipelineRun,
    PipelineStatus,
    PipelineType,
)

_STATUS_MAP = {
    "success": PipelineStatus.SUCCEEDED,
    "error": PipelineStatus.FAILED,
    "cancelled": PipelineStatus.CANCELLED,
    "running": PipelineStatus.RUNNING,
    "pass": PipelineStatus.SUCCEEDED,
    "fail": PipelineStatus.FAILED,
    "warn": PipelineStatus.SUCCEEDED,
}


class DbtCloudCollector(BaseCollector):
    """Collect job runs from dbt Cloud API v2."""

    engine_name = "dbt"

    def __init__(
        self,
        account_id: int,
        api_token: str,
        *,
        environment: str = "production",
        base_url: str = "https://cloud.getdbt.com",
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.account_id = account_id
        self.api_token = api_token
        self.environment = environment
        self.base_url = base_url.rstrip("/")
        self._client = client

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Token {self.api_token}"}

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}/api/v2/accounts/{self.account_id}{path}"
        if self._client:
            r = await self._client.get(url, headers=self._headers(), params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        async with httpx.AsyncClient() as c:
            r = await c.get(url, headers=self._headers(), params=params, timeout=30)
            r.raise_for_status()
            return r.json()

    def _run_to_pipeline_run(self, run: dict[str, Any]) -> PipelineRun:
        started_raw = run.get("started_at") or run.get("created_at", "")
        finished_raw = run.get("finished_at")

        try:
            from dateutil.parser import parse

            started = (
                parse(started_raw).replace(tzinfo=timezone.utc)
                if started_raw
                else datetime.now(timezone.utc)
            )
            finished = parse(finished_raw).replace(tzinfo=timezone.utc) if finished_raw else None
        except Exception:
            started = datetime.now(timezone.utc)
            finished = None

        raw_status = run.get("status_humanized", "running").lower()
        status = _STATUS_MAP.get(raw_status, PipelineStatus.RUNNING)

        return PipelineRun(
            name=f"dbt.{run.get('job', {}).get('name', str(run.get('job_id', 'unknown')))}",
            run_id=str(run.get("id", "")),
            engine=PipelineEngine.DBT,
            status=status,
            started_at=started,
            finished_at=finished,
            pipeline_type=PipelineType.BATCH,
            environment=self.environment,
            io=PipelineIO(
                rows_written=(
                    (run.get("artifacts", {}).get("run_results", {}).get("results") or [{}])[0].get(
                        "rows_affected"
                    )
                    if run.get("artifacts")
                    else None
                ),
            ),
            lineage=PipelineLineage(),
            raw=run,
        )

    async def collect(
        self,
        *,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[PipelineRun]:
        params: dict[str, Any] = {"limit": min(limit, 100), "order_by": "-id"}
        data = await self._get("/runs/", params)
        runs = data.get("data", [])
        return [self._run_to_pipeline_run(r) for r in runs[:limit]]


class DbtArtifactsCollector(BaseCollector):
    """Read a local dbt run_results.json artifact (no API key needed)."""

    engine_name = "dbt"

    def __init__(
        self,
        run_results_path: Path,
        manifest_path: Path | None = None,
        *,
        environment: str = "production",
    ) -> None:
        self.run_results_path = run_results_path
        self.manifest_path = manifest_path
        self.environment = environment

    async def collect(
        self,
        *,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[PipelineRun]:
        data = json.loads(self.run_results_path.read_text(encoding="utf-8"))
        metadata = data.get("metadata", {})
        results = data.get("results", [])

        invocation_id = metadata.get("invocation_id", "unknown")

        nodes_by_id: dict[str, Any] = {}
        if self.manifest_path and self.manifest_path.exists():
            manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
            nodes_by_id = manifest.get("nodes", {})

        runs: list[PipelineRun] = []
        for result in results[:limit]:
            unique_id = result.get("unique_id", "")
            node = nodes_by_id.get(unique_id, {})
            status_str = result.get("status", "pass").lower()
            status = _STATUS_MAP.get(status_str, PipelineStatus.SUCCEEDED)

            sources: list[str] = []
            if node.get("depends_on", {}).get("nodes"):
                sources = node["depends_on"]["nodes"]

            rows = result.get("adapter_response", {}).get("rows_affected")

            runs.append(
                PipelineRun(
                    name=f"dbt.{unique_id}",
                    run_id=f"{invocation_id}.{unique_id}",
                    engine=PipelineEngine.DBT,
                    status=status,
                    started_at=datetime.now(timezone.utc),
                    pipeline_type=PipelineType.INCREMENTAL,
                    environment=self.environment,
                    io=PipelineIO(rows_written=rows),
                    lineage=PipelineLineage(
                        sources=sources,
                        destinations=[unique_id],
                    ),
                    quality=PipelineQuality(
                        rules_failed=0 if status == PipelineStatus.SUCCEEDED else 1,
                    ),
                    raw=result,
                )
            )

        return runs
