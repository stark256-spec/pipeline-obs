"""Databricks collector — Jobs API v2.1 and Delta Live Tables API."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from pipeline_obs.collectors.base import BaseCollector
from pipeline_obs.schema import (
    CloudProvider,
    PipelineCost,
    PipelineEngine,
    PipelineIO,
    PipelineLineage,
    PipelineRun,
    PipelineStatus,
    PipelineType,
)

_STATUS_MAP = {
    "SUCCESS": PipelineStatus.SUCCEEDED,
    "SUCCEEDED": PipelineStatus.SUCCEEDED,
    "FAILED": PipelineStatus.FAILED,
    "CANCELED": PipelineStatus.CANCELLED,
    "TIMEDOUT": PipelineStatus.FAILED,
    "RUNNING": PipelineStatus.RUNNING,
    "SKIPPED": PipelineStatus.SKIPPED,
    "COMPLETED": PipelineStatus.SUCCEEDED,
}


def _ts(ms: int | None) -> datetime | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


class DatabricksCollector(BaseCollector):
    """Collect job runs from the Databricks Jobs REST API v2.1.

    Args:
        host: Databricks workspace URL (e.g. https://adb-xxx.azuredatabricks.net)
        token: Personal access token or service principal token
        cloud: Cloud provider (aws|azure|gcp)
        environment: Deployment environment label
    """

    engine_name = "databricks"

    def __init__(
        self,
        host: str,
        token: str,
        *,
        cloud: CloudProvider = CloudProvider.AZURE,
        environment: str = "production",
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.host = host.rstrip("/")
        self.token = token
        self.cloud = cloud
        self.environment = environment
        self._client = client

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.host}/api/2.1{path}"
        if self._client:
            r = await self._client.get(url, headers=self._headers(), params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        async with httpx.AsyncClient() as c:
            r = await c.get(url, headers=self._headers(), params=params, timeout=30)
            r.raise_for_status()
            return r.json()

    def _run_to_pipeline_run(self, run: dict[str, Any]) -> PipelineRun:
        started = _ts(run.get("start_time"))
        finished = _ts(run.get("end_time"))
        state = run.get("state", {})
        life_cycle = state.get("life_cycle_state", "")
        result = state.get("result_state", "")
        raw_status = result or life_cycle
        status = _STATUS_MAP.get(raw_status.upper(), PipelineStatus.RUNNING)

        cluster = run.get("cluster_spec", {}).get("new_cluster", {})
        num_workers = cluster.get("num_workers", 0)
        duration_s = (
            int((run.get("end_time", 0) - run.get("start_time", 0)) / 1000)
            if run.get("end_time")
            else None
        )

        task_names = [t.get("task_key", "") for t in run.get("tasks", [])]
        sources: list[str] = []
        destinations: list[str] = []
        for task in run.get("tasks", []):
            nb = task.get("notebook_task", {})
            if nb.get("base_parameters"):
                params = nb["base_parameters"]
                if "source" in params:
                    sources.append(params["source"])
                if "destination" in params:
                    destinations.append(params["destination"])

        job_name = run.get("run_name", "") or str(run.get("job_id", ""))

        return PipelineRun(
            name=job_name,
            run_id=str(run.get("run_id", "")),
            engine=PipelineEngine.DATABRICKS,
            status=status,
            started_at=started or datetime.now(timezone.utc),
            finished_at=finished,
            pipeline_type=PipelineType.BATCH,
            cloud=self.cloud,
            environment=self.environment,
            error_message=state.get("state_message"),
            io=PipelineIO(),
            cost=PipelineCost(
                compute_seconds=num_workers * (duration_s or 0),
            ),
            lineage=PipelineLineage(sources=sources, destinations=destinations),
            raw=run,
        )

    async def collect(
        self,
        *,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[PipelineRun]:
        params: dict[str, Any] = {"limit": min(limit, 25), "expand_tasks": "true"}
        if since:
            params["start_time_from"] = int(since.timestamp() * 1000)

        runs: list[PipelineRun] = []
        offset = 0
        while len(runs) < limit:
            params["offset"] = offset
            data = await self._get("/jobs/runs/list", params)
            batch = data.get("runs", [])
            if not batch:
                break
            runs.extend(self._run_to_pipeline_run(r) for r in batch)
            if not data.get("has_more"):
                break
            offset += len(batch)

        return runs[:limit]


class DeltaLiveTablesCollector(BaseCollector):
    """Collect pipeline runs from Databricks Delta Live Tables API."""

    engine_name = "delta_live_tables"

    def __init__(
        self,
        host: str,
        token: str,
        *,
        cloud: CloudProvider = CloudProvider.AZURE,
        environment: str = "production",
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.host = host.rstrip("/")
        self.token = token
        self.cloud = cloud
        self.environment = environment
        self._client = client

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.host}/api/2.0{path}"
        if self._client:
            r = await self._client.get(url, headers=self._headers(), params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        async with httpx.AsyncClient() as c:
            r = await c.get(url, headers=self._headers(), params=params, timeout=30)
            r.raise_for_status()
            return r.json()

    async def collect(
        self,
        *,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[PipelineRun]:
        pipelines_data = await self._get("/pipelines")
        pipelines = pipelines_data.get("statuses", [])

        runs: list[PipelineRun] = []
        for pipeline in pipelines[:limit]:
            pipeline_id = pipeline.get("pipeline_id", "")
            pipeline_name = pipeline.get("name", pipeline_id)
            state = pipeline.get("state", "IDLE")
            status = _STATUS_MAP.get(state.upper(), PipelineStatus.RUNNING)

            latest = pipeline.get("latest_updates", [{}])[0] if pipeline.get("latest_updates") else {}
            run_id = latest.get("update_id", f"{pipeline_id}-latest")

            runs.append(
                PipelineRun(
                    name=pipeline_name,
                    run_id=run_id,
                    engine=PipelineEngine.DELTA_LIVE_TABLES,
                    status=status,
                    started_at=datetime.now(timezone.utc),
                    pipeline_type=PipelineType.STREAMING,
                    cloud=self.cloud,
                    environment=self.environment,
                    lineage=PipelineLineage(
                        destinations=pipeline.get("target", "").split(",") if pipeline.get("target") else []
                    ),
                    raw=pipeline,
                )
            )

        return runs
