"""Base class for all pipeline collectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from pipeline_obs.schema import PipelineRun


class BaseCollector(ABC):
    """Fetch recent pipeline runs from a specific engine and return PipelineRun objects."""

    engine_name: str = "unknown"

    @abstractmethod
    async def collect(
        self,
        *,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[PipelineRun]:
        """Return pipeline runs completed after `since`, up to `limit`."""
        ...

    async def health_check(self) -> bool:
        """Return True if the backend is reachable."""
        try:
            await self.collect(limit=1)
            return True
        except Exception:
            return False
