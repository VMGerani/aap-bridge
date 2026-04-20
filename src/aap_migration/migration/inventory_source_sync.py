"""Trigger inventory source updates and poll sync status on the inventory_source object.

The controller does not reliably expose live job status on ``inventory_updates/<id>/``;
status is read from repeated ``GET inventory_sources/<id>/`` responses (and common
``summary_fields`` fallbacks).
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from aap_migration.client.exceptions import AAPMigrationError, APIError
from aap_migration.utils.logging import get_logger

if TYPE_CHECKING:
    from aap_migration.client.base_client import BaseAPIClient
    from aap_migration.config import PerformanceConfig

logger = get_logger(__name__)

# Unified job lifecycle (inventory_update uses the same status strings as other jobs)
_ACTIVE_STATUSES = frozenset({"pending", "waiting", "running", "new", "never updated"})
_SUCCESS_STATUSES = frozenset({"successful"})


def _sync_status_from_inventory_source_payload(data: dict[str, Any]) -> str:
    """Read sync/job status from an ``inventory_sources/<id>/`` GET response.

    The controller exposes status on the inventory source (not reliably on
    ``inventory_updates/<id>/`` in all deployments). Try common locations.
    """
    if not isinstance(data, dict):
        return ""
    s = (data.get("status") or "").strip().lower()
    if s:
        return s
    sf = data.get("summary_fields")
    if isinstance(sf, dict):
        for key in ("inventory_source", "last_job", "last_update"):
            block = sf.get(key)
            if isinstance(block, dict):
                st = (block.get("status") or "").strip().lower()
                if st:
                    return st
    return ""


def collect_inventory_source_target_ids_for_sync(
    import_results: list[dict[str, Any]] | None,
) -> list[int]:
    """Return target inventory_source IDs that should run an update after import.

    Skips policy skips and entries without a concrete ``id`` (e.g. already-mapped skips).
    """
    if not import_results:
        return []
    ids: list[int] = []
    for r in import_results:
        if not r or not isinstance(r, dict):
            continue
        if r.get("_skipped") and r.get("policy_skip"):
            continue
        rid = r.get("id")
        if rid is not None:
            ids.append(int(rid))
    return list(dict.fromkeys(ids))


async def trigger_inventory_source_update(
    client: BaseAPIClient, inventory_source_id: int
) -> int:
    """Launch ``inventory_sources/<id>/update/`` and return inventory_update job id.

    AAP launch endpoints are POST-first. Some environments also allow GET launch,
    so we fall back to GET only when POST is not allowed.
    """
    path = f"inventory_sources/{inventory_source_id}/update/"
    data: dict[str, Any]
    try:
        data = await client.post(path)
    except APIError as e:
        if e.status_code != 405:
            raise
        data = await client.get(path)
    job_id = data.get("inventory_update") if isinstance(data, dict) else None
    if job_id is None and isinstance(data, dict):
        job_id = data.get("id")
    if job_id is None:
        logger.error(
            "inventory_source_update_no_job_id",
            inventory_source_id=inventory_source_id,
            response_keys=list(data.keys()) if isinstance(data, dict) else None,
        )
        raise ValueError(
            f"inventory_sources/{inventory_source_id}/update/ did not return a job id"
        )
    return int(job_id)


async def wait_for_inventory_source_sync(
    client: BaseAPIClient,
    inventory_source_id: int,
    *,
    poll_interval: float,
    timeout_seconds: int,
) -> dict[str, Any]:
    """Poll ``inventory_sources/<id>/`` until sync status leaves active states."""
    deadline = time.monotonic() + timeout_seconds
    path = f"inventory_sources/{inventory_source_id}/"
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = await client.get(path)
        status = _sync_status_from_inventory_source_payload(last)
        if status and status not in _ACTIVE_STATUSES:
            return last
        await asyncio.sleep(poll_interval)
    last_status = _sync_status_from_inventory_source_payload(last)
    raise TimeoutError(
        f"inventory_source {inventory_source_id} sync did not finish within {timeout_seconds}s "
        f"(last status={last_status!r})"
    )


async def sync_inventory_sources_after_import(
    client: BaseAPIClient,
    inventory_source_ids: list[int],
    perf: PerformanceConfig,
) -> None:
    """For each target inventory source, trigger update and wait for the job to complete."""
    if not inventory_source_ids:
        return

    ids = list(dict.fromkeys(inventory_source_ids))
    interval = float(perf.inventory_source_update_poll_interval_seconds)
    timeout = int(perf.inventory_source_update_job_timeout_seconds)
    fail_on_error = perf.inventory_source_sync_fail_on_job_failure
    max_conc = min(
        int(perf.inventory_source_sync_max_concurrent),
        max(1, len(ids)),
    )
    sem = asyncio.Semaphore(max_conc)

    async def run_one(is_id: int) -> None:
        async with sem:
            try:
                job_id = await trigger_inventory_source_update(client, is_id)
                logger.info(
                    "inventory_source_update_triggered",
                    inventory_source_id=is_id,
                    inventory_update_id=job_id,
                )
                final = await wait_for_inventory_source_sync(
                    client,
                    is_id,
                    poll_interval=interval,
                    timeout_seconds=timeout,
                )
                status = _sync_status_from_inventory_source_payload(final)
                ok = status in _SUCCESS_STATUSES
                if ok:
                    logger.info(
                        "inventory_source_sync_finished",
                        inventory_source_id=is_id,
                        inventory_update_id=job_id,
                        status=status,
                    )
                else:
                    logger.warning(
                        "inventory_source_sync_finished_non_success",
                        inventory_source_id=is_id,
                        inventory_update_id=job_id,
                        status=status,
                    )
                    if fail_on_error:
                        raise AAPMigrationError(
                            f"Inventory update {job_id} for inventory_source {is_id} "
                            f"ended with status {status!r}"
                        )
            except TimeoutError as e:
                logger.error(
                    "inventory_source_sync_timeout",
                    inventory_source_id=is_id,
                    error=str(e),
                )
                if fail_on_error:
                    raise AAPMigrationError(str(e)) from e
            except Exception as e:
                logger.error(
                    "inventory_source_sync_failed",
                    inventory_source_id=is_id,
                    error=str(e),
                )
                if fail_on_error:
                    raise

    await asyncio.gather(*[run_one(i) for i in ids])
