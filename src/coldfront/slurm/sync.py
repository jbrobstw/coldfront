# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

"""
Slurm sync engine.

Provides the core synchronization logic between ColdFront's local models
and the Slurm REST API (``slurmrestd``).  Three entry points:

* ``run_sync()`` — full reconciliation sync for one or all clusters.
* ``enqueue_activate_allocation()`` — targeted handler for allocation
  activation.
* ``enqueue_deactivate_allocation()`` — targeted handler for allocation
  expire / revoke.
* ``enqueue_remove_project_user()`` — targeted handler for ProjectUser
  deletion.

The ``enqueue_*()`` wrappers are thin — they check the auto-sync gate and
delegate to the task queue.  The actual REST API logic lives in the
corresponding ``_run_*()`` functions so that the CLI command can call them
directly without going through the queue.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Any

from django.conf import settings
from django.utils import timezone

from coldfront.core.models import Job
from coldfront.ras.choices import AllocationStatusChoices
from coldfront.ras.models import Allocation, ProjectUser
from coldfront.slurm.choices import (
    SlurmPartitionStateChoices,
    SlurmPreemptModeChoices,
    SlurmPriorityFlagsChoices,
    SlurmPriorityTypeChoices,
    SlurmPriorityUsageResetPeriodChoices,
)
from coldfront.slurm.client import SlurmClient
from coldfront.slurm.client.exceptions import (
    SlurmAlreadyExistsException,
)
from coldfront.slurm.models import (
    SlurmAccount,
    SlurmAccountUsage,
    SlurmAssociation,
    SlurmCluster,
    SlurmPartition,
    SlurmQOS,
    SlurmUser,
)
from coldfront.slurm.parser import ParsedSlurmConfig, parse_slurm_conf, parse_tres_weights

__all__ = (
    "SyncReport",
    "UsageSyncReport",
    "run_sync",
    "_run_usage_sync",
    "enqueue_activate_allocation",
    "enqueue_deactivate_allocation",
    "enqueue_throttle_allocation",
    "enqueue_hold_allocation",
    "enqueue_set_fields_allocation",
    "enqueue_restore_allocation",
    "enqueue_remove_project_user",
    "import_cluster_from_conf",
    "import_cluster_from_api",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SyncReport
# ---------------------------------------------------------------------------


@dataclass
class SyncReport:
    """Detailed report returned by sync operations."""

    cluster: str
    success: bool = False
    accounts_created: int = 0
    accounts_deleted: int = 0
    associations_created: int = 0
    associations_updated: int = 0
    associations_deleted: int = 0
    users_created: int = 0
    users_updated: int = 0
    users_deleted: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    duration_ms: int = 0


@dataclass
class UsageSyncReport:
    """Detailed report returned by :func:`_run_usage_sync`."""

    cluster: str
    success: bool = False
    days_synced: int = 0
    accounts_updated: int = 0
    jobs_ingested: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    duration_ms: int = 0


# ---------------------------------------------------------------------------
# SlurmClient factory
# ---------------------------------------------------------------------------


def _build_client(cluster: SlurmCluster | None = None) -> SlurmClient | None:
    """Build a :class:`SlurmClient` from Django settings.

    Looks up the cluster's name in ``settings.SLURMRESTD_CLUSTERS``.
    Falls back to the ``"default"`` entry when the cluster is not listed
    or when ``cluster`` is ``None``.

    Returns ``None`` if slurmrestd is not configured (empty URL).
    """
    clusters_config = getattr(settings, "SLURMRESTD_CLUSTERS", {})

    # Resolve config dict for this cluster (or "default")
    key = cluster.name if cluster is not None else "default"
    config = clusters_config.get(key) or clusters_config.get("default", {})

    url = config.get("url", "")
    if not url:
        return None

    jwt = config.get("jwt_token", "")
    version = config.get("api_version", "") or None
    timeout = config.get("timeout", 30)
    retries = config.get("retries", 3)
    backoff = config.get("retry_backoff", 1.5)

    client = SlurmClient(
        base_url=url,
        jwt_token=jwt,
        version=version,
        timeout=timeout,
        retries=retries,
        retry_backoff=backoff,
    )

    if version is None:
        try:
            client.discover_version()
        except RuntimeError:
            logger.warning("Slurm API version discovery failed — client will not be usable")
            return None

    return client


# ---------------------------------------------------------------------------
# Auto-sync gate
# ---------------------------------------------------------------------------


def _auto_sync_enabled(cluster: SlurmCluster | None = None) -> bool:
    """Return ``True`` if automated Slurm sync is enabled.

    Checks the cluster's config in ``settings.SLURMRESTD_CLUSTERS`` first;
    falls back to the ``"default"`` entry when the cluster is not listed
    or when ``cluster`` is ``None``.
    """
    clusters_config = getattr(settings, "SLURMRESTD_CLUSTERS", {})
    key = cluster.name if cluster is not None else "default"
    config = clusters_config.get(key) or clusters_config.get("default", {})
    return config.get("auto_sync_enabled", False)


# ---------------------------------------------------------------------------
# Targeted handler wrappers (enqueue helpers)
# ---------------------------------------------------------------------------


def enqueue_activate_allocation(allocation_id: int, cluster_id: int | None = None) -> None:
    """Enqueue a targeted task to create associations for an allocation.

    Called by the ViewFlow callback when an allocation is activated.
    Respects the per-cluster ``auto_sync_enabled`` gate — if ``cluster_id``
    is provided, checks that cluster's setting; otherwise checks the
    ``"default"`` cluster config.

    The actual work is done by :func:`_run_activate_allocation` in the
    worker process.
    """
    cluster = SlurmCluster.objects.get(pk=cluster_id) if cluster_id is not None else None
    if not _auto_sync_enabled(cluster):
        logger.debug(
            "Slurm auto sync disabled — skipping activate enqueue for allocation %s",
            allocation_id,
        )
        return

    logger.info("Enqueuing Slurm activate for allocation %s", allocation_id)
    Job.enqueue(
        "coldfront.slurm.sync._run_activate_allocation",
        name=f"SlurmActivate:allocation-{allocation_id}",
        priority=3,
        allocation_id=allocation_id,
    )


def enqueue_deactivate_allocation(allocation_id: int, cluster_id: int | None = None) -> None:
    """Enqueue a targeted task to remove associations for an allocation.

    Called by ViewFlow callbacks when an allocation is expired or revoked.
    Respects the per-cluster ``auto_sync_enabled`` gate — if ``cluster_id``
    is provided, checks that cluster's setting; otherwise checks the
    ``"default"`` cluster config.
    """
    cluster = SlurmCluster.objects.get(pk=cluster_id) if cluster_id is not None else None
    if not _auto_sync_enabled(cluster):
        logger.debug(
            "Slurm auto sync disabled — skipping deactivate enqueue for allocation %s",
            allocation_id,
        )
        return

    logger.info("Enqueuing Slurm deactivate for allocation %s", allocation_id)
    Job.enqueue(
        "coldfront.slurm.sync._run_deactivate_allocation",
        name=f"SlurmDeactivate:allocation-{allocation_id}",
        priority=3,
        allocation_id=allocation_id,
    )


def enqueue_remove_project_user(project_id: int, user_id: int, cluster_ids: list[int] | None = None) -> None:
    """Enqueue a targeted task to remove a user from all allocations.

    Called by the ``post_delete`` signal handler when a ProjectUser is
    deleted.  Respects the per-cluster ``auto_sync_enabled`` gate — if
    ``cluster_ids`` is provided, checks each cluster's setting;
    otherwise checks the ``"default"`` cluster config.
    """
    if cluster_ids:
        for cid in cluster_ids:
            try:
                cluster = SlurmCluster.objects.get(pk=cid)
                if _auto_sync_enabled(cluster):
                    break  # at least one cluster has sync enabled
            except SlurmCluster.DoesNotExist:
                continue
        else:
            # no cluster has sync enabled
            logger.debug(
                "Slurm auto sync disabled — skipping remove-project-user enqueue for project %s user %s",
                project_id,
                user_id,
            )
            return
    elif not _auto_sync_enabled():
        logger.debug(
            "Slurm auto sync disabled — skipping remove-project-user enqueue for project %s user %s",
            project_id,
            user_id,
        )
        return

    logger.info(
        "Enqueuing Slurm remove-project-user for project %s user %s",
        project_id,
        user_id,
    )
    Job.enqueue(
        "coldfront.slurm.sync._run_remove_project_user",
        name=f"SlurmRemoveProjectUser:project-{project_id}:user-{user_id}",
        priority=3,
        project_id=project_id,
        user_id=user_id,
    )


# ---------------------------------------------------------------------------
# Targeted handler implementations (called by worker or CLI)
# ---------------------------------------------------------------------------


def _run_activate_allocation(*, allocation_id: int) -> SyncReport:
    """Create Slurm associations for a newly activated allocation.

    Called by the worker process (or directly by CLI).  Builds a minimal
    config payload containing only the entities needed for this allocation
    and sends it via ``POST /config``.
    """
    report = SyncReport(cluster="", success=False)

    try:
        allocation = Allocation.objects.get(pk=allocation_id)
    except Allocation.DoesNotExist:
        report.errors.append(f"Allocation {allocation_id} not found")
        return report

    resource = allocation.resource_object
    if resource is None:
        report.errors.append(f"Allocation {allocation_id} has no resource")
        return report

    # Determine cluster
    if isinstance(resource, SlurmCluster):
        cluster = resource
    elif isinstance(resource, SlurmPartition):
        cluster = resource.cluster
    else:
        report.errors.append(f"Allocation {allocation_id} targets non-slurm resource")
        return report

    report.cluster = cluster.name

    client = _build_client(cluster)
    if client is None:
        report.errors.append("slurmrestd not configured (SLURMRESTD_URL is empty)")
        return report

    # Load SlurmAssociation
    try:
        association = SlurmAssociation.objects.get(allocation=allocation)
    except SlurmAssociation.DoesNotExist:
        report.errors.append(f"No SlurmAssociation for allocation {allocation_id}")
        return report

    slurm_account = association.slurm_account
    if slurm_account is None:
        report.errors.append(f"SlurmAssociation for allocation {allocation_id} has no slurm_account set")
        return report

    # Ensure the account exists in Slurm
    try:
        client.create_accounts([_build_account_payload(slurm_account, allocation)])
        report.accounts_created += 1
    except SlurmAlreadyExistsException:
        report.warnings.append(f"Account '{slurm_account.name}' already exists")
        report.accounts_created += 1  # idempotent — desired state achieved
    except Exception as exc:
        report.errors.append(f"Failed to create account '{slurm_account.name}': {exc}")

    # Account-level association: registers the account on the cluster, places
    # it under its parent, and (when SU limits are enforced) carries GrpTresMins.
    # This MUST precede user associations so the account exists on the cluster.
    parent_name = association.parent.name if association.parent else None
    account_assoc_payload = _build_account_assoc_payload(slurm_account, cluster, parent=parent_name)
    try:
        client.create_associations([account_assoc_payload])
    except SlurmAlreadyExistsException:
        pass  # idempotent upsert — desired state achieved
    except Exception as exc:
        report.errors.append(f"Failed to create account association for '{slurm_account.name}': {exc}")

    # Create associations and users for each ProjectUser
    project = allocation.project
    for user in _iter_project_members(project):
        # Ensure SlurmUser exists
        _slurm_user = _ensure_slurm_user(user, cluster, slurm_account, association)

        # Ensure user exists in Slurm before creating the association
        user_payload = _build_user_payload(user, cluster, slurm_account)
        if user_payload is None:
            report.errors.append(f"Failed to build user payload for '{user.username}'")
            continue

        try:
            client.create_users([user_payload])
            report.users_created += 1
        except SlurmAlreadyExistsException:
            report.users_updated += 1
        except Exception as exc:
            report.errors.append(f"Failed to create user '{user.username}': {exc}")
            continue

        # Build assoc_rec_set entry
        assoc_payload = _build_assoc_payload(association, user, cluster, resource)
        try:
            client.create_associations([assoc_payload])
            report.associations_created += 1
        except SlurmAlreadyExistsException:
            report.associations_updated += 1
        except Exception as exc:
            report.errors.append(f"Failed to create association for {user.username}: {exc}")

    # Re-read what Slurm actually stored (a ``-1`` reset becomes unset) so the
    # ColdFront association reflects the real Slurm limits.
    _readback_association_limits(client, association, cluster, resource)

    # Trigger slurmctld cache refresh
    _reconfigure(client)

    report.success = len(report.errors) == 0
    return report


def _run_deactivate_allocation(*, allocation_id: int) -> SyncReport:
    """Remove associations and kill jobs for a deactivated allocation.

    Called when an allocation is expired or revoked.
    """
    report = SyncReport(cluster="", success=False)

    try:
        allocation = Allocation.objects.get(pk=allocation_id)
    except Allocation.DoesNotExist:
        report.errors.append(f"Allocation {allocation_id} not found")
        return report

    resource = allocation.resource_object
    if resource is None:
        report.errors.append(f"Allocation {allocation_id} has no resource")
        return report

    if isinstance(resource, SlurmCluster):
        cluster = resource
    elif isinstance(resource, SlurmPartition):
        cluster = resource.cluster
    else:
        report.errors.append(f"Allocation {allocation_id} targets non-slurm resource")
        return report

    report.cluster = cluster.name

    client = _build_client(cluster)
    if client is None:
        report.errors.append("slurmrestd not configured (SLURMRESTD_URL is empty)")
        return report

    try:
        association = SlurmAssociation.objects.get(allocation=allocation)
    except SlurmAssociation.DoesNotExist:
        report.success = True
        return report

    slurm_account = association.slurm_account
    if slurm_account is None:
        report.success = True
        return report

    partition_name = resource.name if isinstance(resource, SlurmPartition) else ""

    # Kill running jobs for each ProjectUser
    project = allocation.project
    for user in _iter_project_members(project):
        _kill_user_jobs(client, user, slurm_account, partition_name)

        # Delete the association
        delete_params: dict[str, Any] = {
            "account": slurm_account.name,
            "user": user.username,
            "cluster": cluster.name,
        }
        if partition_name:
            delete_params["partition"] = partition_name
        try:
            client.delete_associations(**delete_params)
            report.associations_deleted += 1
        except Exception as exc:
            report.errors.append(f"Failed to delete association for {user.username}: {exc}")

        # Reconcile SlurmUser
        _reconcile_slurm_user(user, cluster, slurm_account)

    # Trigger slurmctld cache refresh
    _reconfigure(client)

    report.success = len(report.errors) == 0
    return report


# ---------------------------------------------------------------------------
# Expiry-strategy workers (modify, not remove)
# ---------------------------------------------------------------------------


def _run_modify_allocation_associations(
    *,
    allocation_id: int,
    serialize_overrides: dict[str, Any],
    kill_jobs: bool = False,
    description: str = "modify",
) -> SyncReport:
    """Re-POST an allocation's user associations with modified limits/QOS.

    Because ``POST /slurmdb/{version}/associations/`` is an upsert, this issues
    an association payload per project member carrying only the keyword
    arguments in ``serialize_overrides`` (passed through to
    :meth:`SlurmClient.serialize_association`).  Membership and the account are
    left intact — only the association's limits/QOS change.  This is the shared
    worker behind the ``throttle``, ``hold`` and ``set_fields`` expiry
    strategies.
    """
    report = SyncReport(cluster="", success=False)

    try:
        allocation = Allocation.objects.get(pk=allocation_id)
    except Allocation.DoesNotExist:
        report.errors.append(f"Allocation {allocation_id} not found")
        return report

    resource = allocation.resource_object
    if resource is None:
        report.errors.append(f"Allocation {allocation_id} has no resource")
        return report

    if isinstance(resource, SlurmCluster):
        cluster = resource
    elif isinstance(resource, SlurmPartition):
        cluster = resource.cluster
    else:
        report.errors.append(f"Allocation {allocation_id} targets non-slurm resource")
        return report

    report.cluster = cluster.name

    client = _build_client(cluster)
    if client is None:
        report.errors.append("slurmrestd not configured (SLURMRESTD_URL is empty)")
        return report

    try:
        association = SlurmAssociation.objects.get(allocation=allocation)
    except SlurmAssociation.DoesNotExist:
        report.success = True
        return report

    slurm_account = association.slurm_account
    if slurm_account is None:
        report.success = True
        return report

    partition_name = resource.name if isinstance(resource, SlurmPartition) else ""

    for user in _iter_project_members(allocation.project):
        if kill_jobs:
            _kill_user_jobs(client, user, slurm_account, partition_name)

        payload = SlurmClient.serialize_association(
            account=slurm_account.name,
            user=user.username,
            cluster=cluster.name,
            partition=partition_name,
            **serialize_overrides,
        )
        try:
            client.create_associations([payload])
            report.associations_updated += 1
        except Exception as exc:
            report.errors.append(f"Failed to {description} association for {user.username}: {exc}")

    # Trigger slurmctld cache refresh
    _reconfigure(client)

    report.success = len(report.errors) == 0
    return report


def enqueue_throttle_allocation(allocation_id: int, cluster_id: int | None = None) -> None:
    """Enqueue a task that throttles an expired allocation to zero jobs.

    Sets ``max_jobs=0`` and ``max_submit_jobs=0`` on every association for the
    allocation (no running or pending jobs), leaving account membership and
    user associations in place.  Respects the per-cluster ``auto_sync_enabled``
    gate.
    """
    cluster = SlurmCluster.objects.get(pk=cluster_id) if cluster_id is not None else None
    if not _auto_sync_enabled(cluster):
        logger.debug(
            "Slurm auto sync disabled — skipping throttle enqueue for allocation %s",
            allocation_id,
        )
        return

    logger.info("Enqueuing Slurm throttle for allocation %s", allocation_id)
    Job.enqueue(
        "coldfront.slurm.sync._run_throttle_allocation",
        name=f"SlurmThrottle:allocation-{allocation_id}",
        priority=3,
        allocation_id=allocation_id,
    )


def _run_throttle_allocation(*, allocation_id: int) -> SyncReport:
    """Worker: set GrpJobs=0 / GrpSubmitJobs=0 on an expired allocation."""
    return _run_modify_allocation_associations(
        allocation_id=allocation_id,
        serialize_overrides={"max_jobs": 0, "max_submit_jobs": 0},
        kill_jobs=True,
        description="throttle",
    )


def enqueue_hold_allocation(allocation_id: int, cluster_id: int | None = None, *, hold_qos: str | None = None) -> None:
    """Enqueue a task that moves an expired allocation onto a frozen QOS.

    Sets each association's QOS to ``hold_qos`` (a QOS the site configures to
    block new work) without removing associations.  Respects the per-cluster
    ``auto_sync_enabled`` gate.
    """
    cluster = SlurmCluster.objects.get(pk=cluster_id) if cluster_id is not None else None
    if not _auto_sync_enabled(cluster):
        logger.debug(
            "Slurm auto sync disabled — skipping hold enqueue for allocation %s",
            allocation_id,
        )
        return

    if not hold_qos:
        logger.warning(
            "No hold QOS configured — skipping hold enqueue for allocation %s",
            allocation_id,
        )
        return

    logger.info("Enqueuing Slurm hold for allocation %s (qos=%s)", allocation_id, hold_qos)
    Job.enqueue(
        "coldfront.slurm.sync._run_hold_allocation",
        name=f"SlurmHold:allocation-{allocation_id}",
        priority=3,
        allocation_id=allocation_id,
        hold_qos=hold_qos,
    )


def _run_hold_allocation(*, allocation_id: int, hold_qos: str) -> SyncReport:
    """Worker: set each association's QOS to the configured frozen QOS."""
    return _run_modify_allocation_associations(
        allocation_id=allocation_id,
        serialize_overrides={"qoslevel": [hold_qos], "default_qos": hold_qos},
        kill_jobs=True,
        description="hold",
    )


def enqueue_set_fields_allocation(
    allocation_id: int,
    cluster_id: int | None = None,
    *,
    fields: dict[str, Any] | None = None,
) -> None:
    """Enqueue a task that applies arbitrary association limit overrides.

    ``fields`` is a mapping of :meth:`SlurmClient.serialize_association` keyword
    arguments (e.g. ``{"max_jobs": 0, "grp_tres": {"cpu": 0}}``) applied to
    every association for the allocation.  Respects the per-cluster
    ``auto_sync_enabled`` gate.
    """
    cluster = SlurmCluster.objects.get(pk=cluster_id) if cluster_id is not None else None
    if not _auto_sync_enabled(cluster):
        logger.debug(
            "Slurm auto sync disabled — skipping set_fields enqueue for allocation %s",
            allocation_id,
        )
        return

    if not fields:
        logger.warning(
            "No set_fields configured — skipping set_fields enqueue for allocation %s",
            allocation_id,
        )
        return

    logger.info("Enqueuing Slurm set_fields for allocation %s", allocation_id)
    Job.enqueue(
        "coldfront.slurm.sync._run_set_fields_allocation",
        name=f"SlurmSetFields:allocation-{allocation_id}",
        priority=3,
        allocation_id=allocation_id,
        fields=fields,
    )


def _run_set_fields_allocation(*, allocation_id: int, fields: dict[str, Any]) -> SyncReport:
    """Worker: apply arbitrary serialize_association overrides to an allocation."""
    return _run_modify_allocation_associations(
        allocation_id=allocation_id,
        serialize_overrides=dict(fields),
        kill_jobs=False,
        description="set_fields",
    )


# ``{"set": false, "infinite": true}`` tells slurmrestd to clear a numeric limit
# back to unlimited (equivalent to setting it to ``-1`` with sacctmgr).
_UINT32_INFINITE = {"set": False, "infinite": True, "number": 0}


def _no_val_number(value: Any) -> int | None:
    """Read a ``uint32_no_val_struct`` from a slurmrestd response into an int.

    slurmrestd returns numeric limits as
    ``{"set": bool, "infinite": bool, "number": N}``.  A limit that is unset or
    reported as infinite has no concrete value, so ``None`` is returned (which
    ColdFront stores as ``NULL``).  A bare integer is accepted for robustness.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        if not value.get("set", False) or value.get("infinite", False):
            return None
        number = value.get("number")
        return int(number) if number is not None else None
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _readback_association_limits(
    client: SlurmClient,
    association: SlurmAssociation,
    cluster: SlurmCluster,
    resource: SlurmCluster | SlurmPartition,
) -> None:
    """Re-read an association's job limits from Slurm and store them in ColdFront.

    After a ``-1`` (reset) limit is pushed, slurmdbd clears the limit and reports
    it as unset.  This queries the association back from slurmrestd and persists
    the resulting ``max_jobs``/``max_submit_jobs`` so ColdFront matches Slurm.
    Failures are logged and swallowed so they never break the activation flow.
    """
    slurm_account = association.slurm_account
    if slurm_account is None:
        return

    partition_name = resource.name if isinstance(resource, SlurmPartition) else ""

    try:
        response = client.get_associations(
            account=slurm_account.name,
            cluster=cluster.name,
            partition=partition_name or None,
        )
    except Exception as exc:
        logger.warning(
            "Failed to read back association limits for allocation %s: %s",
            association.allocation_id,
            exc,
        )
        return

    entries = response.get("associations", []) if isinstance(response, dict) else []
    # Prefer an account-level (``user == ""``) entry; fall back to the first.
    entry = None
    for candidate in entries:
        if not candidate.get("user"):
            entry = candidate
            break
    if entry is None and entries:
        entry = entries[0]
    if entry is None:
        return

    jobs = (entry.get("max") or {}).get("jobs") or {}
    new_max_jobs = _no_val_number(jobs.get("active"))
    new_max_submit_jobs = _no_val_number(jobs.get("total"))

    changed_fields: list[str] = []
    if association.max_jobs != new_max_jobs:
        association.max_jobs = new_max_jobs
        changed_fields.append("max_jobs")
    if association.max_submit_jobs != new_max_submit_jobs:
        association.max_submit_jobs = new_max_submit_jobs
        changed_fields.append("max_submit_jobs")

    if changed_fields:
        association.save(update_fields=changed_fields)
        logger.info(
            "Synced association limits for allocation %s from Slurm: %s",
            association.allocation_id,
            ", ".join(f"{f}={getattr(association, f)}" for f in changed_fields),
        )


def enqueue_restore_allocation(allocation_id: int, cluster_id: int | None = None) -> None:
    """Enqueue a task that restores an allocation's normal association limits.

    This is the inverse of the ``throttle``/``hold``/``set_fields`` expiry
    strategies.  It re-asserts each association's stored limits and QOS and
    explicitly clears the job-count and QOS fields those strategies modify so a
    renewed allocation is no longer throttled or held.  Respects the per-cluster
    ``auto_sync_enabled`` gate.
    """
    cluster = SlurmCluster.objects.get(pk=cluster_id) if cluster_id is not None else None
    if not _auto_sync_enabled(cluster):
        logger.debug(
            "Slurm auto sync disabled — skipping restore enqueue for allocation %s",
            allocation_id,
        )
        return

    logger.info("Enqueuing Slurm restore for allocation %s", allocation_id)
    Job.enqueue(
        "coldfront.slurm.sync._run_restore_allocation",
        name=f"SlurmRestore:allocation-{allocation_id}",
        priority=3,
        allocation_id=allocation_id,
    )


def _run_restore_allocation(*, allocation_id: int) -> SyncReport:
    """Worker: undo expiry modifications by re-asserting normal association state.

    Re-posts each user association with its stored limits/QOS (via
    :func:`_build_assoc_payload`).  Because a slurmrestd upsert never clears a
    field that is absent from the payload, any job-count limit or QOS left out
    of the stored values is explicitly cleared to unlimited here so an earlier
    ``throttle``/``hold``/``set_fields`` modification cannot persist.
    """
    report = SyncReport(cluster="", success=False)

    try:
        allocation = Allocation.objects.get(pk=allocation_id)
    except Allocation.DoesNotExist:
        report.errors.append(f"Allocation {allocation_id} not found")
        return report

    resource = allocation.resource_object
    if resource is None:
        report.errors.append(f"Allocation {allocation_id} has no resource")
        return report

    if isinstance(resource, SlurmCluster):
        cluster = resource
    elif isinstance(resource, SlurmPartition):
        cluster = resource.cluster
    else:
        report.errors.append(f"Allocation {allocation_id} targets non-slurm resource")
        return report

    report.cluster = cluster.name

    client = _build_client(cluster)
    if client is None:
        report.errors.append("slurmrestd not configured (SLURMRESTD_URL is empty)")
        return report

    try:
        association = SlurmAssociation.objects.get(allocation=allocation)
    except SlurmAssociation.DoesNotExist:
        report.success = True
        return report

    slurm_account = association.slurm_account
    if slurm_account is None:
        report.success = True
        return report

    for user in _iter_project_members(allocation.project):
        payload = _build_assoc_payload(association, user, cluster, resource)

        # Explicitly clear the job-count limits that ``throttle``/``set_fields``
        # may have zeroed when the allocation carries no stored value.
        jobs_block = payload.setdefault("max", {}).setdefault("jobs", {})
        jobs_block.setdefault("active", dict(_UINT32_INFINITE))
        jobs_block.setdefault("total", dict(_UINT32_INFINITE))

        # Ensure QOS is re-asserted so a ``hold`` QOS is replaced; an empty list
        # removes any lingering hold QOS when the allocation defines none.
        payload.setdefault("qos", payload.get("qos") or [])

        try:
            client.create_associations([payload])
            report.associations_updated += 1
        except Exception as exc:
            report.errors.append(f"Failed to restore association for {user.username}: {exc}")

    _reconfigure(client)

    report.success = len(report.errors) == 0
    return report


def _run_remove_project_user(*, project_id: int, user_id: int) -> SyncReport:
    """Remove a user from all allocations on a project.

    Called when a ProjectUser record is deleted.
    """
    report = SyncReport(cluster="", success=False)

    try:
        user_model = settings.AUTH_USER_MODEL
        from django.apps import apps

        User = apps.get_model(user_model)
        user = User.objects.get(pk=user_id)
    except Exception as exc:
        report.errors.append(f"User {user_id} not found: {exc}")
        return report

    # Find all active allocations for this project
    project_allocations = Allocation.objects.filter(
        project_id=project_id,
        status=AllocationStatusChoices.STATUS_ACTIVE,
    )

    # Collect unique clusters involved
    clusters_seen: set[int] = set()
    for allocation in project_allocations:
        resource = allocation.resource_object
        if resource is None:
            continue
        if isinstance(resource, SlurmCluster):
            clusters_seen.add(resource.pk)
        elif isinstance(resource, SlurmPartition):
            clusters_seen.add(resource.cluster_id)

    # Use the first cluster for client building (all clusters on a project
    # should share the same slurmrestd, but if they don't, the client will
    # be built from the first cluster's config)
    first_cluster = SlurmCluster.objects.filter(pk__in=clusters_seen).first() if clusters_seen else None
    client = _build_client(first_cluster)
    if client is None:
        report.errors.append("slurmrestd not configured (SLURMRESTD_URL is empty)")
        return report

    # Reload the query (we consumed it above)
    project_allocations = Allocation.objects.filter(
        project_id=project_id,
        status=AllocationStatusChoices.STATUS_ACTIVE,
    )

    for allocation in project_allocations:
        resource = allocation.resource_object
        if resource is None:
            continue

        if isinstance(resource, SlurmCluster):
            cluster = resource
        elif isinstance(resource, SlurmPartition):
            cluster = resource.cluster
        else:
            continue

        if report.cluster == "":
            report.cluster = cluster.name

        try:
            association = SlurmAssociation.objects.get(allocation=allocation)
        except SlurmAssociation.DoesNotExist:
            continue

        slurm_account = association.slurm_account
        if slurm_account is None:
            continue

        partition_name = resource.name if isinstance(resource, SlurmPartition) else ""

        # Kill jobs
        _kill_user_jobs(client, user, slurm_account, partition_name)

        # Delete association
        delete_params: dict[str, Any] = {
            "account": slurm_account.name,
            "user": user.username,
            "cluster": cluster.name,
        }
        if partition_name:
            delete_params["partition"] = partition_name
        try:
            client.delete_associations(**delete_params)
            report.associations_deleted += 1
        except Exception as exc:
            report.errors.append(f"Failed to delete association for {user.username}: {exc}")

        # Reconcile SlurmUser
        _reconcile_slurm_user(user, cluster, slurm_account)

    # Trigger slurmctld cache refresh
    _reconfigure(client)

    report.success = len(report.errors) == 0
    return report


# ---------------------------------------------------------------------------
# Full reconciliation sync
# ---------------------------------------------------------------------------


def run_sync(cluster_id: int | None = None) -> list[SyncReport]:
    """Perform a full reconciliation sync for one or all clusters.

    Uses ``POST /slurmdb/{version}/config`` to upsert the full desired
    accounting state for each cluster, then queries Slurm for orphaned
    associations and deletes them.

    Args:
        cluster_id: Optional cluster PK.  If ``None``, syncs all clusters.

    Returns:
        List of ``SyncReport`` — one per cluster processed.
    """
    reports: list[SyncReport] = []

    clusters = SlurmCluster.objects.all()
    if cluster_id is not None:
        clusters = clusters.filter(pk=cluster_id)

    for cluster in clusters:
        client = _build_client(cluster)
        if client is None:
            report = SyncReport(cluster=cluster.name, success=False)
            report.errors.append("slurmrestd not configured (SLURMRESTD_URL is empty)")
            reports.append(report)
            continue
        report = _sync_cluster(client, cluster)
        reports.append(report)

    return reports


def _sync_cluster(client: SlurmClient, cluster: SlurmCluster) -> SyncReport:
    """Sync a single cluster with slurmrestd."""
    start = timezone.now()
    report = SyncReport(cluster=cluster.name, success=False)

    # ---- Step 1: Build the full config payload ----
    config = _build_config_payload(cluster)
    if config is None:
        report.warnings.append(f"Cluster '{cluster.name}' has no active associations — nothing to upsert")
        report.success = True
        report.duration_ms = 0
        return report

    # ---- Step 2: Upsert via dedicated endpoints ----
    #
    # Older/newer slurmrestd builds have proven much more reliable when we
    # send explicit collection payloads to /accounts/, /users/, and
    # /associations/ than when we POST the whole synthetic /config payload in
    # one shot.  In particular, /config can partially parse association data,
    # emit warnings like "ignored field [fairshare] in DATA_PARSER_ASSOC", and
    # still return success, which leaves users/groups created but associations
    # missing.  Using the dedicated endpoints also lets us account for each
    # object individually and tolerate "already exists" conflicts per record.
    for account_payload in config.get("accounts", []):
        try:
            client.create_accounts([account_payload])
            report.accounts_created += 1
        except SlurmAlreadyExistsException:
            # Desired state already exists; treat as converged.
            report.accounts_created += 1
        except Exception as exc:
            report.errors.append(f"Failed to upsert account '{account_payload.get('name', '')}': {exc}")

    for user_payload in config.get("users", []):
        try:
            client.create_users([user_payload])
            report.users_created += 1
        except SlurmAlreadyExistsException:
            report.users_updated += 1
        except Exception as exc:
            report.errors.append(f"Failed to upsert user '{user_payload.get('name', '')}': {exc}")

    for assoc_payload in config.get("associations", []):
        try:
            client.create_associations([assoc_payload])
            report.associations_created += 1
        except SlurmAlreadyExistsException:
            report.associations_updated += 1
        except Exception as exc:
            report.errors.append(
                "Failed to upsert association "
                f"(account={assoc_payload.get('account', '')}, "
                f"user={assoc_payload.get('user', '')}, "
                f"cluster={assoc_payload.get('cluster', '')}, "
                f"partition={assoc_payload.get('partition', '')}): {exc}"
            )

    if report.errors:
        report.duration_ms = int((timezone.now() - start).total_seconds() * 1000)
        return report

    # ---- Step 3: Find and delete orphaned associations ----
    try:
        existing = client.get_associations(cluster=cluster.name, with_deleted=False)
    except Exception as exc:
        report.errors.append(f"Failed to query existing associations: {exc}")
        report.duration_ms = int((timezone.now() - start).total_seconds() * 1000)
        return report

    # Build set of expected (account, user, partition) tuples from active
    # associations in ColdFront
    expected = _build_expected_tuples(cluster)

    # Scope orphan deletion to every account ColdFront owns on this cluster,
    # not just accounts that currently have an expected tuple.  Otherwise a
    # user whose allocation expired (leaving its account with no expected
    # tuples) would never be cleaned up, and ColdFront would "lose" control of
    # that membership instead of acting as the source of truth.  Accounts not
    # owned by ColdFront are left untouched.
    managed_accounts = set(SlurmAccount.objects.filter(cluster=cluster).values_list("name", flat=True))

    # Delete orphaned associations
    for assoc in existing.get("associations", []):
        acct = assoc.get("account", "")
        user = assoc.get("user", "")
        partition = assoc.get("partition", "")
        key = (acct, user, partition)

        if not user:
            continue
        if acct not in managed_accounts:
            continue

        if key in expected:
            continue

        # This association exists in Slurm but has no matching active
        # allocation in ColdFront — delete it
        try:
            delete_params: dict[str, Any] = {
                "account": acct,
                "user": user,
                "cluster": cluster.name,
            }
            if partition:
                delete_params["partition"] = partition
            client.delete_associations(**delete_params)
            report.associations_deleted += 1
        except Exception as exc:
            report.errors.append(
                f"Failed to delete orphaned association (account={acct}, user={user}, partition={partition}): {exc}"
            )

    # ---- Step 4: Trigger slurmctld cache refresh ----
    _reconfigure(client)

    report.duration_ms = int((timezone.now() - start).total_seconds() * 1000)
    report.success = len(report.errors) == 0
    return report


# ---------------------------------------------------------------------------
# SU usage sync
# ---------------------------------------------------------------------------

# Hard cap on how many past days a single usage sync pass may catch up.
# Bounded by slurmdbd live-DB retention (Purge/Archive); a large catch-up
# costs up to one REST query per day per cluster.
USAGE_SYNC_MAX_CATCHUP_DAYS = 60


# TRES ids used in ``tres.allocated`` (Slurm internal TRES_ARRAY indices).
_TRES_NODE = 4
_TRES_BILLING = 5


def _utc_day_bounds(day: datetime.date) -> tuple[int, int]:
    """Return (start, end) UNIX timestamps for the UTC midnight window of a day."""
    start = timezone.make_aware(datetime.combine(day, time.min)).timestamp()
    end = start + 86400
    return int(start), int(end)


def _tres_count(tres_allocated: list[dict[str, Any]] | None, tres_id: int) -> int:
    """Return the integer count for a TRES entry in ``tres.allocated``."""
    if not tres_allocated:
        return 0
    for entry in tres_allocated:
        if entry.get("id") == tres_id:
            try:
                return int(float(entry.get("count", 0)))
            except (TypeError, ValueError):
                return 0
    return 0


@dataclass
class _DayAggregates:
    """Accumulated per-account aggregates for one day."""

    billing_units_consumed: float = 0.0
    walltime_sec_consumed: int = 0
    node_hours_consumed: float = 0.0
    job_count_consumed: int = 0
    billing_units_completed: float = 0.0
    walltime_sec_completed: int = 0
    job_count_completed: int = 0
    node_hours_by_partition: dict[str, float] = field(default_factory=dict)
    billing_by_qos: dict[str, float] = field(default_factory=dict)


def _roll_up_job_usage(
    jobs: list[dict[str, Any]],
    day_start: int,
    day_end: int,
) -> dict[str, _DayAggregates]:
    """Aggregate job records into per-account daily usage.

    Uses a single overlap query result for both attribution models:

    * ``*_consumed`` — ``billing_count x overlap_sec / 3600`` per day
      (pro-rated; matches sreport's accounting report).
    * ``*_completed`` — ``billing_count x full_elapsed / 3600`` charged on
      the day the job completed (matches sreport's job report; informational).

    Jobs with no start time (pending) and zero-overlap jobs are skipped.

    Args:
        jobs: Job dicts from :meth:`SlurmClient.get_job_usage`.
        day_start: UNIX start of the day window (inclusive).
        day_end: UNIX end of the day window (exclusive).

    Returns:
        Dict mapping account name to :class:`_DayAggregates`.
    """
    agg: dict[str, _DayAggregates] = {}

    for job in jobs:
        start_ts = int(job.get("time", {}).get("start", 0) or 0)
        end_ts = int(job.get("time", {}).get("end", 0) or 0)
        elapsed = int(job.get("time", {}).get("elapsed", 0) or 0)
        if start_ts == 0:
            # Pending / never-started — no usage to attribute.
            continue

        # Still-running jobs are clipped at the window end.
        effective_end = end_ts if end_ts != 0 else day_end
        overlap = max(0, min(effective_end, day_end) - max(start_ts, day_start))
        if overlap <= 0:
            continue

        account = job.get("account", "")
        partition = job.get("partition", "") or ""
        qos = job.get("qos", "") or ""

        billing_count = _tres_count(job.get("tres", {}).get("allocated"), _TRES_BILLING)
        node_count = _tres_count(job.get("tres", {}).get("allocated"), _TRES_NODE)

        a = agg.setdefault(account, _DayAggregates())

        # Consumed (pro-rated) attribution
        a.billing_units_consumed += billing_count * overlap / 3600.0
        a.walltime_sec_consumed += overlap
        a.node_hours_consumed += node_count * overlap / 3600.0
        a.job_count_consumed += 1
        if partition:
            a.node_hours_by_partition[partition] = (
                a.node_hours_by_partition.get(partition, 0.0) + node_count * overlap / 3600.0
            )
        if qos:
            a.billing_by_qos[qos] = a.billing_by_qos.get(qos, 0.0) + billing_count * overlap / 3600.0

        # Completed (completion-day) attribution — only when the job
        # finished inside this window.
        if end_ts != 0 and day_start <= end_ts < day_end:
            a.billing_units_completed += billing_count * elapsed / 3600.0
            a.walltime_sec_completed += elapsed
            a.job_count_completed += 1

    return agg


def _resolve_catchup_range(cluster: SlurmCluster) -> tuple[datetime.date, datetime.date] | None:
    """Return the (start, end) day range the daily sync should ingest.

    Resumes from ``cluster.last_usage_sync + 1`` up to yesterday, capped at
    ``USAGE_SYNC_MAX_CATCHUP_DAYS`` days (most recent days win). Returns
    ``None`` when there are no days to sync.
    """
    today = timezone.localdate()
    yesterday = today - timedelta(days=1)

    if cluster.last_usage_sync is None:
        start = yesterday - timedelta(days=USAGE_SYNC_MAX_CATCHUP_DAYS - 1)
    else:
        start = cluster.last_usage_sync + timedelta(days=1)
        floor = yesterday - timedelta(days=USAGE_SYNC_MAX_CATCHUP_DAYS - 1)
        if start < floor:
            start = floor

    if start > yesterday:
        return None
    return start, yesterday


def _run_usage_sync(
    cluster: SlurmCluster,
    start_day: datetime.date,
    end_day: datetime.date,
) -> UsageSyncReport:
    """Ingest per-day job usage for a cluster into :class:`SlurmAccountUsage`.

    Range-based and idempotent (``update_or_create`` per account/day); the
    caller decides the day range. On success, advances
    ``cluster.last_usage_sync`` to ``end_day``.

    Args:
        cluster: Cluster to sync.
        start_day: First day to ingest (inclusive).
        end_day: Last day to ingest (inclusive).

    Returns:
        :class:`UsageSyncReport`.
    """
    start = timezone.now()
    report = UsageSyncReport(cluster=cluster.name, success=False)
    client = _build_client(cluster)
    if client is None:
        report.errors.append("slurmrestd not configured (SLURMRESTD_URL is empty)")
        return report

    # Cache known accounts for name -> PK lookups.
    accounts = {}
    for acct in SlurmAccount.objects.filter(cluster=cluster).select_related("cluster"):
        accounts.setdefault(acct.name, acct)

    day = start_day
    while day <= end_day:
        day_start, day_end = _utc_day_bounds(day)
        try:
            jobs = client.get_job_usage(day_start, day_end, cluster.name)
        except Exception as exc:
            report.errors.append(f"Failed to query jobs for {day}: {exc}")
            report.duration_ms = int((timezone.now() - start).total_seconds() * 1000)
            return report
        report.jobs_ingested += len(jobs)
        report.days_synced += 1

        for account_name, a in _roll_up_job_usage(jobs, day_start, day_end).items():
            acct = accounts.get(account_name)
            if acct is None:
                # Account not managed by ColdFront (e.g. root) — skip.
                continue
            SlurmAccountUsage.objects.update_or_create(
                cluster=cluster,
                account=acct,
                period_start=day,
                defaults={
                    "period_end": day,
                    "billing_units_consumed": a.billing_units_consumed,
                    "walltime_sec_consumed": a.walltime_sec_consumed,
                    "node_hours_consumed": a.node_hours_consumed,
                    "job_count_consumed": a.job_count_consumed,
                    "billing_units_completed": a.billing_units_completed,
                    "walltime_sec_completed": a.walltime_sec_completed,
                    "job_count_completed": a.job_count_completed,
                    "node_hours_by_partition": a.node_hours_by_partition,
                    "billing_by_qos": a.billing_by_qos,
                },
            )
            report.accounts_updated += 1

        day += timedelta(days=1)

    cluster.last_usage_sync = end_day
    cluster.save(update_fields=["last_usage_sync"])

    report.duration_ms = int((timezone.now() - start).total_seconds() * 1000)
    report.success = True
    return report


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iter_project_members(project):
    """
    Yield every user who should receive Slurm access for a project.

    This always includes the project owner, then any ProjectUser members,
    de-duplicated by user PK.
    """
    seen_user_ids = set()

    owner = getattr(project, "owner", None)
    if owner is not None and owner.pk not in seen_user_ids:
        seen_user_ids.add(owner.pk)
        yield owner

    for project_user in ProjectUser.objects.filter(project=project).select_related("user"):
        user = project_user.user
        if user is None or user.pk in seen_user_ids:
            continue
        seen_user_ids.add(user.pk)
        yield user


def _topo_sort_accounts(accounts: Any) -> list[SlurmAccount]:
    """Order accounts so every parent precedes its children.

    The Slurm account hierarchy is expressed through
    :attr:`SlurmAssociation.parent` (an association's account has that account
    as its parent).  Accounts must be created in parent-before-child order so a
    child account's associations can reference an already-existing parent.

    Accounts whose parent is outside the supplied set (or unknown) are treated
    as roots.  Any cycle is broken deterministically by falling back to name
    order for the remaining accounts.
    """
    account_list = list(accounts)
    by_pk = {acct.pk: acct for acct in account_list}

    # Map child account pk -> parent account pk (within this set only).
    parent_of: dict[int, int] = {}
    for assoc in SlurmAssociation.objects.filter(
        slurm_account_id__in=by_pk.keys(),
        parent__isnull=False,
    ).values("slurm_account_id", "parent_id"):
        child_pk = assoc["slurm_account_id"]
        parent_pk = assoc["parent_id"]
        if parent_pk in by_pk and parent_pk != child_pk:
            parent_of[child_pk] = parent_pk

    ordered: list[SlurmAccount] = []
    placed: set[int] = set()

    def place(pk: int, visiting: set[int]) -> None:
        if pk in placed or pk in visiting:
            return
        visiting.add(pk)
        parent_pk = parent_of.get(pk)
        if parent_pk is not None:
            place(parent_pk, visiting)
        visiting.discard(pk)
        if pk not in placed:
            placed.add(pk)
            ordered.append(by_pk[pk])

    for acct in sorted(account_list, key=lambda a: a.name):
        place(acct.pk, set())

    return ordered


def _build_config_payload(cluster: SlurmCluster) -> dict[str, Any] | None:
    """Build the full ``openapi_slurmdbd_config_resp`` payload for a cluster.

    Returns ``None`` if the cluster has no active associations.
    """
    from coldfront.slurm.dump import _get_active_associations

    active = _get_active_associations(cluster)
    if not active:
        return None

    # Collect unique accounts
    account_ids = set()
    for a in active:
        if a.slurm_account_id:
            account_ids.add(a.slurm_account_id)

    accounts = _topo_sort_accounts(SlurmAccount.objects.filter(pk__in=account_ids))

    # Map account pk -> parent account name (from the association hierarchy) so
    # account-level associations can establish parent_account correctly.
    parent_name_by_account: dict[int, str] = {}
    for row in (
        SlurmAssociation.objects.filter(slurm_account_id__in=account_ids, parent__isnull=False)
        .select_related("parent")
        .values("slurm_account_id", "parent__name")
    ):
        parent_name_by_account.setdefault(row["slurm_account_id"], row["parent__name"])

    # Account-level associations establish the account hierarchy (parent_account)
    # and carry account-wide limits (GrpTresMins).  They MUST be sent before any
    # user associations so the account exists under its parent first.  Collect
    # them separately and prepend them to the association list below.
    account_assoc_payloads: list[dict[str, Any]] = []
    for acct in accounts:
        account_assoc_payload = _build_account_assoc_payload(
            acct,
            cluster,
            parent=parent_name_by_account.get(acct.pk),
        )
        account_assoc_payloads.append(account_assoc_payload)

    # Build user association payloads
    user_assoc_payloads: list[dict[str, Any]] = []
    user_payloads = []
    seen_user_ids: set[int] = set()
    for a in active:
        allocation = a.allocation
        if not allocation:
            continue
        resource = allocation.resource_object
        if resource is None:
            continue

        for user in _iter_project_members(allocation.project):
            if user.pk not in seen_user_ids:
                user_payload = _build_user_payload(user, cluster, a.slurm_account)
                if user_payload is not None:
                    user_payloads.append(user_payload)
                    seen_user_ids.add(user.pk)

            user_assoc_payloads.append(_build_assoc_payload(a, user, cluster, resource))

    # Account-level associations first, then user associations.
    assoc_payloads = account_assoc_payloads + user_assoc_payloads

    # Build account payloads
    account_allocation_map = {}
    for assoc in active:
        if assoc.slurm_account_id and assoc.allocation and assoc.slurm_account_id not in account_allocation_map:
            account_allocation_map[assoc.slurm_account_id] = assoc.allocation

    account_payloads = [_build_account_payload(acct, account_allocation_map.get(acct.pk)) for acct in accounts]

    # Build cluster payload
    cluster_payload = {
        "name": cluster.name,
    }

    return {
        "clusters": [cluster_payload],
        "accounts": account_payloads,
        "users": user_payloads,
        "associations": assoc_payloads,
    }


def _build_effective_qos(
    account: SlurmAccount | None,
    association: SlurmAssociation,
) -> list[str] | None:
    """
    Compute the effective QOS list for an association by merging
    account-level and association-level qos_add/qos_remove.

    The effective list is:
        (account_qos_add ∪ assoc_qos_add) ∖ (account_qos_remove ∪ assoc_qos_remove)

    Returns ``None`` if both sources have no QOS add/remove entries.
    """
    qos_add_names: set[str] = set()
    qos_remove_names: set[str] = set()

    # Account-level QOS
    if account:
        qos_add_names.update(q.name for q in account.qos_add.all())
        qos_remove_names.update(q.name for q in account.qos_remove.all())

    # Association-level QOS
    qos_add_names.update(q.name for q in association.qos_add.all())
    qos_remove_names.update(q.name for q in association.qos_remove.all())

    # Compute effective list
    effective = list(qos_add_names - qos_remove_names)
    return effective if effective else None


def _build_assoc_payload(
    association: SlurmAssociation,
    user: Any,
    cluster: SlurmCluster,
    resource: SlurmCluster | SlurmPartition,
) -> dict[str, Any]:
    """Build a nested ``assoc`` payload entry from ColdFront models.

    Delegates to :meth:`SlurmClient.serialize_association` so the nested
    schema, ``uint32_no_val_struct`` wrapping, and ``tres_list`` conversion
    live in a single place.
    """
    slurm_account = association.slurm_account
    partition_name = resource.name if isinstance(resource, SlurmPartition) else ""

    max_wall = None
    if association.max_wall_duration_per_job is not None:
        # Slurm wall limits are expressed in minutes.
        max_wall = int(association.max_wall_duration_per_job.total_seconds() // 60)

    # NOTE: ``parent_account`` is intentionally NOT set on user associations.
    # A user association places ``user`` under ``account``; setting
    # ``parent_account`` here would reparent the *user* under the account's
    # parent (e.g. "cs") instead of under ``account`` itself. The account's
    # own position in the hierarchy is established by the account-level
    # association (``user=""``) built in ``_build_account_assoc_payload``.
    return SlurmClient.serialize_association(
        account=slurm_account.name if slurm_account else "",
        user=user.username,
        cluster=cluster.name,
        partition=partition_name,
        default_qos=association.default_qos.name if association.default_qos else None,
        fairshare=association.fairshare,
        max_jobs=association.max_jobs,
        max_submit_jobs=association.max_submit_jobs,
        max_tres_per_job=association.max_tres_per_job or None,
        max_tres_mins_per_job=association.max_tres_mins_per_job or None,
        max_wall_duration_per_job=max_wall,
        qoslevel=_build_effective_qos(slurm_account, association),
    )


def _build_account_payload(
    account: SlurmAccount,
    allocation: Allocation | None = None,
) -> dict[str, Any]:
    """Build the Slurm account payload sent to ``POST /accounts/``.

    slurmdbd requires non-empty ``description`` and ``organization`` when
    creating an account.  ColdFront therefore normalizes both values here:

    * ``description``: ``SlurmAccount.description`` when present, otherwise the
      Slurm account name.
    * ``organization``: the Allocation's Tenant name when one is assigned,
      otherwise the Slurm account name.
    """
    description = (account.description or "").strip() or account.name
    organization = account.name

    tenant = getattr(allocation, "tenant", None) if allocation is not None else None
    if tenant is not None and getattr(tenant, "name", ""):
        organization = tenant.name

    return {
        "name": account.name,
        "description": description,
        "organization": organization,
    }


def _build_user_payload(
    user: Any,
    cluster: SlurmCluster,
    fallback_account: SlurmAccount | None,
) -> dict[str, Any] | None:
    """Build a Slurm user payload for full sync.

    Full sync should converge from current project membership even when a
    corresponding ``SlurmUser`` row has not yet been created locally. When no
    ``SlurmUser`` exists, fall back to the active association's account so the
    user record is still sent to slurmdbd before the association upsert.
    """
    slurm_user = (
        SlurmUser.objects.filter(user=user, cluster=cluster)
        .select_related(
            "default_account",
            "default_qos",
        )
        .first()
    )

    default_account = slurm_user.default_account if slurm_user and slurm_user.default_account else fallback_account
    if default_account is None:
        return None

    return SlurmClient.serialize_user(
        name=user.username,
        default_account=default_account.name,
        default_wckey=slurm_user.default_wckey if slurm_user else None,
        admin_level=slurm_user.admin_level if slurm_user and slurm_user.admin_level else 0,
    )


def _build_account_assoc_payload(
    account: SlurmAccount,
    cluster: SlurmCluster,
    parent: str | None = None,
) -> dict[str, Any]:
    """Build the account-level association (``user=""``) payload.

    This association registers the account on the cluster and places it in
    the hierarchy via ``parent_account``.  It is always emitted (even with no
    limits) because an account is only usable on a cluster once it has an
    account-level association; without it the account exists in slurmdbd's
    acct_table but never appears under the cluster.

    GrpTresMins (the account's SU grant) is an association-level field carried
    here, but only when the cluster enforces SU limits and a grant is set.
    """
    grp_tres_mins = None
    if cluster.enforce_su_limits and account.service_units:
        grp_tres_mins = {"billing": account.service_units * 60}

    return SlurmClient.serialize_association(
        account=account.name,
        user="",
        cluster=cluster.name,
        partition="",
        parent=parent,
        grp_tres_mins=grp_tres_mins,
    )


def _sync_association_qos(
    association: SlurmAssociation,
    cluster: SlurmCluster,
) -> None:
    """
    Targeted sync of QOS changes for a single association.

    Called when ``qos_add`` or ``qos_remove`` changes on either the
    association itself or its parent ``SlurmAccount``.  Builds an
    association payload with the merged effective QOS list and sends
    it via ``POST /associations/`` (upsert).

    Only syncs if the association has an active allocation and a
    ``slurm_account`` set.
    """
    allocation = association.allocation
    if not allocation:
        return
    if allocation.status != AllocationStatusChoices.STATUS_ACTIVE:
        return
    if not association.slurm_account:
        return

    resource = allocation.resource_object
    if resource is None:
        return
    if not isinstance(resource, (SlurmCluster, SlurmPartition)):
        return

    client = _build_client(cluster)
    if client is None:
        logger.warning(
            "slurmrestd not configured — cannot sync QOS for association %s",
            association.pk,
        )
        return

    # Build and send association payload for each project user
    project = allocation.project
    for pu in ProjectUser.objects.filter(project=project).select_related("user"):
        user = pu.user
        if user is None:
            continue

        assoc_payload = _build_assoc_payload(association, user, cluster, resource)
        try:
            client.create_associations([assoc_payload])
        except SlurmAlreadyExistsException:
            pass  # idempotent — desired state achieved
        except Exception as exc:
            logger.warning(
                "Failed to sync QOS for association %s (user=%s): %s",
                association.pk,
                user.username,
                exc,
            )


def _build_expected_tuples(cluster: SlurmCluster) -> set[tuple[str, str, str]]:
    """Build set of ``(account, user, partition)`` tuples expected in Slurm.

    Only includes active allocations with a non-null slurm_account.
    """
    from coldfront.slurm.dump import _get_active_associations

    expected: set[tuple[str, str, str]] = set()
    for a in _get_active_associations(cluster):
        acct = a.slurm_account
        if acct is None:
            continue
        allocation = a.allocation
        if not allocation:
            continue
        resource = allocation.resource_object
        if resource is None:
            continue
        partition = resource.name if isinstance(resource, SlurmPartition) else ""
        for user in _iter_project_members(allocation.project):
            expected.add((acct.name, user.username, partition))
    return expected


def _ensure_slurm_user(
    user: Any,
    cluster: SlurmCluster,
    slurm_account: SlurmAccount,
    association: SlurmAssociation,
) -> SlurmUser:
    """Get or create a SlurmUser record for a user+cluster.

    Uses ``get_or_create`` — existing records are never modified.
    """
    su, _created = SlurmUser.objects.get_or_create(
        user=user,
        cluster=cluster,
        defaults={"default_account": slurm_account},
    )
    return su


def _reconcile_slurm_user(
    user: Any,
    cluster: SlurmCluster,
    removed_account: SlurmAccount,
) -> None:
    """Reconcile a SlurmUser after an association deletion.

    If the removed account was the user's default, find another active
    allocation on this cluster and update the default.  If no active
    allocations remain, delete the SlurmUser entirely.
    """
    try:
        su = SlurmUser.objects.get(user=user, cluster=cluster)
    except SlurmUser.DoesNotExist:
        return

    if su.default_account != removed_account:
        return  # not affected

    # Find another active allocation on this cluster
    from coldfront.slurm.dump import _get_active_associations

    active = _get_active_associations(cluster)
    for a in active:
        allocation = a.allocation
        if not allocation:
            continue
        if allocation.project and allocation.project.users.filter(user=user).exists():
            if a.slurm_account and a.slurm_account != removed_account:
                su.default_account = a.slurm_account
                su.save()
                return

    # No other active allocation found — delete the SlurmUser
    su.delete()


def _kill_user_jobs(
    client: SlurmClient,
    user: Any,
    account: SlurmAccount,
    partition_name: str,
) -> None:
    """Kill all running jobs for a user on a given account+partition."""
    kill_msg: dict[str, Any] = {
        "user_name": user.username,
        "account": account.name,
        "signal": "SIGTERM",
    }
    if partition_name:
        kill_msg["partition"] = partition_name
    try:
        client.kill_jobs(kill_msg)
    except Exception as exc:
        logger.warning(
            "Failed to kill jobs for user %s account %s partition '%s': %s",
            user.username,
            account.name,
            partition_name,
            exc,
        )


def _reconfigure(client: SlurmClient) -> None:
    """Trigger slurmctld to reload its cached association data."""
    try:
        client._request("GET", client._slurm_path("reconfigure"))
    except Exception as exc:
        logger.warning("Slurmctld reconfigure failed: %s", exc)


# ---------------------------------------------------------------------------
# Cluster import (slurm.conf / REST API)
# ---------------------------------------------------------------------------


@dataclass
class ImportReport:
    """Report from a cluster import operation."""

    cluster: str
    success: bool = False
    cluster_created: bool = False
    cluster_updated: bool = False
    cluster_found: bool = False
    partitions_created: int = 0
    partitions_updated: int = 0
    partitions_found: int = 0
    qos_created: int = 0
    qos_found: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _cluster_billing_kwargs(parsed: ParsedSlurmConfig) -> dict[str, Any]:
    """Build SlurmCluster kwargs for the SU billing TRES config.

    Only includes keys present in the parsed slurm.conf. Never sets
    enforce_su_limits (a ColdFront-only decision, not in slurm.conf).
    """
    kwargs: dict[str, Any] = {}
    if parsed.default_tres_billing_weights:
        kwargs["default_tres_billing_weights"] = parse_tres_weights(parsed.default_tres_billing_weights)
    if parsed.priority_flags:
        flags = [f.strip() for f in parsed.priority_flags.split(",") if f.strip()]
        kwargs["priority_flags"] = [f for f in flags if f in SlurmPriorityFlagsChoices.values()]
    if parsed.priority_type:
        if parsed.priority_type in SlurmPriorityTypeChoices.values():
            kwargs["priority_type"] = parsed.priority_type
    if parsed.priority_decay_half_life:
        kwargs["priority_decay_half_life"] = parsed.priority_decay_half_life
    if parsed.priority_usage_reset_period:
        if parsed.priority_usage_reset_period in SlurmPriorityUsageResetPeriodChoices.values():
            kwargs["priority_usage_reset_period"] = parsed.priority_usage_reset_period
    return kwargs


def import_cluster_from_conf(
    conf_path: str,
    noop: bool = False,
    update: bool = False,
) -> ImportReport:
    """Import a cluster from a slurm.conf file.

    Parses the config file, then creates or updates SlurmCluster,
    SlurmPartition, and SlurmQOS records in ColdFront.

    Args:
        conf_path: Path to the slurm.conf file.
        noop: If True, print what would happen without writing to DB.
        update: If True, update existing records; otherwise skip.

    Returns:
        An ImportReport with counts and errors.
    """
    report = ImportReport(cluster="", success=False)

    parsed = parse_slurm_conf(conf_path)
    if not parsed.cluster_name:
        report.errors.append("No ClusterName found in slurm.conf")
        return report

    report.cluster = parsed.cluster_name

    # --- QOS ---
    for qos_name in sorted(parsed.qos_names):
        if noop:
            report.qos_found += 1
            continue
        try:
            SlurmQOS.objects.get(name=qos_name)
            report.qos_found += 1
        except SlurmQOS.DoesNotExist:
            SlurmQOS.objects.create(name=qos_name)
            report.qos_created += 1

    # --- Cluster ---
    cluster_billing = _cluster_billing_kwargs(parsed)
    if noop:
        report.cluster_created = True  # Would create
    else:
        try:
            cluster = SlurmCluster.objects.get(name=parsed.cluster_name)
            if update:
                for k, v in cluster_billing.items():
                    setattr(cluster, k, v)
                cluster.save()
                report.cluster_updated = True
            else:
                report.cluster_found = True
        except SlurmCluster.DoesNotExist:
            cluster = SlurmCluster.objects.create(
                name=parsed.cluster_name,
                **cluster_billing,
            )
            report.cluster_created = True

    # --- Partitions ---
    if not noop and not parsed.partitions:
        report.warnings.append("No partitions found in slurm.conf")
        report.success = True
        return report

    for pp in parsed.partitions:
        # Skip DEFAULT template partition (it's not a real partition)
        if pp.name.upper() == "DEFAULT":
            continue

        if noop:
            report.partitions_created += 1
            continue

        # Build partition kwargs
        kwargs: dict[str, Any] = {
            "cluster": cluster,
            "name": pp.name,
        }
        if pp.nodes:
            kwargs["nodes"] = pp.nodes
        if pp.priority is not None:
            kwargs["priority"] = pp.priority
        kwargs["is_default"] = pp.is_default
        if pp.default_time:
            kwargs["default_time"] = _parse_duration(pp.default_time)
        if pp.state:
            # Validate state against SlurmPartitionStateChoices
            state_upper = pp.state.upper()
            if state_upper in SlurmPartitionStateChoices.values():
                kwargs["state"] = state_upper
            else:
                kwargs["state"] = pp.state
        if pp.preempt_mode:
            # Validate preempt_mode against SlurmPreemptModeChoices
            preempt_upper = pp.preempt_mode.upper()
            if preempt_upper in SlurmPreemptModeChoices.values():
                kwargs["preempt_mode"] = preempt_upper
            else:
                kwargs["preempt_mode"] = pp.preempt_mode
        if pp.def_mem_per_cpu is not None:
            kwargs["def_mem_per_cpu"] = pp.def_mem_per_cpu
        if pp.tres_billing_weights:
            kwargs["tres_billing_weights"] = parse_tres_weights(pp.tres_billing_weights)

        try:
            partition = SlurmPartition.objects.get(cluster=cluster, name=pp.name)
            if update:
                for k, v in kwargs.items():
                    setattr(partition, k, v)
                partition.save()
                report.partitions_updated += 1
            else:
                report.partitions_found += 1
        except SlurmPartition.DoesNotExist:
            partition = SlurmPartition(**kwargs)
            partition.save()
            report.partitions_created += 1

        # Link QOS references
        if not noop:
            # AllowQOS (whitelist) -> partition.allow_qos M2M
            if pp.allow_qos and pp.allow_qos.upper() != "ALL":
                allow_qos_names = [n.strip() for n in pp.allow_qos.split(",") if n.strip()]
                allow_qos_objs = SlurmQOS.objects.filter(name__in=allow_qos_names)
                partition.allow_qos.set(allow_qos_objs)

            # QOS (assigned partition QOS) -> partition.qos FK
            if pp.qos and pp.qos.upper() != "ALL":
                try:
                    partition.qos = SlurmQOS.objects.get(name=pp.qos.strip())
                    partition.save()
                except SlurmQOS.DoesNotExist:
                    pass

    report.success = len(report.errors) == 0
    return report


def import_cluster_from_api(
    cluster_name: str,
    client: SlurmClient | None = None,
    noop: bool = False,
    update: bool = False,
) -> ImportReport:
    """Import a cluster from the Slurm REST API.

    Fetches partition info from slurmctld and QOS info from slurmdbd,
    then creates or updates models in ColdFront.

    Args:
        cluster_name: Name of the cluster to import.
        client: A SlurmClient instance. If None, builds one from settings.
        noop: If True, print what would happen without writing to DB.
        update: If True, update existing records; otherwise skip.

    Returns:
        An ImportReport with counts and errors.
    """
    report = ImportReport(cluster=cluster_name, success=False)

    # Build client if needed
    if client is None:
        client = _build_client(None)
        if client is None:
            report.errors.append("slurmrestd not configured")
            return report

    # Fetch partitions from slurmctld
    try:
        part_data = client.get_partitions()
    except Exception as exc:
        report.errors.append(f"Failed to fetch partitions: {exc}")
        return report

    partitions = part_data.get("partitions", [])
    if not partitions:
        report.warnings.append("No partitions returned by slurmctld")
        report.success = True
        return report

    # Fetch QOS from slurmdbd
    try:
        qos_data = client.get_qos()
    except Exception as exc:
        report.warnings.append(f"Failed to fetch QOS (non-fatal): {exc}")
        qos_data = {}

    api_qos_list = qos_data.get("qos", [])
    api_qos_names: set[str] = set()
    for q in api_qos_list:
        if isinstance(q, dict):
            qname = q.get("name", "")
            if qname:
                api_qos_names.add(qname)

    # --- QOS ---
    if noop:
        report.qos_found = len(api_qos_names)
    else:
        for qos_name in sorted(api_qos_names):
            try:
                SlurmQOS.objects.get(name=qos_name)
                report.qos_found += 1
            except SlurmQOS.DoesNotExist:
                SlurmQOS.objects.create(
                    name=qos_name,
                )
                report.qos_created += 1

    # --- Cluster ---
    if noop:
        report.cluster_created = True
    else:
        try:
            cluster = SlurmCluster.objects.get(name=cluster_name)
            if update:
                cluster.save()
                report.cluster_updated = True
            else:
                report.cluster_found = True
        except SlurmCluster.DoesNotExist:
            cluster = SlurmCluster.objects.create(
                name=cluster_name,
            )
            report.cluster_created = True

    # --- Partitions ---
    for part in partitions:
        if not isinstance(part, dict):
            continue
        part_name = part.get("name", "")
        if not part_name:
            continue

        if noop:
            report.partitions_created += 1
            continue

        # Build kwargs from API response fields
        kwargs: dict[str, Any] = {
            "cluster": cluster,
            "name": part_name,
        }

        nodes_str = part.get("nodes", "")
        if isinstance(nodes_str, dict):
            # nodes may be a dict with 'nodes' key containing the list
            nodes_list = nodes_str.get("nodes", nodes_str.get("name", ""))
            if isinstance(nodes_list, list):
                kwargs["nodes"] = ",".join(nodes_list)
            else:
                kwargs["nodes"] = str(nodes_list)
        elif nodes_str:
            kwargs["nodes"] = nodes_str

        priority = part.get("priority")
        if priority is not None:
            try:
                kwargs["priority"] = int(priority)
            except (ValueError, TypeError):
                pass

        # State
        state = part.get("state", "")
        if isinstance(state, dict):
            state = state.get("state", "UP")
        state_upper = state.upper() if state else "UP"
        # Validate against SlurmPartitionStateChoices
        if state_upper in SlurmPartitionStateChoices.values():
            kwargs["state"] = state_upper
        else:
            kwargs["state"] = state_upper  # still accept it but warn

        # Default flag
        kwargs["is_default"] = part.get("default", "NO") == "YES"

        # Preempt mode
        preempt = part.get("preempt_mode", "")
        if isinstance(preempt, dict):
            preempt = preempt.get("preempt_mode", "")
        preempt_upper = preempt.upper()
        # Validate against SlurmPreemptModeChoices
        if preempt_upper and preempt_upper in SlurmPreemptModeChoices.values():
            kwargs["preempt_mode"] = preempt_upper
        elif preempt:
            kwargs["preempt_mode"] = preempt
        else:
            kwargs["preempt_mode"] = ""

        # Max time -> max_wall_duration_per_job
        max_time = part.get("max_time", "")
        if isinstance(max_time, dict):
            max_time = max_time.get("number", 0)
        if isinstance(max_time, (int, float)) and max_time > 0:
            kwargs["max_wall_duration_per_job"] = timedelta(minutes=int(max_time))

        # Default time
        default_time = part.get("default_time", "")
        if isinstance(default_time, dict):
            default_time = default_time.get("number", 0)
        if isinstance(default_time, (int, float)) and default_time > 0:
            kwargs["default_time"] = timedelta(minutes=int(default_time))

        # DefMemPerCPU
        def_mem = part.get("def_mem_per_cpu")
        if def_mem is not None:
            try:
                kwargs["def_mem_per_cpu"] = int(def_mem)
            except (ValueError, TypeError):
                pass

        # QOS references
        allow_qos_names: set[str] = set()
        allow_qos = part.get("allow_qos", "")
        if isinstance(allow_qos, str) and allow_qos and allow_qos.upper() != "ALL":
            allow_qos_names.update(n.strip() for n in allow_qos.split(","))
        qos_assigned = part.get("qos", "")

        try:
            partition = SlurmPartition.objects.get(cluster=cluster, name=part_name)
            if update:
                for k, v in kwargs.items():
                    if k != "cluster":
                        setattr(partition, k, v)
                partition.save()
                report.partitions_updated += 1
            else:
                report.partitions_found += 1
        except SlurmPartition.DoesNotExist:
            partition = SlurmPartition(**kwargs)
            partition.save()
            report.partitions_created += 1

        # Link QOS references
        # AllowQOS (whitelist) -> partition.allow_qos M2M
        if allow_qos_names:
            allow_qos_objs = SlurmQOS.objects.filter(name__in=list(allow_qos_names))
            partition.allow_qos.set(allow_qos_objs)

        # QOS (assigned partition QOS) -> partition.qos FK
        if isinstance(qos_assigned, str) and qos_assigned and qos_assigned.upper() != "ALL":
            try:
                partition.qos = SlurmQOS.objects.get(name=qos_assigned.strip())
                partition.save()
            except SlurmQOS.DoesNotExist:
                pass

    report.success = len(report.errors) == 0
    return report


def _parse_duration(duration_str: str) -> timedelta | None:
    """Parse a Slurm duration string into a timedelta.

    Supports formats:
        ``24:00:00`` (HH:MM:SS)
        ``01:00:00`` (HH:MM:SS)
        ``72:00:00`` (HH:MM:SS)
        ``30-0`` (days-hours)
        ``00:30:00`` (HH:MM:SS)
    """
    if not duration_str:
        return None

    duration_str = duration_str.strip()

    # Try HH:MM:SS format
    if ":" in duration_str:
        parts = duration_str.split(":")
        if len(parts) == 3:
            try:
                h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
                return timedelta(hours=h, minutes=m, seconds=s)
            except ValueError:
                pass

    # Try days-hours format (e.g., "30-0")
    if "-" in duration_str:
        parts = duration_str.split("-", 1)
        try:
            days = int(parts[0])
            hours = int(parts[1]) if len(parts) > 1 else 0
            return timedelta(days=days, hours=hours)
        except ValueError:
            pass

    # Try plain minutes
    try:
        return timedelta(minutes=int(duration_str))
    except ValueError:
        return None
