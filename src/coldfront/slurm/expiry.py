# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

"""
Pluggable Slurm allocation-expiry strategies.

ColdFront's core auto-expiry trigger (``coldfront.ras.jobs.ExpireAllocationsJob``)
is resource-agnostic: it simply transitions due allocations to EXPIRED and lets
listeners decide what expiry *means* for their resource.  For Slurm, expiry is
represented by a configurable *strategy* rather than a single hard-coded action.

A strategy is a callable with the signature::

    def strategy(allocation, cluster, *, config) -> None: ...

where ``allocation`` is the expired :class:`~coldfront.ras.models.Allocation`,
``cluster`` is the resolved :class:`~coldfront.slurm.models.SlurmCluster`, and
``config`` is an :class:`ExpiryConfig` carrying the site/cluster settings a
strategy may need (e.g. a hold QOS name, or arbitrary ``set_fields`` overrides).

Strategies register themselves with :func:`register_slurm_expiry_strategy`.  The
registry is shared by built-in strategies *and* external plugins, and the set of
registered strategies is what populates the per-cluster strategy selector in the
UI (see :func:`strategy_choices`).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from django.utils.translation import gettext_lazy as _

if TYPE_CHECKING:
    from coldfront.ras.models import Allocation
    from coldfront.slurm.models import SlurmCluster

logger = logging.getLogger(__name__)


@dataclass
class ExpiryConfig:
    """Resolved configuration passed to an expiry strategy.

    Values are resolved per-allocation from the cluster override (if set) with a
    fallback to the global ``SLURM_EXPIRY_*`` settings.
    """

    cluster_id: int | None = None
    hold_qos: str = ""
    set_fields: dict[str, Any] = field(default_factory=dict)


# ``name -> (callable, human_label)``
_STRATEGIES: dict[str, tuple[Callable[..., None], Any]] = {}


def register_slurm_expiry_strategy(
    name: str,
    label: Any | None = None,
) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """Register a Slurm expiry strategy under ``name``.

    Usage::

        @register_slurm_expiry_strategy("throttle", label="Throttle to zero jobs")
        def throttle(allocation, cluster, *, config): ...

    Args:
        name: Unique strategy identifier stored on ``SlurmCluster`` and in
            settings.  Re-registering an existing name overrides it (so plugins
            can replace built-ins intentionally).
        label: Human-readable label shown in the UI.  Defaults to ``name``.

    Returns:
        The decorator that registers and returns the strategy callable unchanged.
    """

    def decorator(func: Callable[..., None]) -> Callable[..., None]:
        if name in _STRATEGIES:
            logger.info("Overriding existing Slurm expiry strategy %r", name)
        _STRATEGIES[name] = (func, label if label is not None else name)
        return func

    return decorator


def get_strategy(name: str) -> Callable[..., None] | None:
    """Return the strategy callable registered under ``name`` (or ``None``)."""
    entry = _STRATEGIES.get(name)
    return entry[0] if entry is not None else None


def list_strategies() -> list[str]:
    """Return the names of all registered strategies, sorted."""
    return sorted(_STRATEGIES)


def strategy_choices() -> list[tuple[str, Any]]:
    """Return ``(name, label)`` pairs for every registered strategy, sorted.

    Used to populate the per-cluster strategy form field so built-in *and*
    plugin-provided strategies surface in the UI.
    """
    return sorted(((name, label) for name, (_func, label) in _STRATEGIES.items()), key=lambda pair: pair[0])


# ---------------------------------------------------------------------------
# Built-in strategies
# ---------------------------------------------------------------------------


@register_slurm_expiry_strategy("remove_associations", label=_("Remove associations (default)"))
def _strategy_remove_associations(allocation: Allocation, cluster: SlurmCluster, *, config: ExpiryConfig) -> None:
    """Kill jobs and delete the allocation's Slurm associations.

    This is ColdFront's historical expiry behavior and the default strategy.
    """
    from coldfront.slurm.sync import enqueue_deactivate_allocation

    enqueue_deactivate_allocation(allocation.pk, cluster_id=cluster.pk)


@register_slurm_expiry_strategy("throttle", label=_("Throttle to zero jobs"))
def _strategy_throttle(allocation: Allocation, cluster: SlurmCluster, *, config: ExpiryConfig) -> None:
    """Set ``GrpJobs=0``/``GrpSubmitJobs=0`` without removing associations.

    Running jobs are killed; the account, users, and associations remain so the
    allocation can be reactivated cleanly.
    """
    from coldfront.slurm.sync import enqueue_throttle_allocation

    enqueue_throttle_allocation(allocation.pk, cluster_id=cluster.pk)


@register_slurm_expiry_strategy("hold", label=_("Move to hold QOS"))
def _strategy_hold(allocation: Allocation, cluster: SlurmCluster, *, config: ExpiryConfig) -> None:
    """Move the allocation's associations onto a configured frozen QOS."""
    from coldfront.slurm.sync import enqueue_hold_allocation

    enqueue_hold_allocation(allocation.pk, cluster_id=cluster.pk, hold_qos=config.hold_qos)


@register_slurm_expiry_strategy("set_fields", label=_("Apply custom association fields"))
def _strategy_set_fields(allocation: Allocation, cluster: SlurmCluster, *, config: ExpiryConfig) -> None:
    """Apply arbitrary ``serialize_association`` overrides to the associations."""
    from coldfront.slurm.sync import enqueue_set_fields_allocation

    enqueue_set_fields_allocation(allocation.pk, cluster_id=cluster.pk, fields=config.set_fields)


@register_slurm_expiry_strategy("noop", label=_("Do nothing"))
def _strategy_noop(allocation: Allocation, cluster: SlurmCluster, *, config: ExpiryConfig) -> None:
    """Take no Slurm action on expiry (sites that handle expiry externally)."""
    logger.debug(
        "Slurm expiry strategy 'noop' — no action for allocation %s on cluster %s",
        allocation.pk,
        cluster.name,
    )


# ---------------------------------------------------------------------------
# Resolution + dispatch
# ---------------------------------------------------------------------------


def resolve_strategy_name(cluster: SlurmCluster) -> str:
    """Return the effective strategy name for ``cluster``.

    Per-cluster ``expiry_strategy`` wins; otherwise falls back to the global
    ``SLURM_EXPIRY_STRATEGY`` setting (default ``"remove_associations"``).
    """
    from django.conf import settings

    name = (getattr(cluster, "expiry_strategy", "") or "").strip()
    if name:
        return name
    return getattr(settings, "SLURM_EXPIRY_STRATEGY", "remove_associations")


def resolve_config(cluster: SlurmCluster) -> ExpiryConfig:
    """Resolve the :class:`ExpiryConfig` for ``cluster``.

    Each value prefers the per-cluster override and falls back to the matching
    global ``SLURM_EXPIRY_*`` setting.
    """
    from django.conf import settings

    hold_qos = (getattr(cluster, "expiry_hold_qos", "") or "").strip()
    if not hold_qos:
        hold_qos = getattr(settings, "SLURM_EXPIRY_HOLD_QOS", "") or ""

    set_fields = getattr(cluster, "expiry_set_fields", None) or {}
    if not set_fields:
        set_fields = getattr(settings, "SLURM_EXPIRY_SET_FIELDS", {}) or {}

    return ExpiryConfig(cluster_id=cluster.pk, hold_qos=hold_qos, set_fields=dict(set_fields))


def dispatch_expiry(allocation: Allocation, cluster: SlurmCluster) -> None:
    """Resolve and run the effective expiry strategy for ``allocation``.

    Strategy resolution and dispatch never raise: an unknown or failing strategy
    is logged and swallowed so it cannot break the allocation transition that
    triggered it.
    """
    name = resolve_strategy_name(cluster)
    strategy = get_strategy(name)
    if strategy is None:
        logger.error(
            "Unknown Slurm expiry strategy %r for cluster %s — skipping allocation %s",
            name,
            cluster.name,
            allocation.pk,
        )
        return

    config = resolve_config(cluster)
    try:
        strategy(allocation, cluster, config=config)
    except Exception:
        logger.exception(
            "Slurm expiry strategy %r failed for allocation %s on cluster %s",
            name,
            allocation.pk,
            cluster.name,
        )
