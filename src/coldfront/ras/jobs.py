# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

from datetime import timedelta

from django.conf import settings
from django.utils import timezone

from coldfront.core.jobs.registry import system_job
from coldfront.core.jobs.runner import JobRunner
from coldfront.ras.choices import AllocationStatusChoices
from coldfront.ras.flows.allocations import AllocationStatusFlow
from coldfront.ras.models import Allocation, ProjectInvite


@system_job(interval=1440)  # daily
class PruneProjectInviteJob(JobRunner):
    """
    Deletes expired and accepted project invites.

    Expiration is computed (never stored) from ``created`` plus
    ``INVITE_CODE_EXPIRE_SECONDS``; the accept view independently rejects
    expired invites, so this job is hygiene cleanup.

    Expired invites are always deleted. Accepted invites are only deleted when
    their ``accepted_at`` timestamp is older than
    ``settings.ACCEPTED_INVITE_RETENTION`` days. If the retention period is zero
    (or negative), accepted invites are kept indefinitely.
    """

    class Meta:
        name = "coldfront.ras.jobs.ProjectInviteExpirationJob"

    def run(self, *_args, **_kwargs):
        # Expired invites are always deleted, regardless of retention.
        expired = ProjectInvite.objects.expired()
        expired_count = expired.count()
        expired.delete()
        self.logger.info("Deleted %s expired invites", expired_count)

        # Accepted invites are only deleted once they are older than the
        # retention period (measured from their accepted_at timestamp).
        retention = getattr(settings, "ACCEPTED_INVITE_RETENTION", 90)
        if retention > 0:
            cutoff = timezone.now() - timedelta(days=retention)
            accepted_old = ProjectInvite.objects.accepted().filter(accepted_at__lt=cutoff)
            accepted_count = accepted_old.count()
            accepted_old.delete()
            self.logger.info(
                "Deleted %s accepted invites older than %s days",
                accepted_count,
                retention,
            )
        else:
            self.logger.info(
                "ACCEPTED_INVITE_RETENTION=%s, skipping accepted invite cleanup",
                retention,
            )

        return True


@system_job(interval=getattr(settings, "ALLOCATION_AUTO_EXPIRE_JOB_INTERVAL", 1440))
class ExpireAllocationsJob(JobRunner):
    """
    Periodically expire active allocations whose end date has passed.

    This is the resource-agnostic trigger for allocation expiry. It finds every
    ``ACTIVE`` allocation with an ``end_date`` in the past and drives the real
    ``AllocationStatusFlow.expire`` transition — exactly the same code path a
    human would take from the UI. That means the ``allocation_status_change``
    signal fires and every registered target callback runs, so each resource
    type / plugin decides how an expired allocation is represented on its
    backend (e.g. the Slurm plugin applies its configured expiry strategy).

    There is no grace period: an allocation is expired as soon as its
    ``end_date`` is in the past.

    The job is a no-op when ``settings.ALLOCATION_AUTO_EXPIRE_ENABLED`` is
    False. A plugin may veto expiry for specific allocations by registering an
    ``"expire"`` transition permission callback on ``AllocationStatusFlow``;
    such callbacks receive ``user=None`` for system-initiated expiry.
    """

    class Meta:
        name = "coldfront.ras.jobs.ExpireAllocationsJob"

    def run(self, *_args, **_kwargs):
        if not getattr(settings, "ALLOCATION_AUTO_EXPIRE_ENABLED", True):
            self.logger.info("ALLOCATION_AUTO_EXPIRE_ENABLED is False, skipping allocation expiry")
            return True

        now = timezone.now()
        due = Allocation.objects.filter(
            status=AllocationStatusChoices.STATUS_ACTIVE,
            end_date__isnull=False,
            end_date__lt=now,
        )

        expired_count = 0
        skipped_count = 0
        for allocation in due.iterator():
            flow = AllocationStatusFlow(allocation)
            transition = flow.expire

            # Guard against races / unexpected state and honor any plugin veto
            # registered as an "expire" permission callback (user=None for a
            # system-initiated expiry).
            if not transition.can_proceed():
                skipped_count += 1
                continue
            if not transition.has_perm(None):
                self.logger.info(
                    "Expiry vetoed by permission callback for allocation %s",
                    allocation.pk,
                )
                skipped_count += 1
                continue

            try:
                flow.expire()
                expired_count += 1
            except Exception:
                self.logger.exception("Failed to expire allocation %s", allocation.pk)
                skipped_count += 1

        self.logger.info(
            "Allocation auto-expiry complete: %s expired, %s skipped",
            expired_count,
            skipped_count,
        )
        return True
