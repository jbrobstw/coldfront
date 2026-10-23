# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

from datetime import timedelta

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase, override_settings
from django.utils import timezone

from coldfront.core.jobs.registry import system_jobs
from coldfront.ras.choices import AllocationStatusChoices
from coldfront.ras.flows.allocations import AllocationStatusFlow
from coldfront.ras.jobs import ExpireAllocationsJob
from coldfront.ras.models import Allocation, Project, Resource, ResourceType
from coldfront.users.models import User


class ExpireAllocationsJobTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = User.objects.create(username="owner")
        cls.project = Project.objects.create(name="Project 1", owner=cls.user)
        cls.resource_type = ResourceType.objects.create(name="Cluster", slug="cluster")
        cls.resource = Resource.objects.create(name="Resource 1", slug="r-1", resource_type=cls.resource_type)
        cls.resource_ct = ContentType.objects.get_for_model(Resource)

    def _make_allocation(self, *, status, end_date):
        return Allocation.objects.create(
            justification="Need resources",
            project=self.project,
            owner=self.user,
            resource_object_type=self.resource_ct,
            resource_object_id=self.resource.pk,
            status=status,
            end_date=end_date,
        )

    def _run_job(self):
        job = ExpireAllocationsJob(ExpireAllocationsJob.get_jobs().first() or ExpireAllocationsJob.enqueue())
        job.run()

    def test_registered_as_system_job(self):
        self.assertIn(ExpireAllocationsJob, system_jobs)

    def test_meta_name(self):
        self.assertEqual(ExpireAllocationsJob.name, "coldfront.ras.jobs.ExpireAllocationsJob")

    def test_expires_active_past_end_date(self):
        allocation = self._make_allocation(
            status=AllocationStatusChoices.STATUS_ACTIVE,
            end_date=timezone.now() - timedelta(days=1),
        )
        self._run_job()
        allocation.refresh_from_db()
        self.assertEqual(allocation.status, AllocationStatusChoices.STATUS_EXPIRED)

    def test_ignores_future_end_date(self):
        allocation = self._make_allocation(
            status=AllocationStatusChoices.STATUS_ACTIVE,
            end_date=timezone.now() + timedelta(days=1),
        )
        self._run_job()
        allocation.refresh_from_db()
        self.assertEqual(allocation.status, AllocationStatusChoices.STATUS_ACTIVE)

    def test_ignores_null_end_date(self):
        allocation = self._make_allocation(
            status=AllocationStatusChoices.STATUS_ACTIVE,
            end_date=None,
        )
        self._run_job()
        allocation.refresh_from_db()
        self.assertEqual(allocation.status, AllocationStatusChoices.STATUS_ACTIVE)

    def test_ignores_non_active(self):
        allocation = self._make_allocation(
            status=AllocationStatusChoices.STATUS_REQUESTED,
            end_date=timezone.now() - timedelta(days=1),
        )
        self._run_job()
        allocation.refresh_from_db()
        self.assertEqual(allocation.status, AllocationStatusChoices.STATUS_REQUESTED)

    @override_settings(ALLOCATION_AUTO_EXPIRE_ENABLED=False)
    def test_no_op_when_disabled(self):
        allocation = self._make_allocation(
            status=AllocationStatusChoices.STATUS_ACTIVE,
            end_date=timezone.now() - timedelta(days=1),
        )
        self._run_job()
        allocation.refresh_from_db()
        self.assertEqual(allocation.status, AllocationStatusChoices.STATUS_ACTIVE)

    def test_permission_callback_veto_skips_allocation(self):
        allocation = self._make_allocation(
            status=AllocationStatusChoices.STATUS_ACTIVE,
            end_date=timezone.now() - timedelta(days=1),
        )

        def veto(_obj, _user):
            return False

        AllocationStatusFlow.register_transition_permission_callback("expire", veto)
        try:
            self._run_job()
        finally:
            AllocationStatusFlow._transition_permission_callbacks.get("expire", []).remove(veto)

        allocation.refresh_from_db()
        self.assertEqual(allocation.status, AllocationStatusChoices.STATUS_ACTIVE)
