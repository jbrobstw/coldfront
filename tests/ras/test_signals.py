# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

from django.contrib.auth import get_user_model
from django.test import TestCase

from coldfront.ras.models import Project, ProjectUser
from coldfront.users.models import Group

User = get_user_model()


class ProjectUserGroupMirroringTest(TestCase):
    """The ProjectUser post_save/post_delete signals mirror project membership
    into the project's associated group for every path (view, API, ORM, bulk)."""

    @classmethod
    def setUpTestData(cls):
        cls.owner = User.objects.create(username="owner")
        cls.user = User.objects.create(username="member")

    def test_orm_create_adds_member_to_group(self):
        group = Group.objects.create(name="sig-create")
        project = Project.objects.create(name="P-create", owner=self.owner, group=group)

        ProjectUser.objects.create(project=project, user=self.user)

        self.assertIn(self.user, group.users.all())

    def test_orm_delete_removes_member_from_group(self):
        group = Group.objects.create(name="sig-delete")
        project = Project.objects.create(name="P-delete", owner=self.owner, group=group)
        pu = ProjectUser.objects.create(project=project, user=self.user)
        self.assertIn(self.user, group.users.all())

        pu.delete()

        self.assertNotIn(self.user, group.users.all())

    def test_create_without_group_is_noop(self):
        project = Project.objects.create(name="P-nogroup", owner=self.owner)

        # Should not raise even though the project has no group.
        ProjectUser.objects.create(project=project, user=self.user)

    def test_delete_keeps_member_when_shared_group_still_has_them(self):
        group = Group.objects.create(name="sig-shared")
        project_a = Project.objects.create(name="P-a", owner=self.owner, group=group)
        project_b = Project.objects.create(name="P-b", owner=self.owner, group=group)
        pu_a = ProjectUser.objects.create(project=project_a, user=self.user)
        ProjectUser.objects.create(project=project_b, user=self.user)
        self.assertIn(self.user, group.users.all())

        # Removing from one project must not remove them from the shared group
        # because another project with the same group still has them.
        pu_a.delete()

        self.assertIn(self.user, group.users.all())
