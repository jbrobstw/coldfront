# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

"""
Management command to create a useful baseline set of ColdFront roles and
object permissions.

ColdFront ships with no built-in roles, so any non-superuser is effectively
unable to do anything beyond the defaults configured in ``DEFAULT_PERMISSIONS``.
This command bootstraps four common personas found at an HPC center:

* ``PI`` — Principal Investigator. Manages their own projects, project
  membership, requests and renews allocations, and submits change requests.
* ``Project Manager`` — A non-PI delegate who can do most of what a PI does on
  projects they belong to, minus the PI-only actions (creating/deleting
  projects and transferring ownership).
* ``Researcher`` — A student or researcher with view-only access to the
  projects and allocations they are a member of.
* ``Cluster Admin`` — Operates the cluster: full control over Slurm and storage
  objects, resources, and allocation review/lifecycle actions, but no access to
  ColdFront user/permission/tenancy administration.

Running the command is idempotent: it creates any missing roles and object
permissions and updates the actions/constraints of ones it previously created
so the command can be re-run safely after upgrades.

Usage::

    coldfront setup_default_roles
    coldfront setup_default_roles --noop
"""

from __future__ import annotations

import logging

from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.translation import gettext_lazy as _

from coldfront.users.models import ObjectPermission, Role

logger = logging.getLogger(__name__)

# Reusable constraint fragments ------------------------------------------------

# Objects whose project is owned by the current user.
PROJECT_OWNER = {"project__owner": "$user"}
# Objects whose project the current user is a member of.
PROJECT_MEMBER = {"project__users__user": "$user"}
# Objects directly owned by the current user.
SELF_OWNER = {"owner": "$user"}

# Allocation change requests reach the project through their allocation.
CR_PROJECT_OWNER = {"allocation__project__owner": "$user"}
CR_PROJECT_MEMBER = {"allocation__project__users__user": "$user"}

# Allocation-extension models (Slurm associations, storage quotas) reach the
# project through their allocation as well.
EXT_PROJECT_OWNER = {"allocation__project__owner": "$user"}
EXT_PROJECT_MEMBER = {"allocation__project__users__user": "$user"}


# Permission catalog -----------------------------------------------------------
#
# Each entry describes a single ObjectPermission. ``object_types`` is a list of
# ``(app_label, model)`` tuples. ``constraints`` follows the ObjectPermission
# convention: a list of dicts is OR'd, ``None`` grants unrestricted access.
#
# Names follow the documented ``{access}/{scope}/{model}`` convention so the
# catalog stays readable in the admin UI.

PERMISSIONS = {
    # -- PI / Project Manager: projects they own or belong to -----------------
    "write/self/project": {
        "description": _("Manage projects you own (PI)"),
        "object_types": [("ras", "project")],
        "actions": ["view", "add", "change", "delete"],
        "constraints": [SELF_OWNER],
    },
    "write:edit/team/project": {
        "description": _("Edit projects you own or manage"),
        "object_types": [("ras", "project")],
        "actions": ["view", "change"],
        "constraints": [{"owner": "$user"}, {"users__user": "$user"}],
    },
    "write/team/projectuser": {
        "description": _("Manage membership of projects you own or belong to"),
        "object_types": [("ras", "projectuser")],
        "actions": ["view", "add", "change", "delete"],
        "constraints": [PROJECT_OWNER, PROJECT_MEMBER],
    },
    # -- Allocations ----------------------------------------------------------
    "write:request:renew/self/allocation": {
        "description": _("Request, renew, and manage allocations for your own projects (PI)"),
        "object_types": [("ras", "allocation")],
        "actions": ["view", "add", "change", "request", "renew"],
        "constraints": [SELF_OWNER, PROJECT_OWNER],
    },
    "write:request:renew/team/allocation": {
        "description": _("Request and renew allocations for projects you manage"),
        "object_types": [("ras", "allocation")],
        "actions": ["view", "add", "request", "renew"],
        "constraints": [SELF_OWNER, PROJECT_OWNER, PROJECT_MEMBER],
    },
    # -- Allocation change requests -------------------------------------------
    "write:request/self/allocationchangerequest": {
        "description": _("Submit change requests for allocations on your own projects (PI)"),
        "object_types": [("ras", "allocationchangerequest")],
        "actions": ["view", "add", "change", "request"],
        "constraints": [CR_PROJECT_OWNER],
    },
    "write:request/team/allocationchangerequest": {
        "description": _("Submit change requests for allocations on projects you manage"),
        "object_types": [("ras", "allocationchangerequest")],
        "actions": ["view", "add", "request"],
        "constraints": [CR_PROJECT_OWNER, CR_PROJECT_MEMBER],
    },
    # -- Comments (collaboration on your own projects/allocations) ------------
    "write:add/team/commententry": {
        "description": _("Add comments on objects you own or collaborate on"),
        "object_types": [("core", "commententry")],
        "actions": ["view", "add"],
        "constraints": None,
    },
    "write:edit/self/commententry": {
        "description": _("Edit and delete your own comments"),
        "object_types": [("core", "commententry")],
        "actions": ["view", "change", "delete"],
        "constraints": [{"created_by": "$user"}],
    },
    # -- Researcher: read-only team access ------------------------------------
    "read/team/project": {
        "description": _("View projects you belong to"),
        "object_types": [("ras", "project")],
        "actions": ["view"],
        "constraints": [{"owner": "$user"}, {"users__user": "$user"}],
    },
    "read/team/projectuser": {
        "description": _("View membership of projects you belong to"),
        "object_types": [("ras", "projectuser")],
        "actions": ["view"],
        "constraints": [{"user": "$user"}, PROJECT_OWNER, PROJECT_MEMBER],
    },
    "read/team/allocation": {
        "description": _("View allocations for projects you belong to"),
        "object_types": [("ras", "allocation")],
        "actions": ["view"],
        "constraints": [SELF_OWNER, PROJECT_OWNER, PROJECT_MEMBER],
    },
    "read/team/allocationchangerequest": {
        "description": _("View allocation change requests for projects you belong to"),
        "object_types": [("ras", "allocationchangerequest")],
        "actions": ["view"],
        "constraints": [CR_PROJECT_OWNER, CR_PROJECT_MEMBER],
    },
    "read/team/slurmassociation": {
        "description": _("View Slurm associations for allocations you belong to"),
        "object_types": [("slurm", "slurmassociation")],
        "actions": ["view"],
        "constraints": [EXT_PROJECT_OWNER, EXT_PROJECT_MEMBER],
    },
    "read/team/storagequota": {
        "description": _("View storage quotas for allocations you belong to"),
        "object_types": [("storage", "storagequota")],
        "actions": ["view"],
        "constraints": [EXT_PROJECT_OWNER, EXT_PROJECT_MEMBER],
    },
    # -- Cluster Admin: allocation review & lifecycle -------------------------
    "write:review/all/allocation": {
        "description": _("Review and drive the lifecycle of all allocations"),
        "object_types": [("ras", "allocation")],
        "actions": [
            "view",
            "add",
            "change",
            "delete",
            "approve",
            "deny",
            "activate",
            "expire",
            "revoke",
            "renew",
        ],
        "constraints": None,
    },
    "write:review/all/allocationchangerequest": {
        "description": _("Review, approve, deny, and apply all allocation change requests"),
        "object_types": [("ras", "allocationchangerequest")],
        "actions": ["view", "add", "change", "delete", "approve", "deny", "apply"],
        "constraints": None,
    },
    "read/all/project": {
        "description": _("View all projects"),
        "object_types": [("ras", "project")],
        "actions": ["view"],
        "constraints": None,
    },
    "read/all/projectuser": {
        "description": _("View all project memberships"),
        "object_types": [("ras", "projectuser")],
        "actions": ["view"],
        "constraints": None,
    },
    # -- Cluster Admin: resources and cluster objects -------------------------
    "write/all/resource": {
        "description": _("Manage generic resources and resource types"),
        "object_types": [("ras", "resource"), ("ras", "resourcetype")],
        "actions": ["view", "add", "change", "delete"],
        "constraints": None,
    },
    "write/all/slurm": {
        "description": _("Manage all Slurm cluster objects"),
        "object_types": [
            ("slurm", "slurmcluster"),
            ("slurm", "slurmpartition"),
            ("slurm", "slurmaccount"),
            ("slurm", "slurmassociation"),
            ("slurm", "slurmuser"),
            ("slurm", "slurmqos"),
        ],
        "actions": ["view", "add", "change", "delete"],
        "constraints": None,
    },
    "write/all/storage": {
        "description": _("Manage all storage objects"),
        "object_types": [
            ("storage", "storageresource"),
            ("storage", "storagecluster"),
            ("storage", "storagequota"),
            ("storage", "storagesnapshotpolicy"),
        ],
        "actions": ["view", "add", "change", "delete"],
        "constraints": None,
    },
    "write:add/all/commententry": {
        "description": _("View and add comments on any object"),
        "object_types": [("core", "commententry")],
        "actions": ["view", "add"],
        "constraints": None,
    },
}


# Role catalog -----------------------------------------------------------------
#
# Each role maps to the permission keys above. Weight controls ordering and
# precedence (lower runs first / higher priority).

ROLES = {
    "PI": {
        "description": _("Principal Investigator: owns and manages projects and allocations"),
        "weight": 100,
        "permissions": [
            "write/self/project",
            "write/team/projectuser",
            "write:request:renew/self/allocation",
            "write:request/self/allocationchangerequest",
            "read/team/slurmassociation",
            "read/team/storagequota",
            "write:add/team/commententry",
            "write:edit/self/commententry",
        ],
    },
    "Project Manager": {
        "description": _("Non-PI delegate: manages membership, allocations, and change requests for their projects"),
        "weight": 110,
        "permissions": [
            "write:edit/team/project",
            "write/team/projectuser",
            "write:request:renew/team/allocation",
            "write:request/team/allocationchangerequest",
            "read/team/slurmassociation",
            "read/team/storagequota",
            "write:add/team/commententry",
            "write:edit/self/commententry",
        ],
    },
    "Researcher": {
        "description": _("Student or researcher: view-only access to projects and allocations they belong to"),
        "weight": 120,
        "permissions": [
            "read/team/project",
            "read/team/projectuser",
            "read/team/allocation",
            "read/team/allocationchangerequest",
            "read/team/slurmassociation",
            "read/team/storagequota",
            "write:add/team/commententry",
            "write:edit/self/commententry",
        ],
    },
    "Cluster Admin": {
        "description": _("Operates the cluster: full control of Slurm/storage/resources and allocation review"),
        "weight": 90,
        "permissions": [
            "read/all/project",
            "read/all/projectuser",
            "write:review/all/allocation",
            "write:review/all/allocationchangerequest",
            "write/all/resource",
            "write/all/slurm",
            "write/all/storage",
            "write:add/all/commententry",
            "write:edit/self/commententry",
        ],
    },
}


class Command(BaseCommand):
    help = "Create baseline ColdFront roles and object permissions (PI, Project Manager, Researcher, Cluster Admin)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--noop",
            action="store_true",
            help=_("Show what would be created without writing to the database"),
        )

    def _content_types(self, object_types):
        cts = []
        for app_label, model in object_types:
            try:
                cts.append(ContentType.objects.get_by_natural_key(app_label, model))
            except ContentType.DoesNotExist:
                self.stderr.write(
                    self.style.WARNING(
                        _("Skipping unknown content type {app}.{model} (app not installed?)").format(
                            app=app_label, model=model
                        )
                    )
                )
        return cts

    def handle(self, *args, **options):
        noop = options.get("noop", False)

        with transaction.atomic():
            permissions = self._sync_permissions(noop)
            self._sync_roles(permissions, noop)

            if noop:
                self.stdout.write(self.style.WARNING(_("Dry run: rolling back, no changes were saved.")))
                transaction.set_rollback(True)

        if not noop:
            self.stdout.write(self.style.SUCCESS(_("Default roles and permissions are up to date.")))

    def _sync_permissions(self, noop):
        resolved = {}
        for name, spec in PERMISSIONS.items():
            cts = self._content_types(spec["object_types"])
            if not cts:
                # Every referenced app was missing; skip this permission entirely.
                continue

            obj_perm, created = ObjectPermission.objects.get_or_create(
                name=name,
                defaults={
                    "description": str(spec["description"]),
                    "actions": spec["actions"],
                    "constraints": spec["constraints"],
                    "enabled": True,
                },
            )

            if not created:
                obj_perm.description = str(spec["description"])
                obj_perm.actions = spec["actions"]
                obj_perm.constraints = spec["constraints"]
                obj_perm.enabled = True
                obj_perm.save()

            obj_perm.object_types.set(cts)
            resolved[name] = obj_perm

            verb = _("Created") if created else _("Updated")
            self.stdout.write(f"  {verb} permission: {name}")

        return resolved

    def _sync_roles(self, permissions, noop):
        for name, spec in ROLES.items():
            role, created = Role.objects.get_or_create(
                name=name,
                defaults={
                    "description": str(spec["description"]),
                    "weight": spec["weight"],
                },
            )

            if not created:
                role.description = str(spec["description"])
                role.weight = spec["weight"]
                role.save()

            perms = [permissions[p] for p in spec["permissions"] if p in permissions]
            role.object_permissions.set(perms)

            verb = _("Created") if created else _("Updated")
            self.stdout.write(f"{verb} role: {name} ({len(perms)} permissions)")
