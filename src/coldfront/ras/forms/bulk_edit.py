# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

from crispy_forms.layout import Fieldset
from django import forms
from django.utils.translation import gettext_lazy as _

from coldfront.context import current_request
from coldfront.forms import (
    AllocatableResourceBulkEditForm,
    OrganizationalModelBulkEditForm,
    PrimaryModelBulkEditForm,
)
from coldfront.forms.widgets import BulkEditNullBooleanSelect
from coldfront.ras.choices import AllocationStatusChoices, ResourceStatusChoices
from coldfront.ras.models import Allocation, Project, ProjectUser, Resource, ResourceType
from coldfront.users.models import Group, User
from coldfront.utils.forms import add_blank_choice

#
# Resource Types
#


class ResourceTypeBulkEditForm(OrganizationalModelBulkEditForm):
    color = forms.CharField(
        max_length=100,
        required=False,
        label=_("Color"),
    )

    model = ResourceType
    nullable_fields = (
        "description",
        "color",
    )

    @property
    def fieldsets(self):
        return [
            Fieldset(
                _("Resource Type"),
                "description",
                "color",
            ),
        ]


#
# Resources
#


class ResourceBulkEditForm(AllocatableResourceBulkEditForm, PrimaryModelBulkEditForm):
    resource_type = forms.ModelChoiceField(
        queryset=ResourceType.objects.all(),
        required=False,
        label=_("Resource Type"),
    )
    status = forms.ChoiceField(
        choices=add_blank_choice(ResourceStatusChoices),
        required=False,
        label=_("Status"),
    )
    parent = forms.ModelChoiceField(
        queryset=Resource.objects.all(),
        required=False,
        label=_("Parent"),
    )

    model = Resource
    nullable_fields = (
        "tenant_group",
        "tenant",
        "description",
        "locked",
        "schema",
        "resource_type",
        "status",
        "parent",
    )

    @property
    def fieldsets(self):
        return [
            Fieldset(
                _("Resource"),
                "description",
                "locked",
                "schema",
            ),
            Fieldset(
                _("Resource Type & Status"),
                "resource_type",
                "status",
                "parent",
            ),
        ]


#
# Projects
#


class ProjectBulkEditForm(OrganizationalModelBulkEditForm):
    owner = forms.ModelChoiceField(
        queryset=User.objects.all(),
        required=False,
        label=_("Owner"),
    )
    group = forms.ModelChoiceField(
        queryset=Group.objects.all(),
        required=False,
        label=_("Group"),
    )

    model = Project
    nullable_fields = (
        "tenant_group",
        "tenant",
        "description",
        "owner",
        "group",
    )

    @property
    def fieldsets(self):
        return [
            Fieldset(
                _("Project"),
                "description",
                "owner",
                "group",
            ),
        ]


#
# Project Users
#


class ProjectUserBulkEditForm(PrimaryModelBulkEditForm):
    project = forms.ModelChoiceField(
        queryset=Project.objects.all(),
        required=False,
        label=_("Project"),
    )

    model = ProjectUser

    @property
    def fieldsets(self):
        return [
            Fieldset(
                _("Project User"),
                "project",
            ),
        ]


#
# Allocations
#


class AllocationBulkEditForm(PrimaryModelBulkEditForm):
    project = forms.ModelChoiceField(
        queryset=Project.objects.all(),
        required=False,
        label=_("Project"),
    )
    owner = forms.ModelChoiceField(
        queryset=User.objects.all(),
        required=False,
        label=_("Owner"),
    )
    status = forms.ChoiceField(
        choices=add_blank_choice(AllocationStatusChoices),
        required=False,
        label=_("Status"),
    )
    start_date = forms.DateTimeField(
        required=False,
        label=_("Start Date"),
    )
    end_date = forms.DateTimeField(
        required=False,
        label=_("End Date"),
    )
    justification = forms.CharField(
        widget=forms.Textarea(attrs={"rows": 3}),
        required=False,
        label=_("Justification"),
    )
    auto_expire = forms.NullBooleanField(
        label=_("Auto expire"),
        required=False,
        widget=BulkEditNullBooleanSelect,
    )
    expiration_grace_days = forms.IntegerField(
        required=False,
        min_value=0,
        label=_("Expiration grace days"),
    )
    model = Allocation
    nullable_fields = (
        "tenant_group",
        "tenant",
        "description",
        "project",
        "owner",
        "status",
        "start_date",
        "end_date",
        "justification",
        "expiration_grace_days",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        request = current_request.get()
        user = getattr(request, "user", None)
        self._show_expiration_controls = bool(user and user.is_authenticated and user.is_superuser)
        if not self._show_expiration_controls:
            self.fields.pop("auto_expire", None)
            self.fields.pop("expiration_grace_days", None)

    @property
    def fieldsets(self):
        fieldsets = [
            Fieldset(
                _("Allocation"),
                "description",
                "project",
                "owner",
                "status",
            ),
        ]
        if self._show_expiration_controls:
            fieldsets.append(Fieldset(_("Expiration"), "auto_expire", "expiration_grace_days"))
        fieldsets.append(
            Fieldset(
                _("Dates"),
                "start_date",
                "end_date",
            ),
        )
        fieldsets.append(
            Fieldset(
                _("Text"),
                "justification",
            ),
        )
        return fieldsets
