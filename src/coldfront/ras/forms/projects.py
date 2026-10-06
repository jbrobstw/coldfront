# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

from crispy_forms.layout import Fieldset
from django import forms
from django.core.exceptions import ObjectDoesNotExist
from django.urls import reverse
from django.utils.translation import gettext_lazy as _

from coldfront.forms import (
    OrganizationalModelForm,
    PrimaryModelForm,
    PrimaryModelImportForm,
    TenancyForm,
    TenancyImportForm,
)
from coldfront.forms.fields import CSVModelChoiceField
from coldfront.forms.widgets import APISelectWidget
from coldfront.ras.models import Project, ProjectUser
from coldfront.users.directory import (
    candidate_label_from_token,
    directory_get_user,
    provision_local_user_from_candidate,
    resolve_user_from_candidate_token,
    user_to_candidate_token,
)
from coldfront.users.models import Group, User
from coldfront.utils.forms import get_field_value


class ProjectForm(TenancyForm, OrganizationalModelForm):
    class Meta:
        model = Project
        fields = [
            "name",
            "slug",
            "description",
            "group",
            "tags",
            "tenant",
            "tenant_group",
        ]

    fieldsets = (
        Fieldset(
            _("Project"),
            "name",
            "slug",
            "description",
            "group",
        ),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Only admins can modify slug
        if hasattr(self, "user") and self.user and self.user.is_authenticated and self.user.is_superuser:
            return

        self.fields["slug"].widget.attrs["disabled"] = "disabled"
        self.fields["slug"].required = False
        self.fields["slug"].disabled = True


class ProjectUserForm(PrimaryModelForm):
    user = forms.ChoiceField(
        label=_("User"),
        required=True,
        widget=APISelectWidget(),
        help_text=_("Search local users and any enabled directory providers."),
    )

    class Meta:
        model = ProjectUser
        fields = [
            "project",
            "user",
        ]

    fieldsets = (
        Fieldset(
            _("Project User"),
            "project",
            "user",
        ),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["user"].widget.attrs["data-url"] = reverse("users-api:user-candidates")
        self._seed_user_choices()

        if project_id := get_field_value(self, "project"):
            try:
                Project.objects.get(pk=project_id)
                self.fields["project"].widget.attrs["data-readonly"] = "readonly"
            except ObjectDoesNotExist:
                pass

    def _seed_user_choices(self):
        current_value = None
        current_label = None

        if self.instance.pk and self.instance.user_id:
            current_value = user_to_candidate_token(self.instance.user)
            current_label = str(self.instance.user)
        elif self.is_bound:
            current_value = self.data.get(self.add_prefix("user"))
            if current_value:
                current_label = candidate_label_from_token(current_value)

        if current_value:
            self.fields["user"].choices = [(current_value, current_label or current_value)]
            self.initial["user"] = current_value
        else:
            self.fields["user"].choices = []

    def clean_user(self):
        token = self.cleaned_data["user"]
        return resolve_user_from_candidate_token(token)


class ProjectImportForm(TenancyImportForm, PrimaryModelImportForm):
    owner = CSVModelChoiceField(
        label=_("Owner"),
        queryset=User.objects.all(),
        required=True,
        to_field_name="username",
        help_text=_("Owner of the project"),
        error_messages={
            "invalid_choice": _("User not found."),
        },
    )

    group = CSVModelChoiceField(
        label=_("Group"),
        queryset=Group.objects.all(),
        required=False,
        to_field_name="name",
        help_text=_("Group that maps to this project"),
    )

    class Meta:
        model = Project
        fields = [
            "name",
            "owner",
            "description",
            "group",
            "tags",
            "tenant",
        ]


class ProjectUserImportForm(PrimaryModelImportForm):
    user = forms.CharField(
        label=_("User"),
        required=True,
        help_text=_("Username of an existing local user or an exact directory username eligible for provisioning."),
    )

    project = CSVModelChoiceField(
        label=_("Project"),
        queryset=Project.objects.all(),
        required=True,
        to_field_name="name",
        error_messages={
            "invalid_choice": _("Project not found."),
        },
    )

    class Meta:
        model = ProjectUser
        fields = [
            "user",
            "project",
        ]

    def clean_user(self):
        username = (self.cleaned_data.get("user") or "").strip()
        if not username:
            raise forms.ValidationError(_("This field is required."))

        local_user = User.objects.filter(username__iexact=username).first()
        if local_user:
            return local_user

        candidate = directory_get_user(username=username)
        if candidate is None:
            raise forms.ValidationError(_("User not found locally or in any enabled directory provider."))

        provisioned = provision_local_user_from_candidate(candidate)
        if provisioned is None:
            raise forms.ValidationError(
                _("User '{username}' has not logged in and directory provisioning is disabled.").format(
                    username=username
                )
            )

        return provisioned
