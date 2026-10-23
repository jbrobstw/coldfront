# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

from django import forms
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _

# Slurm stores "inherit the parent account's fairshare" as the sentinel -1
# (slurmrestd ``shares_raw``). In the UI it is entered/displayed as "parent".
FAIRSHARE_PARENT = "parent"
FAIRSHARE_PARENT_VALUE = -1


class FairshareField(forms.CharField):
    """Accept a non-negative integer or the literal ``parent``.

    Slurm's fairshare accepts a positive integer or ``parent`` (inherit the
    parent account's value). ``parent`` is stored as ``-1`` and rendered back
    as ``parent``.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("label", _("Fairshare"))
        kwargs.setdefault(
            "help_text",
            _("A non-negative integer, or 'parent' to inherit the parent's fairshare."),
        )
        super().__init__(*args, **kwargs)

    def to_python(self, value):
        value = super().to_python(value)
        if value in self.empty_values:
            return None
        if isinstance(value, str) and value.strip().lower() == FAIRSHARE_PARENT:
            return FAIRSHARE_PARENT_VALUE
        try:
            number = int(value)
        except (TypeError, ValueError):
            raise ValidationError(
                _("Enter a non-negative integer or 'parent'."),
                code="invalid",
            )
        if number < FAIRSHARE_PARENT_VALUE:
            raise ValidationError(
                _("Fairshare cannot be less than -1."),
                code="min_value",
            )
        return number

    def prepare_value(self, value):
        if value == FAIRSHARE_PARENT_VALUE:
            return FAIRSHARE_PARENT
        return value
