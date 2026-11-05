# SPDX-FileCopyrightText: (C) DigitalOcean, LLC
# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

import json

from django import forms
from djmoney.forms.widgets import MoneyWidget


class AuthorsWidget(forms.Widget):
    """
    Render a list of ``{"name", "orcid"}`` author objects as an interactive
    "tag list" (text box + add button, with editable/removable rows) instead of
    a raw JSON textarea.

    The canonical value is kept as a JSON string in a hidden input so the field
    continues to round-trip the same ``{"name", "orcid"}`` shape used by the
    provider-import flows. The interactive UI is progressive enhancement: if
    JavaScript is unavailable, the hidden input still submits whatever value it
    holds.
    """

    template_name = "widgets/authors_input.html"

    def get_context(self, name, value, attrs):
        context = super().get_context(name, value, attrs)
        authors = value
        if isinstance(authors, str):
            try:
                authors = json.loads(authors) if authors else []
            except json.JSONDecodeError:
                authors = []
        if not isinstance(authors, list):
            authors = []
        context["widget"]["authors"] = [a for a in authors if isinstance(a, dict) and a.get("name")]
        return context


class MarkdownWidget(forms.Textarea):
    """
    Provide a live preview for Markdown-formatted content.
    """

    template_name = "widgets/markdown_input.html"

    def __init__(self, attrs=None):
        # Markdown fields should use monospace font
        default_attrs = {
            "class": "font-monospace",
        }
        if attrs:
            default_attrs.update(attrs)

        super().__init__(default_attrs)


class MoneyWidget(MoneyWidget):
    """
    Render a django-money amount + currency pair as an input-group.

    The amount input and the currency select are emitted as siblings inside a
    single ``.input-group`` so they sit side-by-side in one row. The amount
    input receives ``form-control`` and the currency select ``form-select``.
    """

    template_name = "widgets/money_input.html"

    # Render with a normal <label> like the other form fields instead of
    # Django's default <fieldset>/<legend> wrapper for MultiWidgets.
    use_fieldset = False

    def get_context(self, name, value, attrs):
        context = super().get_context(name, value, attrs)
        subwidgets = context["widget"]["subwidgets"]
        if len(subwidgets) >= 2:
            subwidgets[1]["attrs"]["class"] = "form-select"
        return context
