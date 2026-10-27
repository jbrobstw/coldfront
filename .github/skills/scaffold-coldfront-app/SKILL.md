---
name: scaffold-coldfront-app
description: "Use when creating a new ColdFront Django app under src/coldfront/. Scaffolds the standard package layout (models, views, forms, tables, filtersets, API, urls), registers the app, and creates matching templates and tests so it is CI-passing and consistent with existing apps like slurm and storage."
---

# Scaffold a New ColdFront App

Follow this when asked to add a new app (feature area) to ColdFront. Use
`src/coldfront/storage/` or `src/coldfront/slurm/` as reference implementations —
read them before generating code.

## 1. Create the package layout

```
src/coldfront/<app>/
├── __init__.py
├── apps.py                      # AppConfig.ready() MUST call register_models()
├── models.py                    # subclass PrimaryModel / OrganizationalModel / etc.
├── views.py                     # @register_model_view decorated CRUD views
├── forms/__init__.py
├── forms/model_forms.py         # model forms + CSV import forms
├── forms/filterset_forms.py     # filter forms for list views
├── tables.py                    # django-tables2 tables
├── filtersets.py                # django-filter filter sets
├── urls.py                      # URL routing
├── api/__init__.py
├── api/serializers/__init__.py
├── api/serializers/<model>.py   # DRF serializers (PrimaryModelSerializer)
├── api/serializers/nested.py    # nested serializers — MUST set Meta.brief_fields
├── api/urls.py
├── api/views.py                 # DRF viewsets (ColdFrontModelViewSet)
```

Every `.py` file starts with the SPDX header (see the `spdx-headers` instructions):

```python
# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0
```

## 2. AppConfig (critical)

`apps.py` `ready()` **must** call `register_models(*self.get_models())` and import
`views` so decorators run. Without this: no changelog views, no object types, and
`NoReverseMatch` errors.

```python
class <App>Config(AppConfig):
    name = "coldfront.<app>"
    label = "<app>"
    verbose_name = _("<Human Name>")

    def ready(self):
        from coldfront.models.features import register_models

        register_models(*self.get_models())

        from . import views  # noqa: F401
```

## 3. Models

Derive from the appropriate base in `src/coldfront/models/base.py`
(`PrimaryModel`, `OrganizationalModel`, `NestedGroupModel`). Opt into features via
mixins from `src/coldfront/models/features.py`. For FKs to groups, use
`from coldfront.users.models import Group` (NOT Django's auth Group).

## 4. Register the app

- Add to `INSTALLED_APPS` in `src/coldfront/config/base.py`
- Add app URLs and API URL patterns to `src/coldfront/config/urls.py`
- Add navigation items in `src/coldfront/navigation/menu.py` via `get_model_item()`

## 5. Templates (do NOT put them in the app package)

All templates live under `src/coldfront/templates/`, never `src/coldfront/<app>/templates/`.
At minimum create:

- `src/coldfront/templates/<app>/base.html` — extends `generic/base.html`
- `src/coldfront/templates/<app>/<model>.html` — detail view (required for view tests)

## 6. Tests

Create `tests/<app>/` mirroring other apps (`test_views.py`, `test_api.py`,
`test_filtersets.py`, `test_tables.py`, `test_forms.py`). See the `tests` instructions
for pitfalls (FK PKs in `create_data`, `brief_fields`, tenancy `form_data`, CSV
`to_field_name`).

## 7. Validate (all must pass)

```bash
uv run coldfront makemigrations --check   # generate & commit migrations if needed
uv run ruff check && uv run ruff format --check
uv run djangofmt src/coldfront/           # commit any reformatted templates
uv run reuse lint
COLDFRONT_ENV=.env.testing uv run -m pytest tests/<app>/
```
