---
description: "Conventions and known pitfalls for writing ColdFront tests under tests/."
applyTo: "tests/**/*.py"
---

# ColdFront Test Conventions

Tests run with **pytest** (not Django's test runner). Always set the test env file:

```bash
COLDFRONT_ENV=.env.testing uv run -m pytest tests/<app>/test_views.py
```

`DJANGO_SETTINGS_MODULE` comes from the `[tool.pytest.ini_options]` table in
`pyproject.toml`. `COLDFRONT_ENV=.env.testing` still supplies the test secrets and the
test `PLUGINS` list, so always set it when running the suite.

## Layout

Mirror the app package under `tests/<app>/`:

```
tests/<app>/
├── __init__.py
├── test_views.py      # ViewTestCases.PrimaryObjectViewTestCase
├── test_api.py        # APIViewTestCases.APIViewTestCase
├── test_filtersets.py
├── test_tables.py
├── test_forms.py
```

## Known pitfalls

1. **API tests with FK fields need real PKs in `create_data`.** Pre-create related
   objects in `setUpTestData` and reference their actual PKs; do not hard-code `1`.
2. **Nested serializers require `brief_fields` in `Meta`.** `get_prefetches_for_serializer()`
   reads `Meta.brief_fields` when `nested=True`; omitting it breaks API tests.
3. **Detail templates are required for view tests.** A missing detail template renders
   Django's 500 handler. Ensure `src/coldfront/templates/<app>/base.html` and
   `.../<model>.html` exist.
4. **CSV/import test data uses `to_field_name` values, not PKs.** A
   `CSVModelChoiceField(to_field_name="name")` matches on the name string — use
   `"Test Cluster"`, not `"1"`.
5. **Tenancy-aware models:** `form_data` must include `"tenant_group": None` and
   `"tenant": None`.

## Coverage

Every non-trivial function should have at least as many tests as its cyclomatic
complexity. The CI command is `COLDFRONT_ENV=.env.testing uv run -m coverage run -m pytest`.
