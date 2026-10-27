---
description: "Require SPDX license headers on new source files (enforced by `uv run reuse lint` in CI)."
applyTo: "src/coldfront/**/*.py,tests/**/*.py,manage.py"
---

# SPDX License Headers

Every Python source file must begin with an SPDX header. CI runs `uv run reuse lint`
and fails if any file is missing copyright or license information.

## New files

Use the single-copyright form:

```python
# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0
```

## Rules

- The header must be the **very first thing** in the file, before any imports or code.
- Do **not** remove or alter the existing two-line copyright on files carried over from
  upstream (`# SPDX-FileCopyrightText: (C) DigitalOcean, LLC` followed by the University
  at Buffalo line) — preserve both lines when editing those files.
- Non-code files (`.md`, `.yml`, `.html`, `.css`, etc.) are covered by blanket annotations
  in `REUSE.toml`; do not add inline headers to them.
- When adding a new file type or a file that cannot carry a comment header, add an
  annotation entry to `REUSE.toml` instead.
- Verify compliance with `uv run reuse lint` before considering a change complete.
