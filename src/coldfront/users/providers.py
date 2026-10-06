# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DirectoryUserCandidate:
    """
    A normalized user candidate returned by a directory provider.
    """

    username: str
    first_name: str = ""
    last_name: str = ""
    email: str = ""
    source: str = "local"
    external_id: str = ""
    display: str = ""
    exists_locally: bool = False
    provider: str = "local"
    local_id: int | None = None

    def finalize(self):
        """Populate any derived presentation fields."""
        if not self.display:
            full_name = f"{self.first_name} {self.last_name}".strip()
            self.display = f"{self.username} ({full_name})" if full_name else self.username
        if not self.source:
            self.source = self.provider
        return self


class BaseDirectoryProvider:
    """
    Base class for pluggable directory providers.
    """

    key = "base"
    verbose_name = "Base Provider"
    supports_search = True
    supports_exact_lookup = True
    supports_provision = False

    def search(self, query, *, limit=20, request=None):
        raise NotImplementedError

    def get_user(self, *, username=None, external_id=None, request=None):
        raise NotImplementedError

    def provision(self, candidate, *, request=None):
        raise NotImplementedError


def candidate_from_user(user):
    """Build a normalized candidate from a local Django User instance."""
    candidate = DirectoryUserCandidate(
        username=user.username,
        first_name=user.first_name or "",
        last_name=user.last_name or "",
        email=user.email or "",
        source="local",
        external_id=str(user.pk),
        display=str(user),
        exists_locally=True,
        provider="local",
        local_id=user.pk,
    )
    return candidate.finalize()
