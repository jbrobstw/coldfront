# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

from django.db.models import Q

from coldfront.users.models import User
from coldfront.users.providers import BaseDirectoryProvider, candidate_from_user


class LocalDirectoryProvider(BaseDirectoryProvider):
    """
    Default core provider that searches local Django User rows.
    """

    key = "local"
    verbose_name = "Local Users"
    supports_search = True
    supports_exact_lookup = True
    supports_provision = False

    def search(self, query, *, limit=20, request=None):
        if not query:
            return []

        qs = (
            User.objects.filter(
                Q(username__icontains=query)
                | Q(first_name__icontains=query)
                | Q(last_name__icontains=query)
                | Q(email__icontains=query)
            )
            .order_by("username")[:limit]
        )
        return [candidate_from_user(user) for user in qs]

    def get_user(self, *, username=None, external_id=None, request=None):
        if username:
            user = User.objects.filter(username__iexact=username).first()
            return candidate_from_user(user) if user else None

        if external_id:
            user = User.objects.filter(pk=external_id).first()
            return candidate_from_user(user) if user else None

        return None

    def provision(self, candidate, *, request=None):
        return None
