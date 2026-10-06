# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

from django.apps import AppConfig


class UsersConfig(AppConfig):
    name = "coldfront.users"

    def ready(self):
        from coldfront.models.features import register_models
        from coldfront.registry import get_directory_provider, register_directory_provider
        from coldfront.users.providers_local import LocalDirectoryProvider

        # Register models
        register_models(*self.get_models())

        # Ensure the built-in local provider is always registered exactly once.
        if get_directory_provider(LocalDirectoryProvider.key) is None:
            register_directory_provider(LocalDirectoryProvider)

        from . import (
            signals,  # noqa: F401
        )
