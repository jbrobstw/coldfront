# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from django.conf import settings
from django.core import signing
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.utils.translation import gettext_lazy as _

from coldfront.registry import get_directory_provider
from coldfront.users.models import User
from coldfront.users.providers import candidate_from_user

TOKEN_SALT = "coldfront.users.directory.candidate"


def _enabled_provider_keys():
    provider_order = list(getattr(settings, "DIRECTORY_PROVIDER_ORDER", ["local"]))

    if not getattr(settings, "DIRECTORY_PROVIDERS_ENABLED", False):
        return ["local"]

    if "local" not in provider_order:
        provider_order.insert(0, "local")

    return provider_order


def _get_provider_instance(key):
    provider_class = get_directory_provider(key)
    if provider_class is None:
        raise ImproperlyConfigured(
            _("Directory provider '{key}' is enabled in settings but not registered.").format(key=key)
        )
    return provider_class()


def get_active_directory_providers():
    return [_get_provider_instance(key) for key in _enabled_provider_keys()]


def _annotate_local_state(candidate):
    local_user = User.objects.filter(username__iexact=candidate.username).first()
    if local_user:
        candidate.exists_locally = True
        candidate.local_id = local_user.pk
        if candidate.provider == "local":
            candidate.display = str(local_user)
    return candidate.finalize()


def directory_search_users(query, *, limit=None, request=None):
    """
    Search local users plus any enabled external providers.

    Duplicate usernames are collapsed, preferring the local result when both
    a local and external candidate exist.
    """
    if not query:
        return []

    limit = limit or getattr(settings, "DIRECTORY_USER_SEARCH_LIMIT", 20)
    deduped = {}

    for provider in get_active_directory_providers():
        if not getattr(provider, "supports_search", True):
            continue

        for candidate in provider.search(query, limit=limit, request=request):
            candidate = _annotate_local_state(candidate)
            key = candidate.username.casefold()

            if key not in deduped:
                deduped[key] = candidate
                continue

            # Prefer the local candidate over an external candidate.
            if deduped[key].provider != "local" and candidate.provider == "local":
                deduped[key] = candidate

    results = list(deduped.values())
    return results[:limit]


def directory_get_user(*, username=None, external_id=None, provider=None, request=None):
    """
    Resolve one exact candidate from the configured providers.
    """
    if provider:
        candidate = _get_provider_instance(provider).get_user(
            username=username,
            external_id=external_id,
            request=request,
        )
        return _annotate_local_state(candidate) if candidate else None

    # Default behavior: local first, then enabled external providers.
    local_candidate = _get_provider_instance("local").get_user(username=username, external_id=external_id, request=request)
    if local_candidate:
        return _annotate_local_state(local_candidate)

    for provider_instance in get_active_directory_providers():
        if provider_instance.key == "local":
            continue
        if not getattr(provider_instance, "supports_exact_lookup", True):
            continue
        candidate = provider_instance.get_user(username=username, external_id=external_id, request=request)
        if candidate:
            return _annotate_local_state(candidate)

    return None


def provision_local_user_from_candidate(candidate, *, request=None):
    """
    Create a local Django User from an external directory candidate.

    Returns the existing or newly-created User, or None when provisioning is
    not allowed.
    """
    existing = User.objects.filter(username__iexact=candidate.username).first()
    if existing:
        return existing

    if candidate.provider == "local":
        return None

    if not getattr(settings, "DIRECTORY_USER_PROVISION_ENABLED", False):
        return None

    allowed = set(getattr(settings, "DIRECTORY_USER_PROVISION_ALLOWED_PROVIDERS", []))
    if candidate.provider not in allowed:
        return None

    provider_instance = _get_provider_instance(candidate.provider)
    if getattr(provider_instance, "supports_provision", False):
        provisioned = provider_instance.provision(candidate, request=request)
        if provisioned is not None:
            return provisioned

    user = User(
        username=candidate.username,
        first_name=candidate.first_name,
        last_name=candidate.last_name,
        email=candidate.email,
        is_active=True,
    )
    user.set_unusable_password()
    user.full_clean()
    user.save()
    return user


def candidate_to_token(candidate):
    payload = {
        "provider": candidate.provider,
        "username": candidate.username,
        "first_name": candidate.first_name,
        "last_name": candidate.last_name,
        "email": candidate.email,
        "external_id": candidate.external_id,
        "display": candidate.display,
        "exists_locally": candidate.exists_locally,
        "local_id": candidate.local_id,
    }
    return signing.dumps(payload, salt=TOKEN_SALT)


def candidate_label_from_token(token):
    try:
        payload = signing.loads(token, salt=TOKEN_SALT)
    except signing.BadSignature:
        return token

    if payload.get("display"):
        return payload["display"]

    full_name = f"{payload.get('first_name', '')} {payload.get('last_name', '')}".strip()
    username = payload.get("username", "")
    return f"{username} ({full_name})" if full_name else username


def user_to_candidate_token(user):
    return candidate_to_token(candidate_from_user(user))


def serialize_candidate(candidate):
    return {
        "id": candidate_to_token(candidate),
        "kind": "local" if candidate.provider == "local" else "directory",
        "username": candidate.username,
        "display": candidate.display,
        "first_name": candidate.first_name,
        "last_name": candidate.last_name,
        "email": candidate.email,
        "provider": candidate.provider,
        "source": candidate.source,
        "external_id": candidate.external_id,
        "exists_locally": candidate.exists_locally,
        "local_id": candidate.local_id,
    }


def resolve_user_from_candidate_token(token, *, request=None):
    """
    Resolve a signed candidate token to a local Django User.

    - local candidates resolve directly to an existing User
    - external candidates resolve to an existing local User when present
    - otherwise an exact provider lookup is performed and the user is
      provisioned locally if provisioning is enabled for that provider
    """
    try:
        payload = signing.loads(token, salt=TOKEN_SALT)
    except signing.BadSignature as exc:
        raise ValidationError(_("Invalid user selection.")) from exc

    local_id = payload.get("local_id")
    if local_id:
        try:
            return User.objects.get(pk=local_id)
        except User.DoesNotExist as exc:
            raise ValidationError(_("Selected local user no longer exists.")) from exc

    username = payload.get("username")
    provider = payload.get("provider")
    external_id = payload.get("external_id")

    existing = User.objects.filter(username__iexact=username).first()
    if existing:
        return existing

    candidate = directory_get_user(
        username=username,
        external_id=external_id,
        provider=provider,
        request=request,
    )
    if candidate is None:
        raise ValidationError(_("Selected directory user could not be resolved."))

    provisioned = provision_local_user_from_candidate(candidate, request=request)
    if provisioned is None:
        raise ValidationError(
            _(
                "User '{username}' has not logged in yet and directory provisioning is disabled for provider '{provider}'."
            ).format(username=username, provider=provider)
        )

    return provisioned
