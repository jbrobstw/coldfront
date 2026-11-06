# SPDX-FileCopyrightText: (C) DigitalOcean, LLC
# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0


from coldfront.users.querysets import RestrictedQuerySet


def add_blank_choice(choices):
    """
    Add a blank choice to the beginning of a choices list.
    """
    return ((None, "---------"),) + tuple(choices)


def restrict_form_fields(form, user, action="view"):
    """
    Restrict all form fields which reference a RestrictedQuerySet. This ensures that users see only permitted objects
    as available choices.

    When editing an existing object, the object's current value for a restricted field is always kept selectable even
    if the user lacks permission to view it. Otherwise a required field would appear unset and block saving.
    """
    instance = getattr(form, "instance", None)
    for name, field in form.fields.items():
        if hasattr(field, "queryset") and issubclass(field.queryset.__class__, RestrictedQuerySet):
            field.queryset = field.queryset.restrict(user, action)
            _preserve_current_choices(field, name, instance)


def _preserve_current_choices(field, name, instance):
    """
    Re-add the current value(s) of a restricted field, taken from the form's instance, to the field's queryset so an
    existing selection the user cannot otherwise view remains a valid choice.
    """
    if instance is None or not getattr(instance, "pk", None):
        return

    model = field.queryset.model
    try:
        current = getattr(instance, name)
    except AttributeError:
        return

    if hasattr(current, "all"):
        current_pks = list(current.values_list("pk", flat=True))
    elif current is not None:
        current_pks = [current.pk]
    else:
        current_pks = []

    if current_pks:
        field.queryset = (field.queryset | model.objects.filter(pk__in=current_pks)).distinct()


def get_field_value(form, field_name):
    """
    Return the current bound or initial value associated with a form field, prior to calling
    clean() for the form.
    """
    field = form.fields[field_name]

    if form.is_bound and field_name in form.data:
        if (value := form.data[field_name]) is None:
            return None
        if hasattr(field, "valid_value") and field.valid_value(value):
            return value

    return form.get_initial_for_field(field, field_name)
