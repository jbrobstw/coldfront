# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

"""Tests for Slurm form fields."""

from django.core.exceptions import ValidationError
from django.test import SimpleTestCase

from coldfront.slurm.forms.fields import FairshareField


class FairshareFieldTestCase(SimpleTestCase):
    """FairshareField accepts a non-negative integer or ``parent`` (<-> -1)."""

    def setUp(self):
        self.field = FairshareField(required=False)

    def test_parent_cleans_to_minus_one(self):
        self.assertEqual(self.field.clean("parent"), -1)

    def test_parent_is_case_insensitive(self):
        self.assertEqual(self.field.clean("Parent"), -1)
        self.assertEqual(self.field.clean("PARENT"), -1)

    def test_positive_integer_passes_through(self):
        self.assertEqual(self.field.clean("100"), 100)

    def test_zero_is_allowed(self):
        self.assertEqual(self.field.clean("0"), 0)

    def test_empty_cleans_to_none(self):
        self.assertIsNone(self.field.clean(""))

    def test_minus_one_input_is_allowed(self):
        self.assertEqual(self.field.clean("-1"), -1)

    def test_value_below_minus_one_is_rejected(self):
        with self.assertRaises(ValidationError):
            self.field.clean("-2")

    def test_non_numeric_non_parent_is_rejected(self):
        with self.assertRaises(ValidationError):
            self.field.clean("banana")

    def test_prepare_value_renders_minus_one_as_parent(self):
        self.assertEqual(self.field.prepare_value(-1), "parent")

    def test_prepare_value_passes_through_other_values(self):
        self.assertEqual(self.field.prepare_value(100), 100)
        self.assertIsNone(self.field.prepare_value(None))
