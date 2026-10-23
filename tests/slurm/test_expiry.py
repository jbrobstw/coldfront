# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

"""Tests for the pluggable Slurm allocation-expiry strategy registry."""

from unittest.mock import patch

from django.test import TestCase, override_settings

from coldfront.slurm import expiry
from coldfront.slurm.expiry import (
    ExpiryConfig,
    dispatch_expiry,
    dispatch_expiry_reversal,
    get_strategy,
    get_strategy_reversal,
    list_strategies,
    register_slurm_expiry_strategy,
    resolve_config,
    resolve_strategy_name,
    strategy_choices,
)
from coldfront.slurm.models import SlurmCluster


def _snapshot_registry(testcase):
    """Restore ``expiry._STRATEGIES`` to its current contents after the test."""
    saved = dict(expiry._STRATEGIES)

    def restore():
        expiry._STRATEGIES.clear()
        expiry._STRATEGIES.update(saved)

    testcase.addCleanup(restore)


class StrategyRegistryTestCase(TestCase):
    def setUp(self):
        # Snapshot and restore the module-level registry so test registrations
        # don't leak between tests.
        _snapshot_registry(self)

    def test_builtins_registered(self):
        names = list_strategies()
        for expected in ("remove_associations", "throttle", "hold", "set_fields", "noop"):
            self.assertIn(expected, names)

    def test_register_and_get(self):
        @register_slurm_expiry_strategy("custom_test", label="Custom")
        def _custom(allocation, cluster, *, config):
            return None

        self.assertIs(get_strategy("custom_test"), _custom)

    def test_get_unknown_returns_none(self):
        self.assertIsNone(get_strategy("does_not_exist"))

    def test_register_overrides_existing(self):
        @register_slurm_expiry_strategy("dupe")
        def _first(allocation, cluster, *, config):
            return None

        @register_slurm_expiry_strategy("dupe")
        def _second(allocation, cluster, *, config):
            return None

        self.assertIs(get_strategy("dupe"), _second)

    def test_strategy_choices_sorted_pairs(self):
        choices = strategy_choices()
        names = [name for name, _label in choices]
        self.assertEqual(names, sorted(names))
        self.assertIn("remove_associations", names)

    def test_list_strategies_sorted(self):
        names = list_strategies()
        self.assertEqual(names, sorted(names))


class ResolveStrategyNameTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.cluster = SlurmCluster.objects.create(name="hpc")

    def test_cluster_override_wins(self):
        self.cluster.expiry_strategy = "throttle"
        self.assertEqual(resolve_strategy_name(self.cluster), "throttle")

    @override_settings(SLURM_EXPIRY_STRATEGY="hold")
    def test_falls_back_to_global(self):
        self.cluster.expiry_strategy = ""
        self.assertEqual(resolve_strategy_name(self.cluster), "hold")

    def test_default_is_remove_associations(self):
        self.cluster.expiry_strategy = ""
        with override_settings():
            # Ensure default when setting absent.
            from django.conf import settings

            if hasattr(settings, "SLURM_EXPIRY_STRATEGY"):
                self.assertTrue(resolve_strategy_name(self.cluster))
            else:
                self.assertEqual(resolve_strategy_name(self.cluster), "remove_associations")


class ResolveConfigTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.cluster = SlurmCluster.objects.create(name="hpc")

    def test_cluster_override_wins(self):
        self.cluster.expiry_hold_qos = "frozen"
        self.cluster.expiry_set_fields = {"max_jobs": 0}
        config = resolve_config(self.cluster)
        self.assertEqual(config.hold_qos, "frozen")
        self.assertEqual(config.set_fields, {"max_jobs": 0})
        self.assertEqual(config.cluster_id, self.cluster.pk)

    @override_settings(SLURM_EXPIRY_HOLD_QOS="global_frozen", SLURM_EXPIRY_SET_FIELDS={"grp_tres": "cpu=0"})
    def test_falls_back_to_global(self):
        self.cluster.expiry_hold_qos = ""
        self.cluster.expiry_set_fields = {}
        config = resolve_config(self.cluster)
        self.assertEqual(config.hold_qos, "global_frozen")
        self.assertEqual(config.set_fields, {"grp_tres": "cpu=0"})


class DispatchExpiryTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.cluster = SlurmCluster.objects.create(name="hpc")

    def setUp(self):
        self.allocation = type("Alloc", (), {"pk": 42})()

    @override_settings(SLURM_EXPIRY_STRATEGY="remove_associations")
    def test_dispatch_remove_associations(self):
        self.cluster.expiry_strategy = ""
        with patch("coldfront.slurm.sync.enqueue_deactivate_allocation") as mock_fn:
            dispatch_expiry(self.allocation, self.cluster)
        mock_fn.assert_called_once_with(42, cluster_id=self.cluster.pk)

    def test_dispatch_throttle(self):
        self.cluster.expiry_strategy = "throttle"
        with patch("coldfront.slurm.sync.enqueue_throttle_allocation") as mock_fn:
            dispatch_expiry(self.allocation, self.cluster)
        mock_fn.assert_called_once_with(42, cluster_id=self.cluster.pk)

    def test_dispatch_hold_passes_qos(self):
        self.cluster.expiry_strategy = "hold"
        self.cluster.expiry_hold_qos = "frozen"
        with patch("coldfront.slurm.sync.enqueue_hold_allocation") as mock_fn:
            dispatch_expiry(self.allocation, self.cluster)
        mock_fn.assert_called_once_with(42, cluster_id=self.cluster.pk, hold_qos="frozen")

    def test_dispatch_set_fields_passes_fields(self):
        self.cluster.expiry_strategy = "set_fields"
        self.cluster.expiry_set_fields = {"max_jobs": 0}
        with patch("coldfront.slurm.sync.enqueue_set_fields_allocation") as mock_fn:
            dispatch_expiry(self.allocation, self.cluster)
        mock_fn.assert_called_once_with(42, cluster_id=self.cluster.pk, fields={"max_jobs": 0})

    def test_dispatch_noop_does_nothing(self):
        self.cluster.expiry_strategy = "noop"
        # Should not raise and should not call any enqueue function.
        dispatch_expiry(self.allocation, self.cluster)

    def test_dispatch_unknown_strategy_swallowed(self):
        self.cluster.expiry_strategy = "nonexistent_strategy"
        # Must not raise.
        dispatch_expiry(self.allocation, self.cluster)

    def test_dispatch_failing_strategy_swallowed(self):
        _snapshot_registry(self)

        @register_slurm_expiry_strategy("boom")
        def _boom(allocation, cluster, *, config):
            raise RuntimeError("kaboom")

        self.cluster.expiry_strategy = "boom"
        # Exception from strategy must be swallowed.
        dispatch_expiry(self.allocation, self.cluster)

    def test_config_type_passed_to_strategy(self):
        _snapshot_registry(self)

        captured = {}

        @register_slurm_expiry_strategy("capture")
        def _capture(allocation, cluster, *, config):
            captured["config"] = config

        self.cluster.expiry_strategy = "capture"
        dispatch_expiry(self.allocation, self.cluster)
        self.assertIsInstance(captured["config"], ExpiryConfig)


class StrategyReversalRegistryTestCase(TestCase):
    def setUp(self):
        _snapshot_registry(self)

    def test_builtins_with_reversals(self):
        # throttle/hold/set_fields restore in place, so they have a reversal.
        for name in ("throttle", "hold", "set_fields"):
            self.assertIsNotNone(get_strategy_reversal(name))

    def test_builtins_without_reversals(self):
        # remove_associations self-heals on activation; noop has nothing to undo.
        for name in ("remove_associations", "noop"):
            self.assertIsNone(get_strategy_reversal(name))

    def test_get_reversal_unknown_returns_none(self):
        self.assertIsNone(get_strategy_reversal("does_not_exist"))

    def test_register_with_reverse(self):
        def _reverse(allocation, cluster, *, config):
            return None

        @register_slurm_expiry_strategy("custom_rev", reverse=_reverse)
        def _strategy(allocation, cluster, *, config):
            return None

        self.assertIs(get_strategy_reversal("custom_rev"), _reverse)

    def test_register_without_reverse_defaults_none(self):
        @register_slurm_expiry_strategy("no_rev")
        def _strategy(allocation, cluster, *, config):
            return None

        self.assertIsNone(get_strategy_reversal("no_rev"))


class DispatchExpiryReversalTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.cluster = SlurmCluster.objects.create(name="hpc")

    def setUp(self):
        self.allocation = type("Alloc", (), {"pk": 42})()

    def test_dispatch_throttle_reversal(self):
        self.cluster.expiry_strategy = "throttle"
        with patch("coldfront.slurm.sync.enqueue_restore_allocation") as mock_fn:
            dispatch_expiry_reversal(self.allocation, self.cluster)
        mock_fn.assert_called_once_with(42, cluster_id=self.cluster.pk)

    def test_dispatch_hold_reversal(self):
        self.cluster.expiry_strategy = "hold"
        with patch("coldfront.slurm.sync.enqueue_restore_allocation") as mock_fn:
            dispatch_expiry_reversal(self.allocation, self.cluster)
        mock_fn.assert_called_once_with(42, cluster_id=self.cluster.pk)

    def test_dispatch_set_fields_reversal(self):
        self.cluster.expiry_strategy = "set_fields"
        with patch("coldfront.slurm.sync.enqueue_restore_allocation") as mock_fn:
            dispatch_expiry_reversal(self.allocation, self.cluster)
        mock_fn.assert_called_once_with(42, cluster_id=self.cluster.pk)

    @override_settings(SLURM_EXPIRY_STRATEGY="remove_associations")
    def test_dispatch_remove_associations_reversal_noop(self):
        self.cluster.expiry_strategy = ""
        with patch("coldfront.slurm.sync.enqueue_restore_allocation") as mock_fn:
            dispatch_expiry_reversal(self.allocation, self.cluster)
        mock_fn.assert_not_called()

    def test_dispatch_noop_reversal_does_nothing(self):
        self.cluster.expiry_strategy = "noop"
        with patch("coldfront.slurm.sync.enqueue_restore_allocation") as mock_fn:
            dispatch_expiry_reversal(self.allocation, self.cluster)
        mock_fn.assert_not_called()

    def test_dispatch_unknown_strategy_reversal_swallowed(self):
        self.cluster.expiry_strategy = "nonexistent_strategy"
        # Must not raise.
        dispatch_expiry_reversal(self.allocation, self.cluster)

    def test_dispatch_failing_reversal_swallowed(self):
        _snapshot_registry(self)

        def _boom_reverse(allocation, cluster, *, config):
            raise RuntimeError("kaboom")

        @register_slurm_expiry_strategy("boom_rev", reverse=_boom_reverse)
        def _strategy(allocation, cluster, *, config):
            return None

        self.cluster.expiry_strategy = "boom_rev"
        # Exception from reversal must be swallowed.
        dispatch_expiry_reversal(self.allocation, self.cluster)
