# SPDX-FileCopyrightText: (C) University at Buffalo
#
# SPDX-License-Identifier: Apache-2.0

import logging

from django.test import TestCase
from django_tasks import task as task_decorator

from coldfront.core.choices import JobStatusChoices
from coldfront.core.jobs.runner import JobRunner
from coldfront.core.management.commands.db_worker import Worker
from coldfront.core.models import Job


def logging_task():
    """A bare task that logs through the ``coldfront`` logger and returns a value."""
    logging.getLogger("coldfront.tests.logging_task").info("bare task ran")
    return {"result": "ok"}


class ReturningRunner(JobRunner):
    """A JobRunner that logs and whose run() returns a value (ignored by handle)."""

    def run(self):
        self.logger.info("runner ran")
        return {"result": "runner"}


class FailingRunnerForWorker(JobRunner):
    """A JobRunner that fails so we can assert the worker preserves its status."""

    def run(self):
        from coldfront.core.exceptions import JobFailed

        raise JobFailed("controlled failure")


def _make_worker():
    return Worker(
        queue_names=["default"],
        interval=1.0,
        batch=True,
        startup_delay=False,
        max_tasks=None,
        worker_id="test-worker",
        excluded_queue_names=[],
    )


class RunJobBareTaskLogCaptureTestCase(TestCase):
    """A bare @task's log output should be captured into Job.log_entries."""

    def _enqueue_bare_task(self, job_id):
        task = task_decorator()(logging_task)
        job = Job.objects.create(
            name="logging_task",
            job_id=job_id,
            task_path=task.module_path,
            args_kwargs={"args": [], "kwargs": {}},
            status=JobStatusChoices.STATUS_PENDING,
        )
        return job

    def test_bare_task_logs_captured(self):
        job = self._enqueue_bare_task("11111111-1111-1111-1111-111111111111")
        _make_worker().run_job(job)
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_COMPLETED)
        self.assertTrue(job.log_entries)
        messages = [entry["message"] for entry in job.log_entries]
        self.assertIn("bare task ran", messages)

    def test_bare_task_return_value_stored_in_data(self):
        job = self._enqueue_bare_task("22222222-2222-2222-2222-222222222222")
        _make_worker().run_job(job)
        job.refresh_from_db()
        self.assertEqual(job.data, {"result": "ok"})

    def test_log_handler_removed_after_run(self):
        job = self._enqueue_bare_task("33333333-3333-3333-3333-333333333333")
        _make_worker().run_job(job)
        root = logging.getLogger("coldfront")
        from coldfront.core.jobs.runner import JobLogHandler

        self.assertFalse(any(isinstance(h, JobLogHandler) for h in root.handlers))


class RunJobRunnerFinalizationTestCase(TestCase):
    """A JobRunner must not be re-finalized by the worker after handle()."""

    def _enqueue_runner(self, job_id, runner_cls):
        runner_path = f"{runner_cls.__module__}.{runner_cls.__qualname__}"
        job = Job.objects.create(
            name=runner_cls.__name__,
            job_id=job_id,
            task_path=runner_path,
            args_kwargs={"args": [], "kwargs": {"_runner_cls_path": runner_path}},
            status=JobStatusChoices.STATUS_PENDING,
        )
        return job

    def test_runner_logs_preserved(self):
        job = self._enqueue_runner("44444444-4444-4444-4444-444444444444", ReturningRunner)
        _make_worker().run_job(job)
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_COMPLETED)
        messages = [entry["message"] for entry in job.log_entries]
        self.assertIn("runner ran", messages)

    def test_worker_does_not_overwrite_runner_status(self):
        job = self._enqueue_runner("55555555-5555-5555-5555-555555555555", FailingRunnerForWorker)
        _make_worker().run_job(job)
        job.refresh_from_db()
        # handle() set FAILED; the worker must not reset it to COMPLETED.
        self.assertEqual(job.status, JobStatusChoices.STATUS_FAILED)
