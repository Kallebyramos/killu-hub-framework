"""
KilluHub scheduler.

Wraps APScheduler to run Pipeline jobs on a cron or interval schedule.
Each job is an independent pipeline run — the scheduler just calls
Pipeline(config).run() at the configured frequency.

Usage:
    scheduler = PipelineScheduler()
    scheduler.add_cron_job(
        job_id="orders_sync",
        config=my_pipeline_config,
        cron_expression="0 * * * *",  # every hour
    )
    scheduler.start()  # blocking
"""
import logging
from typing import Callable

from killuhub.core.config import PipelineConfig
from killuhub.ingestion.pipeline import Pipeline

logger = logging.getLogger(__name__)


class PipelineScheduler:
    """
    Schedule pipeline runs using APScheduler's BackgroundScheduler.

    Requires: pip install apscheduler
    """

    def __init__(self):
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
        except ImportError as e:
            raise ImportError(
                "apscheduler is required for PipelineScheduler. "
                "Install with: pip install apscheduler"
            ) from e

        from apscheduler.schedulers.background import BackgroundScheduler
        self._scheduler = BackgroundScheduler()
        self._jobs: dict[str, PipelineConfig] = {}

    # ------------------------------------------------------------------
    # Job registration
    # ------------------------------------------------------------------

    def add_cron_job(
        self,
        job_id: str,
        config: PipelineConfig,
        cron_expression: str,
    ) -> None:
        """
        Schedule a pipeline to run on a cron expression.

        Args:
            job_id:          Unique identifier for this job.
            config:          PipelineConfig for the pipeline.
            cron_expression: Standard 5-field cron (e.g. "0 * * * *").
        """
        minute, hour, day, month, day_of_week = cron_expression.split()
        self._jobs[job_id] = config
        self._scheduler.add_job(
            func=self._run_job,
            trigger="cron",
            id=job_id,
            kwargs={"job_id": job_id},
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            replace_existing=True,
        )
        logger.info("Scheduled cron job '%s' | cron=%s", job_id, cron_expression)

    def add_interval_job(
        self,
        job_id: str,
        config: PipelineConfig,
        seconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
    ) -> None:
        """Schedule a pipeline to run on a fixed interval."""
        self._jobs[job_id] = config
        self._scheduler.add_job(
            func=self._run_job,
            trigger="interval",
            id=job_id,
            kwargs={"job_id": job_id},
            seconds=seconds,
            minutes=minutes,
            hours=hours,
            replace_existing=True,
        )
        logger.info(
            "Scheduled interval job '%s' | every %dh %dm %ds",
            job_id, hours, minutes, seconds,
        )

    def remove_job(self, job_id: str) -> None:
        self._scheduler.remove_job(job_id)
        self._jobs.pop(job_id, None)

    def list_jobs(self) -> list[str]:
        return list(self._jobs)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, blocking: bool = True) -> None:
        """
        Start the scheduler.

        Args:
            blocking: If True (default), block the main thread until
                      KeyboardInterrupt. Set to False when embedding the
                      scheduler in a larger application.
        """
        self._scheduler.start()
        logger.info("Scheduler started with %d job(s).", len(self._jobs))

        if blocking:
            try:
                import time
                while True:
                    time.sleep(1)
            except (KeyboardInterrupt, SystemExit):
                self.stop()

    def stop(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown()
            logger.info("Scheduler stopped.")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run_job(self, job_id: str) -> None:
        config = self._jobs.get(job_id)
        if config is None:
            logger.warning("Job '%s' not found — skipping.", job_id)
            return
        logger.info("Running scheduled job '%s'", job_id)
        try:
            summary = Pipeline(config).run()
            logger.info("Job '%s' complete | %s", job_id, summary)
        except Exception:
            logger.exception("Job '%s' failed.", job_id)
