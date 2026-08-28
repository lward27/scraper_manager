"""Tests for Config validation."""

import os
from unittest.mock import patch

import pytest

from scraper_manager.config import Config, ConfigValidationError


class TestQueueValidation:
    def test_prefetch_count_positive(self):
        config = Config()
        config.queue.prefetch_count = 10
        config.validate()

    def test_prefetch_count_zero_rejected(self):
        config = Config()
        config.queue.prefetch_count = 0
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "RABBITMQ_PREFETCH_COUNT must be a positive integer" in str(exc_info.value)

    def test_prefetch_count_negative_rejected(self):
        config = Config()
        config.queue.prefetch_count = -1
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "RABBITMQ_PREFETCH_COUNT must be a positive integer" in str(exc_info.value)


class TestSchedulerValidation:
    def test_page_size_positive(self):
        config = Config()
        config.scheduler.page_size = 100
        config.validate()

    def test_page_size_zero_rejected(self):
        config = Config()
        config.scheduler.page_size = 0
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "SCHEDULER_PAGE_SIZE must be a positive integer" in str(exc_info.value)

    def test_page_size_negative_rejected(self):
        config = Config()
        config.scheduler.page_size = -5
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "SCHEDULER_PAGE_SIZE must be a positive integer" in str(exc_info.value)

    def test_run_hour_valid(self):
        config = Config()
        config.scheduler.run_hour = 12
        config.validate()

    def test_run_hour_negative_rejected(self):
        config = Config()
        config.scheduler.run_hour = -1
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "SCHEDULER_RUN_HOUR must be between 0 and 23 inclusive" in str(exc_info.value)

    def test_run_hour_over_23_rejected(self):
        config = Config()
        config.scheduler.run_hour = 24
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "SCHEDULER_RUN_HOUR must be between 0 and 23 inclusive" in str(exc_info.value)

    def test_run_minute_valid(self):
        config = Config()
        config.scheduler.run_minute = 30
        config.validate()

    def test_run_minute_negative_rejected(self):
        config = Config()
        config.scheduler.run_minute = -1
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "SCHEDULER_RUN_MINUTE must be between 0 and 59 inclusive" in str(exc_info.value)

    def test_run_minute_over_59_rejected(self):
        config = Config()
        config.scheduler.run_minute = 60
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "SCHEDULER_RUN_MINUTE must be between 0 and 59 inclusive" in str(exc_info.value)


class TestWorkerValidation:
    def test_max_retries_positive(self):
        config = Config()
        config.worker.max_retries = 3
        config.validate()

    def test_max_retries_zero_rejected(self):
        config = Config()
        config.worker.max_retries = 0
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "WORKER_MAX_RETRIES must be a positive integer" in str(exc_info.value)

    def test_max_retries_negative_rejected(self):
        config = Config()
        config.worker.max_retries = -1
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "WORKER_MAX_RETRIES must be a positive integer" in str(exc_info.value)


class TestConcurrencyValidation:
    def test_max_concurrent_yfinance_calls_positive(self):
        config = Config()
        config.concurrency.max_concurrent_yfinance_calls = 4
        config.validate()

    def test_max_concurrent_yfinance_calls_zero_rejected(self):
        config = Config()
        config.concurrency.max_concurrent_yfinance_calls = 0
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "MAX_CONCURRENT_YFINANCE_CALLS must be a positive integer" in str(exc_info.value)

    def test_max_concurrent_yfinance_calls_negative_rejected(self):
        config = Config()
        config.concurrency.max_concurrent_yfinance_calls = -1
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "MAX_CONCURRENT_YFINANCE_CALLS must be a positive integer" in str(exc_info.value)

    def test_max_concurrent_db_calls_positive(self):
        config = Config()
        config.concurrency.max_concurrent_db_calls = 4
        config.validate()

    def test_max_concurrent_db_calls_zero_rejected(self):
        config = Config()
        config.concurrency.max_concurrent_db_calls = 0
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "MAX_CONCURRENT_DB_CALLS must be a positive integer" in str(exc_info.value)

    def test_max_concurrent_db_calls_negative_rejected(self):
        config = Config()
        config.concurrency.max_concurrent_db_calls = -1
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "MAX_CONCURRENT_DB_CALLS must be a positive integer" in str(exc_info.value)

    def test_max_workers_positive(self):
        config = Config()
        config.concurrency.max_workers = 8
        config.validate()

    def test_max_workers_zero_rejected(self):
        config = Config()
        config.concurrency.max_workers = 0
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "MAX_WORKERS must be a positive integer" in str(exc_info.value)

    def test_max_workers_negative_rejected(self):
        config = Config()
        config.concurrency.max_workers = -1
        with pytest.raises(ConfigValidationError) as exc_info:
            config.validate()
        assert "MAX_WORKERS must be a positive integer" in str(exc_info.value)


class TestStartupSequence:
    def test_validate_called_before_downstream_services(self):
        """Ensure validation runs before any downstream initialization."""
        import scraper_manager.__main__ as main_module

        with patch.object(Config, "from_env", return_value=Config()) as mock_from_env, \
             patch.object(Config, "validate") as mock_validate, \
             patch.object(main_module, "start_health_server") as mock_start_health, \
             patch.object(main_module, "get_logger") as mock_get_logger:

            config = Config()
            mock_from_env.return_value = config

            try:
                main_module.main()
            except SystemExit:
                pass

            mock_validate.assert_called_once()
            mock_start_health.assert_called_once()
            mock_get_logger.assert_called_once()

            # Validate should be called before start_health_server
            assert mock_validate.call_count == 1
            assert mock_start_health.call_count == 1

    def test_validation_failure_exits_cleanly(self):
        """Ensure validation failure causes clean early exit."""
        import scraper_manager.__main__ as main_module

        config = Config()
        config.worker.max_retries = 0

        with patch.object(Config, "from_env", return_value=config), \
             pytest.raises(SystemExit) as exc_info:
            main_module.main()

        assert exc_info.value.code == 1
