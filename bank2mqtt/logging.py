"""
Logging module for bank2mqtt application using Loguru.
Provides centralized logging configuration and utilities.
"""

import sys
from typing import Optional

from loguru import logger

from bank2mqtt.config import Config


class LoggingManager:
    """Manages logging configuration for the application."""

    _initialized = False

    @classmethod
    def setup_logging(
        cls,
        console_output: bool = True,
        file_output: bool = True,
        force_reinit: bool = False,
    ) -> None:
        """
        Setup and configure logging for the application.

        Args:
            console_output: Whether to enable console logging
            file_output: Whether to enable file logging
            force_reinit: Force re-initialization even if already done
        """
        if cls._initialized and not force_reinit:
            return

        # Remove default handler if present
        logger.remove()

        # Setup console logging
        if console_output:
            cls._setup_console_logging()

        # Setup file logging
        if file_output:
            cls._setup_file_logging()

        cls._initialized = True
        logger.info("Logging system initialized")
        logger.debug(f"Configuration: {Config.get_config_summary()}")

    @classmethod
    def _setup_console_logging(cls) -> None:
        """Setup console logging handler."""
        log_format = Config.get_log_format()

        # For JSON format, disable colorization
        colorize = Config.LOG_COLORIZE and Config.LOG_FORMAT != "json"

        logger.add(
            sys.stderr,
            format=log_format,
            level=Config.LOG_LEVEL,
            colorize=colorize,
            serialize=Config.LOG_SERIALIZE and Config.LOG_FORMAT == "json",
            backtrace=True,
            diagnose=True,
        )

        logger.debug("Console logging handler configured")

    @classmethod
    def _setup_file_logging(cls) -> None:
        """Setup file logging handler."""
        log_file_path = Config.get_log_file_path()

        if not log_file_path:
            logger.warning("No log file path configured, skipping file logging")
            return

        # Ensure log directory exists
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Use simple format for file logging to avoid color codes
        file_format = (
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
            "{level: <8} | "
            "{name}:{function}:{line} | "
            "{message}"
        )

        if Config.LOG_FORMAT == "json":
            file_format = Config.get_log_format()

        logger.add(
            str(log_file_path),
            format=file_format,
            level=Config.LOG_LEVEL,
            rotation=Config.LOG_ROTATION,
            retention=Config.LOG_RETENTION,
            serialize=Config.LOG_SERIALIZE and Config.LOG_FORMAT == "json",
            backtrace=True,
            diagnose=True,
            enqueue=True,  # Thread-safe logging
        )

        logger.debug(f"File logging handler configured: {log_file_path}")

    @classmethod
    def get_logger(cls, name: Optional[str] = None) -> "logger":
        """
        Get a logger instance for the given name.

        Args:
            name: Logger name (usually __name__)

        Returns:
            Configured logger instance
        """
        if not cls._initialized:
            cls.setup_logging()

        if name:
            return logger.bind(name=name)
        return logger

    @classmethod
    def shutdown(cls) -> None:
        """Shutdown the logging system."""
        logger.info("Shutting down logging system")
        logger.remove()
        cls._initialized = False


# Convenience function for getting logger
def get_logger(name: Optional[str] = None) -> "logger":
    """
    Get a configured logger instance.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured logger instance
    """
    return LoggingManager.get_logger(name)


# Auto-initialize logging when module is imported
def init_logging():
    """Initialize logging system."""
    LoggingManager.setup_logging()


# Function to setup logging with custom parameters
def setup_logging(
    console_output: bool = True, file_output: bool = True, force_reinit: bool = False
) -> None:
    """
    Setup logging with custom parameters.

    Args:
        console_output: Whether to enable console logging
        file_output: Whether to enable file logging
        force_reinit: Force re-initialization even if already done
    """
    LoggingManager.setup_logging(console_output, file_output, force_reinit)


# Context manager for temporary log level changes
class LogLevel:
    """Context manager for temporarily changing log level."""

    def __init__(self, level: str):
        self.level = level
        self.original_level = None

    def __enter__(self):
        self.original_level = logger._core.min_level
        logger.remove()
        LoggingManager.setup_logging(force_reinit=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.original_level is not None:
            logger.remove()
            LoggingManager.setup_logging(force_reinit=True)


# Exception logging decorator
def log_exceptions(logger_instance=None):
    """
    Decorator to automatically log exceptions.

    Args:
        logger_instance: Logger to use (defaults to module logger)
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            log = logger_instance or get_logger(func.__module__)
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log.exception(f"Exception in {func.__name__}: {e}", exc_info=True)
                raise

        return wrapper

    return decorator
