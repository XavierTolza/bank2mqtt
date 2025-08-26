"""
Configuration module for bank2mqtt application.
Handles all configuration settings including logging.
"""

import os
from pathlib import Path
from typing import Optional
from platformdirs import user_log_dir


class Config:
    """Configuration class for bank2mqtt application."""

    # Application info
    APP_NAME = "bank2mqtt"

    # Powens API configuration
    POWENS_DOMAIN = os.getenv("POWENS_DOMAIN")
    POWENS_CLIENT_ID = os.getenv("POWENS_CLIENT_ID")
    POWENS_CLIENT_SECRET = os.getenv("POWENS_CLIENT_SECRET")
    POWENS_CALLBACK_URL = os.getenv("POWENS_CALLBACK_URL")

    # Logging configuration
    LOG_LEVEL = os.getenv("BANK2MQTT_LOG_LEVEL", "INFO").upper()
    LOG_FILE = os.getenv("BANK2MQTT_LOG_FILE")
    LOG_DIR = os.getenv("BANK2MQTT_LOG_DIR")
    LOG_ROTATION = os.getenv("BANK2MQTT_LOG_ROTATION", "10 MB")
    LOG_RETENTION = os.getenv("BANK2MQTT_LOG_RETENTION", "1 month")
    LOG_FORMAT = os.getenv("BANK2MQTT_LOG_FORMAT", "default")
    LOG_COLORIZE = os.getenv("BANK2MQTT_LOG_COLORIZE", "true").lower() == "true"
    LOG_SERIALIZE = os.getenv("BANK2MQTT_LOG_SERIALIZE", "false").lower() == "true"

    @classmethod
    def get_log_dir(cls) -> Path:
        """Get the logging directory."""
        if cls.LOG_DIR:
            log_dir = Path(cls.LOG_DIR)
        else:
            log_dir = Path(user_log_dir(cls.APP_NAME))

        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir

    @classmethod
    def get_log_file_path(cls) -> Optional[Path]:
        """Get the full path to the log file."""
        if cls.LOG_FILE:
            if Path(cls.LOG_FILE).is_absolute():
                return Path(cls.LOG_FILE)
            else:
                return cls.get_log_dir() / cls.LOG_FILE
        else:
            return cls.get_log_dir() / f"{cls.APP_NAME}.log"

    @classmethod
    def get_log_format(cls) -> str:
        """Get the log format string."""
        formats = {
            "default": (
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                "<level>{message}</level>"
            ),
            "simple": (
                "<green>{time:HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<level>{message}</level>"
            ),
            "detailed": (
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "<magenta>PID:{process}</magenta> | "
                "<blue>Thread:{thread}</blue> - "
                "<level>{message}</level>"
            ),
            "json": (
                "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
                "{level} | "
                "{name}:{function}:{line} | "
                "{message}"
            ),
        }
        return formats.get(cls.LOG_FORMAT, formats["default"])

    @classmethod
    def validate_required_env_vars(cls) -> None:
        """Validate that required environment variables are set."""
        required_vars = ["POWENS_DOMAIN", "POWENS_CLIENT_ID", "POWENS_CLIENT_SECRET"]
        missing_vars = [var for var in required_vars if not getattr(cls, var)]

        if missing_vars:
            raise ValueError(
                f"Required environment variables are missing: {', '.join(missing_vars)}"
            )

    @classmethod
    def get_config_summary(cls) -> dict:
        """Get a summary of current configuration (without sensitive data)."""
        client_id_masked = (
            cls.POWENS_CLIENT_ID[:8] + "..." if cls.POWENS_CLIENT_ID else None
        )

        return {
            "app_name": cls.APP_NAME,
            "powens_domain": cls.POWENS_DOMAIN,
            "powens_client_id": client_id_masked,
            "powens_callback_url": cls.POWENS_CALLBACK_URL,
            "log_level": cls.LOG_LEVEL,
            "log_file": str(cls.get_log_file_path()),
            "log_dir": str(cls.get_log_dir()),
            "log_rotation": cls.LOG_ROTATION,
            "log_retention": cls.LOG_RETENTION,
            "log_format": cls.LOG_FORMAT,
            "log_colorize": cls.LOG_COLORIZE,
            "log_serialize": cls.LOG_SERIALIZE,
        }
