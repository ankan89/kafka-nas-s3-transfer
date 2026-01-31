"""
Centralized logging configuration using scm_utilities CDP logging.
Singleton pattern for consistent logging across the application.
"""

import logging
import os
import sys
from scm_utilities.CDPLogging import logger_pyth
from scm_utilities.CDPLogging.logger_pyth import LogLevel
from scm_utilities.Constant.technical import Status

from src.config_loader import ConfigLoader

# Create a standard logger for fallback
app_logger = logging.getLogger("nas-watcher")
app_logger.setLevel(logging.INFO)

# Create a handler
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Add the handler to the logger
app_logger.addHandler(handler)

# Set AWS checksum environment variables
os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "when_required"
os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required"


class CdpLogger:
    """
    Singleton CDP Logger for centralized logging.
    Sends logs to Dell's central logging API.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        try:
            config_props = ConfigLoader()
            logging_url = config_props.get("logging", {}).get("url", "")
            dp_name = config_props.get("logging", {}).get("dp_name", "DP03_Production")
            pod_name = config_props.get("logging", {}).get("pod_name", "DP03_Production_Nas_Watcher")
            domain = config_props.get("logging", {}).get("domain", "MAKE")
            sub_domain = config_props.get("logging", {}).get("sub_domain", "MAKE")
            app_id = config_props.get("logging", {}).get("app_id", "1007201")
            platform = config_props.get("logging", {}).get("platform", "KOB")

            # Initialize CDP Logger
            self.logger_app = logger_pyth.LoggerApp(
                log_level=LogLevel.info,
                loggerAPI=logging_url,
                cert_file_path="./src/logging.crt"
            )

            # Set context
            self.logger_app.context["Domain"] = domain
            self.logger_app.context["Subdomain"] = sub_domain
            self.logger_app.context["ApplicationId"] = ""
            self.logger_app.context["ApplicationName"] = dp_name
            self.logger_app.context["Platformsource"] = "KOB"
            self.logger_app.context["Sourcetype"] = "NAS"
            self.logger_app.context["cmdb_id"] = app_id

            self._initialized = True

        except Exception as e:
            # Fallback to standard logging if CDP logger fails
            app_logger.warning(f"Failed to initialize CDP logger: {e}")
            self.logger_app = None
            self._initialized = True

    @classmethod
    def get_instance(cls):
        """Returns the logger singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _log(self, level: LogLevel, message: str, status=Status.Running,
             operation=None, correlation_id=None, source=None):
        """Internal logging method."""
        if self.logger_app:
            self.logger_app.app_spec_backend["DataSource"] = source
            self.logger_app.app_spec_backend["Status"] = status
            self.logger_app.context["traceId"] = correlation_id
            self.logger_app.context["spanId"] = correlation_id
            self.logger_app.app_spec_backend["LogMessage"] = message
            self.logger_app.app_spec_backend["MethodName"] = sys._getframe(2).f_code.co_name
            self.logger_app.app_spec_backend["MicroserviceName"] = "Kafka-NAS-S3-Transfer"

            self.logger_app.log(level, message)

        # Always print to console
        print(f"[{level.name.upper()}] [{source or 'APP'}] {message}")

    def log_debug(self, message, status=Status.Running, operation=None,
                  correlation_id=None, source=None):
        """Log debug message."""
        self._log(LogLevel.debug, message, status, operation, correlation_id, source)

    def log_info(self, message, status=Status.Running, operation=None,
                 correlation_id=None, source=None):
        """Log info message."""
        self._log(LogLevel.info, message, status, operation, correlation_id, source)

    def log_warning(self, message, status=Status.Running, operation=None,
                    correlation_id=None, source=None):
        """Log warning message."""
        self._log(LogLevel.warning, message, status, operation, correlation_id, source)

    def log_error(self, message, status=Status.Failed, operation=None,
                  correlation_id=None, source=None):
        """Log error message."""
        self._log(LogLevel.error, message, status, operation, correlation_id, source)
