import datetime as dt
import json
import os
import logging
from logging.handlers import SMTPHandler
import logging.config

from pathlib import Path
import concurrent_log_handler

import logging
import json
import datetime as dt

LOG_RECORD_BUILTIN_ATTRS = set(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()
)


class MyJSONFormatter(logging.Formatter):
    def __init__(self, fmt_keys=None):
        """
        fmt_keys: dict mapping output keys to LogRecord attributes
        e.g., {"level": "levelname", "message": "message"}
        """
        super(MyJSONFormatter, self).__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    def format(self, record):
        """
        Return the formatted log record as a JSON string
        """
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record):
        # Always include these fields
        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }

        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info:
            always_fields["stack_info"] = self.formatStack(record.stack_info)

        # Build dict using fmt_keys mapping
        message = {}
        for key, val in self.fmt_keys.items():
            # first try always_fields, fallback to record attribute
            if val in always_fields:
                message[key] = always_fields.pop(val)
            else:
                message[key] = getattr(record, val, None)

        # Add remaining always_fields
        message.update(always_fields)

        # Include extra fields not in built-in LogRecord attributes
        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                message[key] = val

        return message


class NonErrorFilter(logging.Filter):
    def filter(self, record):
        """
        Return True if record should pass the filter
        Only allow log levels <= INFO
        """
        return record.levelno <= logging.INFO


def setup_logging():

    # Determine paths
    ROOT_DIR = Path(__file__).resolve().parents[1]
    config_file = ROOT_DIR / "logging_config.json"

    # Ensure log directory exists
    log_file = ROOT_DIR / "logs"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    if config_file.exists():
        with open(config_file, "rt", encoding="utf8") as f:
            config_dict = json.load(f)

        # allow log level override via environment variable
        env_log_level = os.getenv("LOG_LEVEL")
        if env_log_level:
            config_dict["loggers"]["root"]["level"] = env_log_level.upper()
        logging.config.dictConfig(config_dict)
    else:
        logging.basicConfig(
            level=os.getenv("LOG_LEVEL", "INFO").upper(),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
