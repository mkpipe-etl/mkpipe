import json
import logging
import logging.handlers
import os
import time
import traceback
from pathlib import Path
from typing import Optional


_LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
_LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
}
_DEFAULT_LOG_LEVEL = _LOG_LEVELS.get(_LOG_LEVEL.lower(), logging.INFO)


class Logger:
    def __init__(self, name: str, log_dir: Optional[str] = None):
        self.logger = logging.getLogger(name)

        if not self.logger.handlers:
            self.logger.setLevel(_DEFAULT_LOG_LEVEL)

            frmt = (
                '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
                '"log": %(message)s, "module": "%(name)s"}'
            )
            json_formatter = logging.Formatter(frmt)
            json_formatter.converter = time.gmtime

            if log_dir:
                log_path = Path(log_dir)
                log_path.mkdir(parents=True, exist_ok=True)
                file_path = log_path / 'mkpipe.log'
                fh = logging.handlers.TimedRotatingFileHandler(
                    file_path, when='midnight', backupCount=7
                )
                fh.setLevel(_DEFAULT_LOG_LEVEL)
                fh.setFormatter(json_formatter)
                self.logger.addHandler(fh)

            ch = logging.StreamHandler()
            ch.setLevel(_DEFAULT_LOG_LEVEL)
            ch.setFormatter(json_formatter)
            self.logger.addHandler(ch)

    def _format(self, message):
        if isinstance(message, str):
            return json.dumps(message)
        return json.dumps(message, sort_keys=True, default=str)

    def debug(self, message):
        self.logger.debug(self._format(message))

    def info(self, message):
        self.logger.info(self._format(message))

    def warning(self, message):
        self.logger.warning(self._format(message))

    def error(self, message):
        self.logger.error(self._format(message))

    def critical(self, message):
        self.logger.critical(self._format(message))


def get_logger(name: str, log_dir: Optional[str] = None) -> Logger:
    return Logger(name, log_dir)
