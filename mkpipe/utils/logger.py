import json
import traceback
import os
import logging
import time
import logging.handlers
from dagster import get_dagster_logger
from ..config import ROOT_DIR

path = os.path.abspath(os.path.join(ROOT_DIR, 'logs'))
#path = ROOT_DIR / "logs"
#path.mkdir(exist_ok=True)  # Create the directory if it doesn't exist

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()


log_levels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
}

default_log_level = log_levels.get(LOG_LEVEL.lower())
class Logger:
    def __init__(self, name) -> None:
        """
        Custom Usage:
        logger = Logger()
        msg = 'This is a log message'
        logger.log(msg, log_level="error")
        or
        logger.error("This is an error message")
        """
        self.logger = logging.getLogger(name)
        self.dagster_logger = get_dagster_logger()
        if not self.logger.handlers:
            # create the handlers and call logger.addHandler(logging_handler)
            self.logger = logging.getLogger(name)
            # self.logger.setLevel(logging.DEBUG)
            self.logger.setLevel(default_log_level)

            # Create a formatter
            # https://docs.python.org/3/library/logging.html#logrecord-attributes

            frmt = """{"timestamp" : "%(asctime)s", "levelno" : "%(levelno)s", "level" : "%(levelname)s", "message" : %(message)s, "function" : "%(name)s" }"""
            json_formatter = logging.Formatter(frmt)
            json_formatter.converter = time.gmtime  # set timezone as gmtime

            frmt = '%(asctime)s | %(levelno)s | %(levelname)s | %(name)s | %(message)s'
            string_formatter = logging.Formatter(frmt)
            string_formatter.converter = time.gmtime  # set timezone as gmtime

            # Create a file handler
            # Fetch the pod name (e.g., celery-consumer-66894599cd-85dvr)
            pod_name = os.getenv('HOSTNAME', 'unknown_pod')

            # date = datetime.today().strftime('%Y%m%d')
            if not os.path.exists(path):
                os.makedirs(path)
            file_path = os.path.abspath(
                os.path.join(
                    path, f'{pod_name}_log.log'
                )  # Use pod name for unique log file
            )

            # use very short interval for this example, typical 'when' would be 'midnight' and no explicit interval
            fh = logging.handlers.TimedRotatingFileHandler(
                file_path, when='midnight', backupCount=7
            )
            # fh = logging.handlers.TimedRotatingFileHandler(file_path, when="midnight", backupCount=7)
            # fh = logging.FileHandler(file_path, mode='a', encoding='utf8')

            fh.setLevel(default_log_level)
            # fh.setFormatter(json_formatter)
            fh.setFormatter(string_formatter)

            # Create a console handler (optional)
            ch = logging.StreamHandler()
            # ch.setLevel(logging.ERROR)
            ch.setLevel(default_log_level)
            ch.setFormatter(json_formatter)

            # Add handlers to the logger
            self.logger.addHandler(fh)
            # self.logger.addHandler(ch)

    def message_formatter(self, message):
        # msg = json.dumps(str(message))
        msg = json.dumps(message, sort_keys=True, indent=2, separators=(',', ': '))
        return msg

    def log(self, message, log_level='info'):
        msg = self.message_formatter(message)

        logger = self.logger
        if log_level == 'debug':
            logger.debug(msg)
            self.dagster_logger.debug(msg)
        elif log_level == 'info':
            logger.info(msg)
            self.dagster_logger.info(msg)
        elif log_level == 'warning':
            logger.warning(msg)
            self.dagster_logger.warning(msg)
        elif log_level == 'error':
            logger.error(msg)
            self.dagster_logger.error(msg)
        elif log_level == 'critical':
            logger.critical(msg)
            self.dagster_logger.critical(msg)
        else:
            logger.exception(msg)
            self.dagster_logger.exception(msg)
        return

    def debug(self, message):
        msg = self.message_formatter(message)
        self.logger.debug(msg)
        self.dagster_logger.debug(msg)
        return

    def info(self, message):
        msg = self.message_formatter(message)
        self.logger.info(msg)
        self.dagster_logger.info(msg)
        return

    def warning(self, message):
        msg = self.message_formatter(message)
        self.logger.warning(msg)
        self.dagster_logger.warning(msg)
        return

    def error(self, message):
        msg = self.message_formatter(message)
        self.logger.error(msg)
        self.dagster_logger.error(msg)
        return

    def critical(self, message):
        msg = self.message_formatter(message)
        self.logger.critical(msg)
        self.dagster_logger.critical(msg)
        return

    def shutdown(self):
        logging.shutdown()


def log_container(name):
    def inner(func):
        def wrapper(*args, **kwargs):
            # Log some messages
            logger = Logger(name)

            try:
                # send start message
                # message = f'Started function: {func.__name__}'
                # logger.log(message)
                # start_time = time.time()

                # call the function
                result = func(*args, **kwargs)

                # run_time = time.time() - start_time
                # message = (
                #     f'Ended function: {func.__name__}. Time Duration(sec): {run_time} '
                # )
                # logger.log(message)

                return result

            except Exception as e:
                message = str(e) + str(traceback.format_exc()).replace('\n', ' ')
                logger.log(message, log_level='error')
                raise

        return wrapper

    return inner

