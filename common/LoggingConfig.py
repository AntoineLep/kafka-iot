import os
import logging
from logging.handlers import RotatingFileHandler
from common.Utils import expand_var_and_user
from common.KafkaIotException import KafkaIotException


# ----------------------------------------------------------------------------------------------------------------------
def init_logger(log_level, log_location, app_name):
    # Getting log level
    log_level = _define_log_level(log_level)

    # Modify logger log level
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Set file formatter
    formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: " + app_name + " ::  %(message)s")
    log_filename = "%s_log.txt" % app_name
    log_location = expand_var_and_user(log_location)

    if os.path.isdir(log_location):
        log_file_path = os.path.join(log_location, log_filename)
        need_roll = os.path.isfile(log_file_path)

        # Redirect logs into a log file
        file_handler = RotatingFileHandler(log_file_path, backupCount=10, maxBytes=2*1024*1024)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        if need_roll:
            file_handler.doRollover()
        logger.addHandler(file_handler)

        # Redirect logs into the user console
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(log_level)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    else:
        raise KafkaIotException("Log directory: %s does not exist, create it before relaunching the program" %
                                log_location)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def _define_log_level(log_level):
    """
    Define the log level
    :param log_level: The log level to be defined: debug, info, warning, error, critical
    :type log_level: str
    :return:
    """
    if isinstance(log_level, str):
        if log_level.upper() == "DEBUG":
            return logging.DEBUG
        if log_level.upper() == "INFO":
            return logging.INFO
        if log_level.upper() == "WARNING":
            return logging.WARNING
        if log_level.upper() == "ERROR":
            return logging.ERROR
        if log_level.upper() == "CRITICAL":
            return logging.CRITICAL
    return logging.DEBUG
