import logging


# ----------------------------------------------------------------------------------------------------------------------
def log_console_output(title, output_msg):
    """
    Log the output of a subprocess
    :param title: Title to identify the output
    :type title: str
    :param output_msg: Messages to log
    :title output_msg: str
    """
    logging.debug("Console output >>> %s >>> %s" % (title, output_msg))
