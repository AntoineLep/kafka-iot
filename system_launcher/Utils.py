import logging
from common.KafkaIotException import KafkaIotException


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


# ----------------------------------------------------------------------------------------------------------------------
def check_kafka_right_in_topic_name_list(kafka_rights, topic_name_list):
    """
    Check kafka right topics exists in topic list
    :param kafka_rights: Dictionary with read and write topic list
    :type kafka_rights: dict
    :param topic_name_list: List of topic name
    :type topic_name_list: list[str]
    :return: True if all the topics in kafka rights are in topic name list. Raise an exception otherwise
    """
    for read_topic in kafka_rights["READ"]:
        if read_topic != "*" and read_topic not in topic_name_list:
            raise KafkaIotException("READ topic \"%s\" is not declared in topic list" % read_topic)

    for write_topic in kafka_rights["WRITE"]:
        if write_topic != "*" and write_topic not in topic_name_list:
            raise KafkaIotException("WRITE topic \"%s\" is not declared in topic list" % write_topic)
