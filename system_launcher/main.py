import os
import logging
from config.SystemConfig import SystemConfig
from common.LoggingConfig import init_logger
from common.entities.Zookeeper import Zookeeper
from system_launcher.KafkaDriver import KafkaDriver
from common.KafkaIotException import TuxdisException


if __name__ == '__main__':
    # ------------ #
    # LOGGER SETUP
    # ------------ #
    sys_conf = SystemConfig()
    log_level = sys_conf.get("LOG_LEVEL")
    log_location = os.path.join(sys_conf.get("SYSTEM_LAUNCHER.PATH"), sys_conf.get("SYSTEM_LAUNCHER.LOG_DIRECTORY"))
    app_name = sys_conf.get("SYSTEM_LAUNCHER.APP_NAME")
    init_logger(log_level, log_location, app_name)

    logging.info("Starting %s" % app_name)

    # ------------ #
    # KAFKA DRIVER
    # ------------ #
    kafka_path = sys_conf.get("KAFKA.PATH")
    zookeeper = Zookeeper(sys_conf.get("ZOOKEEPER.HOST"), sys_conf.get("ZOOKEEPER.PORT"))
    kafka = KafkaDriver(kafka_path, zookeeper)

    # ------------------ #
    # APP TOPIC CREATION
    # ------------------ #
    existing_topics = set(kafka.list_topic())

    for topic_desc in sys_conf.get("KAFKA.TOPIC_LIST"):
        if "NAME" in topic_desc and "PARTITION_NUMBER" in topic_desc and "REPLICATION_FACTOR" in topic_desc:
            try:
                kafka.create_topic(topic_desc["NAME"], topic_desc["REPLICATION_FACTOR"], topic_desc["PARTITION_NUMBER"])
            except TuxdisException:
                pass
        else:
            raise TuxdisException("NAME | PARTITION_NUMBER | REPLICATION_FACTOR"
                                  " field required but not found in KAFKA.TOPIC_LIST: %s" % str(topic_desc))

    topics = []
    for topic_name in kafka.list_topic():
        topics.append(kafka.describe_topic(topic_name))

    logging.info("End of %s execution" % app_name)
