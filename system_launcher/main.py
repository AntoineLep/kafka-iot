import os
import logging
from config.SystemConfig import SystemConfig
from common.LoggingConfig import init_logger
from common.entities.Zookeeper import Zookeeper
from system_launcher.KafkaDriver import KafkaDriver
from common.KafkaIotException import KafkaIotException
from common.entities.kafka.Topic import Topic


if __name__ == '__main__':
    # ------------ #
    # LOGGER SETUP
    # ------------ #
    sys_conf = SystemConfig()
    log_level = sys_conf.get("LOG_LEVEL")
    log_location = os.path.join(sys_conf.get("SYSTEM_LAUNCHER.PATH"), sys_conf.get("SYSTEM_LAUNCHER.LOG_DIRECTORY"))
    app_name = sys_conf.get("SYSTEM_LAUNCHER.APP_NAME")
    init_logger(log_level, log_location, app_name)

    logging.info("------------------------------")
    logging.info("Starting %s" % app_name)
    logging.info("------------------------------")

    # ------------ #
    # KAFKA DRIVER
    # ------------ #
    logging.info("Initializing kafka driver...")
    kafka_path = sys_conf.get("KAFKA.PATH")
    zookeeper = Zookeeper(sys_conf.get("ZOOKEEPER.HOST"), sys_conf.get("ZOOKEEPER.PORT"))
    kafka = KafkaDriver(kafka_path, zookeeper)
    kafka_broker_list = sys_conf.get("KAFKA.BROKER_LIST")
    logging.info("Kafka driver initialization done!")

    # -------------- #
    # TOPIC CREATION
    # -------------- #
    logging.info("Creating Kafka missing topics...")
    existing_topics_name = set(kafka.list_topic_name())
    config_topics = []

    failed_topic_creation = 0
    for topic_desc in sys_conf.get("KAFKA.TOPIC_LIST"):
        if "NAME" in topic_desc and "PARTITION_NUMBER" in topic_desc and "REPLICATION_FACTOR" in topic_desc:
            try:
                kafka.create_topic(topic_desc["NAME"], topic_desc["REPLICATION_FACTOR"], topic_desc["PARTITION_NUMBER"])
            except KafkaIotException:  # Topic already exists
                failed_topic_creation += 1
            finally:
                config_topics.append(Topic(name=topic_desc["NAME"],
                                           replication_factor=topic_desc["REPLICATION_FACTOR"],
                                           partition_number=topic_desc["PARTITION_NUMBER"],
                                           config=[]))
        else:
            raise KafkaIotException("NAME | PARTITION_NUMBER | REPLICATION_FACTOR "
                                    "field required but not found in KAFKA.TOPIC_LIST: %s" % str(topic_desc))

    existing_topics = []
    for topic_name in kafka.list_topic_name():
        existing_topics.append(kafka.describe_topic(topic_name))

    logging.info("%d topic(s) created!" % (len(existing_topics) - failed_topic_creation))

    # ---------------- #
    # TOPIC ALTERATION
    # ---------------- #

    logging.info("Altering topic configuration...")
    for c_topic in config_topics:
        match_topic = [e_topic for e_topic in existing_topics if e_topic.name == c_topic.name]
        if len(match_topic) == 1:
            match_topic = match_topic[0]
            if match_topic.replication_factor != c_topic.replication_factor:
                if c_topic.replication_factor <= len(kafka_broker_list):
                    kafka.delete_topic(match_topic.name)
                    kafka.create_topic(c_topic.name, c_topic.replication_factor, c_topic.partition_number)
                    logging.warning("Topic \"%s\" has been deleted and recreated in order to update its replication "
                                    "factor from %d to %d. Partition number has been set to %s"
                                    % (c_topic.name, match_topic.replication_factor, c_topic.replication_factor,
                                       c_topic.partition_number))
                else:
                    raise KafkaIotException("Replication factor in configuration file for topic \"%s\" "
                                            "is more than the number of available brokers: %d > %d"
                                            % (c_topic.name, c_topic.replication_factor, len(kafka_broker_list)))
            elif match_topic.partition_number != c_topic.partition_number:
                kafka.alter_topic(c_topic.name, c_topic.partition_number)
                logging.info("Topic \"%s\" partition number has been updated from %d to %d"
                             % (c_topic.name, match_topic.partition_number, c_topic.partition_number))
            else:
                logging.info("Topic \"%s\" configuration (replication factor and partition number) is up to date: "
                             "{replication factor: %d, partition number: %d}"
                             % (c_topic.name, c_topic.replication_factor, c_topic.partition_number))
        else:
            raise KafkaIotException("Topic \"%s\" listed in the configuration file is not currently running in Kafka"
                                    % c_topic.name)

    logging.info("Topic configuration alteration done!")
    logging.info("------------------------------")
    logging.info("End of %s execution" % app_name)
    logging.info("------------------------------")
