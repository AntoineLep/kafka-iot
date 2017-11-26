import os
import logging
import copy
from config.SystemConfig import SystemConfig
from common.LoggingConfig import init_logger
from common.Utils import check_fields_in_dict
from common.entities.Zookeeper import Zookeeper
from system_launcher.KafkaDriver import KafkaDriver
from common.KafkaIotException import KafkaIotException
from common.entities.kafka.Topic import Topic
from common.entities.kafka.KafkaRights import KafkaRights
from common.entities.kafka.GroupId import GroupId


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
        check_fields_in_dict(topic_desc, ["NAME", "REPLICATION_FACTOR", "PARTITION_NUMBER"], "KAFKA.TOPIC_LIST")

        if topic_desc["REPLICATION_FACTOR"] <= len(kafka_broker_list):
            try:
                kafka.create_topic(topic_desc["NAME"], topic_desc["REPLICATION_FACTOR"], topic_desc["PARTITION_NUMBER"])
                logging.info("Topic \"%s\": {replication factor: %d, partition number: %d} created"
                             % (topic_desc["NAME"], topic_desc["REPLICATION_FACTOR"], topic_desc["PARTITION_NUMBER"]))
            except KafkaIotException:  # Topic already exists
                failed_topic_creation += 1
            finally:
                config_topics.append(Topic(name=topic_desc["NAME"],
                                           replication_factor=topic_desc["REPLICATION_FACTOR"],
                                           partition_number=topic_desc["PARTITION_NUMBER"],
                                           config=[]))
        else:
            raise KafkaIotException("Replication factor in configuration file for topic \"%s\" "
                                    "is more than the number of available broker(s): %d > %d"
                                    % (topic_desc["NAME"], topic_desc["REPLICATION_FACTOR"],
                                       len(kafka_broker_list)))

    existing_topics = []
    for topic_name in kafka.list_topic_name():
        existing_topics.append(kafka.describe_topic(topic_name))

    logging.info("%d topic(s) created!" % (len(existing_topics) - failed_topic_creation))
    logging.info("Available topics are: [%s]" % ", ".join([topic.name for topic in existing_topics]))

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
                    logging.warning("Topic \"%s\" has been deleted and re-created in order to update its replication "
                                    "factor from %d to %d. Partition number has been set to %s"
                                    % (c_topic.name, match_topic.replication_factor, c_topic.replication_factor,
                                       c_topic.partition_number))
                else:
                    raise KafkaIotException("Replication factor in configuration file for topic \"%s\" "
                                            "is more than the number of available broker(s): %d > %d"
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

    # Update existing topic info
    existing_topics = []
    for topic_name in kafka.list_topic_name():
        existing_topics.append(kafka.describe_topic(topic_name))

    logging.info("Topic configuration alteration done!")

    # ----------------- #
    # RIGHTS MANAGEMENT
    # ----------------- #

    logging.info("Initializing kafka rights management...")
    conf_rights_inheritance = sys_conf.get("KAFKA.KAFKA_RIGHTS_INHERITANCE")
    rights_inheritance = None
    conf_group_id_list = sys_conf.get("KAFKA.GROUP_ID_LIST")
    group_id_list = []
    conf_app_list = sys_conf.get("APP_LIST")
    app_list = []

    check_fields_in_dict(conf_rights_inheritance, ["READ", "WRITE"], "KAFKA.KAFKA_RIGHTS_INHERITANCE")
    rights_inheritance = KafkaRights(conf_rights_inheritance["READ"], conf_rights_inheritance["WRITE"])

    logging.info("Reading group id configuration...")

    for c_group_id in conf_group_id_list:
        check_fields_in_dict(c_group_id, ["NAME", "KAFKA_RIGHTS"], "KAFKA.GROUP_ID_LIST")

        if not any(g_id.name == c_group_id["NAME"] for g_id in group_id_list):
            check_fields_in_dict(c_group_id["KAFKA_RIGHTS"], ["READ", "WRITE"], "KAFKA.GROUP_ID_LIST.{%s}"
                                 % c_group_id["NAME"])
            group_id_rights = copy.deepcopy(rights_inheritance)
            group_id_rights.extend(KafkaRights(c_group_id["KAFKA_RIGHTS"]["READ"], c_group_id["KAFKA_RIGHTS"]["WRITE"]))
            group_id = GroupId(c_group_id["NAME"], group_id_rights)
            group_id_list.append(group_id)
            logging.info("Group id %s loaded" % str(group_id))
        else:
            raise KafkaIotException("Group id with name \"%s\" is declared several times" % c_group_id["NAME"])

    logging.info("Group id configuration reading done!")

    logging.info("Reading application list...")
    # TODO
    logging.info("Application list reading done!")

    logging.info("Applying rights to group id...")
    # TODO
    # check group id read and write topic exist
    logging.info("Group id rights application done!")

    logging.info("Kafka rights management initialization done!")
    logging.info("------------------------------")
    logging.info("End of %s execution" % app_name)
    logging.info("------------------------------")
