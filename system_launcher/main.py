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
from common.entities.Application import Application
from system_launcher.Utils import check_kafka_right_in_topic_name_list


if __name__ == '__main__':
    # ------------ #
    # LOGGER SETUP
    # ------------ #
    sys_conf = SystemConfig()
    log_level = sys_conf.get("LOG_LEVEL")
    log_location = os.path.join(sys_conf.get("SYSTEM_LAUNCHER.PATH"), sys_conf.get("SYSTEM_LAUNCHER.LOG_DIRECTORY"))
    app_name = sys_conf.get("SYSTEM_LAUNCHER.APP_NAME")
    app_version = sys_conf.get("SYSTEM_LAUNCHER.VERSION")
    init_logger(log_level, log_location, app_name)

    logging.info("------------------------------")
    logging.info("Starting %s V%s" % (app_name, app_version))
    logging.info("------------------------------")

    # ---------------------------------- #
    # CONFIGURATION FILE FORMAT CHECKING
    # ---------------------------------- #

    logging.info("Checking configuration file format...")

    check_fields_in_dict(sys_conf.get("ZOOKEEPER"), ["HOST", "PORT"], "ZOOKEEPER")
    check_fields_in_dict(sys_conf.get("KAFKA"), ["PATH", "BROKER_LIST", "TOPIC_LIST", "KAFKA_RIGHTS_INHERITANCE",
                                                 "GROUP_ID_LIST"], "KAFKA")
    for broker in sys_conf.get("KAFKA.BROKER_LIST"):
        check_fields_in_dict(broker, ["ID", "HOST", "PORT"], "KAFKA.BROKER_LIST")

    for topic in sys_conf.get("KAFKA.TOPIC_LIST"):
        check_fields_in_dict(topic, ["NAME", "REPLICATION_FACTOR", "PARTITION_NUMBER"], "KAFKA.TOPIC_LIST")

    check_fields_in_dict(sys_conf.get("KAFKA.KAFKA_RIGHTS_INHERITANCE"), ["READ", "WRITE"],
                         "KAFKA.KAFKA_RIGHTS_INHERITANCE")

    for group_id in sys_conf.get("KAFKA.GROUP_ID_LIST"):
        check_fields_in_dict(group_id, ["NAME", "KAFKA_RIGHTS"], "KAFKA.GROUP_ID_LIST")
        check_fields_in_dict(group_id["KAFKA_RIGHTS"], ["READ", "WRITE"], "KAFKA.GROUP_ID_LIST.{NAME=%s}"
                             % group_id["NAME"])

    for app in sys_conf.get("APP_LIST"):
        check_fields_in_dict(app, ["NAME", "GROUP_ID"], "APP_LIST")

    logging.info("Configuration file format checking done!")

    # ------------------------------------- #
    # CONFIGURATION FILE COHERENCE CHECKING
    # ------------------------------------- #

    logging.info("Checking configuration file coherence...")

    # check topics appear once
    config_topic_name_list = [config_topic["NAME"] for config_topic in sys_conf.get("KAFKA.TOPIC_LIST")]
    if len(set(config_topic_name_list)) != len(config_topic_name_list):
        raise KafkaIotException("One or several topic(s) in topic list is(are) declared several times")

    # check group ids appear once
    config_group_id_name_list = [config_group_id["NAME"] for config_group_id in sys_conf.get("KAFKA.GROUP_ID_LIST")]
    if len(set(config_group_id_name_list)) != len(config_group_id_name_list):
        raise KafkaIotException("One or several group id(s) in group id list is(are) declared several times")

    # check applications appear once
    config_app_name_list = [config_app["NAME"] for config_app in sys_conf.get("APP_LIST")]
    if len(set(config_app_name_list)) != len(config_app_name_list):
        raise KafkaIotException("One or several app(s) in app list is(are) declared several times")

    # Check kafka rights use existing topics
    check_kafka_right_in_topic_name_list(sys_conf.get("KAFKA.KAFKA_RIGHTS_INHERITANCE"), config_topic_name_list)

    for config_group_id in sys_conf.get("KAFKA.GROUP_ID_LIST"):
        check_kafka_right_in_topic_name_list(config_group_id["KAFKA_RIGHTS"], config_topic_name_list)

    # Check applications use existing group id
    config_group_id_name_list = [config_group_id["NAME"] for config_group_id in sys_conf.get("KAFKA.GROUP_ID_LIST")]

    for config_app in sys_conf.get("APP_LIST"):
        if config_app["GROUP_ID"] not in config_group_id_name_list:
            raise KafkaIotException("Group id \"%s\" used in app \"%s\" is not declared in group id list"
                                    % (config_app["GROUP_ID"], config_app["NAME"]))

    logging.info("Configuration file coherence checking done!")

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
    logging.info("Available topic(s) is(are): [%s]" % ", ".join([topic.name for topic in existing_topics]))

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
    config_rights_inheritance = sys_conf.get("KAFKA.KAFKA_RIGHTS_INHERITANCE")
    rights_inheritance = None
    config_group_id_list = sys_conf.get("KAFKA.GROUP_ID_LIST")
    group_id_list = []
    config_app_list = sys_conf.get("APP_LIST")
    app_list = []

    rights_inheritance = KafkaRights(config_rights_inheritance["READ"], config_rights_inheritance["WRITE"])

    logging.info("Reading group id configuration...")

    for c_group_id in config_group_id_list:
        group_id_rights = copy.deepcopy(rights_inheritance)
        group_id_rights.extend(KafkaRights(c_group_id["KAFKA_RIGHTS"]["READ"], c_group_id["KAFKA_RIGHTS"]["WRITE"]))
        group_id = GroupId(c_group_id["NAME"], group_id_rights)
        group_id_list.append(group_id)

    logging.info("Loaded group id(s) is(are): [%s]" % ", ".join([g_id.name for g_id in group_id_list]))
    logging.info("Group id configuration reading done!")

    logging.info("Reading application list...")

    for c_app in config_app_list:
        app_group_id = [group_id for group_id in group_id_list if group_id.name == c_app["GROUP_ID"]][0]
        app_list.append(Application(c_app["NAME"], app_group_id))

    logging.info("Loaded app(s) is(are): [%s]" % ", ".join([app.name for app in app_list]))
    logging.info("Application list reading done!")

    logging.info("Applying rights to group id...")
    # TODO
    logging.info("Group id rights application done!")

    logging.info("Kafka rights management initialization done!")
    logging.info("------------------------------")
    logging.info("End of %s execution" % app_name)
    logging.info("------------------------------")
