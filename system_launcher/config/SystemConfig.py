from common.ConfigFile import ConfigFile


class SystemConfig(ConfigFile):

    _SYSTEM_CONFIG = {
        "LOG_LEVEL": "INFO",  # DEBUG, INFO, WARNING, ERROR, CRITICAL
        "SYSTEM_LAUNCHER": {
            "PATH": "~/PycharmProjects/kafka-iot/system_launcher",
            "APP_NAME": "System Launcher",
            "GROUP_ID": "system-launcher",
            "LOG_DIRECTORY": "logs",
            "VERSION": "1.0",
        },
        "ZOOKEEPER": {
            "HOST": "localhost",
            "PORT": 2181
        },
        "KAFKA": {
            "PATH": "/opt/Kafka/kafka_2.11-1.0.0",
            "BROKER_LIST": [
                {
                    "ID": 0,
                    "HOST": "localhost",
                    "PORT": "9092"
                },
                {
                    "ID": 1,
                    "HOST": "localhost",
                    "PORT": "9093"
                }
            ],
            # ----------------- #
            # TOPIC DECLARATION
            # ----------------- #
            # /!\ Be aware that a replication factor change or a partition number decrease will result in a topic
            # deletion and re-creation.
            # Note that the replication factor can't be more than the number of available kafka brokers
            "TOPIC_LIST": [
                {"NAME": "ping", "REPLICATION_FACTOR": 2, "PARTITION_NUMBER": 2},
                {"NAME": "log", "REPLICATION_FACTOR": 2, "PARTITION_NUMBER": 5}
            ],
            # ---------------------------------- #
            # GLOBAL KAFKA RIGHTS FOR EVERY APPS
            # ---------------------------------- #
            "KAFKA_RIGHTS_INHERITANCE": {
                "READ": ["ping"],
                "WRITE": ["ping, log"]
            },
            # ---------------------------------------- #
            # GROUP ID LIST WITH SPECIFIC KAFKA RIGHTS
            # ---------------------------------------- #
            "GROUP_ID_LIST": [
                {
                    "NAME": "system-launcher",
                    "KAFKA_RIGHTS": {
                        "READ": ["*"],
                        "WRITE": ["*"],
                    }
                },
                {
                    "NAME": "app-1",
                    "KAFKA_RIGHTS": {
                        "READ": ["toto"],
                        "WRITE": ["titi"],
                    }
                }
            ]
        },
        "APP_LIST": [
            {"NAME": "App1", "GROUP_ID": "app-1"}
        ]
    }

    def __init__(self):
        super(SystemConfig, self).__init__(SystemConfig._SYSTEM_CONFIG)
