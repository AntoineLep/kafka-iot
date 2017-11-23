from common.ConfigFile import ConfigFile


class SystemConfig(ConfigFile):

    _SYSTEM_CONFIG = {
        "LOG_LEVEL": "DEBUG",
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
                }
            ],
            "TOPIC_LIST": [
                {"NAME": "ping", "REPLICATION_FACTOR": 1, "PARTITION_NUMBER": 10},
                {"NAME": "event-generation-request", "REPLICATION_FACTOR": 1, "PARTITION_NUMBER": 10},
                {"NAME": "log", "REPLICATION_FACTOR": 1, "PARTITION_NUMBER": 10}
            ]
        },
        "SYSTEM_LAUNCHER": {
            "PATH": "~/PycharmProjects/kafka-iot/system_launcher",
            "APP_NAME": "System Launcher",
            "LOG_DIRECTORY": "logs",
            "VERSION": "1.0",
            "KAFKA_RIGHTS": {
                "READ": ["ping"],
                "WRITE": ["ping"],
                "GROUP_ID": "system_launcher"
            }
        },
        "INTERNAL_APP_INHERITANCE": {
            "KAFKA_RIGHTS": {
                "READ": ["ping"],
                "WRITE": ["ping"]
            }
        },
        "EVENT_GENERATOR": {
            "NAME": "EventGenerator",
            "TYPE": "internal",
            "USER": "EventGenerator",
            "PASSWORD": "EventGenerator_password",
            "KAFKA_RIGHTS": {
                "READ": ["event_generation_request"],
                "WRITE": ["*"]
            }
        },
        "APPS": [  # USED BY THE EVENT GENERATOR
            {
                "NAME": "App1",
                "TYPE": "internal",
                "USER": "App1",
                "PASSWORD": "app1_password",
                "KAFKA_RIGHTS": {
                    "READ": ["topic_1", "topic_2"],
                    "WRITE": ["event_generation_request", "log"]
                }
            }
        ]
    }

    def __init__(self):
        super(SystemConfig, self).__init__(SystemConfig._SYSTEM_CONFIG)
