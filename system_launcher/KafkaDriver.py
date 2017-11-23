import subprocess
import os
import re
from common.Utils import expand_var_and_user
from system_launcher.Utils import log_console_output
from common.KafkaIotException import KafkaIotException
from common.entities.Zookeeper import Zookeeper
from common.entities.kafka.Topic import Topic
from common.entities.kafka.TopicConfig import TopicConfig


class KafkaDriver(object):
    """Kafka driving"""

    DEFAULT_ENCODING = "utf-8"
    TOPIC_SCRIPT = "bin/kafka-topics.sh"

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, kafka_path, zookeeper):
        """
        KafkaDriver Constructor
        :param kafka_path: Path to Kafka install folder
        :param zookeeper: Zookeeper info
        :type zookeeper: Zookeeper
        """
        self._kafka_path = expand_var_and_user(kafka_path)
        self._zookeeper = zookeeper
        self._zookeeper_full_address = "%s:%d" % (self._zookeeper.host, self._zookeeper.port)

    # ------------------------------------------------------------------------------------------------------------------
    def list_topic(self):
        """
        List the topics available in kafka
        :return: The list of topic available in kafka
        :rtype: list[str]
        """
        p = subprocess.Popen([self._get_topic_script(), "--list", "--zookeeper", self._zookeeper_full_address],
                             stdout=subprocess.PIPE)

        topic_list = []

        while True:
            line = p.stdout.readline()
            if line != b'':
                line = line.decode(KafkaDriver.DEFAULT_ENCODING).replace("\n", "")
                log_console_output("LIST TOPIC", line)
                topic_list.append(line)
            else:
                break
        return topic_list

    # ------------------------------------------------------------------------------------------------------------------
    def describe_topic(self, topic_name, check_topic_existence=True):
        """
        Get the information describing a topic
        :param topic_name: Name of the topic to get the information
        :type topic_name: str
        :param check_topic_existence: Check if the topic exists before trying to describe it
        :type check_topic_existence: bool
        :return: The topic information
        :rtype: Topic
        """
        if check_topic_existence:
            if topic_name not in self.list_topic():
                raise KafkaIotException("Topic \"%s\" doesn't exist" % topic_name)

        p = subprocess.Popen([self._get_topic_script(), "--describe", "--zookeeper", self._zookeeper_full_address,
                              "--topic", topic_name], stdout=subprocess.PIPE)

        topic_partition_count = 1
        topic_replication_factor = 1
        topic_config = []
        i = 0

        while True:
            i += 1
            line = p.stdout.readline()
            if line != b'':
                line = line.decode(KafkaDriver.DEFAULT_ENCODING).replace("\n", "")
                log_console_output("DESCRIBE TOPIC", line)
                if i == 1:  # Topic info
                    matches = re.search(".*PartitionCount:\s*(\d*).*ReplicationFactor:\s*(\d*)", line)
                    if len(matches.groups()) == 2:  # partition count, replication factor
                        topic_partition_count = int(matches.groups()[0])
                        topic_replication_factor = int(matches.groups()[1])
                    else:
                        raise KafkaIotException("Error when attempting to read topic information. "
                                              "The received format doesn't match the required one")
                else:  # Topic config line
                    matches = re.search(".*Partition:\s*(\d*).*Leader:\s*(\d*).*Replicas:\s*(\S*).*Isr:\s*(\S*)",
                                        line)
                    if len(matches.groups()) == 4:  # partition, leader, replicas, isr
                        topic_config.append(TopicConfig(
                            partition=int(matches.groups()[0]),
                            leader=int(matches.groups()[1]),
                            replicas=list(int(rep) for rep in matches.groups()[2].split(",")),
                            isr=list(int(isr) for isr in matches.groups()[3].split(","))
                        ))
                    else:
                        raise KafkaIotException("Error when attempting to read topic config information. "
                                              "The received format doesn't match the required one")
            else:
                break
        return Topic(topic_name, topic_partition_count, topic_replication_factor, topic_config)

    # ------------------------------------------------------------------------------------------------------------------
    def create_topic(self, topic_name, replication_factor, partitions, check_topic_existence=True):
        """
        Attempt to create a topic
        :param topic_name: The name of the topic to be created
        :type topic_name: str
        :param replication_factor: The replication factor of the topic
        :type replication_factor: int
        :param partitions: The number of partition of the topic
        :type partitions: int
        :param check_topic_existence: Check if the topic exists before trying to create it
        :type check_topic_existence: bool
        :return: The created topic information
        """
        if check_topic_existence:
            if topic_name in self.list_topic():
                raise KafkaIotException("Topic \"%s\" already exists" % topic_name)

        p = subprocess.Popen([self._get_topic_script(), "--create", "--zookeeper", self._zookeeper_full_address,
                              "--replication-factor", str(replication_factor), "--partitions", str(partitions),
                              "--topic", topic_name], stdout=subprocess.PIPE)

        while True:
            line = p.stdout.readline()
            if line != b'':
                line = line.decode(KafkaDriver.DEFAULT_ENCODING).replace("\n", "")
                log_console_output("CREATE TOPIC", line)
            else:
                break

            return self.describe_topic(topic_name, check_topic_existence)

    # ------------------------------------------------------------------------------------------------------------------
    def delete_topic(self, topic_name, check_topic_existence=True):
        """
        Attempt to delete a given topic.
        This will have no effect if delete.topic.enable is not set to true in kafka config
        :param topic_name: The name of the topic to delete
        :type topic_name: str
        :param check_topic_existence: Check if the topic exists before trying to delete it
        :type check_topic_existence: bool
        """
        if check_topic_existence:
            if topic_name not in self.list_topic():
                raise KafkaIotException("Topic \"%s\" doesn't exist" % topic_name)

        p = subprocess.Popen([self._get_topic_script(), "--delete", "--zookeeper", self._zookeeper_full_address,
                              "--topic", topic_name], stdout=subprocess.PIPE)

        while True:
            line = p.stdout.readline()
            if line != b'':
                line = line.decode(KafkaDriver.DEFAULT_ENCODING).replace("\n", "")
                log_console_output("DELETE TOPIC", line)
            else:
                break

    # ------------------------------------------------------------------------------------------------------------------
    def alter_topic(self, topic_name, replication_factor=-1, partitions=-1, check_topic_existence=True):
        """
        Alter a topic replication factor and / or partition number
        :param topic_name: Name of the topic to alter
        :type topic_name: str
        :param replication_factor: New replication factor to be applied. No effect when set to -1
        :type replication_factor: int
        :param partitions: New partition number to be applied. No effect when set to -1
        :type partitions: int
        :param check_topic_existence: Check if the topic exists before trying to delete it
        :type check_topic_existence: bool
        :return: The altered topic info
        :rtype: Topic
        """
        if check_topic_existence:
            if topic_name not in self.list_topic():
                raise KafkaIotException("Topic \"%s\" doesn't exist" % topic_name)

        if replication_factor > 0:
            p = subprocess.Popen([self._get_topic_script(), "--alter", "--zookeeper", self._zookeeper_full_address,
                                  "--topic", topic_name, "--replication-factor", str(replication_factor)],
                                 stdout=subprocess.PIPE)

            while True:
                line = p.stdout.readline()
                if line != b'':
                    line = line.decode(KafkaDriver.DEFAULT_ENCODING).replace("\n", "")
                    log_console_output("ALTER TOPIC REPLICATION", line)
                else:
                    break

        if partitions > 0:
            p = subprocess.Popen([self._get_topic_script(), "--alter", "--zookeeper", self._zookeeper_full_address,
                                  "--topic", topic_name, "--partitions", str(partitions)],
                                 stdout=subprocess.PIPE)

            while True:
                line = p.stdout.readline()
                if line != b'':
                    line = line.decode(KafkaDriver.DEFAULT_ENCODING).replace("\n", "")
                    log_console_output("ALTER TOPIC PARTITION", line)
                else:
                    break

        return self.describe_topic(topic_name, check_topic_existence)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def _get_topic_script(self):
        """
        Return the path of the kafka topic management script
        :return: The path of the kafka topic management script
        :rtype: str
        """
        return os.path.join(self._kafka_path, KafkaDriver.TOPIC_SCRIPT)
