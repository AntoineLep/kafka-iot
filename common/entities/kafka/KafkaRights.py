import logging


class KafkaRights(object):
    """Kafka rights"""

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, read, write):
        """
        Kafka right constructor
        :param read: List of topic name with read access
        :type read: list[str]
        :param write: List of topic name with write access
        :type write: list[str]
        """
        self.read = set()
        if "*" in read:
            self.read.update(["*"])
        else:
            self.read.update(read)
        self.write = set()
        if "*" in write:
            self.write.update(["*"])
        else:
            self.write.update(write)
        logging.debug("Kafka rights loaded %s" % str(self))

    # ------------------------------------------------------------------------------------------------------------------
    def extend(self, kafka_rights):
        """
        Extend kafka rights with another kafka rights instance
        :param kafka_rights: The kafka rights to use to extend kafka rights
        :type: KafkaRights
        """
        if "*" not in self.read:
            if "*" in kafka_rights.read:
                self.read.clear()
                self.read.update(["*"])
            else:
                self.read.update(kafka_rights.read)
        if "*" not in self.write:
            if "*" in kafka_rights.write:
                self.write.clear()
                self.write.update(["*"])
            else:
                self.write.update(kafka_rights.write)

    # ------------------------------------------------------------------------------------------------------------------
    def __str__(self):
        return "{read: [%s], write: [%s]}" % (
            ", ".join(topic for topic in self.read),
            ", ".join(topic for topic in self.write)
        )
