import logging


class GroupId(object):
    """Group id"""

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, name, kafka_rights):
        """
        Group id constructor
        :param name: Group id name
        :type name: str
        :param kafka_rights: Kafka rights for the given group id
        :type kafka_rights: KafkaRights
        """
        self.name = name
        self.kafka_rights = kafka_rights
        logging.debug("Group id loaded %s" % str(self))

    # ------------------------------------------------------------------------------------------------------------------
    def __str__(self):
        return "{name: %s, kafka rights: %s}" % (
            self.name,
            str(self.kafka_rights)
        )
