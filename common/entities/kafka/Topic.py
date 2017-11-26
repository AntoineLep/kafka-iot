import logging


class Topic(object):
    """Topic"""

    def __init__(self, name, replication_factor, partition_number, config):
        """
        Topic constructor
        :param name: Topic name
        :type name: str
        :param replication_factor: Topic replication factor
        :type replication_factor: int
        :param partition_number: Topic partition number
        :type partition_number: int
        :param config: Topic config
        :type config: list[Config]
        """
        self.name = name
        self.replication_factor = replication_factor
        self.partition_number = partition_number
        self.config = config
        logging.debug("Topic loaded %s" % str(self))

    def __str__(self):
        return "{name: %s, replication factor: %d, partition number: %d, config: %s%s}" % (
            self.name,
            self.replication_factor,
            self.partition_number,
            "\n" if len(self.config) > 0 else "",
            "\n".join(str(conf) for conf in self.config)
        )
