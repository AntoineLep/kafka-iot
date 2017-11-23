import logging


class Topic(object):
    """Topic"""

    def __init__(self, name, partition_count, replication_factor, config):
        """
        Topic constructor
        :param name: Topic name
        :type name: str
        :param partition_count: Topic partition count
        :type partition_count: int
        :param replication_factor: Topic replication factor
        :type replication_factor: int
        :param config: Topic config
        :type config: list[Config]
        """
        self.name = name
        self.partition_count = partition_count
        self.replication_factor = replication_factor
        self.config = config
        logging.debug("Topic loaded %s" % str(self))

    def __str__(self):
        return "{name: %s, partition count: %d, replication factor: %d, config: \n%s}" % (
            self.name,
            self.partition_count,
            self.replication_factor,
            "\n".join(str(conf) for conf in self.config)
        )
