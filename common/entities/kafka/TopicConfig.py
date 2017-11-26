import logging


class TopicConfig(object):
    """Topic config"""

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, partition, leader, replicas, isr):
        """
        Topic config constructor
        :param partition: Topic config partition number
        :type partition: int
        :param leader: Topic config kafka broker leader
        :type leader: int
        :param replicas: Topic config replicas on kafka broker
        :type replicas: list[int]
        :param isr: Topic config in sync replicas
        :type isr: list[int]
        """
        self.partition = partition
        self.leader = leader
        self.replicas = replicas
        self.isr = isr
        logging.debug("Topic config loaded %s" % str(self))

    # ------------------------------------------------------------------------------------------------------------------
    def __str__(self):
        return "{partition: %d, leader: %d, replicas: %s, isr: %s}" % (
            self.partition,
            self.leader,
            ", ".join(str(rep) for rep in self.replicas),
            ", ".join(str(_isr) for _isr in self.isr)
        )
