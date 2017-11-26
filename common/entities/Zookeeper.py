from common.entities.GenericHost import GenericHost
import logging


class Zookeeper(GenericHost):
    """Zookeeper"""

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, host, port):
        """
        Zookeeper constructor
        :param host: Zookeeper host
        :type host: str
        :param port: Zookeeper port
        :type port: int
        """
        super(Zookeeper, self).__init__(host, port)
        logging.debug("Zookeeper loaded %s" % str(self))

    # ------------------------------------------------------------------------------------------------------------------
    def __str__(self):
        return "{host: %s, port: %d}" % (self.host, self.port)
