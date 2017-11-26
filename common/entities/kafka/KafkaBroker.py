from common.entities.GenericHost import GenericHost
import logging


class KafkaBroker(GenericHost):
    """Kafka broker"""

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, host, port, id_number):
        """
        KafkaBroker constructor
        :param host: Kafka broker host
        :type host: str
        :param port: Kafka broker port
        :type port: int
        """
        super(KafkaBroker, self).__init__(host, port)
        self.id_number = id_number
        logging.debug("Kafka broker loaded %s" % str(self))

    # ------------------------------------------------------------------------------------------------------------------
    def __str__(self):
        return "{host: %s, port: %d, id number: %d}" % (self.host, self.port, self.id_number)
