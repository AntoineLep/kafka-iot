class GenericHost(object):
    """Generic host"""

    def __init__(self, host, port):
        """
        GenericHost constructor
        :param host: Generic host hostname
        :type host: str
        :param port: Generic host port
        :type port: int
        """
        self.host = host
        self.port = port
