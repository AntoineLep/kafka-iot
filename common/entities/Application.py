import logging


class Application(object):
    """Application"""

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, name, group_id):
        """
        Application constructor
        :param name: Application name
        :type name: str
        :param group_id: Group id of the given application
        :type group_id: GroupId
        """
        self.name = name
        self.group_id = group_id
        logging.debug("Application loaded %s" % str(self))

    # ------------------------------------------------------------------------------------------------------------------
    def __str__(self):
        return "{name: [%s], group id: [%s]}" % (
            self.name,
            str(self.group_id)
        )
