from common.KafkaIotException import TuxdisException


class ConfigFile(object):
    """Python config file reader"""

    # ------------------------------------------------------------------------------------------------------------------
    def __init__(self, config_content):
        self._config_content = config_content

    # ------------------------------------------------------------------------------------------------------------------
    def get(self, search_string=None):
        """
        Get a config value using a search string
        :param search_string: Config param separated with a dot '.'
        :type search_string: str
        :return: The value of the config element if it exists, raise an exception otherwise
        """
        if search_string is None:
            return self._config_content

        if isinstance(search_string, str):
            param_array = search_string.split(".")
            performed_param_str = None
            ret = self._config_content

            for param in param_array:
                performed_param_str = performed_param_str + "." + param if performed_param_str is not None else param
                if param in ret:
                    ret = ret[param]
                else:
                    raise TuxdisException("Can't find param %s in config file" % performed_param_str)

            return ret

        else:
            raise TuxdisException("Given parameter for search_string must be a string! %s given instead" %
                                  str(type(search_string)))
