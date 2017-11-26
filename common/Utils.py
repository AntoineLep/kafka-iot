import os
from KafkaIotException import KafkaIotException


# ----------------------------------------------------------------------------------------------------------------------
def expand_var_and_user(path):
    return os.path.expanduser(os.path.expandvars(path))


# ----------------------------------------------------------------------------------------------------------------------
def check_fields_in_dict(dictionary, fields, dictionary_name):
    """
    Check that the fields are in the dict and raise an exception if not
    :param dictionary: The dictionary in which the check has to be done
    :type dictionary: dict
    :param fields: List of fields to check
    :type fields: list[str]
    :param dictionary_name: name of the dictionary (for exception message purpose)
    :type dictionary_name: str
    :return: True if all the fields are in the given dictionary, raise an exception otherwise
    """
    for field in fields:
        if field not in dictionary:
            raise KafkaIotException("%s field(s) required but not found in %s: %s"
                                    % (", ".join(fields), dictionary_name, str(dictionary)))
    return True
