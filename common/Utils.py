import os


# ------------------------------------------------------------------------------------------------------------------
def expand_var_and_user(path):
    return os.path.expanduser(os.path.expandvars(path))
