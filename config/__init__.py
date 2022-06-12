import yaml

def read_config():
    with open("config/config_file.yml", 'r') as f:
        cfg = yaml.safe_load(f)
    return cfg

def read_config_from_file(path):
    """Read a configuration from a file.
    Use this function to read configuration files that are not in the 'main' directory."""
    with open(path, 'r') as f:
        cfg = yaml.load(f)
    return cfg
