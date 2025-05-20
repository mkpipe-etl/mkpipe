import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

load_dotenv()

TIMEZONE = os.getenv('MKPIPE_PROJECT_TIMEZONE', 'UTC')
ROOT_DIR = Path(os.getenv('MKPIPE_PROJECT_DIR', '/app'))
ROOT_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_FILE = str(ROOT_DIR / 'mkpipe_project.yaml')


def update_globals(config):
    """Update global variables based on the provided config dictionary."""
    global_vars = globals()
    for key, value in config.items():
        if key in global_vars:  # Update only if the key exists in the globals
            global_vars[key] = value


def load_config(config_file=None):
    global CONFIG_FILE
    if config_file:
        CONFIG_FILE = config_file
    config_path = Path(CONFIG_FILE).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f'Configuration file not found: {config_path}')

    with config_path.open('r') as f:
        data = yaml.safe_load(f)
        ENV = data.get('default_environment', 'prod')  # Default to 'prod' if not specified
        env_config = data.get(ENV, {})

    return env_config


def get_config_value(keys, file_name):
    """
    Retrieve a specific configuration value using a list of keys.

    Args:
        keys (list): List of keys to retrieve the value (e.g., ['paths', 'bucket_name']).
        file_name (str, optional): Path to the configuration file. Defaults to None.

    Returns:
        The value corresponding to the keys or None if the path is invalid.
    """
    config = load_config(file_name)

    value = config
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    return value
