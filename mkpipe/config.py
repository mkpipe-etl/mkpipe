import os
import yaml
from pathlib import Path

ROOT_DIR = Path(os.getenv('MKPIPE_PROJECT_PATH', '/tmp/mkpipe'))
ROOT_DIR.mkdir(parents=True, exist_ok=True)

timezone = 'UTC'
spark_driver_memory = '4g'
spark_executor_memory = '3g'
partitions_count = 2
default_iterate_max_loop = 1_000
default_iterate_batch_size = 500_000
CONFIG_FILE = None

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
        ENV = data.get(
            'default_environment', 'prod'
        )  # Default to 'prod' if not specified
        env_config = data.get(ENV, {})

    # Extract settings under the 'settings' key
    settings = env_config.get('settings', {})
    update_globals(settings)

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
