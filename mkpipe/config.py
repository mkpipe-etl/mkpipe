import yaml
from pathlib import Path

ROOT_DIR = Path(__file__).parent.resolve()

timezone = 'UTC'
spark_driver_memory = '4g'
spark_executor_memory = '3g'
partitions_count = 2
default_iterate_max_loop = 1_000
default_iterate_batch_size = 500_000


def update_globals(config):
    """Update global variables based on the provided config dictionary."""
    global_vars = globals()
    for key, value in config.items():
        if key in global_vars:  # Update only if the key exists in the globals
            global_vars[key] = value


def load_config(config_file):
    config_path = Path(config_file).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f'Configuration file not found: {config_path}')

    global ROOT_DIR
    ROOT_DIR = config_path.parent

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
