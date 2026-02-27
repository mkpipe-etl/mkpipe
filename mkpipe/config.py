import os
import re
from pathlib import Path
from typing import Any, Dict, Union

import yaml
from dotenv import load_dotenv

from .exceptions import ConfigError
from .models import (
    BackendConfig,
    ConnectionConfig,
    MkpipeConfig,
    PipelineConfig,
    SettingsConfig,
    SparkConfig,
    TableConfig,
)

load_dotenv()

_ENV_VAR_PATTERN = re.compile(r'\$\{([^}]+)\}')


def _resolve_env_vars(value: Any) -> Any:
    if isinstance(value, str):
        def _replacer(match):
            var_name = match.group(1)
            env_val = os.environ.get(var_name)
            if env_val is None:
                raise ConfigError(
                    f"Environment variable '{var_name}' is not set "
                    f"(referenced as '${{{var_name}}}')"
                )
            return env_val

        return _ENV_VAR_PATTERN.sub(_replacer, value)
    elif isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def load_config(path: Union[str, Path]) -> MkpipeConfig:
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    with path.open('r') as f:
        raw = yaml.safe_load(f) or {}

    environment = raw.get('default_environment', 'prod')
    env_data = raw.get(environment)
    if env_data is None:
        raise ConfigError(
            f"Environment '{environment}' not found in config file. "
            f"Available: {[k for k in raw.keys() if k != 'default_environment']}"
        )

    env_data = _resolve_env_vars(env_data)
    version = raw.get('version', 2)

    settings_raw = env_data.get('settings', {})
    settings = SettingsConfig(
        timezone=settings_raw.get('timezone', 'UTC'),
        backend=BackendConfig(**settings_raw.get('backend', {})),
        spark=SparkConfig(**settings_raw.get('spark', {})),
    )

    connections: Dict[str, ConnectionConfig] = {}
    for name, conn_raw in env_data.get('connections', {}).items():
        connections[name] = ConnectionConfig(**conn_raw)

    pipelines = []
    for pipe_raw in env_data.get('pipelines', []):
        tables = []
        for tbl_raw in pipe_raw.get('tables', []):
            tables.append(TableConfig(**tbl_raw))
        pipelines.append(
            PipelineConfig(
                name=pipe_raw['name'],
                source=pipe_raw['source'],
                destination=pipe_raw['destination'],
                tables=tables,
                pass_on_error=pipe_raw.get('pass_on_error', False),
            )
        )

    return MkpipeConfig(
        version=version,
        settings=settings,
        connections=connections,
        pipelines=pipelines,
    )
