import time
from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Dict, Optional, Type

from ..exceptions import BackendError


def retry(max_attempts=5, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception:
                    attempts += 1
                    if attempts >= max_attempts:
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator


class BackendBase(ABC):
    _registry: Dict[str, Type['BackendBase']] = {}

    def __init_subclass__(cls, variant: Optional[str] = None, **kwargs):
        super().__init_subclass__(**kwargs)
        if variant:
            cls._registry[variant] = cls

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.init_table()

    @classmethod
    def _discover_backend(cls, variant: str) -> Optional[Type['BackendBase']]:
        from importlib.metadata import entry_points

        eps = entry_points()
        group = 'mkpipe.backends'
        if hasattr(eps, 'select'):
            matches = eps.select(group=group, name=variant)
        else:
            matches = [ep for ep in eps.get(group, []) if ep.name == variant]

        for ep in matches:
            backend_cls = ep.load()
            if isinstance(backend_cls, type) and issubclass(backend_cls, BackendBase):
                return backend_cls
        return None

    @classmethod
    def create(cls, config: Dict[str, Any]) -> 'BackendBase':
        variant = config.get('variant', 'sqlite')
        backend_class = cls._registry.get(variant)
        if not backend_class:
            backend_class = cls._discover_backend(variant)
        if not backend_class:
            raise BackendError(
                f"No backend found for variant: '{variant}'. "
                f"Available: {list(cls._registry.keys())}"
            )
        return backend_class(config)

    @abstractmethod
    def init_table(self) -> None:
        pass

    @abstractmethod
    def get_table_status(self, pipeline_name: str, table_name: str) -> Optional[str]:
        pass

    @abstractmethod
    def get_last_point(self, pipeline_name: str, table_name: str) -> Optional[str]:
        pass

    @abstractmethod
    def manifest_table_update(
        self,
        pipeline_name: str,
        table_name: str,
        value: Optional[str] = None,
        value_type: Optional[str] = None,
        status: str = 'completed',
        replication_method: str = 'full',
        error_message: Optional[str] = None,
    ) -> None:
        pass
