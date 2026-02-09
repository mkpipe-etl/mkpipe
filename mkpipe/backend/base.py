from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type


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
    def create(cls, config: Dict[str, Any]) -> 'BackendBase':
        variant = config.get('variant', 'sqlite')
        backend_class = cls._registry.get(variant)
        if not backend_class:
            raise ValueError(
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
