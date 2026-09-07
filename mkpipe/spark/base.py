from abc import ABC, abstractmethod
from typing import Dict, Optional, Type

from ..exceptions import PluginNotFoundError
from ..models import ExtractResult, TableConfig


class BaseExtractor(ABC):
    _registry: Dict[str, Type['BaseExtractor']] = {}

    def __init_subclass__(cls, variant: Optional[str] = None, **kwargs):
        super().__init_subclass__(**kwargs)
        if variant:
            cls._registry[variant] = cls

    @classmethod
    def create(cls, variant: str, **kwargs) -> 'BaseExtractor':
        extractor_class = cls._registry.get(variant)
        if not extractor_class:
            from ..plugins.registry import discover_extractor

            extractor_class = discover_extractor(variant)
        if not extractor_class:
            raise PluginNotFoundError(
                f"No extractor found for variant: '{variant}'. "
                f'Available: {list(cls._registry.keys())}'
            )
        return extractor_class(**kwargs)

    @abstractmethod
    def extract(self, table: TableConfig, spark, last_point: Optional[str] = None) -> ExtractResult:
        pass


class BaseLoader(ABC):
    _registry: Dict[str, Type['BaseLoader']] = {}
    ingested_at_column: str = '_ingested_at'
    ingestion_id_column: str = 'mkpipe_id'
    column_name_case: str = 'as_is'
    if_exists: str = 'replace'

    def __init_subclass__(cls, variant: Optional[str] = None, **kwargs):
        super().__init_subclass__(**kwargs)
        if variant:
            cls._registry[variant] = cls

    @classmethod
    def create(cls, variant: str, **kwargs) -> 'BaseLoader':
        loader_class = cls._registry.get(variant)
        if not loader_class:
            from ..plugins.registry import discover_loader

            loader_class = discover_loader(variant)
        if not loader_class:
            raise PluginNotFoundError(
                f"No loader found for variant: '{variant}'. Available: {list(cls._registry.keys())}"
            )
        return loader_class(**kwargs)

    @abstractmethod
    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        pass
