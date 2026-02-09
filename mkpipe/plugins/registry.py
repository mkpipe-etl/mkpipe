import importlib.metadata
from typing import Dict, Optional, Type


def _discover_plugins(group: str) -> Dict[str, Type]:
    plugins = {}
    try:
        entry_points = importlib.metadata.entry_points(group=group)
        for ep in entry_points:
            try:
                plugins[ep.name] = ep.load()
            except Exception:
                pass
    except Exception:
        pass
    return plugins


_EXTRACTOR_CACHE: Dict[str, Type] = {}
_LOADER_CACHE: Dict[str, Type] = {}


def discover_extractor(variant: str) -> Optional[Type]:
    if variant in _EXTRACTOR_CACHE:
        return _EXTRACTOR_CACHE[variant]

    plugins = _discover_plugins('mkpipe.extractors')
    _EXTRACTOR_CACHE.update(plugins)
    return _EXTRACTOR_CACHE.get(variant)


def discover_loader(variant: str) -> Optional[Type]:
    if variant in _LOADER_CACHE:
        return _LOADER_CACHE[variant]

    plugins = _discover_plugins('mkpipe.loaders')
    _LOADER_CACHE.update(plugins)
    return _LOADER_CACHE.get(variant)


def get_extractor(variant: str) -> Type:
    from ..spark.base import BaseExtractor

    cls = BaseExtractor._registry.get(variant)
    if not cls:
        cls = discover_extractor(variant)
    if not cls:
        raise ValueError(
            f"No extractor found for variant: '{variant}'. "
            f"Install the appropriate plugin package, e.g. "
            f"'pip install mkpipe-extractor-{variant}'"
        )
    return cls


def get_loader(variant: str) -> Type:
    from ..spark.base import BaseLoader

    cls = BaseLoader._registry.get(variant)
    if not cls:
        cls = discover_loader(variant)
    if not cls:
        raise ValueError(
            f"No loader found for variant: '{variant}'. "
            f"Install the appropriate plugin package, e.g. "
            f"'pip install mkpipe-loader-{variant}'"
        )
    return cls
