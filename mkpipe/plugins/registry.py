import importlib.metadata


def discover_plugins(group):
    """
    Discover plugins registered under a specific entry point group.
    :param group: Entry point group name (e.g., 'mkpipe.extractors')
    :return: Dictionary of plugin names and their corresponding classes
    """
    try:
        entry_points = importlib.metadata.entry_points(group=group)
        return {ep.name: ep.load() for ep in entry_points}
    except Exception as e:
        print(f'Error discovering plugins: {e}')
        return {}


# Example usage
EXTRACTOR_GROUP = 'mkpipe.extractors'
LOADER_GROUP = 'mkpipe.loaders'

EXTRACTORS = discover_plugins(EXTRACTOR_GROUP)
LOADERS = discover_plugins(LOADER_GROUP)


def get_loader(variant):
    if variant not in LOADERS:
        raise ValueError(f'Unsupported loader type: {variant}')
    return LOADERS.get(variant)


def get_extractor(variant):
    if variant not in EXTRACTORS:
        raise ValueError(f'Unsupported extractor type: {variant}')
    return EXTRACTORS.get(variant)
