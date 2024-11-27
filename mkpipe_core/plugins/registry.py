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
        print(f"Error discovering plugins: {e}")
        return {}

# Example usage
EXTRACTOR_GROUP = "mkpipe.extractors"
LOADER_GROUP = "mkpipe.loaders"

EXTRACTORS = discover_plugins(EXTRACTOR_GROUP)
LOADERS = discover_plugins(LOADER_GROUP)
