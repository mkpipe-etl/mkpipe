from importlib.metadata import entry_points
from pathlib import Path
import importlib

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

def discover_jar_paths(group_list):
    """
    Discover all JAR files from installed mkpipe plugins for the given groups.
    Deduplicates the JAR paths based on the JAR filename.
    """
    jar_paths = set()  # Use a set to avoid duplicates
    jar_names = set()  # Set to track unique JAR filenames

    for group in group_list:
        # print(f"Processing group: {group}")
        for entry_point in entry_points(group=group):
            # print("entry", entry_point)
            try:
                # Load the entry point and get the module where it's defined
                plugin = entry_point.load()
                module_name = plugin.__module__  # Get the module name
                module = importlib.import_module(module_name)
                module_path = Path(module.__file__).parent  # Get the module's directory
                jars_dir = module_path / 'jars'  # Locate the jars directory
                if jars_dir.exists():
                    # Add JAR paths if the filename is not already in the set
                    for jar in jars_dir.glob('*.jar'):
                        if jar.name not in jar_names:
                            jar_paths.add(str(jar))
                            jar_names.add(jar.name)
            except Exception as e:
                print(f'Error loading entry point {entry_point.name}: {e}')
    return sorted(jar_paths)  # Return as a sorted list for consistency


def collect_jars():
    """
    Collect JARs from all plugin groups, deduplicate based on filename, and return their paths.
    """
    group_list = ['mkpipe.extractors', 'mkpipe.loaders']
    jar_paths = discover_jar_paths(group_list)
    # print(f"Collected JAR paths: {jar_paths}")

    str_jar_paths = ','.join(jar_paths)
    print(str_jar_paths)
    return str_jar_paths


