from importlib.metadata import entry_points
from pathlib import Path
import importlib


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
    # print(str_jar_paths)
    return str_jar_paths


if __name__ == '__main__':
    collect_jars()
