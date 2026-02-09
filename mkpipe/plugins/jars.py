import importlib
import importlib.metadata
from pathlib import Path
from typing import List


def discover_jar_paths(group_list: List[str]) -> List[str]:
    jar_paths = set()
    jar_names = set()

    for group in group_list:
        try:
            for entry_point in importlib.metadata.entry_points(group=group):
                try:
                    plugin = entry_point.load()
                    module_name = plugin.__module__
                    module = importlib.import_module(module_name)
                    module_path = Path(module.__file__).parent
                    jars_dir = module_path / 'jars'
                    if jars_dir.exists():
                        for jar in jars_dir.glob('*.jar'):
                            if jar.name not in jar_names:
                                jar_paths.add(str(jar))
                                jar_names.add(jar.name)
                except Exception:
                    pass
        except Exception:
            pass

    return sorted(jar_paths)


def collect_jars() -> str:
    group_list = ['mkpipe.extractors', 'mkpipe.loaders']
    jar_paths = discover_jar_paths(group_list)
    return ','.join(jar_paths)
