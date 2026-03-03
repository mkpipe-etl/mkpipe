import importlib
import importlib.metadata
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

_ENTRY_POINT_GROUPS = ['mkpipe.extractors', 'mkpipe.loaders']


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


def discover_maven_packages(group_list: List[str]) -> List[str]:
    packages = set()

    for group in group_list:
        try:
            for entry_point in importlib.metadata.entry_points(group=group):
                try:
                    plugin = entry_point.load()
                    module_name = plugin.__module__
                    module = importlib.import_module(module_name)
                    jar_packages = getattr(module, 'JAR_PACKAGES', None)
                    if jar_packages:
                        packages.update(jar_packages)
                except Exception:
                    pass
        except Exception:
            pass

    return sorted(packages)


def collect_jars() -> Tuple[str, str]:
    """Collect local JARs and Maven packages for Spark session.

    Local JARs in a plugin's ``jars/`` directory are always included via
    ``spark.jars``.  Maven coordinates from ``JAR_PACKAGES`` are included via
    ``spark.jars.packages`` **unless** the plugin's local jars already satisfy
    them (i.e. a JAR whose name starts with the artifact-id exists locally).
    This allows custom JARs (e.g. mkpipe-tls-helper.jar) to coexist with
    Maven-resolved dependencies.
    """
    jar_paths: List[str] = []
    jar_names: set = set()
    maven_packages: set = set()

    for group in _ENTRY_POINT_GROUPS:
        try:
            for entry_point in importlib.metadata.entry_points(group=group):
                try:
                    plugin = entry_point.load()
                    module_name = plugin.__module__
                    module = importlib.import_module(module_name)
                    module_path = Path(module.__file__).parent
                    jars_dir = module_path / 'jars'
                    local_jars = list(jars_dir.glob('*.jar')) if jars_dir.exists() else []

                    # Always collect local JARs
                    for jar in local_jars:
                        if jar.name not in jar_names:
                            jar_paths.append(str(jar))
                            jar_names.add(jar.name)

                    # Collect Maven packages, skip if already resolved locally
                    local_jar_names = {j.name for j in local_jars}
                    pkg_list = getattr(module, 'JAR_PACKAGES', None)
                    if pkg_list:
                        for pkg in pkg_list:
                            artifact_id = pkg.split(':')[1] if ':' in pkg else ''
                            already_local = any(
                                artifact_id in name for name in local_jar_names
                            )
                            if not already_local:
                                maven_packages.add(pkg)
                except Exception:
                    pass
        except Exception:
            pass

    return ','.join(sorted(jar_paths)), ','.join(sorted(maven_packages))


def _discover_plugin_jar_info(group_list: List[str]) -> Dict[str, List[str]]:
    """Return {plugin_jars_dir: [maven_coordinates]} for all installed plugins."""
    result: Dict[str, List[str]] = {}

    for group in group_list:
        try:
            for entry_point in importlib.metadata.entry_points(group=group):
                try:
                    plugin = entry_point.load()
                    module_name = plugin.__module__
                    module = importlib.import_module(module_name)
                    jar_packages = getattr(module, 'JAR_PACKAGES', None)
                    if not jar_packages:
                        continue
                    module_path = Path(module.__file__).parent
                    jars_dir = str(module_path / 'jars')
                    if jars_dir not in result:
                        result[jars_dir] = []
                    for pkg in jar_packages:
                        if pkg not in result[jars_dir]:
                            result[jars_dir].append(pkg)
                except Exception:
                    pass
        except Exception:
            pass

    return result


def download_jars() -> None:
    """Download all Maven JARs for installed plugins into their jars/ directories.

    Use this for offline/air-gapped environments (e.g. Docker build).
    After running, plugins will use the local JARs instead of Maven resolution.
    """
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    plugin_info = _discover_plugin_jar_info(_ENTRY_POINT_GROUPS)
    if not plugin_info:
        print('No plugins with JAR_PACKAGES found.')
        return

    all_packages = set()
    for pkgs in plugin_info.values():
        all_packages.update(pkgs)

    print(f'Found {len(all_packages)} Maven package(s) across {len(plugin_info)} plugin(s).')

    ivy2_dir = tempfile.mkdtemp(prefix='mkpipe_ivy2_')
    ivy2_jars = Path(ivy2_dir) / 'jars'

    try:
        print(f'Downloading JARs via Spark (ivy cache: {ivy2_dir})...')

        conf = SparkConf()
        conf.setAppName('mkpipe-install-jars')
        conf.setMaster('local[1]')
        conf.set('spark.jars.packages', ','.join(sorted(all_packages)))
        conf.set('spark.jars.ivy', ivy2_dir)
        conf.set('spark.ui.enabled', 'false')

        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        spark.stop()

        if not ivy2_jars.exists():
            print(f'ERROR: No JARs downloaded to {ivy2_jars}')
            return

        downloaded = list(ivy2_jars.glob('*.jar'))
        print(f'Downloaded {len(downloaded)} JAR file(s).')

        for jars_dir in plugin_info:
            dest = Path(jars_dir)
            dest.mkdir(parents=True, exist_ok=True)
            copied = 0
            for jar in downloaded:
                shutil.copy2(str(jar), str(dest / jar.name))
                copied += 1
            print(f'  -> {dest}: {copied} JAR(s) copied')

        print('Done. Plugins will use local JARs at runtime.')

    finally:
        shutil.rmtree(ivy2_dir, ignore_errors=True)
