import logging
import os
import random
import time
from typing import Dict, Optional

import psutil

from ..models import SparkConfig
from ..plugins.jars import comet_jar_paths

logger = logging.getLogger(__name__)

COMET_CONFS = {
    'spark.plugins': 'org.apache.spark.CometPlugin',
    'spark.shuffle.manager': 'org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager',
    'spark.comet.explain.fallback.enabled': 'true',
    # mkpipe sets KryoSerializer; Comet requires its own registrator.
    'spark.kryo.registrator': 'org.apache.comet.CometKryoRegistrator',
}


def _get_container_memory_limit():
    for path in (
        '/sys/fs/cgroup/memory.max',
        '/sys/fs/cgroup/memory/memory.limit_in_bytes',
    ):
        try:
            with open(path, 'r') as f:
                value = f.read().strip()
                if value not in ('max', ''):
                    return int(value) // (1024 * 1024)
        except Exception:
            continue
    return psutil.virtual_memory().total // (1024 * 1024)


def _default_memory():
    total_mem = _get_container_memory_limit()
    driver = f'{max(1024, int(total_mem * 0.2))}m'
    executor = f'{max(2048, int(total_mem * 0.6))}m'
    return driver, executor


def _merge_comet_config(
    extra_config: Dict[str, str],
    packages: str,
    jars: str = '',
) -> Dict[str, str]:
    """Merge Comet defaults into extra_config.

    The Comet package is appended to the effective ``spark.jars.packages``
    value (extra_config takes precedence over the ``packages`` param, matching
    the existing override order). User-provided extra_config values win over
    the Comet defaults. When the Comet JAR is already on the classpath via
    ``spark.jars`` (pre-downloaded by ``mkpipe install-jars``), Maven
    resolution is skipped.
    """
    from ..plugins.jars import comet_package

    merged = {**COMET_CONFS, **extra_config}
    base_packages = extra_config.get('spark.jars.packages') or packages
    if 'comet-spark' in jars or 'comet-spark' in base_packages:
        # Comet JAR served locally, or the user already pinned their own
        # Comet coordinate in extra_config — keep it as-is.
        if base_packages:
            merged['spark.jars.packages'] = base_packages
    else:
        comet_pkg = comet_package()
        merged['spark.jars.packages'] = (
            f'{base_packages},{comet_pkg}' if base_packages else comet_pkg
        )
    return merged


def _create_with_retry(
    conf,
    max_attempts: int = 3,
    base_delay: float = 5.0,
):
    """Create a SparkSession with retry and jittered backoff.

    When multiple mkpipe processes start simultaneously (e.g. via Dagster
    multiprocess executor), JVM startup causes heavy CPU contention.
    Some JVMs fail to send their gateway port in time, raising
    ``JAVA_GATEWAY_EXITED``.  Retrying with random delay staggers the
    JVM startups and resolves the issue.
    """
    from pyspark.sql import SparkSession

    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            session = SparkSession.builder.config(conf=conf).getOrCreate()
            if attempt > 1:
                logger.info(
                    'SparkSession created on attempt %d/%d',
                    attempt,
                    max_attempts,
                )
            return session
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            # Jittered backoff: base_delay * attempt + random 0-3s
            delay = base_delay * attempt + random.uniform(0, 3)
            logger.warning(
                'SparkSession creation failed (attempt %d/%d): %s. Retrying in %.1fs...',
                attempt,
                max_attempts,
                exc,
                delay,
            )
            time.sleep(delay)

    raise RuntimeError(
        f'Failed to create SparkSession after {max_attempts} attempts'
    ) from last_error


def create_spark_session(
    config: Optional[SparkConfig] = None,
    jars: str = '',
    packages: str = '',
    timezone: str = 'UTC',
    app_name: str = 'mkpipe',
):
    from pyspark import SparkConf

    default_driver_mem, default_executor_mem = _default_memory()

    master = 'local[*]'
    driver_memory = default_driver_mem
    executor_memory = default_executor_mem
    extra_config = {}

    if config:
        master = config.master or 'local[*]'
        driver_memory = config.driver_memory or default_driver_mem
        executor_memory = config.executor_memory or default_executor_mem
        extra_config = config.extra_config or {}

    if config and config.comet:
        extra_config = _merge_comet_config(extra_config, packages, jars)
        comet_packages = extra_config.get('spark.jars.packages')
        if comet_packages:
            logger.info('Comet enabled (packages: %s)', comet_packages)
        else:
            logger.info('Comet enabled (local JAR via spark.jars)')

    driver_java_options = (
        f'-Duser.timezone={timezone} -XX:ErrorFile=/tmp/java_error%p.log -XX:HeapDumpPath=/tmp '
    )
    executor_java_options = f'-Duser.timezone={timezone} '

    conf = SparkConf()
    conf.setAppName(app_name)
    conf.setMaster(master)
    conf.set('spark.driver.memory', driver_memory)
    conf.set('spark.executor.memory', executor_memory)

    if jars:
        conf.set('spark.jars', jars)
        # Only the Comet JAR goes on the driver/executor classpath: Comet
        # accesses package-private Spark internals and must share the app
        # classloader with Spark core classes. Plugin JARs stay in the task
        # classloader only — on the app classpath their bundled libraries
        # (e.g. jackson) shadow Spark's own and break session init.
        comet_jars = comet_jar_paths(jars)
        if comet_jars:
            classpath = os.pathsep.join(comet_jars)
            conf.set('spark.driver.extraClassPath', classpath)
            conf.set('spark.executor.extraClassPath', classpath)

    if packages:
        conf.set('spark.jars.packages', packages)

    conf.set('spark.network.timeout', '600s')
    conf.set('spark.sql.parquet.datetimeRebaseModeInRead', 'CORRECTED')
    conf.set('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED')
    conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    conf.set('spark.kryoserializer.buffer.max', '1g')
    conf.set('spark.sql.session.timeZone', timezone)
    conf.set('spark.driver.extraJavaOptions', driver_java_options)
    conf.set('spark.executor.extraJavaOptions', executor_java_options)

    for key, value in extra_config.items():
        conf.set(key, value)

    return _create_with_retry(conf)
