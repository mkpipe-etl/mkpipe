import psutil
from typing import Optional

from ..models import SparkConfig


def _get_container_memory_limit():
    try:
        with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as f:
            return int(f.read()) // (1024 * 1024)
    except Exception:
        return psutil.virtual_memory().total // (1024 * 1024)


def _default_memory():
    total_mem = _get_container_memory_limit()
    driver = f'{max(1024, int(total_mem * 0.2))}m'
    executor = f'{max(2048, int(total_mem * 0.6))}m'
    return driver, executor


def create_spark_session(
    config: Optional[SparkConfig] = None,
    jars: str = '',
    timezone: str = 'UTC',
    app_name: str = 'mkpipe',
):
    from pyspark.sql import SparkSession
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

    driver_java_options = (
        f'-Duser.timezone={timezone} '
        '-XX:ErrorFile=/tmp/java_error%p.log '
        '-XX:HeapDumpPath=/tmp '
    )
    executor_java_options = f'-Duser.timezone={timezone} '

    conf = SparkConf()
    conf.setAppName(app_name)
    conf.setMaster(master)
    conf.set('spark.driver.memory', driver_memory)
    conf.set('spark.executor.memory', executor_memory)

    if jars:
        conf.set('spark.jars', jars)
        conf.set('spark.driver.extraClassPath', jars)
        conf.set('spark.executor.extraClassPath', jars)

    conf.set('spark.network.timeout', '600s')
    conf.set('spark.sql.parquet.datetimeRebaseModeInRead', 'CORRECTED')
    conf.set('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED')
    conf.set('spark.sql.parquet.int96RebaseModeInRead', 'CORRECTED')
    conf.set('spark.sql.parquet.int96RebaseModeInWrite', 'CORRECTED')
    conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    conf.set('spark.kryoserializer.buffer.max', '1g')
    conf.set('spark.sql.session.timeZone', timezone)
    conf.set('spark.driver.extraJavaOptions', driver_java_options)
    conf.set('spark.executor.extraJavaOptions', executor_java_options)

    for key, value in extra_config.items():
        conf.set(key, value)

    return SparkSession.builder.config(conf=conf).getOrCreate()
