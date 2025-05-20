from pyspark.sql import SparkSession
from pyspark import SparkConf
from ..plugins.registry_jar import collect_jars


def create_spark_session(settings):
    jars = collect_jars()

    conf = SparkConf()
    conf.setAppName(settings.driver_name)
    conf.setMaster('local[*]')
    conf.set('spark.driver.memory', settings.spark_driver_memory)
    conf.set('spark.executor.memory', settings.spark_executor_memory)
    conf.set('spark.jars', jars)  # Distribute jars
    conf.set('spark.driver.extraClassPath', jars)
    conf.set('spark.executor.extraClassPath', jars)
    conf.set('spark.network.timeout', '600s')
    conf.set('spark.sql.parquet.datetimeRebaseModeInRead', 'CORRECTED')
    conf.set('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED')
    conf.set('spark.sql.parquet.int96RebaseModeInRead', 'CORRECTED')
    conf.set('spark.sql.parquet.int96RebaseModeInWrite', 'CORRECTED')
    conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    conf.set('spark.kryoserializer.buffer.max', '1g')

    # Dynamic allocation settings
    conf.set('spark.dynamicAllocation.enabled', 'true')
    conf.set('spark.dynamicAllocation.minExecutors', '1')  # Minimum executors (adjustable)
    conf.set('spark.dynamicAllocation.maxExecutors', '2')  # Maximum executors (adjustable)
    conf.set('spark.dynamicAllocation.initialExecutors', '1')  # Starting number of executors
    conf.set('spark.sql.session.timeZone', settings.timezone)
    conf.set(
        'spark.executor.extraJavaOptions',
        f'-Duser.timezone={settings.timezone}',
    )
    conf.set(
        'spark.driver.extraJavaOptions',
        f'-Duser.timezone={settings.timezone} -XX:ErrorFile=/tmp/java_error%p.log -XX:HeapDumpPath=/tmp',
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark
