from pyspark.sql import SparkSession
from pyspark import SparkConf
from ..plugins.registry_jar import collect_jars


def create_spark_session(settings):
    jars = collect_jars()

    # --- Safely append the flag to existing Java options ---
    driver_java_options = (
        f'-Duser.timezone={settings.timezone} '
        '-XX:ErrorFile=/tmp/java_error%p.log '
        '-XX:HeapDumpPath=/tmp '
    )

    executor_java_options = f'-Duser.timezone={settings.timezone} '

    conf = SparkConf()
    conf.setAppName(settings.driver_name)
    conf.setMaster('local[*]')
    conf.set('spark.driver.memory', settings.spark_driver_memory)
    conf.set('spark.executor.memory', settings.spark_executor_memory)
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

    # Dynamic allocation settings
    conf.set('spark.dynamicAllocation.enabled', 'true')
    conf.set('spark.dynamicAllocation.minExecutors', '1')
    conf.set('spark.dynamicAllocation.maxExecutors', '2')
    conf.set('spark.dynamicAllocation.initialExecutors', '1')
    conf.set('spark.sql.session.timeZone', settings.timezone)

    # Set the updated Java options
    conf.set('spark.driver.extraJavaOptions', driver_java_options)
    conf.set('spark.executor.extraJavaOptions', executor_java_options)

    # Add JDBC-specific logging configurations
    conf.set('spark.sql.sources.bucketing.enabled', 'true')
    conf.set('spark.sql.shuffle.partitions', '4')  # Reduce for local mode
    conf.set('spark.logConf', 'true')

    # Build the SparkSession
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger('org.apache.spark.sql.execution.datasources.jdbc').setLevel(
        logger.Level.DEBUG
    )

    return spark
