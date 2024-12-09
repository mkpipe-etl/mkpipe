import os
import shutil
import json
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark import SparkConf
from ..config import ROOT_DIR
from ..utils import Logger

logger = Logger(__file__)


def create_spark_session(settings):
    conf = SparkConf()
    conf.setAppName(__file__)
    conf.setMaster('local[*]')
    conf.set('spark.driver.memory', settings.spark_driver_memory)
    conf.set('spark.executor.memory', settings.spark_executor_memory)
    conf.set('spark.driver.extraClassPath', settings.jars_path)
    conf.set('spark.network.timeout', '600s')
    conf.set('spark.sql.parquet.datetimeRebaseModeInRead', 'CORRECTED')
    conf.set('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED')
    conf.set('spark.sql.parquet.int96RebaseModeInRead', 'CORRECTED')
    conf.set('spark.sql.parquet.int96RebaseModeInWrite', 'CORRECTED')
    conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    conf.set('spark.kryoserializer.buffer.max', '1g')
    # conf.set("spark.sql.shuffle.partitions", settings.partitions_count)

    # Dynamic allocation settings
    conf.set(
        'spark.executor.memoryOverhead', '1g'
    )  # 512 MB for each executor (adjust as needed)
    conf.set('spark.dynamicAllocation.enabled', 'true')
    conf.set(
        'spark.dynamicAllocation.minExecutors', '1'
    )  # Minimum executors (adjustable)
    conf.set(
        'spark.dynamicAllocation.maxExecutors', '2'
    )  # Maximum executors (adjustable)
    conf.set(
        'spark.dynamicAllocation.initialExecutors', '1'
    )  # Starting number of executors
    conf.set(
        'spark.driver.extraJavaOptions',
        '-XX:ErrorFile=/tmp/java_error%p.log -XX:HeapDumpPath=/tmp',
    )

    return SparkSession.builder.config(conf=conf).getOrCreate()


def remove_partitioned_parquet(directory_path):
    """
    Deletes all files and subdirectories in the given directory_path.

    :param directory_path: The root directory of the partitioned Parquet files to delete.
    """
    try:
        # Check if the directory exists
        if os.path.exists(directory_path) and os.path.isdir(directory_path):
            # Remove the entire directory tree
            shutil.rmtree(directory_path)
            logger.info(
                {'message': f'Partitioned Parquet files deleted from {directory_path}'}
            )
        else:
            logger.warning(
                {'message': f'The directory {directory_path} does not exist.'}
            )
    except Exception as e:
        logger.error({'message': f'Error deleting partitioned Parquet files: {e}'})


def write_schema(schema, table_name):
    folder_path = os.path.abspath(os.path.join(ROOT_DIR, 'artifacts', 'schemas'))
    # schema = df.schema.json()
    json_object = json.dumps(schema, indent=4)
    schema_path = os.path.join(folder_path, f'schema_{table_name}.json')
    with open(schema_path, 'w') as f:
        f.write(json_object)
    return


def read_schema(table_name):
    folder_path = os.path.abspath(os.path.join(ROOT_DIR, 'artifacts', 'schemas'))
    schema_path = os.path.join(folder_path, f'schema_{table_name}.json')
    with open(schema_path, 'r') as f:
        schema_map = json.load(f)
    schema = StructType.fromJson(schema_map)
    return schema
