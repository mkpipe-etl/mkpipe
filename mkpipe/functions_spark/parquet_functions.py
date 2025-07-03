import os
import shutil
import json
from pyspark.sql.types import StructType
from ..config import ROOT_DIR
from ..utils import Logger


def remove_partitioned_parquet(directory_path):
    """
    Deletes all files and subdirectories in the given directory_path.

    :param directory_path: The root directory of the partitioned Parquet files to delete.
    """
    logger = Logger(__file__)
    try:
        # Check if the directory exists
        if os.path.exists(directory_path) and os.path.isdir(directory_path):
            # Remove the entire directory tree
            shutil.rmtree(directory_path)
            logger.info({'message': f'Partitioned Parquet files deleted from {directory_path}'})
        else:
            logger.warning({'message': f'The directory {directory_path} does not exist.'})
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
