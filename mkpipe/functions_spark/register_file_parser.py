from .parquet_functions import create_spark_session
from ..config import (
    partitions_count,
)


PARSERS = {}


def register_parser(file_type):
    def decorator(fn):
        PARSERS[file_type] = fn
        return fn

    return decorator


def parse_parquet(data,settings):
    file_path = data['path']
    custom_partition_count = data.get('partition_count', partitions_count)
    spark = create_spark_session(settings)
    df = spark.read.parquet(file_path)
    # df = spark.read.parquet(file_path).repartition(custom_partition_count)
    return df


@register_parser('parquet')
def parqut_parser(data,settings):
    return parse_parquet(data,settings)


def get_parser(file_type):
    return PARSERS.get(file_type)
