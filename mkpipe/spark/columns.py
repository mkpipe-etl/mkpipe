from datetime import datetime
from typing import List, Optional

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import TimestampType


def add_etl_columns(df: DataFrame, etl_time: datetime, dedup_columns: Optional[List[str]] = None) -> DataFrame:
    if 'etl_time' in df.columns:
        df = df.drop('etl_time')
    df = df.withColumn('etl_time', F.lit(etl_time).cast(TimestampType()))

    if dedup_columns:
        hash_cols = [F.coalesce(F.col(c).cast('string'), F.lit('')) for c in dedup_columns]
        df = df.withColumn('mkpipe_id', F.xxhash64(*hash_cols))

    return df
