from datetime import datetime
from typing import List, Optional

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import TimestampType


def add_etl_columns(
    df: DataFrame,
    etl_time: datetime,
    dedup_columns: Optional[List[str]] = None,
    ingested_at_column: str = '_ingested_at',
) -> DataFrame:
    if ingested_at_column in df.columns:
        df = df.drop(ingested_at_column)
    df = df.withColumn(ingested_at_column, F.lit(etl_time).cast(TimestampType()))

    if dedup_columns:
        hash_cols = [F.coalesce(F.col(c).cast('string'), F.lit('')) for c in dedup_columns]
        df = df.withColumn('mkpipe_id', F.xxhash64(*hash_cols))

    return df
