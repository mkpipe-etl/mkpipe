from datetime import datetime
from typing import List, Optional

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import TimestampType

_VALID_CASE_OPTIONS = ('lower', 'upper', 'as_is')


def add_etl_columns(
    df: DataFrame,
    etl_time: datetime,
    dedup_columns: Optional[List[str]] = None,
    ingested_at_column: str = '_ingested_at',
    ingestion_id_column: str = 'mkpipe_id',
) -> DataFrame:
    if ingested_at_column in df.columns:
        df = df.drop(ingested_at_column)
    df = df.withColumn(ingested_at_column, F.lit(etl_time).cast(TimestampType()))

    if dedup_columns:
        hash_cols = [F.coalesce(F.col(c).cast('string'), F.lit('')) for c in dedup_columns]
        df = df.withColumn(ingestion_id_column, F.xxhash64(*hash_cols))

    return df


def normalize_column_names(df: DataFrame, case: str = 'as_is') -> DataFrame:
    """Rename all DataFrame columns to the specified case.

    Args:
        df: Input DataFrame.
        case: One of ``'lower'``, ``'upper'``, or ``'as_is'``.

    Returns:
        DataFrame with renamed columns.
    """
    if case == 'as_is':
        return df

    if case == 'lower':
        return df.toDF(*[c.lower() for c in df.columns])
    if case == 'upper':
        return df.toDF(*[c.upper() for c in df.columns])

    return df
