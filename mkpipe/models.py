from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ReplicationMethod(str, Enum):
    FULL = 'full'
    INCREMENTAL = 'incremental'


class TableConfig(BaseModel):
    name: str
    target_name: str
    replication_method: ReplicationMethod = ReplicationMethod.FULL
    iterate_column: Optional[str] = None
    iterate_column_type: Optional[str] = None
    partitions_column: Optional[str] = None
    partitions_count: int = 10
    fetchsize: int = 100_000
    batchsize: int = 10_000
    write_partitions: Optional[int] = None
    custom_query: Optional[str] = None
    custom_query_file: Optional[str] = None
    transform: Optional[str] = None
    pass_on_error: bool = False
    dedup_columns: Optional[List[str]] = None


class ConnectionConfig(BaseModel):
    variant: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    warehouse: Optional[str] = None
    private_key_file: Optional[str] = None
    private_key_file_pwd: Optional[str] = None
    mongo_uri: Optional[str] = None
    collection: Optional[str] = None
    bucket_name: Optional[str] = None
    s3_prefix: Optional[str] = None
    aws_access_key: Optional[str] = None
    aws_secret_key: Optional[str] = None
    region: Optional[str] = None
    api_key: Optional[str] = None
    oauth_token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    credentials_file: Optional[str] = None
    extra: Dict[str, Any] = Field(default_factory=dict)


class SparkConfig(BaseModel):
    master: Optional[str] = None
    driver_memory: Optional[str] = None
    executor_memory: Optional[str] = None
    extra_config: Dict[str, str] = Field(default_factory=dict)


class BackendConfig(BaseModel):
    variant: str = 'sqlite'
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    extra: Dict[str, Any] = Field(default_factory=dict)


class SettingsConfig(BaseModel):
    timezone: str = 'UTC'
    backend: BackendConfig = Field(default_factory=BackendConfig)
    spark: SparkConfig = Field(default_factory=SparkConfig)


class PipelineConfig(BaseModel):
    name: str
    source: str
    destination: str
    tables: List[TableConfig]
    pass_on_error: bool = False


class MkpipeConfig(BaseModel):
    version: int = 2
    settings: SettingsConfig = Field(default_factory=SettingsConfig)
    connections: Dict[str, ConnectionConfig] = Field(default_factory=dict)
    pipelines: List[PipelineConfig] = Field(default_factory=list)


@dataclass
class ExtractResult:
    df: Any = None
    write_mode: str = 'overwrite'
    last_point_value: Optional[str] = None
