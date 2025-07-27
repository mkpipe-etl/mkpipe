from pydantic import BaseModel
from typing import Optional
import psutil


def get_container_memory_limit():
    """Get memory limit from cgroups (for Docker/K8s)"""
    try:
        with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as f:
            return int(f.read()) // (1024 * 1024)
    except Exception:
        return psutil.virtual_memory().total // (1024 * 1024)


total_mem = get_container_memory_limit()
driver_mem = f'{max(1024, int(total_mem * 0.2))}m'
executor_mem = f'{max(2048, int(total_mem * 0.6))}m'


class PipeSettings(BaseModel):
    timezone: str = 'UTC'
    compression_codec: str = 'zstd'  # Options: snappy, gzip, zstd, lz4, none
    spark_driver_memory: str = driver_mem
    spark_executor_memory: str = executor_mem
    partitions_count: int = 10
    ROOT_DIR: str
    driver_name: Optional[str] = None


class InputTask(BaseModel):
    extractor_variant: str
    table_extract_conf: dict
    loader_variant: str
    table_load_conf: dict
    priority: Optional[int] = None
    data: Optional[dict] = None
    settings: PipeSettings
