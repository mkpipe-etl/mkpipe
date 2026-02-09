import os
from pathlib import Path
from typing import Optional, Union

from .config import load_config
from .models import ExtractResult, MkpipeConfig, TableConfig
from .utils import get_logger

logger = get_logger(__name__)


def _get_config(config: Union[str, Path, MkpipeConfig]) -> MkpipeConfig:
    if isinstance(config, MkpipeConfig):
        return config
    return load_config(config)


def _ensure_spark(spark, cfg: MkpipeConfig):
    if spark is not None:
        return spark

    from .plugins.jars import collect_jars
    from .spark.session import create_spark_session

    jars = collect_jars()
    return create_spark_session(
        config=cfg.settings.spark,
        jars=jars,
        timezone=cfg.settings.timezone,
    )


def _create_backend(cfg: MkpipeConfig):
    from .backend.base import BackendBase

    backend_dict = cfg.settings.backend.model_dump()
    return BackendBase.create(backend_dict)


def _run_table(
    extractor,
    loader,
    table: TableConfig,
    backend,
    spark,
    pipeline_name: str,
    transform_fn=None,
):
    target_name = table.target_name
    replication_method = table.replication_method.value
    pass_on_error = table.pass_on_error

    try:
        status = backend.get_table_status(pipeline_name, target_name)
        if status in ('extracting', 'loading'):
            logger.info({
                'table': target_name,
                'status': 'skipped',
                'reason': 'already in progress',
            })
            return

        # --- EXTRACT ---
        backend.manifest_table_update(
            pipeline_name=pipeline_name,
            table_name=target_name,
            status='extracting',
            replication_method=replication_method,
            error_message='',
        )

        last_point = backend.get_last_point(pipeline_name, target_name)
        data = extractor.extract(table, spark, last_point=last_point)

        # --- TRANSFORM ---
        if data.df is not None and transform_fn is not None:
            logger.info({'table': target_name, 'status': 'transforming'})
            data.df = transform_fn(data.df)

        # --- LOAD ---
        if data.df is not None:
            backend.manifest_table_update(
                pipeline_name=pipeline_name,
                table_name=target_name,
                status='loading',
                replication_method=replication_method,
                error_message='',
            )
            loader.load(table, data, spark)

        # --- COMPLETED ---
        backend.manifest_table_update(
            pipeline_name=pipeline_name,
            table_name=target_name,
            value=data.last_point_value,
            value_type=table.iterate_column_type,
            status='completed',
            replication_method=replication_method,
            error_message='',
        )

        logger.info({
            'table': target_name,
            'pipeline': pipeline_name,
            'status': 'completed',
        })

    except Exception as e:
        error_msg = str(e)
        logger.error({
            'table': target_name,
            'pipeline': pipeline_name,
            'status': 'failed',
            'error': error_msg,
        })

        try:
            backend.manifest_table_update(
                pipeline_name=pipeline_name,
                table_name=target_name,
                status='failed',
                replication_method=replication_method,
                error_message=error_msg,
            )
        except Exception:
            pass

        if not pass_on_error:
            raise


def run(
    config: Union[str, Path, MkpipeConfig],
    pipeline: Optional[str] = None,
    table: Optional[str] = None,
    spark=None,
):
    cfg = _get_config(config)
    spark = _ensure_spark(spark, cfg)
    backend = _create_backend(cfg)

    pipelines = cfg.pipelines
    if pipeline:
        pipelines = [p for p in pipelines if p.name == pipeline]
        if not pipelines:
            raise ValueError(
                f"Pipeline '{pipeline}' not found. "
                f"Available: {[p.name for p in cfg.pipelines]}"
            )

    from .plugins.registry import get_extractor, get_loader

    for pipe in pipelines:
        source_conn = cfg.connections.get(pipe.source)
        if not source_conn:
            raise ValueError(
                f"Connection '{pipe.source}' not found for pipeline '{pipe.name}'"
            )
        dest_conn = cfg.connections.get(pipe.destination)
        if not dest_conn:
            raise ValueError(
                f"Connection '{pipe.destination}' not found for pipeline '{pipe.name}'"
            )

        extractor_cls = get_extractor(source_conn.variant)
        loader_cls = get_loader(dest_conn.variant)

        extractor = extractor_cls(connection=source_conn)
        loader = loader_cls(connection=dest_conn)

        tables = pipe.tables
        if table:
            tables = [
                t for t in tables
                if t.name == table or t.target_name == table
            ]

        for tbl in tables:
            transform_fn = None
            if tbl.transform:
                from .transform.loader import load_transform_fn
                config_dir = None
                if isinstance(config, (str, Path)):
                    config_dir = str(Path(config).parent)
                transform_fn = load_transform_fn(tbl.transform, base_dir=config_dir)

            effective_pass = tbl.pass_on_error or pipe.pass_on_error
            tbl_with_pass = tbl.model_copy(update={'pass_on_error': effective_pass})

            _run_table(
                extractor=extractor,
                loader=loader,
                table=tbl_with_pass,
                backend=backend,
                spark=spark,
                pipeline_name=pipe.name,
                transform_fn=transform_fn,
            )


def extract(
    config: Union[str, Path, MkpipeConfig],
    table: str,
    pipeline: Optional[str] = None,
    spark=None,
) -> ExtractResult:
    cfg = _get_config(config)
    spark = _ensure_spark(spark, cfg)

    from .plugins.registry import get_extractor

    backend = _create_backend(cfg)

    for pipe in cfg.pipelines:
        if pipeline and pipe.name != pipeline:
            continue

        source_conn = cfg.connections.get(pipe.source)
        if not source_conn:
            continue

        for tbl in pipe.tables:
            if tbl.name == table or tbl.target_name == table:
                extractor_cls = get_extractor(source_conn.variant)
                extractor = extractor_cls(connection=source_conn)
                last_point = backend.get_last_point(pipe.name, tbl.target_name)
                return extractor.extract(tbl, spark, last_point=last_point)

    raise ValueError(f"Table '{table}' not found in any pipeline")


def load(
    config: Union[str, Path, MkpipeConfig],
    table: str,
    df,
    write_mode: str = 'overwrite',
    pipeline: Optional[str] = None,
    spark=None,
) -> None:
    cfg = _get_config(config)
    spark = _ensure_spark(spark, cfg)

    from .plugins.registry import get_loader

    for pipe in cfg.pipelines:
        if pipeline and pipe.name != pipeline:
            continue

        dest_conn = cfg.connections.get(pipe.destination)
        if not dest_conn:
            continue

        for tbl in pipe.tables:
            if tbl.name == table or tbl.target_name == table:
                loader_cls = get_loader(dest_conn.variant)
                loader = loader_cls(connection=dest_conn)
                data = ExtractResult(df=df, write_mode=write_mode)
                loader.load(tbl, data, spark)
                return

    raise ValueError(f"Table '{table}' not found in any pipeline")
