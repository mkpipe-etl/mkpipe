from __future__ import annotations

from .models import ExtractResult, TableConfig, WriteStrategy


def resolve_write_strategy(table: TableConfig, data: ExtractResult) -> WriteStrategy:
    """Resolve effective write strategy.

    When the extractor signals ``write_mode='overwrite'`` (full extract,
    e.g. because the last-point was reset), the strategy is forced to
    REPLACE regardless of the configured ``write_strategy``.  This avoids
    expensive upsert/merge operations on a full dataset and prevents
    stale rows from remaining in the target table.

    Otherwise, if the table has an explicit ``write_strategy``, use it.
    As a last resort, infer from ``ExtractResult.write_mode``.
    """
    if data.write_mode == 'overwrite':
        return WriteStrategy.REPLACE
    if table.write_strategy is not None:
        return table.write_strategy
    return WriteStrategy.APPEND
