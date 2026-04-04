from __future__ import annotations

from .models import ExtractResult, TableConfig, WriteStrategy


def resolve_write_strategy(table: TableConfig, data: ExtractResult) -> WriteStrategy:
    """Resolve effective write strategy.

    If table has explicit write_strategy, use it.
    Otherwise, infer from ExtractResult.write_mode for backward compatibility.
    """
    if table.write_strategy is not None:
        return table.write_strategy
    if data.write_mode == 'overwrite':
        return WriteStrategy.REPLACE
    return WriteStrategy.APPEND
