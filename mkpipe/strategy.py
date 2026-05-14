from __future__ import annotations

from .models import ExtractResult, TableConfig, WriteStrategy


def resolve_write_strategy(table: TableConfig, data: ExtractResult) -> WriteStrategy:
    """Resolve effective write strategy.

    When the table has an explicit ``write_strategy`` (e.g. upsert or merge),
    that strategy is always honoured — even on a full extract.  Upsert/merge
    operations use ``ON CONFLICT … DO UPDATE`` (or the dialect equivalent)
    which safely handles duplicates without requiring a prior truncate.

    When no explicit strategy is configured and the extractor signals
    ``write_mode='overwrite'`` (full extract / last-point reset), the
    strategy is set to REPLACE so the target table is refreshed.

    As a last resort, fall back to APPEND.
    """
    if table.write_strategy is not None:
        return table.write_strategy
    if data.write_mode == 'overwrite':
        return WriteStrategy.REPLACE
    return WriteStrategy.APPEND
