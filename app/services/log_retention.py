from typing import Any, Optional, Sequence, Tuple

from sqlalchemy import and_, asc, delete, desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.orm.attributes import InstrumentedAttribute

from app.db.models import AmsStreamStatusHistory, AmsSyncLog, LogEntry

DELETE_BATCH_SIZE = 1000


async def _trim_group(
    session: AsyncSession,
    model: Any,
    identity_conditions: Sequence[ColumnElement[bool]],
    timestamp_column: InstrumentedAttribute,
    retention_limit: int,
    commit_batches: bool,
) -> int:
    cutoff_stmt = (
        select(timestamp_column, model.id)
        .where(*identity_conditions)
        .order_by(desc(timestamp_column), desc(model.id))
        .offset(retention_limit - 1)
        .limit(1)
    )
    cutoff = (await session.execute(cutoff_stmt)).first()
    if cutoff is None:
        return 0

    cutoff_ts, cutoff_id = cutoff
    older_than_cutoff = or_(
        timestamp_column < cutoff_ts,
        and_(timestamp_column == cutoff_ts, model.id < cutoff_id),
    )
    deleted_count = 0

    while True:
        batch_stmt = (
            select(model.id)
            .where(*identity_conditions, older_than_cutoff)
            .order_by(asc(timestamp_column), asc(model.id))
            .limit(DELETE_BATCH_SIZE)
        )
        ids = (await session.execute(batch_stmt)).scalars().all()
        if not ids:
            break
        await session.execute(delete(model).where(model.id.in_(ids)))
        deleted_count += len(ids)
        if commit_batches:
            await session.commit()

    return deleted_count


async def retain_log_entries(
    session: AsyncSession,
    retention_limit: int,
    groups: Optional[Sequence[Tuple[Optional[str], str]]] = None,
    commit_batches: bool = False,
) -> int:
    if groups is None:
        result = await session.execute(
            select(LogEntry.app_name, LogEntry.stream_id)
            .where(LogEntry.stream_id.isnot(None))
            .distinct()
        )
        groups = result.all()

    deleted_count = 0
    for app_name, stream_id in groups:
        if not stream_id:
            continue
        conditions = [
            LogEntry.app_name.is_(None)
            if app_name is None
            else LogEntry.app_name == app_name,
            LogEntry.stream_id == stream_id,
        ]
        deleted_count += await _trim_group(
            session,
            LogEntry,
            conditions,
            LogEntry.ts,
            retention_limit,
            commit_batches,
        )
    return deleted_count


async def retain_sync_logs(
    session: AsyncSession,
    retention_limit: int,
    app_names: Optional[Sequence[str]] = None,
    commit_batches: bool = False,
) -> int:
    if app_names is None:
        result = await session.execute(
            select(AmsSyncLog.app_name)
            .where(AmsSyncLog.app_name.isnot(None))
            .distinct()
        )
        app_names = [row[0] for row in result.all()]

    deleted_count = 0
    for app_name in set(app_names):
        conditions = [AmsSyncLog.app_name == app_name]
        deleted_count += await _trim_group(
            session,
            AmsSyncLog,
            conditions,
            AmsSyncLog.created_at,
            retention_limit,
            commit_batches,
        )
    return deleted_count


async def retain_status_history(
    session: AsyncSession,
    retention_limit: int,
    groups: Optional[Sequence[Tuple[str, str]]] = None,
    commit_batches: bool = False,
) -> int:
    if groups is None:
        result = await session.execute(
            select(AmsStreamStatusHistory.app_name, AmsStreamStatusHistory.stream_id)
            .distinct()
        )
        groups = result.all()

    deleted_count = 0
    for app_name, stream_id in groups:
        conditions = [
            AmsStreamStatusHistory.app_name == app_name,
            AmsStreamStatusHistory.stream_id == stream_id,
        ]
        deleted_count += await _trim_group(
            session,
            AmsStreamStatusHistory,
            conditions,
            AmsStreamStatusHistory.ts,
            retention_limit,
            commit_batches,
        )
    return deleted_count


async def delete_expired_log_entries(
    session: AsyncSession,
    cutoff_ts: int,
    commit_batches: bool = True,
) -> int:
    deleted_count = 0
    while True:
        ids_stmt = (
            select(LogEntry.id)
            .where(LogEntry.ts < cutoff_ts)
            .order_by(asc(LogEntry.ts), asc(LogEntry.id))
            .limit(DELETE_BATCH_SIZE)
        )
        ids = (await session.execute(ids_stmt)).scalars().all()
        if not ids:
            break
        await session.execute(delete(LogEntry).where(LogEntry.id.in_(ids)))
        deleted_count += len(ids)
        if commit_batches:
            await session.commit()
    return deleted_count
