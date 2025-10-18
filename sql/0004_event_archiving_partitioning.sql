-- Partition backend scaffolding
--
-- Adds a retention classification column to the primary events table so deployments
-- can steer hot/cold routing without relying on the separate `events_archive` table.
-- Operators opting into PostgreSQL native partitioning should use this column as the
-- LIST partition key (e.g. values in ('hot', 'cold')) and may optionally create
-- RANGE sub-partitions on `created_at`.

alter table if exists events
    add column if not exists retention_class text not null default 'hot'
        check (retention_class in ('hot', 'cold'));

comment on column events.retention_class
    is 'Hot/cold classification used by the partition archive backend (hot by default)';

create index if not exists events_retention_stream_idx
    on events (retention_class, stream_id, stream_seq);

create index if not exists events_retention_created_idx
    on events (retention_class, created_at);
