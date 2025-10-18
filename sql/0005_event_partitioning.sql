-- Convert the events table to a partitioned layout (hot/cold + monthly buckets).

-- Disable redirect trigger while we swap tables.
drop trigger if exists rf_events_redirect_archived_trg on events;

-- Preserve existing data in a legacy table/sequence for backfill.
alter table if exists events rename to events_legacy;

do $$
declare
    seq_name text;
begin
    select pg_get_serial_sequence('events_legacy', 'global_seq') into seq_name;
    if seq_name is not null then
        execute format('alter sequence %s rename to events_global_seq_seq_legacy', seq_name);
    end if;
end;
$$;

create sequence events_global_seq_seq;

create table events (
    retention_class text not null default 'hot'
        check (retention_class in ('hot', 'cold')),
    global_seq bigint not null default nextval('events_global_seq_seq'),
    stream_id uuid not null,
    stream_seq int not null,
    event_type text not null,
    body jsonb not null,
    headers jsonb not null default '{}'::jsonb,
    causation_id uuid null,
    correlation_id uuid null,
    event_version int not null default 1,
    user_id text null,
    is_tombstone boolean not null default false,
    created_at timestamptz not null default now(),
    primary key (retention_class, created_at, global_seq)
) partition by list (retention_class);

create table events_hot
    partition of events
    for values in ('hot')
    partition by range (created_at);

create table events_cold
    partition of events
    for values in ('cold')
    partition by range (created_at);

create table events_hot_default partition of events_hot default;
create table events_cold_default partition of events_cold default;

insert into events (
    retention_class,
    stream_id,
    stream_seq,
    event_type,
    body,
    headers,
    causation_id,
    correlation_id,
    event_version,
    user_id,
    is_tombstone,
    created_at,
    global_seq
)
select
    coalesce(retention_class, 'hot'),
    stream_id,
    stream_seq,
    event_type,
    body,
    headers,
    causation_id,
    correlation_id,
    event_version,
    user_id,
    is_tombstone,
    created_at,
    global_seq
from events_legacy
order by global_seq;

select setval(
    'events_global_seq_seq',
    coalesce((select max(global_seq) from events), 0) + 1,
    true
);

drop table events_legacy;
drop sequence if exists events_global_seq_seq_legacy;

create unique index if not exists events_stream_partition_uq
    on events (retention_class, created_at, stream_id, stream_seq);

create unique index if not exists events_idemp_key_uq
    on events (retention_class, created_at, (headers ->> 'idempotency_key'))
    where headers ? 'idempotency_key';

create index if not exists events_stream_retention_idx
    on events (retention_class, stream_id, stream_seq);

do $$
begin
    if exists (select 1 from pg_proc where proname = 'rf_events_redirect_archived') then
        execute '
            create trigger rf_events_redirect_archived_trg
                before insert on events
                for each row
                execute function rf_events_redirect_archived()
        ';
    end if;
end;
$$;
