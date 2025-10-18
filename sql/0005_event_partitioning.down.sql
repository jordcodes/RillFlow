-- Revert the events table to its non-partitioned form.

drop trigger if exists rf_events_redirect_archived_trg on events;

alter table if exists events rename to events_partitioned;
alter sequence if exists events_global_seq_seq rename to events_global_seq_seq_part;

create sequence events_global_seq_seq;

create table events (
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
    retention_class text not null default 'hot'
        check (retention_class in ('hot', 'cold')),
    primary key (global_seq),
    unique (stream_id, stream_seq)
);

insert into events (
    global_seq,
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
    retention_class
)
select
    global_seq,
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
    retention_class
from events_partitioned
order by global_seq;

select setval(
    'events_global_seq_seq',
    coalesce((select max(global_seq) from events), 0) + 1,
    true
);

drop table events_partitioned cascade;
drop sequence events_global_seq_seq_part;

create unique index if not exists events_idemp_key_uq
    on events ((headers ->> 'idempotency_key'))
    where headers ? 'idempotency_key';

create index if not exists events_stream_idx
    on events (stream_id, stream_seq);

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
