-- documents
create table if not exists docs (
    id uuid primary key,
    doc jsonb not null,
    version int not null default 0,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists docs_gin on docs using gin (doc);

-- events
create table if not exists events (
    global_seq bigserial primary key,
    stream_id uuid not null,
    stream_seq int not null,
    event_type text not null,
    body jsonb not null,
    headers jsonb not null default '{}'::jsonb,
    causation_id uuid null,
    correlation_id uuid null,
    created_at timestamptz not null default now(),
    unique (stream_id, stream_seq)
);

-- projection checkpoints
create table if not exists projections (
    name text primary key,
    last_seq bigint not null default 0,
    updated_at timestamptz not null default now()
);

-- snapshots
create table if not exists snapshots (
    stream_id uuid primary key,
    version int not null,
    body jsonb not null,
    created_at timestamptz not null default now()
);

-- subscriptions (consumer checkpoints)
create table if not exists subscriptions (
    name text primary key,
    last_seq bigint not null default 0,
    filter jsonb not null default '{}'::jsonb,
    paused boolean not null default false,
    backoff_until timestamptz null,
    updated_at timestamptz not null default now()
);

-- subscription dead-letter queue
create table if not exists subscription_dlq (
    id bigserial primary key,
    name text not null,
    global_seq bigint not null,
    event_type text not null,
    body jsonb not null,
    error text not null,
    failed_at timestamptz not null default now()
);

-- NOTIFY wakeups for new events (optional listener can subscribe to this channel)
create or replace function rf_notify_event() returns trigger as $$
begin
  perform pg_notify('rillflow_events', new.global_seq::text);
  return null;
end;
$$ language plpgsql;

do $$
begin
  if not exists (
    select 1 from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
    where t.tgname = 'rf_events_notify' and c.relname = 'events'
  ) then
    execute 'create trigger rf_events_notify after insert on events for each row execute function rf_notify_event()';
  end if;
end$$;
