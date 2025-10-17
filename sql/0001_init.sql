-- documents
create table if not exists docs (
    tenant_id text null,
    id uuid primary key,
    doc jsonb not null,
    version int not null default 0,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    deleted_at timestamptz null,
    created_by text null,
    last_modified_by text null,
    docs_search tsvector null
);

create index if not exists docs_gin on docs using gin (doc);

create index if not exists docs_fts_idx on docs using gin (docs_search);

create or replace function rf_docs_search_update() returns trigger as $$
begin
  new.docs_search := jsonb_to_tsvector('english', new.doc, '["all"]');
  return new;
end;
$$ language plpgsql;

do $$
begin
  if not exists (
    select 1 from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
    where t.tgname = 'rf_docs_fts_biu' and c.relname = 'docs'
  ) then
    create trigger rf_docs_fts_biu before insert or update of doc on docs for each row execute function rf_docs_search_update();
  end if;
end$$;

-- document change history
create table if not exists docs_history (
    hist_id bigserial primary key,
    tenant_id text null,
    id uuid not null,
    version int not null,
    doc jsonb not null,
    modified_at timestamptz not null default now(),
    modified_by text null,
    op text not null
);

create or replace function rf_docs_history() returns trigger as $$
begin
  if TG_OP = 'UPDATE' then
    insert into docs_history(id, version, doc, modified_at, modified_by, op)
    values (OLD.id, OLD.version, OLD.doc, now(), current_user, 'UPDATE');

return NEW;

elsif TG_OP = 'DELETE' then
insert into
    docs_history (
        id,
        version,
        doc,
        modified_at,
        modified_by,
        op
    )
values (
        OLD.id,
        OLD.version,
        OLD.doc,
        now(),
        current_user,
        'DELETE'
    );

return OLD;

else return NEW;

end if;

end;

$$ language plpgsql;

do $$
begin
  if not exists (
    select 1 from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
    where t.tgname = 'rf_docs_history_update' and c.relname = 'docs'
  ) then
    create trigger rf_docs_history_update after update on docs for each row execute function rf_docs_history();
  end if;
  if not exists (
    select 1 from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
    where t.tgname = 'rf_docs_history_delete' and c.relname = 'docs'
  ) then
    create trigger rf_docs_history_delete after delete on docs for each row execute function rf_docs_history();
  end if;
end$$;

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
    event_version int not null default 1,
    tenant_id text null,
    user_id text null,
    is_tombstone boolean not null default false,
    created_at timestamptz not null default now(),
    unique (stream_id, stream_seq)
);

-- archived events (moved from events)
create table if not exists events_archive (
    global_seq bigint primary key,
    stream_id uuid not null,
    stream_seq int not null,
    event_type text not null,
    body jsonb not null,
    headers jsonb not null default '{}'::jsonb,
    causation_id uuid null,
    correlation_id uuid null,
    event_version int not null default 1,
    tenant_id text null,
    user_id text null,
    is_tombstone boolean not null default false,
    created_at timestamptz not null
);

-- optional idempotency: unique header key if present (partial unique index)
create unique index if not exists events_idemp_key_uq on events (
    (headers ->> 'idempotency_key')
)
where (headers ? 'idempotency_key');

-- stream aliases (human key to stream_id)
create table if not exists stream_aliases (
    tenant_id text null,
    alias text primary key,
    stream_id uuid not null,
    created_at timestamptz not null default now()
);

-- projection checkpoints
create table if not exists projections (
    tenant_id text null,
    name text primary key,
    last_seq bigint not null default 0,
    updated_at timestamptz not null default now()
);

-- event schema registry
create table if not exists event_schemas (
    event_type text not null,
    version int not null,
    schema jsonb not null,
    created_at timestamptz not null default now(),
    primary key(event_type, version)
);

-- snapshots
create table if not exists snapshots (
    tenant_id text null,
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

-- subscription consumer groups (per-group checkpoints)
create table if not exists subscription_groups (
    name text not null,
    grp text not null,
    last_seq bigint not null default 0,
    paused boolean not null default false,
    backoff_until timestamptz null,
    max_in_flight int null,
    updated_at timestamptz not null default now(),
    primary key (name, grp)
);

-- backfill: add max_in_flight if table already existed
alter table if exists subscription_groups
add column if not exists max_in_flight int null;

-- subscription group leases (single worker per group)
create table if not exists subscription_group_leases (
    name text not null,
    grp text not null,
    leased_by text not null,
    lease_until timestamptz not null,
    updated_at timestamptz not null default now(),
    primary key (name, grp)
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
