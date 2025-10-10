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
