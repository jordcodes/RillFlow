-- Stream lifecycle metadata and cold storage redirect scaffolding

-- Track per-stream archival metadata and retention state.
create table if not exists rf_streams (
    stream_id uuid primary key,
    tenant_id text null,
    archived_at timestamptz null,
    archived_by text null,
    archive_reason text null,
    retention_class text not null default 'hot'
        check (retention_class in ('hot', 'cold')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists rf_streams_retention_idx
    on rf_streams (retention_class, archived_at);

-- Keep `updated_at` fresh on write.
create or replace function rf_streams_touch_updated_at() returns trigger as $$
begin
    new.updated_at := now();
    return new;
end;
$$ language plpgsql;

do $$
begin
    if not exists (
        select 1
        from pg_trigger t
        join pg_class c on c.oid = t.tgrelid
        where t.tgname = 'rf_streams_touch_updated_at_trg'
          and c.relname = 'rf_streams'
    ) then
        create trigger rf_streams_touch_updated_at_trg
            before update on rf_streams
            for each row
            execute function rf_streams_touch_updated_at();
    end if;
end;
$$;

-- Cold storage table enhancements.
alter table if exists events_archive
    add column if not exists migrated_at timestamptz not null default now();

create index if not exists events_archive_stream_created_idx
    on events_archive (stream_id, created_at);

-- Configuration toggle for redirect behaviour.
create table if not exists rf_archive_config (
    singleton boolean primary key default true,
    redirect_enabled boolean not null default false,
    updated_at timestamptz not null default now()
);

insert into rf_archive_config (singleton)
values (true)
on conflict (singleton) do nothing;

create or replace function rf_archive_redirect_enabled() returns boolean as $$
declare
    enabled boolean;
begin
    select redirect_enabled
      into enabled
      from rf_archive_config
     where singleton;
    return coalesce(enabled, false);
end;
$$ language plpgsql stable;

create or replace function rf_archive_set_redirect(enabled boolean) returns void as $$
begin
    insert into rf_archive_config (singleton, redirect_enabled, updated_at)
    values (true, enabled, now())
    on conflict (singleton) do update
        set redirect_enabled = excluded.redirect_enabled,
            updated_at = now();
end;
$$ language plpgsql;

-- Redirect archived streams into cold storage before they hit the hot table.
create or replace function rf_events_redirect_archived() returns trigger as $$
declare
    stream_retention record;
    use_cold boolean := false;
begin
    if not rf_archive_redirect_enabled() then
        return new;
    end if;

    select retention_class, archived_at
      into stream_retention
      from rf_streams
     where stream_id = new.stream_id
     limit 1;

    if stream_retention is null then
        return new;
    end if;

    use_cold := (stream_retention.archived_at is not null)
             or (stream_retention.retention_class = 'cold');

    if not use_cold then
        return new;
    end if;

    insert into events_archive (
        global_seq,
        stream_id,
        stream_seq,
        event_type,
        body,
        headers,
        causation_id,
        correlation_id,
        event_version,
        tenant_id,
        user_id,
        is_tombstone,
        created_at,
        migrated_at
    )
    values (
        new.global_seq,
        new.stream_id,
        new.stream_seq,
        new.event_type,
        new.body,
        new.headers,
        new.causation_id,
        new.correlation_id,
        new.event_version,
        new.tenant_id,
        new.user_id,
        new.is_tombstone,
        new.created_at,
        now()
    )
    on conflict (global_seq) do update
        set stream_id = excluded.stream_id,
            stream_seq = excluded.stream_seq,
            event_type = excluded.event_type,
            body = excluded.body,
            headers = excluded.headers,
            causation_id = excluded.causation_id,
            correlation_id = excluded.correlation_id,
            event_version = excluded.event_version,
            tenant_id = excluded.tenant_id,
            user_id = excluded.user_id,
            is_tombstone = excluded.is_tombstone,
            created_at = excluded.created_at,
            migrated_at = excluded.migrated_at;

    -- Swallow the insert into the hot table.
    return null;
end;
$$ language plpgsql;

do $$
begin
    if not exists (
        select 1
        from pg_trigger t
        join pg_class c on c.oid = t.tgrelid
        where t.tgname = 'rf_events_redirect_archived_trg'
          and c.relname = 'events'
    ) then
        create trigger rf_events_redirect_archived_trg
            before insert on events
            for each row
            execute function rf_events_redirect_archived();
    end if;
end;
$$;
