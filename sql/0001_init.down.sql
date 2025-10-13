-- Rollback for 0001_init.sql
-- Assumes search_path is set to the target schema (and public) by the caller

-- Drop triggers and functions (docs full-text search)
do $$
begin
  if exists (
    select 1 from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
    where t.tgname = 'rf_docs_fts_biu' and c.relname = 'docs'
  ) then
    execute 'drop trigger rf_docs_fts_biu on docs';
  end if;
end$$;

drop function if exists rf_docs_search_update();

-- Drop triggers and function (docs history)
do $$
begin
  if exists (
    select 1 from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
    where t.tgname = 'rf_docs_history_update' and c.relname = 'docs'
  ) then
    execute 'drop trigger rf_docs_history_update on docs';

end if;

if exists (
    select 1
    from pg_trigger t
        join pg_class c on c.oid = t.tgrelid
    where
        t.tgname = 'rf_docs_history_delete'
        and c.relname = 'docs'
) then
execute 'drop trigger rf_docs_history_delete on docs';

end if;

end $$;

drop function if exists rf_docs_history ();

-- Drop trigger and function (events notify)
do $$
begin
  if exists (
    select 1 from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
    where t.tgname = 'rf_events_notify' and c.relname = 'events'
  ) then
    execute 'drop trigger rf_events_notify on events';
  end if;
end$$;

drop function if exists rf_notify_event();

-- Drop indexes (safe if not present)
drop index if exists docs_fts_idx;
drop index if exists docs_gin;
drop index if exists events_idemp_key_uq;

-- Drop tables in dependency-safe order
drop table if exists subscription_dlq;
drop table if exists subscription_group_leases;
drop table if exists subscription_groups;
drop table if exists subscriptions;
drop table if exists snapshots;
drop table if exists projections;
drop table if exists stream_aliases;
drop table if exists events_archive;
drop table if exists events;
drop table if exists docs_history;
drop table if exists event_schemas;
drop table if exists docs;
