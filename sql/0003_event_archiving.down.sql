drop trigger if exists rf_events_redirect_archived_trg on events;
drop function if exists rf_events_redirect_archived();

drop function if exists rf_archive_set_redirect(boolean);
drop function if exists rf_archive_redirect_enabled();
drop table if exists rf_archive_config;

drop index if exists events_archive_stream_created_idx;
alter table if exists events_archive
    drop column if exists migrated_at;

drop trigger if exists rf_streams_touch_updated_at_trg on rf_streams;
drop function if exists rf_streams_touch_updated_at();
drop index if exists rf_streams_retention_idx;
drop table if exists rf_streams;
