drop index if exists events_retention_created_idx;
drop index if exists events_retention_stream_idx;

alter table if exists events
    drop column if exists retention_class;
