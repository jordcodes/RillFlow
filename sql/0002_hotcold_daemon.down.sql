drop index if exists rf_daemon_nodes_lease_idx;
drop index if exists rf_daemon_nodes_hot_role_uq;
drop index if exists rf_daemon_nodes_node_uq;
alter table if exists rf_daemon_nodes drop column if exists lease_token;
alter table if exists rf_daemon_nodes drop column if exists min_cold_standbys;
drop table if exists rf_daemon_nodes;
