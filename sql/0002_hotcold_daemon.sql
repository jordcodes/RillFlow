-- Hot/Cold daemon coordination table
create table if not exists rf_daemon_nodes (
    daemon_id uuid not null,
    cluster text not null,
    node_name text not null,
    role text not null default 'cold',
    heartbeat_at timestamptz not null default now(),
    lease_until timestamptz not null,
    lease_token uuid null,
    min_cold_standbys integer not null default 0,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (cluster, daemon_id)
);

create unique index if not exists rf_daemon_nodes_node_uq
    on rf_daemon_nodes (cluster, node_name);

create unique index if not exists rf_daemon_nodes_hot_role_uq
    on rf_daemon_nodes (cluster)
    where role = 'hot';

create index if not exists rf_daemon_nodes_lease_idx
    on rf_daemon_nodes (cluster, lease_until);
