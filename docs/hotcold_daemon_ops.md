# Hot/Cold Projection Daemon Operations

This guide aggregates everything operators need to roll out and operate the high-availability projection daemon introduced in `sql/0002_hotcold_daemon.sql`.

## Prerequisites

- Postgres 13+ with advisory locks enabled (default).
- Rillflow binaries built from the hot/cold branch (contains Phase 1–10 changes).
- Ability to run at least two daemon processes (one hot, ≥1 cold standby).

## Migration

1. Ensure migrations are up to date:

   ```bash
   cargo sqlx migrate run --database-url "$DATABASE_URL"
   ```

   This adds `rf_daemon_nodes` plus supporting indexes and the optional `lease_token` column.

2. The migration is additive. Rollback uses `sql/0002_hotcold_daemon.down.sql`, which drops the coordination table and indexes.

## Configuration

- `--cluster-mode hotcold`: enables HA.
- `--cluster-name <name>`: required when `hotcold` is selected; pick a shared identifier per logical cluster.
- Timing knobs:
  - `--heartbeat-secs` (default 5s)
  - `--lease-ttl-secs` (default 30s)
  - `--lease-grace-secs` (default 5s)
  - `--min-cold-standbys` (enforced via metrics/alerts; exported on every node and surfaced in cluster health output).
- Optional overrides:
  - `--node-name` to make log output/metrics readable.
  - `--daemon-id` to reuse an existing row during rolling restarts.

## Rollout Procedure

1. **Deploy schema** (see Migration above).
2. **Launch first daemon** in hot/cold mode:

   ```bash
   cargo run --bin rillflow -- projections run-loop \
     --cluster-mode hotcold \
     --cluster-name billing-projections \
     --health-bind 0.0.0.0:8080 \
     --database-url "$DATABASE_URL"
   ```

   This node becomes the initial leader.

3. **Launch standby nodes** with the same cluster name:

   ```bash
   cargo run --bin rillflow -- projections run-loop \
     --cluster-mode hotcold \
     --cluster-name billing-projections \
     --node-name billing-daemon-2 \
     --database-url "$DATABASE_URL"
   ```

   Standbys remain cold until promotion.

4. **Verify cluster state**:
   - CLI: `cargo run --bin rillflow -- projections cluster-status --database-url "$DATABASE_URL"`
   - Manual failover tooling:
     - Promote a standby: `cargo run --bin rillflow -- projections promote --cluster billing-projections --node billing-daemon-2 --database-url "$DATABASE_URL"`
     - Demote the current leader (or a specific node): `cargo run --bin rillflow -- projections demote --cluster billing-projections --database-url "$DATABASE_URL"`
   - Prometheus: scrape `/metrics` and confirm `daemon_cluster_healthy{cluster="billing-projections"} == 1`.
   - JSON health: `curl http://<host>:8080/` to view per-node heartbeat/lease data and `min_cold_standbys` targets.

5. **Rolling restart / replacement**:
   - Stop the hot node (`SIGINT`); it demotes itself and notifies others.
   - Confirm a cold node promotes (`cluster-status`, metrics, or logs).
   - Start the replacement node; it joins as cold automatically.

## Verification & Smoke Tests

- **Functional**: `cargo test --test projection_run_loop hot_node_failover_promotes_standby` (requires Docker running locally).
- **Chaos**: `cargo test --test projection_run_loop rapid_failover_stress_executes_all_events`.
- **Unit**: `cargo test --lib metrics::tests::snapshot_daemon_clusters_groups_nodes`.

Run these tests in CI nightly or before major upgrades.

## Observability & Alerting

- Metrics to scrape:
  - `daemon_cluster_healthy{cluster="..."}` (1=healthy, 0=problem). Page on sustained zero.
  - `daemon_cluster_standby_nodes{cluster="..."}` and `daemon_cluster_required_standbys{cluster="..."}` (warn if standby < required).
  - `daemon_node_hot{cluster="...",node="..."}` (per-node leadership).
  - `daemon_node_heartbeat_age_seconds` and `daemon_node_lease_seconds` (lag monitoring).
  - `daemon_cluster_hot_nodes` (should be 1).
- CLI visibility: `projections cluster-status` prints operator-friendly summaries, including lease/heartbeat ages.
- Logs:
  - Promotions/demotions emit structured logs; aggregate to detect flapping.

## Rollback

1. Stop additional daemons; revert to a single `cluster-mode single` process (or stop all processes).
2. Drop `rf_daemon_nodes` using `sql/0002_hotcold_daemon.down.sql` (only if required).
3. Revert configuration changes as needed.

## Operational Tips

- Choose `lease_ttl` slightly above the longest projection batch time; keep `lease_grace` minimal (2–5s) to bound failover.
- For noisy workloads, lower `heartbeat-secs` to detect failures faster.
- Set `--min_cold_standbys` to the number of cold nodes you expect online; surface alerts via `daemon_cluster_required_standbys` vs `daemon_cluster_standby_nodes`.
- In staging, run the chaos test to calibrate timing knobs before production rollout.
- Maintain at least two standbys in production; configure alerting if `daemon_cluster_nodes < desired`.
- Metrics cache evicts nodes whose lease expired more than 5s ago, preventing ghost entries after a crash.
