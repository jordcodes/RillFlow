use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};

pub struct Metrics {
    // Documents
    pub doc_reads_total: AtomicU64,
    pub doc_writes_total: AtomicU64,
    pub doc_conflicts_total: AtomicU64,

    // Projections
    pub proj_events_processed_total: AtomicU64,

    // Subscriptions
    pub subs_delivered_total: AtomicU64,
    pub subs_pending_gauge: AtomicU64,

    // Snapshotter
    pub snapshot_candidates_gauge: AtomicU64,
    pub snapshot_max_gap_gauge: AtomicU64,

    // Events
    pub event_appends_total: AtomicU64,
    pub event_conflicts_total: AtomicU64,

    // Pool
    pub pool_total_gauge: AtomicU64,
    pub pool_active_gauge: AtomicU64,
    pub pool_idle_gauge: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            doc_reads_total: AtomicU64::new(0),
            doc_writes_total: AtomicU64::new(0),
            doc_conflicts_total: AtomicU64::new(0),
            proj_events_processed_total: AtomicU64::new(0),
            subs_delivered_total: AtomicU64::new(0),
            subs_pending_gauge: AtomicU64::new(0),
            snapshot_candidates_gauge: AtomicU64::new(0),
            snapshot_max_gap_gauge: AtomicU64::new(0),
            event_appends_total: AtomicU64::new(0),
            event_conflicts_total: AtomicU64::new(0),
            pool_total_gauge: AtomicU64::new(0),
            pool_active_gauge: AtomicU64::new(0),
            pool_idle_gauge: AtomicU64::new(0),
        }
    }
}

static METRICS: OnceLock<Metrics> = OnceLock::new();
static TENANT_METRICS: OnceLock<Mutex<HashMap<String, TenantCounters>>> = OnceLock::new();
static QUERY_DURATIONS: OnceLock<Mutex<HashMap<String, (u64, u64)>>> = OnceLock::new();
static SLOW_QUERY_COUNT: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
static SLOW_QUERY_THRESHOLD_MS: OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();
static SLOW_QUERY_EXPLAIN: OnceLock<std::sync::atomic::AtomicBool> = OnceLock::new();
#[allow(clippy::type_complexity)]
static PROJECTION_STATS: OnceLock<Mutex<HashMap<String, (u64, u64, u64)>>> = OnceLock::new();
static DAEMON_NODE_METRICS: OnceLock<Mutex<HashMap<(String, String), DaemonNodeMetric>>> =
    OnceLock::new();

#[derive(Default, Clone)]
struct TenantCounters {
    doc_reads_total: u64,
    doc_writes_total: u64,
    doc_conflicts_total: u64,
    proj_events_processed_total: u64,
    subs_delivered_total: u64,
    subs_pending_gauge: u64,
    snapshot_candidates_gauge: u64,
    snapshot_max_gap_gauge: u64,
    event_appends_total: u64,
    event_conflicts_total: u64,
    pool_total_gauge: u64,
    pool_active_gauge: u64,
    pool_idle_gauge: u64,
}

#[derive(Clone)]
struct DaemonNodeMetric {
    is_hot: bool,
    last_heartbeat: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    min_cold_standbys: u32,
}

#[derive(Clone, Debug)]
pub struct DaemonNodeSnapshot {
    pub cluster: String,
    pub node: String,
    pub is_hot: bool,
    pub last_heartbeat: DateTime<Utc>,
    pub lease_until: DateTime<Utc>,
    pub min_cold_standbys: u32,
}

#[derive(Clone, Debug)]
pub struct DaemonClusterSnapshot {
    pub cluster: String,
    pub total_nodes: u64,
    pub hot_nodes: u64,
    pub standby_nodes: u64,
    pub required_standbys: u32,
    pub nodes: Vec<DaemonNodeSnapshot>,
}

pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::default)
}

fn tenant_metrics_map() -> &'static Mutex<HashMap<String, TenantCounters>> {
    TENANT_METRICS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn update_tenant_metrics(tenant: &str, update: impl FnOnce(&mut TenantCounters)) {
    match tenant_metrics_map().lock() {
        Ok(mut guard) => {
            let entry = guard
                .entry(tenant.to_string())
                .or_insert_with(TenantCounters::default);
            update(entry);
        }
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            let entry = guard
                .entry(tenant.to_string())
                .or_insert_with(TenantCounters::default);
            update(entry);
        }
    }
}

fn snapshot_tenant_metrics() -> Vec<(String, TenantCounters)> {
    match tenant_metrics_map().lock() {
        Ok(guard) => guard
            .iter()
            .map(|(tenant, counters)| (tenant.clone(), counters.clone()))
            .collect(),
        Err(poisoned) => {
            let guard = poisoned.into_inner();
            guard
                .iter()
                .map(|(tenant, counters)| (tenant.clone(), counters.clone()))
                .collect()
        }
    }
}

fn node_metrics_map() -> &'static Mutex<HashMap<(String, String), DaemonNodeMetric>> {
    DAEMON_NODE_METRICS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn snapshot_daemon_node_metrics() -> Vec<DaemonNodeSnapshot> {
    let now = Utc::now();
    match node_metrics_map().lock() {
        Ok(mut guard) => {
            guard.retain(|_, metric| metric.lease_until + chrono::Duration::seconds(5) > now);
            guard
                .iter()
                .map(|((cluster, node), metric)| DaemonNodeSnapshot {
                    cluster: cluster.clone(),
                    node: node.clone(),
                    is_hot: metric.is_hot,
                    last_heartbeat: metric.last_heartbeat,
                    lease_until: metric.lease_until,
                    min_cold_standbys: metric.min_cold_standbys,
                })
                .collect()
        }
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            guard.retain(|_, metric| metric.lease_until + chrono::Duration::seconds(5) > now);
            guard
                .iter()
                .map(|((cluster, node), metric)| DaemonNodeSnapshot {
                    cluster: cluster.clone(),
                    node: node.clone(),
                    is_hot: metric.is_hot,
                    last_heartbeat: metric.last_heartbeat,
                    lease_until: metric.lease_until,
                    min_cold_standbys: metric.min_cold_standbys,
                })
                .collect()
        }
    }
}

pub fn snapshot_daemon_clusters() -> Vec<DaemonClusterSnapshot> {
    let mut by_cluster: BTreeMap<String, Vec<DaemonNodeSnapshot>> = BTreeMap::new();
    for node in snapshot_daemon_node_metrics() {
        by_cluster
            .entry(node.cluster.clone())
            .or_default()
            .push(node);
    }

    by_cluster
        .into_iter()
        .map(|(cluster, nodes)| {
            let total_nodes = nodes.len() as u64;
            let hot_nodes = nodes.iter().filter(|n| n.is_hot).count() as u64;
            let standby_nodes = total_nodes.saturating_sub(hot_nodes);
            let required_standbys = nodes.iter().map(|n| n.min_cold_standbys).max().unwrap_or(0);
            DaemonClusterSnapshot {
                cluster,
                total_nodes,
                hot_nodes,
                standby_nodes,
                required_standbys,
                nodes,
            }
        })
        .collect()
}

fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg_attr(not(test), allow(dead_code))]
fn format_duration(duration: chrono::Duration) -> String {
    let millis = duration.num_milliseconds();
    let sign = if millis < 0 { "-" } else { "" };
    let abs_millis = millis.abs();
    if abs_millis >= 60_000 {
        let minutes = abs_millis / 60_000;
        let seconds = (abs_millis % 60_000) / 1000;
        format!("{}{}m{}s", sign, minutes, seconds)
    } else if abs_millis >= 1_000 {
        let secs = abs_millis as f64 / 1_000.0;
        format!("{}{:0.3}s", sign, secs)
    } else {
        format!("{}{}ms", sign, abs_millis)
    }
}

fn write_metric(
    buf: &mut String,
    name: &str,
    kind: &str,
    global_value: u64,
    tenants: &[(String, TenantCounters)],
    getter: fn(&TenantCounters) -> u64,
) {
    let _ = writeln!(buf, "# TYPE {} {}", name, kind);
    let _ = writeln!(buf, "{} {}", name, global_value);
    for (tenant, counters) in tenants {
        let value = getter(counters);
        if value == 0 {
            continue;
        }
        let escaped = escape_label(tenant);
        let _ = writeln!(buf, "{}{{tenant=\"{}\"}} {}", name, escaped, value);
    }
}

fn write_gauge(
    buf: &mut String,
    name: &str,
    global_value: u64,
    tenants: &[(String, TenantCounters)],
    getter: fn(&TenantCounters) -> u64,
) {
    write_metric(buf, name, "gauge", global_value, tenants, getter);
}

pub fn record_doc_read(tenant: Option<&str>) {
    metrics().doc_reads_total.fetch_add(1, Ordering::Relaxed);
    if let Some(t) = tenant {
        update_tenant_metrics(t, |c| c.doc_reads_total += 1);
    }
}

pub fn record_doc_write(tenant: Option<&str>, count: u64) {
    if count == 0 {
        return;
    }
    metrics()
        .doc_writes_total
        .fetch_add(count, Ordering::Relaxed);
    if let Some(t) = tenant {
        update_tenant_metrics(t, |c| c.doc_writes_total += count);
    }
}

pub fn record_doc_conflict(tenant: Option<&str>) {
    metrics()
        .doc_conflicts_total
        .fetch_add(1, Ordering::Relaxed);
    if let Some(t) = tenant {
        update_tenant_metrics(t, |c| c.doc_conflicts_total += 1);
    }
}

pub fn record_proj_events_processed(tenant: Option<&str>, count: u64) {
    if count == 0 {
        return;
    }
    metrics()
        .proj_events_processed_total
        .fetch_add(count, Ordering::Relaxed);
    if let Some(t) = tenant {
        update_tenant_metrics(t, |c| c.proj_events_processed_total += count);
    }
}

pub fn record_subs_delivered(tenant: Option<&str>, count: u64) {
    if count == 0 {
        return;
    }
    metrics()
        .subs_delivered_total
        .fetch_add(count, Ordering::Relaxed);
    if let Some(t) = tenant {
        update_tenant_metrics(t, |c| c.subs_delivered_total += count);
    }
}

pub fn record_subs_pending(tenant: Option<&str>, value: u64) {
    metrics().subs_pending_gauge.store(value, Ordering::Relaxed);
    if let Some(t) = tenant {
        update_tenant_metrics(t, |c| c.subs_pending_gauge = value);
    }
}

pub fn record_snapshot_candidates(tenant: Option<&str>, value: u64) {
    metrics()
        .snapshot_candidates_gauge
        .store(value, Ordering::Relaxed);
    if let Some(t) = tenant {
        update_tenant_metrics(t, |c| c.snapshot_candidates_gauge = value);
    }
}

pub fn record_snapshot_max_gap(tenant: Option<&str>, value: u64) {
    metrics()
        .snapshot_max_gap_gauge
        .store(value, Ordering::Relaxed);
    if let Some(t) = tenant {
        update_tenant_metrics(t, |c| c.snapshot_max_gap_gauge = value);
    }
}

pub fn record_event_appends(tenant: Option<&str>, count: u64) {
    if count == 0 {
        return;
    }
    metrics()
        .event_appends_total
        .fetch_add(count, Ordering::Relaxed);
    if let Some(t) = tenant {
        update_tenant_metrics(t, |c| c.event_appends_total += count);
    }
}

pub fn record_event_conflict(tenant: Option<&str>) {
    metrics()
        .event_conflicts_total
        .fetch_add(1, Ordering::Relaxed);
    if let Some(t) = tenant {
        update_tenant_metrics(t, |c| c.event_conflicts_total += 1);
    }
}

pub fn record_pool_gauges(total: u64, active: u64, idle: u64) {
    metrics().pool_total_gauge.store(total, Ordering::Relaxed);
    metrics().pool_active_gauge.store(active, Ordering::Relaxed);
    metrics().pool_idle_gauge.store(idle, Ordering::Relaxed);
}

pub fn record_daemon_node_state(
    cluster: &str,
    node: &str,
    is_hot: bool,
    last_heartbeat: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    min_cold_standbys: u32,
) {
    let key = (cluster.to_string(), node.to_string());
    match node_metrics_map().lock() {
        Ok(mut guard) => {
            guard.insert(
                key,
                DaemonNodeMetric {
                    is_hot,
                    last_heartbeat,
                    lease_until,
                    min_cold_standbys,
                },
            );
        }
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            guard.insert(
                key,
                DaemonNodeMetric {
                    is_hot,
                    last_heartbeat,
                    lease_until,
                    min_cold_standbys,
                },
            );
        }
    }
}

pub fn clear_daemon_node_state(cluster: &str, node: &str) {
    let key = (cluster.to_string(), node.to_string());
    match node_metrics_map().lock() {
        Ok(mut guard) => {
            guard.remove(&key);
        }
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            guard.remove(&key);
        }
    }
}

pub fn render_prometheus() -> String {
    let m = metrics();
    let tenants = snapshot_tenant_metrics();
    let mut s = String::new();
    write_metric(
        &mut s,
        "doc_reads_total",
        "counter",
        m.doc_reads_total.load(Ordering::Relaxed),
        &tenants,
        |c| c.doc_reads_total,
    );
    write_metric(
        &mut s,
        "doc_writes_total",
        "counter",
        m.doc_writes_total.load(Ordering::Relaxed),
        &tenants,
        |c| c.doc_writes_total,
    );
    write_metric(
        &mut s,
        "doc_conflicts_total",
        "counter",
        m.doc_conflicts_total.load(Ordering::Relaxed),
        &tenants,
        |c| c.doc_conflicts_total,
    );
    write_metric(
        &mut s,
        "proj_events_processed_total",
        "counter",
        m.proj_events_processed_total.load(Ordering::Relaxed),
        &tenants,
        |c| c.proj_events_processed_total,
    );
    write_metric(
        &mut s,
        "subs_delivered_total",
        "counter",
        m.subs_delivered_total.load(Ordering::Relaxed),
        &tenants,
        |c| c.subs_delivered_total,
    );
    write_gauge(
        &mut s,
        "subs_pending_gauge",
        m.subs_pending_gauge.load(Ordering::Relaxed),
        &tenants,
        |c| c.subs_pending_gauge,
    );
    write_gauge(
        &mut s,
        "snapshot_candidates_gauge",
        m.snapshot_candidates_gauge.load(Ordering::Relaxed),
        &tenants,
        |c| c.snapshot_candidates_gauge,
    );
    write_gauge(
        &mut s,
        "snapshot_max_gap_gauge",
        m.snapshot_max_gap_gauge.load(Ordering::Relaxed),
        &tenants,
        |c| c.snapshot_max_gap_gauge,
    );
    write_metric(
        &mut s,
        "event_appends_total",
        "counter",
        m.event_appends_total.load(Ordering::Relaxed),
        &tenants,
        |c| c.event_appends_total,
    );
    write_metric(
        &mut s,
        "event_conflicts_total",
        "counter",
        m.event_conflicts_total.load(Ordering::Relaxed),
        &tenants,
        |c| c.event_conflicts_total,
    );
    write_gauge(
        &mut s,
        "pool_total_gauge",
        m.pool_total_gauge.load(Ordering::Relaxed),
        &tenants,
        |c| c.pool_total_gauge,
    );
    write_gauge(
        &mut s,
        "pool_active_gauge",
        m.pool_active_gauge.load(Ordering::Relaxed),
        &tenants,
        |c| c.pool_active_gauge,
    );
    write_gauge(
        &mut s,
        "pool_idle_gauge",
        m.pool_idle_gauge.load(Ordering::Relaxed),
        &tenants,
        |c| c.pool_idle_gauge,
    );

    let cluster_snapshots = snapshot_daemon_clusters();
    if !cluster_snapshots.is_empty() {
        let now = Utc::now();
        let _ = writeln!(s, "# TYPE daemon_node_hot gauge");
        let _ = writeln!(s, "# TYPE daemon_node_lease_seconds gauge");
        let _ = writeln!(s, "# TYPE daemon_node_heartbeat_age_seconds gauge");
        let _ = writeln!(s, "# TYPE daemon_cluster_nodes gauge");
        let _ = writeln!(s, "# TYPE daemon_cluster_hot gauge");
        let _ = writeln!(s, "# TYPE daemon_cluster_hot_nodes gauge");
        let _ = writeln!(s, "# TYPE daemon_cluster_standby_nodes gauge");
        let _ = writeln!(s, "# TYPE daemon_cluster_required_standbys gauge");
        let _ = writeln!(s, "# TYPE daemon_cluster_healthy gauge");

        for cluster in &cluster_snapshots {
            let escaped_cluster = escape_label(&cluster.cluster);
            let _ = writeln!(
                s,
                "daemon_cluster_nodes{{cluster=\"{}\"}} {}",
                escaped_cluster, cluster.total_nodes
            );
            let _ = writeln!(
                s,
                "daemon_cluster_hot{{cluster=\"{}\"}} {}",
                escaped_cluster,
                if cluster.hot_nodes > 0 { 1 } else { 0 }
            );
            let _ = writeln!(
                s,
                "daemon_cluster_hot_nodes{{cluster=\"{}\"}} {}",
                escaped_cluster, cluster.hot_nodes
            );
            let _ = writeln!(
                s,
                "daemon_cluster_standby_nodes{{cluster=\"{}\"}} {}",
                escaped_cluster, cluster.standby_nodes
            );
            let _ = writeln!(
                s,
                "daemon_cluster_required_standbys{{cluster=\"{}\"}} {}",
                escaped_cluster, cluster.required_standbys
            );
            let healthy = if cluster.hot_nodes == 1
                && cluster.standby_nodes as u32 >= cluster.required_standbys
            {
                1
            } else {
                0
            };
            let _ = writeln!(
                s,
                "daemon_cluster_healthy{{cluster=\"{}\"}} {}",
                escaped_cluster, healthy
            );

            for node in &cluster.nodes {
                let escaped_node = escape_label(&node.node);
                let hot_value = if node.is_hot { 1 } else { 0 };
                let lease_remaining = node
                    .lease_until
                    .signed_duration_since(now)
                    .num_milliseconds() as f64
                    / 1000.0;
                let heartbeat_age_ms = now
                    .signed_duration_since(node.last_heartbeat)
                    .num_milliseconds();
                let heartbeat_age = if heartbeat_age_ms <= 0 {
                    0.0
                } else {
                    heartbeat_age_ms as f64 / 1000.0
                };

                let _ = writeln!(
                    s,
                    "daemon_node_hot{{cluster=\"{}\",node=\"{}\"}} {}",
                    escaped_cluster, escaped_node, hot_value
                );
                let _ = writeln!(
                    s,
                    "daemon_node_lease_seconds{{cluster=\"{}\",node=\"{}\"}} {:.3}",
                    escaped_cluster, escaped_node, lease_remaining
                );
                let _ = writeln!(
                    s,
                    "daemon_node_heartbeat_age_seconds{{cluster=\"{}\",node=\"{}\"}} {:.3}",
                    escaped_cluster, escaped_node, heartbeat_age
                );
            }
        }
    }

    // query durations as summary: per operation name
    if let Ok(map) = QUERY_DURATIONS.get_or_init(Default::default).lock() {
        for (op, (count, sum_ms)) in map.iter() {
            let escaped = escape_label(op);
            let _ = writeln!(
                s,
                "# TYPE query_duration_ms summary\nquery_duration_ms_count{{op=\"{}\"}} {}\nquery_duration_ms_sum{{op=\"{}\"}} {}",
                escaped, count, escaped, sum_ms
            );
        }
    }

    // slow query counters by op
    if let Ok(map) = SLOW_QUERY_COUNT.get_or_init(Default::default).lock() {
        for (op, count) in map.iter() {
            let escaped = escape_label(op);
            let _ = writeln!(
                s,
                "# TYPE slow_queries_total counter\nslow_queries_total{{op=\"{}\"}} {}",
                escaped, count
            );
        }
    }

    // projection stats: total events and batch durations
    if let Ok(map) = PROJECTION_STATS.get_or_init(Default::default).lock() {
        for (name, (events_total, batch_count, batch_sum_ms)) in map.iter() {
            let escaped = escape_label(name);
            let _ = writeln!(
                s,
                "# TYPE projection_events_total counter\nprojection_events_total{{name=\"{}\"}} {}",
                escaped, events_total
            );
            let _ = writeln!(
                s,
                "# TYPE projection_batch_duration_ms summary\nprojection_batch_duration_ms_count{{name=\"{}\"}} {}\nprojection_batch_duration_ms_sum{{name=\"{}\"}} {}",
                escaped, batch_count, escaped, batch_sum_ms
            );
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::{
        clear_daemon_node_state, format_duration, node_metrics_map, record_daemon_node_state,
        snapshot_daemon_clusters,
    };
    use chrono::{Duration as ChronoDuration, Utc};

    #[test]
    fn format_duration_handles_ranges() {
        assert_eq!(
            format_duration(ChronoDuration::milliseconds(1500)),
            "1.500s"
        );
        assert_eq!(
            format_duration(ChronoDuration::milliseconds(65_000)),
            "1m5s"
        );
        assert_eq!(
            format_duration(ChronoDuration::milliseconds(-250)),
            "-250ms"
        );
    }

    #[test]
    fn snapshot_daemon_clusters_groups_nodes() {
        let now = Utc::now();
        {
            let mut guard = node_metrics_map().lock().unwrap();
            guard.clear();
        }

        record_daemon_node_state(
            "alpha",
            "node-a",
            true,
            now,
            now + ChronoDuration::seconds(10),
            2,
        );
        record_daemon_node_state(
            "alpha",
            "node-b",
            false,
            now - ChronoDuration::seconds(5),
            now + ChronoDuration::seconds(8),
            2,
        );
        record_daemon_node_state(
            "beta",
            "node-c",
            false,
            now - ChronoDuration::seconds(2),
            now + ChronoDuration::seconds(5),
            0,
        );

        let clusters = snapshot_daemon_clusters();
        assert_eq!(clusters.len(), 2);

        let alpha = clusters.iter().find(|c| c.cluster == "alpha").unwrap();
        assert_eq!(alpha.total_nodes, 2);
        assert_eq!(alpha.hot_nodes, 1);
        assert_eq!(alpha.standby_nodes, 1);
        assert_eq!(alpha.required_standbys, 2);
        assert!(alpha.nodes.iter().any(|n| n.node == "node-a" && n.is_hot));
        assert!(alpha.nodes.iter().any(|n| n.node == "node-b" && !n.is_hot));

        let beta = clusters.iter().find(|c| c.cluster == "beta").unwrap();
        assert_eq!(beta.total_nodes, 1);
        assert_eq!(beta.hot_nodes, 0);
        assert_eq!(beta.standby_nodes, 1);
        assert_eq!(beta.required_standbys, 0);
        assert!(beta.nodes.iter().any(|n| n.node == "node-c" && !n.is_hot));

        clear_daemon_node_state("alpha", "node-a");
        clear_daemon_node_state("alpha", "node-b");
        clear_daemon_node_state("beta", "node-c");
        let clusters = snapshot_daemon_clusters();
        assert!(clusters.is_empty());
    }
}

pub fn record_query_duration(op: &str, dur: Duration) {
    if let Ok(mut map) = QUERY_DURATIONS.get_or_init(Default::default).lock() {
        let entry = map.entry(op.to_string()).or_insert((0, 0));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.saturating_add(dur.as_millis() as u64);
    }
    if dur >= slow_query_threshold() {
        if let Ok(mut map) = SLOW_QUERY_COUNT.get_or_init(Default::default).lock() {
            let entry = map.entry(op.to_string()).or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }
}

pub fn set_slow_query_threshold(threshold: Duration) {
    let atom = SLOW_QUERY_THRESHOLD_MS.get_or_init(|| std::sync::atomic::AtomicU64::new(500));
    atom.store(
        threshold.as_millis() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
}

pub fn slow_query_threshold() -> Duration {
    let atom = SLOW_QUERY_THRESHOLD_MS.get_or_init(|| std::sync::atomic::AtomicU64::new(500));
    Duration::from_millis(atom.load(std::sync::atomic::Ordering::Relaxed))
}

pub fn set_slow_query_explain(enabled: bool) {
    let atom = SLOW_QUERY_EXPLAIN.get_or_init(|| std::sync::atomic::AtomicBool::new(false));
    atom.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

pub fn slow_query_explain_enabled() -> bool {
    let atom = SLOW_QUERY_EXPLAIN.get_or_init(|| std::sync::atomic::AtomicBool::new(false));
    atom.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn record_projection_batch(name: &str, events_processed: u64, dur: Duration) {
    if let Ok(mut map) = PROJECTION_STATS.get_or_init(Default::default).lock() {
        let entry = map.entry(name.to_string()).or_insert((0, 0, 0));
        entry.0 = entry.0.saturating_add(events_processed);
        entry.1 = entry.1.saturating_add(1);
        entry.2 = entry.2.saturating_add(dur.as_millis() as u64);
    }
}
