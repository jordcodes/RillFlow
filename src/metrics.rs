use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

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
static PROJECTION_STATS: OnceLock<Mutex<HashMap<String, (u64, u64, u64)>>> = OnceLock::new();

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

fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
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
            let _ = writeln!(s, "# TYPE slow_queries_total counter\nslow_queries_total{{op=\"{}\"}} {}", escaped, count);
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
