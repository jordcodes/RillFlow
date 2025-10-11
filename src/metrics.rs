use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

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
        }
    }
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::default)
}

pub fn render_prometheus() -> String {
    let m = metrics();
    let mut s = String::new();
    // docs
    let _ = writeln!(
        s,
        "# TYPE doc_reads_total counter\ndoc_reads_total {}",
        m.doc_reads_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        s,
        "# TYPE doc_writes_total counter\ndoc_writes_total {}",
        m.doc_writes_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        s,
        "# TYPE doc_conflicts_total counter\ndoc_conflicts_total {}",
        m.doc_conflicts_total.load(Ordering::Relaxed)
    );
    // projections
    let _ = writeln!(
        s,
        "# TYPE proj_events_processed_total counter\nproj_events_processed_total {}",
        m.proj_events_processed_total.load(Ordering::Relaxed)
    );
    // subscriptions
    let _ = writeln!(
        s,
        "# TYPE subs_delivered_total counter\nsubs_delivered_total {}",
        m.subs_delivered_total.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        s,
        "# TYPE subs_pending_gauge gauge\nsubs_pending_gauge {}",
        m.subs_pending_gauge.load(Ordering::Relaxed)
    );
    // snapshotter
    let _ = writeln!(
        s,
        "# TYPE snapshot_candidates_gauge gauge\nsnapshot_candidates_gauge {}",
        m.snapshot_candidates_gauge.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        s,
        "# TYPE snapshot_max_gap_gauge gauge\nsnapshot_max_gap_gauge {}",
        m.snapshot_max_gap_gauge.load(Ordering::Relaxed)
    );
    s
}
