//! Node metrics, with a Prometheus text export.

use std::fmt::Write as _;

/// Counters and last values of one node. Times are in milliseconds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metrics {
    /// Epochs sealed.
    pub epochs_sealed: u64,
    /// Merges done.
    pub merges: u64,
    /// Merges undone for a manifest of higher priority.
    pub unmerges: u64,
    /// Manifests published as leader.
    pub manifests_published: u64,
    /// Local time from the end of an epoch to its merge, last merge.
    pub epoch_duration_ms: u64,
    /// Sum of [`epoch_duration_ms`](Metrics::epoch_duration_ms).
    pub epoch_duration_ms_total: u64,
    /// Wall time of the last merge.
    pub merge_time_ms: u64,
    /// Sum of [`merge_time_ms`](Metrics::merge_time_ms).
    pub merge_time_ms_total: u64,
    /// Bytes of this node's sealed logs.
    pub own_log_bytes: u64,
    /// Bytes of the logs in merged manifests.
    pub merged_log_bytes: u64,
    /// Logs merged in an epoch later than their own (late logs).
    pub late_logs: u64,
    /// Conflicts recorded by merges.
    pub conflicts: u64,
    /// Root hash mismatches with a peer on the same manifest.
    pub hash_mismatches: u64,
    /// Repairs of the base state from a peer, after a mismatch.
    pub repairs: u64,
    /// Catch-ups from a peer's base state (too far behind, or on a
    /// manifest chain of lower priority).
    pub catch_ups: u64,
    /// Own logs given up: older than the lateness bound.
    pub abandoned_logs: u64,
    /// Entries of given-up logs, written to `_conflicts`.
    pub stale_entries: u64,
    /// Messages received.
    pub messages_in: u64,
    /// Messages sent.
    pub messages_out: u64,
    /// Messages from peers that were not valid.
    pub rejected: u64,
    /// The last merged epoch.
    pub merged_epoch: Option<u64>,
}

impl Metrics {
    /// Returns the metrics in the Prometheus text format, labeled with the
    /// node ID.
    pub fn render(&self, node: u32) -> String {
        let mut out = String::new();
        let counters = [
            ("epochs_sealed", "Epochs sealed", self.epochs_sealed),
            ("merges", "Merges done", self.merges),
            ("unmerges", "Merges undone for a manifest of higher priority", self.unmerges),
            ("manifests_published", "Manifests published as leader", self.manifests_published),
            ("epoch_duration_ms_total", "Sum of epoch end to merge times", self.epoch_duration_ms_total),
            ("merge_time_ms_total", "Sum of merge times", self.merge_time_ms_total),
            ("own_log_bytes", "Bytes of own sealed logs", self.own_log_bytes),
            ("merged_log_bytes", "Bytes of merged logs", self.merged_log_bytes),
            ("late_logs", "Logs merged after their epoch", self.late_logs),
            ("conflicts", "Conflicts recorded by merges", self.conflicts),
            ("hash_mismatches", "Root hash mismatches on the same manifest", self.hash_mismatches),
            ("repairs", "Repairs after a mismatch", self.repairs),
            ("catch_ups", "Catch-ups from a peer's base state", self.catch_ups),
            ("abandoned_logs", "Own logs given up after the lateness bound", self.abandoned_logs),
            ("stale_entries", "Entries of given-up logs", self.stale_entries),
            ("messages_in", "Messages received", self.messages_in),
            ("messages_out", "Messages sent", self.messages_out),
            ("rejected", "Messages from peers that were not valid", self.rejected),
        ];
        for (name, help, v) in counters {
            let _ = writeln!(out, "# HELP synod_{name} {help}.\n# TYPE synod_{name} counter");
            let _ = writeln!(out, "synod_{name}{{node=\"{node}\"}} {v}");
        }
        let gauges = [
            ("epoch_duration_ms", "Epoch end to merge, last merge", Some(self.epoch_duration_ms)),
            ("merge_time_ms", "Time of the last merge", Some(self.merge_time_ms)),
            ("merged_epoch", "Last merged epoch", self.merged_epoch),
        ];
        for (name, help, v) in gauges {
            let _ = writeln!(out, "# HELP synod_{name} {help}.\n# TYPE synod_{name} gauge");
            if let Some(v) = v {
                let _ = writeln!(out, "synod_{name}{{node=\"{node}\"}} {v}");
            }
        }
        out
    }
}
