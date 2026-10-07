//! Long work runs as jobs: an id plus a receipt, polled, read once, then acknowledged.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::RngExt;
use serde_json::{json, Value};

/// Finished jobs nobody acknowledged are dropped after this long.
const KEEP_FINISHED: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Queued,
    Running,
    Done,
    Failed,
    Cancelled,
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Done => "done",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
        }
    }

    fn finished(self) -> bool {
        matches!(self, JobState::Done | JobState::Failed | JobState::Cancelled)
    }
}

struct Job {
    receipt: String,
    kind: String,
    state: JobState,
    result: Option<Value>,
    error: Option<String>,
    finished_at: Option<Instant>,
    cancel: Arc<AtomicBool>,
}

#[derive(Default)]
pub struct Jobs {
    next: u64,
    map: HashMap<String, Job>,
}

fn receipt() -> String {
    let mut b = [0u8; 16];
    rand::rng().fill(&mut b[..]);
    hex::encode(b)
}

impl Jobs {
    /// Registers a job and returns its id, receipt and cancel flag.
    pub fn add(&mut self, kind: &str) -> (String, String, Arc<AtomicBool>) {
        self.prune();
        self.next += 1;
        let id = format!("j{}", self.next);
        let r = receipt();
        let cancel = Arc::new(AtomicBool::new(false));
        self.map.insert(
            id.clone(),
            Job {
                receipt: r.clone(),
                kind: kind.into(),
                state: JobState::Queued,
                result: None,
                error: None,
                finished_at: None,
                cancel: cancel.clone(),
            },
        );
        (id, r, cancel)
    }

    fn get(&self, id: &str, receipt: &str) -> Option<&Job> {
        self.map.get(id).filter(|j| constant_time_eq(j.receipt.as_bytes(), receipt.as_bytes()))
    }

    /// Moves a queued job to running, unless it was cancelled meanwhile.
    pub fn begin(&mut self, id: &str) -> bool {
        match self.map.get_mut(id) {
            Some(j) if j.state == JobState::Queued => {
                j.state = JobState::Running;
                true
            }
            _ => false,
        }
    }

    pub fn finish(&mut self, id: &str, outcome: Result<Value, String>) -> Option<JobState> {
        let j = self.map.get_mut(id)?;
        if j.state.finished() {
            return Some(j.state);
        }
        match outcome {
            Ok(v) => {
                j.state = JobState::Done;
                j.result = Some(v);
            }
            Err(e) => {
                j.state = if j.cancel.load(Ordering::SeqCst) { JobState::Cancelled } else { JobState::Failed };
                j.error = Some(e);
            }
        }
        j.finished_at = Some(Instant::now());
        Some(j.state)
    }

    pub fn status(&self, id: &str, receipt: &str) -> Value {
        match self.get(id, receipt) {
            Some(j) => json!({"ok": true, "jobId": id, "kind": j.kind, "state": j.state.as_str(), "error": j.error}),
            None => json!({"ok": false, "error": "unknown job"}),
        }
    }

    pub fn result(&self, id: &str, receipt: &str) -> Value {
        match self.get(id, receipt) {
            None => json!({"ok": false, "error": "unknown job"}),
            Some(j) => match j.state {
                JobState::Done => json!({"ok": true, "result": j.result}),
                JobState::Failed | JobState::Cancelled => json!({"ok": false, "error": j.error, "state": j.state.as_str()}),
                s => json!({"ok": false, "error": format!("job not finished: {}", s.as_str())}),
            },
        }
    }

    /// Forgets a finished job.
    pub fn ack(&mut self, id: &str, receipt: &str) -> bool {
        let finished = self.get(id, receipt).is_some_and(|j| j.state.finished());
        if finished {
            self.map.remove(id);
        }
        finished
    }

    /// Cancels a queued job outright; asks a running one to stop at its next check.
    pub fn cancel(&mut self, id: &str, receipt: &str) -> bool {
        if self.get(id, receipt).is_none() {
            return false;
        }
        let j = self.map.get_mut(id).expect("checked above");
        match j.state {
            JobState::Queued => {
                j.state = JobState::Cancelled;
                j.error = Some("cancelled".into());
                j.finished_at = Some(Instant::now());
                j.cancel.store(true, Ordering::SeqCst);
                true
            }
            JobState::Running => {
                j.cancel.store(true, Ordering::SeqCst);
                true
            }
            _ => false,
        }
    }

    fn prune(&mut self) {
        self.map.retain(|_, j| j.finished_at.is_none_or(|t| t.elapsed() < KEEP_FINISHED));
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle() {
        let mut jobs = Jobs::default();
        let (id, r, _) = jobs.add("open_wallet");
        assert_eq!(jobs.status(&id, &r)["state"], "queued");
        assert_eq!(jobs.status(&id, "wrong")["ok"], false);
        assert!(jobs.begin(&id));
        assert_eq!(jobs.result(&id, &r)["ok"], false);
        assert!(!jobs.ack(&id, &r));
        jobs.finish(&id, Ok(json!({"name": "w"})));
        assert_eq!(jobs.result(&id, &r)["result"]["name"], "w");
        assert!(jobs.ack(&id, &r));
        assert_eq!(jobs.status(&id, &r)["ok"], false);
    }

    #[test]
    fn cancel_queued_and_running() {
        let mut jobs = Jobs::default();
        let (a, ra, _) = jobs.add("rescan");
        assert!(jobs.cancel(&a, &ra));
        assert!(!jobs.begin(&a));
        assert_eq!(jobs.status(&a, &ra)["state"], "cancelled");

        let (b, rb, flag) = jobs.add("rescan");
        jobs.begin(&b);
        assert!(jobs.cancel(&b, &rb));
        assert!(flag.load(Ordering::SeqCst));
        assert_eq!(jobs.finish(&b, Err("stopped".into())), Some(JobState::Cancelled));
    }

    #[test]
    fn receipts_differ() {
        let mut jobs = Jobs::default();
        let (_, r1, _) = jobs.add("x");
        let (_, r2, _) = jobs.add("x");
        assert_ne!(r1, r2);
        assert_eq!(r1.len(), 32);
    }
}
