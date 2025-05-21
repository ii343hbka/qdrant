use std::sync::Arc;
use std::sync::atomic::{self, AtomicU64, AtomicUsize};
use std::time::{Duration, SystemTime};

use crate::operations::types::{CollectionError, CollectionResult};

#[derive(Debug)]
pub struct PartialSnapshotMeta {
    ongoing_create_snapshot_requests_tracker: RequestTracker,
    recovery_lock: Arc<tokio::sync::Mutex<()>>,
    read_lock: Arc<tokio::sync::Semaphore>,
    recovery_timestamp: AtomicU64,
}

impl PartialSnapshotMeta {
    pub fn new() -> Self {
        Self {
            ongoing_create_snapshot_requests_tracker: RequestTracker::new(),
            recovery_lock: Arc::new(tokio::sync::Mutex::new(())),
            read_lock: Arc::new(tokio::sync::Semaphore::new(1)),
            recovery_timestamp: AtomicU64::new(0),
        }
    }

    pub fn ongoing_create_snapshot_requests(&self) -> usize {
        self.ongoing_create_snapshot_requests_tracker.requests()
    }

    pub fn track_create_snapshot_request(&self) -> RequestGuard {
        self.ongoing_create_snapshot_requests_tracker
            .track_request()
    }

    pub fn take_recovery_lock(&self) -> CollectionResult<tokio::sync::OwnedMutexGuard<()>> {
        self.recovery_lock.clone().try_lock_owned().map_err(|_| {
            CollectionError::bad_request("partial snapshot recovery is already in progress")
        })
    }

    pub fn check_read_lock(&self) -> CollectionResult<()> {
        let read_operation_permits = self.read_lock.available_permits();

        if read_operation_permits > 0 {
            Ok(())
        } else {
            Err(CollectionError::ServiceError {
                error: "shard unavailable, partial snapshot recovery is in progress".into(),
                backtrace: None,
            })
        }
    }

    pub fn take_read_lock(&self) -> CollectionResult<tokio::sync::OwnedSemaphorePermit> {
        self.read_lock.clone().try_acquire_owned().map_err(|_| {
            CollectionError::bad_request("partial snapshot recovery is already in progress")
        })
    }

    pub fn recovery_timestamp(&self) -> u64 {
        self.recovery_timestamp.load(atomic::Ordering::Relaxed)
    }

    pub fn snapshot_recovered(&self) {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        self.recovery_timestamp
            .store(timestamp, atomic::Ordering::Relaxed);
    }
}

#[derive(Clone, Debug, Default)]
pub struct RequestTracker {
    requests: Arc<AtomicUsize>,
}

impl RequestTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn requests(&self) -> usize {
        self.requests.load(atomic::Ordering::Relaxed)
    }

    pub fn track_request(&self) -> RequestGuard {
        RequestGuard::new(self.requests.clone())
    }
}

#[derive(Clone, Debug)]
pub struct RequestGuard {
    requests: Arc<AtomicUsize>,
}

impl RequestGuard {
    fn new(requests: Arc<AtomicUsize>) -> Self {
        requests.fetch_add(1, atomic::Ordering::Relaxed);
        Self { requests }
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.requests.fetch_sub(1, atomic::Ordering::Relaxed);
    }
}
