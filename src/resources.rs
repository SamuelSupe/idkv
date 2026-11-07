use crate::{Error, Result};
use parking_lot::{Condvar, Mutex};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Bytes {
    data: Vec<u8>,
    pub(crate) charge: Option<Arc<Reservation>>,
}
impl Bytes {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, charge: None }
    }
    pub fn charged(data: Vec<u8>, charge: Arc<Reservation>) -> Self {
        Self {
            data,
            charge: Some(charge),
        }
    }
}
impl std::ops::Deref for Bytes {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug)]
pub struct Counter {
    used: AtomicUsize,
    limit: AtomicUsize,
}
impl Counter {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            limit: AtomicUsize::new(limit),
        })
    }
    fn take(&self, size: usize) -> bool {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(size)
                    .filter(|next| *next <= self.limit.load(Ordering::Acquire))
            })
            .is_ok()
    }
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Acquire)
    }
    pub fn set_limit(&self, limit: usize) -> Result<()> {
        if limit < self.used() {
            return Err(Error::limit("new quota is below current reservations"));
        }
        self.limit.store(limit, Ordering::Release);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Budget {
    pub global: Arc<Counter>,
    pub tenant: Arc<Counter>,
}
impl Budget {
    pub fn reserve(&self, size: usize) -> Result<Reservation> {
        if !self.global.take(size) {
            return Err(Error::limit("global memory budget exhausted"));
        }
        if !self.tenant.take(size) {
            self.global.used.fetch_sub(size, Ordering::AcqRel);
            return Err(Error::limit("tenant memory budget exhausted"));
        }
        Ok(Reservation {
            budget: self.clone(),
            size,
        })
    }
}
#[derive(Debug)]
pub struct Reservation {
    budget: Budget,
    size: usize,
}
impl Reservation {
    pub fn size(&self) -> usize {
        self.size
    }
    pub fn shrink(&mut self, size: usize) {
        if size < self.size {
            let released = self.size - size;
            self.budget
                .global
                .used
                .fetch_sub(released, Ordering::AcqRel);
            self.budget
                .tenant
                .used
                .fetch_sub(released, Ordering::AcqRel);
            self.size = size;
        }
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget
            .global
            .used
            .fetch_sub(self.size, Ordering::AcqRel);
        self.budget
            .tenant
            .used
            .fetch_sub(self.size, Ordering::AcqRel);
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;
#[derive(Clone, Copy)]
pub(crate) enum IoClass {
    Durable,
    Read,
    Background,
}
#[derive(Default)]
struct TenantQueue {
    durable: VecDeque<Job>,
    reads: VecDeque<Job>,
    background: VecDeque<Job>,
    durable_burst: usize,
    foreground_burst: usize,
}
impl TenantQueue {
    fn len(&self) -> usize {
        self.durable.len() + self.reads.len() + self.background.len()
    }
    fn pop(&mut self) -> Job {
        if !self.background.is_empty()
            && ((self.durable.is_empty() && self.reads.is_empty()) || self.foreground_burst >= 8)
        {
            self.foreground_burst = 0;
            return self.background.pop_front().unwrap();
        }
        self.foreground_burst = (self.foreground_burst + 1).min(8);
        if !self.durable.is_empty() && (self.reads.is_empty() || self.durable_burst < 8) {
            self.durable_burst = (self.durable_burst + 1).min(8);
            self.durable.pop_front().unwrap()
        } else {
            self.durable_burst = 0;
            self.reads.pop_front().unwrap()
        }
    }
}
struct Queue {
    jobs: HashMap<Uuid, TenantQueue>,
    ready: VecDeque<Uuid>,
    count: usize,
    stopped: bool,
    stats: HashMap<Uuid, Arc<TenantIo>>,
}
struct QueueState {
    queue: Mutex<Queue>,
    wake: Condvar,
}

/// Round-robin tenant queues isolate a large batch from another tenant's I/O.
/// Jobs perform bounded file operations; callers never execute recursively here.
pub struct IoScheduler {
    inner: Arc<QueueState>,
    workers: Vec<thread::JoinHandle<()>>,
}
pub struct IoTask<T> {
    receiver: mpsc::Receiver<Result<T>>,
    abandoned: Option<Arc<AtomicBool>>,
    budget: Option<crate::read::ReadBudget>,
}
impl<T> Drop for IoTask<T> {
    fn drop(&mut self) {
        if let Some(abandoned) = &self.abandoned {
            abandoned.store(true, Ordering::Release);
        }
    }
}
impl<T> IoTask<T> {
    pub fn wait(self) -> Result<T> {
        let Some(budget) = &self.budget else {
            return self
                .receiver
                .recv()
                .map_err(|_| Error::new("INTERNAL", "I/O worker stopped"))?;
        };
        loop {
            budget.check()?;
            let remaining = budget
                .deadline
                .saturating_duration_since(std::time::Instant::now());
            match self
                .receiver
                .recv_timeout(remaining.min(std::time::Duration::from_millis(10)))
            {
                Ok(result) => {
                    budget.check()?;
                    return result;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(Error::new("INTERNAL", "I/O worker stopped"));
                }
            }
        }
    }
}
impl IoScheduler {
    pub fn new(workers: usize) -> Arc<Self> {
        let inner = Arc::new(QueueState {
            queue: Mutex::new(Queue {
                jobs: HashMap::new(),
                ready: VecDeque::new(),
                count: 0,
                stopped: false,
                stats: HashMap::new(),
            }),
            wake: Condvar::new(),
        });
        let mut handles = Vec::new();
        for _ in 0..workers {
            let state = inner.clone();
            handles.push(thread::spawn(move || {
                loop {
                    let job = {
                        let mut q = state.queue.lock();
                        while q.ready.is_empty() && !q.stopped {
                            state.wake.wait(&mut q);
                        }
                        if q.ready.is_empty() && q.stopped {
                            break;
                        }
                        let id = q.ready.pop_front().unwrap();
                        let jobs = q.jobs.get_mut(&id).unwrap();
                        let job = jobs.pop();
                        if jobs.len() == 0 {
                            q.jobs.remove(&id);
                        } else {
                            q.ready.push_back(id);
                        }
                        q.count -= 1;
                        job
                    };
                    job();
                }
            }));
        }
        Arc::new(Self {
            inner,
            workers: handles,
        })
    }
    pub fn submit<T: Send + 'static>(
        &self,
        tenant: Uuid,
        f: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<IoTask<T>> {
        self.submit_inner(tenant, IoClass::Durable, None, None, f)
    }
    // Only read jobs may be abandoned. WAL/SST writes must finish even when the
    // submitting caller stops waiting, otherwise an acknowledged batch can be lost.
    pub(crate) fn submit_read<T: Send + 'static>(
        &self,
        tenant: Uuid,
        budget: Option<&crate::read::ReadBudget>,
        background: bool,
        f: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<IoTask<T>> {
        if let Some(budget) = budget {
            budget.check()?;
        }
        self.submit_inner(
            tenant,
            if background {
                IoClass::Background
            } else {
                IoClass::Read
            },
            Some(Arc::new(AtomicBool::new(false))),
            budget.cloned(),
            f,
        )
    }
    fn submit_inner<T: Send + 'static>(
        &self,
        tenant: Uuid,
        class: IoClass,
        abandoned: Option<Arc<AtomicBool>>,
        budget: Option<crate::read::ReadBudget>,
        f: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<IoTask<T>> {
        let mut q = self.inner.queue.lock();
        // Reads have a bounded backlog. Durable work is already bounded by its
        // submitting writers and must not be rejected by another tenant's reads.
        if q.stopped || (abandoned.is_some() && q.count >= 4096) {
            return Err(Error::limit("I/O queue unavailable"));
        }
        let stats = q.stats.entry(tenant).or_default().clone();
        let (tx, rx) = mpsc::sync_channel(1);
        let queued = std::time::Instant::now();
        let job_abandoned = abandoned.clone();
        let job_budget = budget.clone();
        let job: Job = Box::new(move || {
            stats
                .wait_ns
                .fetch_add(queued.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let check = if job_abandoned
                .as_ref()
                .is_some_and(|a| a.load(Ordering::Acquire))
            {
                Err(Error::new("CANCELLED", "queued read abandoned"))
            } else {
                job_budget.as_ref().map_or(Ok(()), |budget| budget.check())
            };
            if let Err(error) = check {
                stats.cancelled.fetch_add(1, Ordering::Relaxed);
                let _ = tx.send(Err(error));
                return;
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
                .unwrap_or_else(|_| Err(Error::new("INTERNAL", "I/O worker panicked")));
            stats.completed.fetch_add(1, Ordering::Relaxed);
            let _ = tx.send(result);
        });
        let fresh = !q.jobs.contains_key(&tenant);
        let jobs = q.jobs.entry(tenant).or_default();
        match class {
            IoClass::Durable => jobs.durable.push_back(job),
            IoClass::Read => jobs.reads.push_back(job),
            IoClass::Background => jobs.background.push_back(job),
        }
        if fresh {
            q.ready.push_back(tenant);
        }
        q.count += 1;
        self.inner.wake.notify_one();
        Ok(IoTask {
            receiver: rx,
            abandoned,
            budget,
        })
    }
    pub(crate) fn run_class<T: Send + 'static>(
        &self,
        tenant: Uuid,
        class: IoClass,
        f: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.submit_inner(tenant, class, None, None, f)?.wait()
    }
    pub(crate) fn run_background<T: Send + 'static>(
        &self,
        tenant: Uuid,
        f: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.submit_inner(tenant, IoClass::Background, None, None, f)?
            .wait()
    }
    pub fn stats(&self, tenant: Uuid) -> serde_json::Value {
        let q = self.inner.queue.lock();
        if let Some(stats) = q.stats.get(&tenant) {
            serde_json::json!({"completed_jobs":stats.completed.load(Ordering::Relaxed),"cancelled_jobs":stats.cancelled.load(Ordering::Relaxed),"queue_wait_ns":stats.wait_ns.load(Ordering::Relaxed),"queued_jobs":q.jobs.get(&tenant).map_or(0,TenantQueue::len)})
        } else {
            serde_json::json!({"completed_jobs":0,"cancelled_jobs":0,"queue_wait_ns":0,"queued_jobs":0})
        }
    }
    pub fn run<T: Send + 'static>(
        &self,
        tenant: Uuid,
        f: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.submit(tenant, f)?.wait()
    }
}
impl Drop for IoScheduler {
    fn drop(&mut self) {
        self.inner.queue.lock().stopped = true;
        self.inner.wake.notify_all();
        for thread in self.workers.drain(..) {
            let _ = thread.join();
        }
    }
}

pub struct RequestGate {
    active: AtomicUsize,
    closed: AtomicBool,
    limit: usize,
}
pub struct RequestPermit<'a>(&'a RequestGate);
impl RequestGate {
    pub fn new(limit: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            limit,
        }
    }
    pub fn enter(&self) -> Result<RequestPermit<'_>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::new("BUSY", "tenant is draining for recovery"));
        }
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.limit).then_some(n + 1)
            })
            .map_err(|_| Error::limit("tenant concurrency limit reached"))?;
        let permit = RequestPermit(self);
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::new("BUSY", "tenant is draining for recovery"));
        }
        Ok(permit)
    }
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
    pub(crate) fn open(&self) {
        self.closed.store(false, Ordering::Release);
    }
}
impl Drop for RequestPermit<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Default)]
struct TenantIo {
    cancelled: AtomicU64,
    completed: AtomicU64,
    wait_ns: AtomicU64,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reads_overtake_bulk_output_while_background_io_still_progresses() {
        let io = IoScheduler::new(1);
        let tenant = Uuid::new_v4();
        let (entered, ready) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let blocker = io
            .submit(tenant, move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
                Ok(())
            })
            .unwrap();
        ready.recv().unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let order = order.clone();
            tasks.push(
                io.submit_inner(tenant, IoClass::Background, None, None, move || {
                    order.lock().push('B');
                    Ok(())
                })
                .unwrap(),
            );
        }
        for _ in 0..32 {
            let order = order.clone();
            tasks.push(
                io.submit_read(tenant, None, false, move || {
                    order.lock().push('R');
                    Ok(())
                })
                .unwrap(),
            );
        }
        release.send(()).unwrap();
        blocker.wait().unwrap();
        for task in tasks {
            task.wait().unwrap();
        }
        let order = order.lock();
        assert_eq!(&order[..7], &['R'; 7]);
        assert!(order.iter().position(|c| *c == 'B').unwrap() <= 8);
        assert_eq!(order.len(), 64);
    }
    #[test]
    fn durable_io_bypasses_read_backlog_without_starving_reads() {
        let io = IoScheduler::new(1);
        let tenant = Uuid::new_v4();
        let (entered, ready) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let blocker = io
            .submit(tenant, move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
                Ok(())
            })
            .unwrap();
        ready.recv().unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let reads: Vec<_> = (0..32)
            .map(|_| {
                let order = order.clone();
                io.submit_read(tenant, None, false, move || {
                    order.lock().push('R');
                    Ok(())
                })
                .unwrap()
            })
            .collect();
        let writes: Vec<_> = (0..32)
            .map(|_| {
                let order = order.clone();
                io.submit(tenant, move || {
                    order.lock().push('W');
                    Ok(())
                })
                .unwrap()
            })
            .collect();
        release.send(()).unwrap();
        blocker.wait().unwrap();
        for task in reads.into_iter().chain(writes) {
            task.wait().unwrap();
        }
        let order = order.lock();
        assert_eq!(order[0], 'W');
        assert!(order.iter().position(|c| *c == 'R').unwrap() <= 8);
        assert_eq!(order.len(), 64);
    }

    #[test]
    fn abandoned_reads_skip_io_but_dropped_write_handles_still_execute() {
        let io = IoScheduler::new(1);
        let tenant = Uuid::new_v4();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let blocker = io
            .submit(tenant, move || {
                ready_tx.send(()).unwrap();
                let _ = release_rx.recv();
                Ok(())
            })
            .unwrap();
        ready_rx.recv().unwrap();
        let read_runs = Arc::new(AtomicUsize::new(0));
        let counted = read_runs.clone();
        let abandoned = io
            .submit_read(tenant, None, false, move || {
                counted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .unwrap();
        drop(abandoned);
        let write_runs = Arc::new(AtomicUsize::new(0));
        let counted = write_runs.clone();
        drop(
            io.submit(tenant, move || {
                counted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .unwrap(),
        );
        let budget = crate::read::ReadBudget::new(
            &crate::ReadOptions {
                timeout_ms: Some(50),
                ..Default::default()
            },
            &crate::Options::default(),
        )
        .unwrap();
        let counted = read_runs.clone();
        let expired = io
            .submit_read(tenant, Some(&budget), false, move || {
                counted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .unwrap();
        let result = expired.wait();
        let live = io
            .submit_read(Uuid::new_v4(), None, false, || Ok(42))
            .unwrap();
        release_tx.send(()).unwrap();
        blocker.wait().unwrap();
        assert_eq!(live.wait().unwrap(), 42);
        io.run(tenant, || Ok(())).unwrap();
        assert_eq!(result.unwrap_err().code, "DEADLINE_EXCEEDED");
        assert_eq!(read_runs.load(Ordering::Relaxed), 0);
        assert_eq!(write_runs.load(Ordering::Relaxed), 1);
        assert_eq!(io.stats(tenant)["cancelled_jobs"], 2);
    }
}
