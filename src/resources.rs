use crate::{Error, Result};
use parking_lot::{Condvar, Mutex, MutexGuard, RwLock};
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
    state: Arc<CounterState>,
    use_headroom: bool,
    allowance: Option<Arc<Mutex<FlushAllowance>>>,
}
#[derive(Debug)]
struct FlushAllowance {
    available: usize,
    active: bool,
}
pub(crate) struct FlushPermit<'a> {
    _exclusive: Option<MutexGuard<'a, ()>>,
    prepaid: Option<Arc<Counter>>,
}
impl Drop for FlushPermit<'_> {
    fn drop(&mut self) {
        if let Some(counter) = &self.prepaid {
            let mut allowance = counter.allowance.as_ref().unwrap().lock();
            allowance.active = false;
            counter
                .state
                .used
                .fetch_sub(allowance.available, Ordering::AcqRel);
            allowance.available = 0;
        }
    }
}
#[derive(Debug)]
struct CounterState {
    used: AtomicUsize,
    limit: AtomicUsize,
    quota: RwLock<()>,
    headroom: usize,
    pressure: AtomicBool,
    cache_pressure: AtomicBool,
    reclaim: Mutex<()>,
}
impl Counter {
    pub fn new(limit: usize) -> Arc<Self> {
        Self::with_headroom(limit, 0)
    }
    pub(crate) fn with_headroom(limit: usize, headroom: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(CounterState {
                used: AtomicUsize::new(0),
                limit: AtomicUsize::new(limit),
                quota: RwLock::new(()),
                headroom: headroom.min(limit / 8),
                pressure: AtomicBool::new(false),
                cache_pressure: AtomicBool::new(false),
                reclaim: Mutex::new(()),
            }),
            use_headroom: false,
            allowance: None,
        })
    }
    fn take(&self, size: usize) -> bool {
        if let Some(allowance) = &self.allowance {
            let mut allowance = allowance.lock();
            if allowance.active {
                let prepaid = allowance.available.min(size);
                if !self.take_unreserved(size - prepaid) {
                    return false;
                }
                allowance.available -= prepaid;
                return true;
            }
        }
        self.take_unreserved(size)
    }
    fn take_unreserved(&self, size: usize) -> bool {
        if size == 0 {
            return true;
        }
        let _quota = self.state.quota.read();
        let limit = if self.use_headroom {
            self.limit()
        } else {
            self.foreground_limit()
        };
        self.state
            .used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(size).filter(|next| *next <= limit)
            })
            .is_ok()
    }
    fn give(&self, size: usize) {
        if let Some(allowance) = &self.allowance {
            let mut allowance = allowance.lock();
            if allowance.active {
                allowance.available += size;
                return;
            }
        }
        self.state.used.fetch_sub(size, Ordering::AcqRel);
    }
    pub(crate) fn foreground_limit(&self) -> usize {
        self.limit().saturating_sub(self.state.headroom)
    }
    pub(crate) fn under_pressure(&self) -> bool {
        let limit = self.foreground_limit();
        self.pressure_state(&self.state.pressure, limit - limit / 4)
    }
    pub(crate) fn cache_pressure(&self) -> bool {
        let limit = self.foreground_limit();
        self.pressure_state(
            &self.state.cache_pressure,
            limit - limit / 8 - (limit / 64).min(4 << 20),
        )
    }
    fn pressure_state(&self, state: &AtomicBool, exit: usize) -> bool {
        let limit = self.foreground_limit();
        let _ = state.try_update(Ordering::AcqRel, Ordering::Acquire, |active| {
            let used = self.used();
            let next = self.state.headroom != 0
                && if active {
                    used > exit
                } else {
                    used >= limit - limit / 8
                };
            (next != active).then_some(next)
        });
        state.load(Ordering::Acquire)
    }
    pub(crate) fn headroom(&self) -> usize {
        self.state.headroom
    }
    pub(crate) fn headroom_in_use(&self) -> usize {
        self.used().saturating_sub(self.foreground_limit())
    }
    pub fn used(&self) -> usize {
        self.state.used.load(Ordering::Acquire)
    }
    pub fn limit(&self) -> usize {
        self.state.limit.load(Ordering::Acquire)
    }
    pub fn set_limit(&self, limit: usize) -> Result<()> {
        let _quota = self.state.quota.write();
        if limit < self.used() {
            return Err(Error::limit("new quota is below current reservations"));
        }
        self.state.limit.store(limit, Ordering::Release);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Budget {
    pub global: Arc<Counter>,
    pub tenant: Arc<Counter>,
}
impl Budget {
    // Only the pressure flush holding this guard may spend the protected part
    // of the global budget. Normal flushes remain concurrent. Both views charge
    // the same counters; neither global nor tenant hard limits are bypassed.
    pub(crate) fn for_flush(&self, pressure: bool) -> Option<(Self, FlushPermit<'_>)> {
        if self.global.headroom() == 0 {
            return Some((
                self.clone(),
                FlushPermit {
                    _exclusive: None,
                    prepaid: None,
                },
            ));
        }
        if !pressure && self.global.take(self.global.headroom()) {
            // Charge workspace before admitting another flush. Temporary output
            // allocations borrow it and refund it until publication finishes;
            // live SST metadata remains charged after unused credit is released.
            let counter = Arc::new(Counter {
                state: self.global.state.clone(),
                use_headroom: false,
                allowance: Some(Arc::new(Mutex::new(FlushAllowance {
                    available: self.global.headroom(),
                    active: true,
                }))),
            });
            return Some((
                Self {
                    global: counter.clone(),
                    tenant: self.tenant.clone(),
                },
                FlushPermit {
                    _exclusive: None,
                    prepaid: Some(counter),
                },
            ));
        }
        let guard = self.global.state.reclaim.try_lock()?;
        Some((
            Self {
                global: Arc::new(Counter {
                    state: self.global.state.clone(),
                    use_headroom: true,
                    allowance: None,
                }),
                tenant: self.tenant.clone(),
            },
            FlushPermit {
                _exclusive: Some(guard),
                prepaid: None,
            },
        ))
    }
    pub(crate) fn foreground(&self) -> Self {
        if !self.global.use_headroom && self.global.allowance.is_none() {
            return self.clone();
        }
        Self {
            global: Arc::new(Counter {
                state: self.global.state.clone(),
                use_headroom: false,
                allowance: None,
            }),
            tenant: self.tenant.clone(),
        }
    }
    pub fn reserve(&self, size: usize) -> Result<Reservation> {
        if !self.global.take(size) {
            return Err(Error::limit("global memory budget exhausted"));
        }
        if !self.tenant.take(size) {
            self.global.give(size);
            return Err(Error::limit("tenant memory budget exhausted"));
        }
        Ok(Reservation {
            global: self.global.clone(),
            tenant: Some(self.tenant.clone()),
            size,
        })
    }
}
#[derive(Debug)]
pub struct Reservation {
    global: Arc<Counter>,
    tenant: Option<Arc<Counter>>,
    size: usize,
}
impl Reservation {
    pub fn size(&self) -> usize {
        self.size
    }
    pub fn shrink(&mut self, size: usize) {
        if size < self.size {
            let released = self.size - size;
            self.global.give(released);
            if let Some(tenant) = &self.tenant {
                tenant.give(released);
            }
            self.size = size;
        }
    }

    pub(crate) fn release_tenant(&mut self) {
        if let Some(tenant) = self.tenant.take() {
            tenant.give(self.size);
        }
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.shrink(0);
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
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
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
    fn pressure_review_normal_flushes_cannot_admit_the_same_free_memory() {
        let budget = Budget {
            global: Counter::with_headroom(128, 16),
            tenant: Counter::new(128),
        };
        let held = budget.reserve(80).unwrap();
        let (_protected, _exclusive) = budget.for_flush(true).unwrap();
        let flushes: Vec<_> = (0..4).filter_map(|_| budget.for_flush(false)).collect();
        assert_eq!(
            flushes.len(),
            2,
            "each admitted flush needs its own 16 bytes"
        );
        assert!(
            budget.reserve(1).is_err(),
            "foreground callers must not spend promised workspace"
        );
        let work: Vec<_> = flushes
            .iter()
            .map(|(flush, _)| flush.reserve(16).unwrap())
            .collect();
        assert!(flushes[0].0.foreground().reserve(1).is_err());
        drop(work);
        for (flush, _) in &flushes {
            drop(flush.reserve(16).unwrap());
        }
        drop(flushes);
        assert_eq!(budget.global.used(), 80);
        drop(held);
        assert_eq!(budget.global.used(), 0);
    }
    #[test]
    fn flush_workspace_releases_only_unused_credit_and_keeps_published_memory_charged() {
        let budget = Budget {
            global: Counter::with_headroom(128, 16),
            tenant: Counter::new(128),
        };
        let (flush, permit) = budget.for_flush(false).unwrap();
        assert_eq!(budget.global.used(), 16);
        let mut metadata = flush.reserve(12).unwrap();
        assert_eq!(budget.global.used(), 16);
        budget.tenant.set_limit(12).unwrap();
        assert!(flush.reserve(1).is_err());
        metadata.shrink(8);
        assert_eq!(budget.tenant.used(), 8);
        assert_eq!(budget.global.used(), 16);
        drop(permit);
        assert_eq!(budget.global.used(), 8);
        assert_eq!(flush.foreground().global.used(), 8);
        drop(metadata);
        assert_eq!(budget.global.used(), 0);
        assert_eq!(budget.tenant.used(), 0);
        drop(flush.reserve(4).unwrap());
        assert_eq!(budget.global.used(), 0);
    }
    #[test]
    fn pressure_hysteresis_and_headroom_usage_follow_shared_reservations() {
        let budget = Budget {
            global: Counter::with_headroom(128, 16),
            tenant: Counter::new(128),
        };
        let mut memory = budget.reserve(98).unwrap();
        assert!(budget.global.under_pressure());
        memory.shrink(90);
        assert!(budget.global.under_pressure());
        memory.shrink(84);
        assert!(!budget.global.under_pressure());
        let (flush, _guard) = budget.for_flush(true).unwrap();
        let work = flush.reserve(36).unwrap();
        assert_eq!(budget.global.headroom_in_use(), 8);
        assert!(budget.global.under_pressure());
        drop(work);
        assert_eq!(budget.global.headroom_in_use(), 0);
        assert!(!budget.global.under_pressure());
    }
    #[test]
    fn reclamation_headroom_is_exclusive_and_respects_hard_limits() {
        let budget = Budget {
            global: Counter::with_headroom(128, 16),
            tenant: Counter::new(128),
        };
        let normal = budget.reserve(112).unwrap();
        assert!(budget.reserve(1).is_err());
        {
            let (flush, _guard) = budget.for_flush(true).unwrap();
            assert!(budget.for_flush(true).is_none());
            assert!(flush.foreground().reserve(1).is_err());
            let mut work = flush.reserve(16).unwrap();
            assert_eq!(budget.global.used(), 128);
            assert!(flush.reserve(1).is_err());
            assert!(budget.global.set_limit(127).is_err());
            work.shrink(8);
            assert_eq!(budget.global.used(), 120);
            assert_eq!(budget.tenant.used(), 120);
        }
        let (flush, _guard) = budget.for_flush(true).unwrap();
        budget.tenant.set_limit(112).unwrap();
        assert!(flush.reserve(1).is_err());
        assert_eq!(budget.global.used(), 112);
        drop(normal);
        assert_eq!(budget.global.used(), 0);
        assert_eq!(budget.tenant.used(), 0);
    }
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
    #[test]
    fn concurrent_quota_shrink_preserves_reservation_bound() {
        use std::time::{Duration, Instant};
        let tenant = Counter::new(128 << 20);
        let budget = Budget {
            global: Counter::new(512 << 20),
            tenant: tenant.clone(),
        };
        let start = std::sync::Barrier::new(2);
        let stop = AtomicBool::new(false);
        let shrinks = AtomicUsize::new(0);
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut attempts = 0usize;
        let mut violation = None;
        thread::scope(|scope| {
            let setter = scope.spawn(|| {
                start.wait();
                while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
                    tenant.set_limit(128 << 20).unwrap();
                    if tenant.set_limit(64 << 20).is_ok() {
                        shrinks.fetch_add(1, Ordering::Relaxed);
                        for _ in 0..16 {
                            std::hint::spin_loop();
                        }
                    }
                }
            });
            start.wait();
            while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
                attempts += 1;
                if let Ok(reservation) = budget.reserve(128 << 20) {
                    for _ in 0..32 {
                        let limit = tenant.limit();
                        let used = tenant.used();
                        if used > limit {
                            violation = Some((used, limit));
                            stop.store(true, Ordering::Release);
                            break;
                        }
                        std::hint::spin_loop();
                    }
                    drop(reservation);
                }
            }
            stop.store(true, Ordering::Release);
            setter.join().unwrap();
        });
        println!(
            "AUDIT_JSON {}",
            serde_json::json!({"case":"concurrent-quota-shrink","attempts":attempts,"accepted_shrinks":shrinks.load(Ordering::Acquire),"overshoot":violation,"final_used":tenant.used()})
        );
        assert!(
            violation.is_none(),
            "a successful reservation exceeds concurrently accepted quota"
        );
    }
}
