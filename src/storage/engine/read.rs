use super::*;
#[derive(Clone)]
pub(crate) struct ScanRange {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub prefix: Option<Vec<u8>>,
}
pub(crate) struct RangeScan<'a> {
    scan: Scan<'a>,
    ranges: Vec<ScanRange>,
    current: usize,
}
impl RangeScan<'_> {
    pub(crate) fn range_index(&self) -> usize {
        self.current
    }
    pub(crate) fn next_range(&mut self, stats: &mut ReadStats) -> Result<()> {
        self.current += 1;
        if let Some(range) = self.ranges.get(self.current) {
            self.scan.seek(range, stats)?;
        }
        Ok(())
    }
    pub(crate) fn next(&mut self, stats: &mut ReadStats) -> Result<Option<Record>> {
        while self.current < self.ranges.len() {
            if let Some(record) = self.scan.next(stats)? {
                return Ok(Some(record));
            }
            self.next_range(stats)?;
        }
        Ok(None)
    }
}
impl Snapshot {
    /// Sorted, non-overlapping ranges reuse file cursors and their charged blocks.
    pub(crate) fn scan_ranges(
        &self,
        ranges: Vec<ScanRange>,
        stats: &mut ReadStats,
        budget: Option<&crate::read::ReadBudget>,
    ) -> Result<RangeScan<'_>> {
        self.check()?;
        if ranges
            .windows(2)
            .any(|pair| pair[0].end.as_ref().is_none_or(|end| end > &pair[1].start))
        {
            return Err(Error::invalid("scan ranges must be sorted and disjoint"));
        }
        let start = ranges.first().map_or_else(Vec::new, |r| r.start.clone());
        let end = ranges
            .last()
            .and_then(|r| r.end.clone())
            .or_else(|| ranges.is_empty().then(Vec::new));
        let mut scan = Scan::unstarted(&self.0.view, self.seq(), start, end, Some(self), stats)?;
        if let Some(budget) = budget {
            budget.check()?;
            scan.set_read_budget(budget);
        }
        if let Some(range) = ranges.first() {
            scan.seek(range, stats)?;
        }
        Ok(RangeScan {
            scan,
            ranges,
            current: 0,
        })
    }
    pub fn seq(&self) -> u64 {
        self.0.seq
    }
    pub fn tenant(&self) -> Uuid {
        self.0.engine.info.id
    }
    pub fn generation(&self) -> Uuid {
        self.0.engine.runtime_id
    }
    pub fn check(&self) -> Result<()> {
        if Instant::now() >= self.0.expires {
            return Err(Error::new("CURSOR_EXPIRED", "snapshot lease expired"));
        }
        if self.0.engine.closed.load(Ordering::Acquire) {
            return Err(Error::new("UNAVAILABLE", "tenant closed"));
        }
        Ok(())
    }
    pub fn get(&self, key: &[u8], stats: &mut ReadStats) -> Result<Option<Arc<Bytes>>> {
        self.check()?;
        Ok(get_one(&self.0.view, key, self.seq(), stats)?.and_then(|r| r.value))
    }
    pub fn multi_get(
        &self,
        keys: &[Vec<u8>],
        stats: &mut ReadStats,
    ) -> Result<Vec<Option<Arc<Bytes>>>> {
        self.check()?;
        get_many(&self.0.view, keys, self.seq(), stats)
            .map(|r| r.into_iter().map(|r| r.and_then(|r| r.value)).collect())
    }
    pub(crate) fn multi_get_controlled(
        &self,
        keys: &[Vec<u8>],
        stats: &mut ReadStats,
        budget: &crate::read::ReadBudget,
    ) -> Result<Vec<Option<Arc<Bytes>>>> {
        self.check()?;
        budget.check()?;
        let out = get_many_controlled(&self.0.view, keys, self.seq(), stats, Some(budget))?;
        budget.check()?;
        Ok(out.into_iter().map(|r| r.and_then(|r| r.value)).collect())
    }
    pub(crate) fn get_controlled(
        &self,
        key: &[u8],
        stats: &mut ReadStats,
        budget: &crate::read::ReadBudget,
    ) -> Result<Option<Arc<Bytes>>> {
        self.check()?;
        budget.check()?;
        let out = get_one_controlled(&self.0.view, key, self.seq(), stats, Some(budget))?
            .and_then(|r| r.value);
        budget.check()?;
        Ok(out)
    }
    pub(crate) fn scan_controlled(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        prefix: Option<Vec<u8>>,
        stats: &mut ReadStats,
        budget: &crate::read::ReadBudget,
    ) -> Result<Scan<'_>> {
        self.check()?;
        budget.check()?;
        let range = ScanRange {
            start: start.clone(),
            end: end.clone(),
            prefix,
        };
        let mut scan = Scan::unstarted(&self.0.view, self.seq(), start, end, Some(self), stats)?;
        scan.set_read_budget(budget);
        scan.seek(&range, stats)?;
        Ok(scan)
    }
    pub fn scan<'a>(
        &'a self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        filter_prefix: Option<Vec<u8>>,
        stats: &mut ReadStats,
    ) -> Result<Scan<'a>> {
        self.check()?;
        Scan::new(
            &self.0.view,
            self.seq(),
            start,
            end,
            filter_prefix,
            Some(self),
            stats,
        )
    }
}

pub(super) fn get_one(
    view: &View,
    key: &[u8],
    seq: u64,
    stats: &mut ReadStats,
) -> Result<Option<Record>> {
    get_one_controlled(view, key, seq, stats, None)
}
fn get_one_controlled(
    view: &View,
    key: &[u8],
    seq: u64,
    stats: &mut ReadStats,
    budget: Option<&crate::read::ReadBudget>,
) -> Result<Option<Record>> {
    let mut found = view.mem.get(key, seq);
    for mem in view.frozen.iter().rev() {
        if let Some(record) = mem.get(key, seq)
            && found.as_ref().is_none_or(|r| r.seq < record.seq)
        {
            found = Some(record);
        }
    }
    let mut read = |table: &Arc<Table>| -> Result<()> {
        if found.as_ref().is_none_or(|r| r.seq < table.info.max_seq)
            && let Some(record) = match budget {
                Some(_) => table.get_controlled(key, seq, stats, budget),
                None => table.get(key, seq, stats),
            }?
            && found.as_ref().is_none_or(|r| r.seq < record.seq)
        {
            found = Some(record);
        }
        Ok(())
    };
    for (level, tables) in view.tables.levels.iter().enumerate() {
        if level == 0 {
            for table in tables {
                read(table)?;
            }
        } else {
            let index = tables.partition_point(|t| t.info.last.as_slice() < key);
            if let Some(table) = tables.get(index) {
                read(table)?;
            }
        }
    }
    Ok(found)
}

pub(super) fn get_many(
    view: &View,
    keys: &[Vec<u8>],
    seq: u64,
    stats: &mut ReadStats,
) -> Result<Vec<Option<Record>>> {
    get_many_controlled(view, keys, seq, stats, None)
}
fn get_many_controlled(
    view: &View,
    keys: &[Vec<u8>],
    seq: u64,
    stats: &mut ReadStats,
    budget: Option<&crate::read::ReadBudget>,
) -> Result<Vec<Option<Record>>> {
    if keys.windows(2).all(|w| w[0] < w[1]) {
        return get_sorted(view, keys, seq, stats, budget);
    }
    let mut grouped = BTreeMap::<&Vec<u8>, Vec<usize>>::new();
    for (position, key) in keys.iter().enumerate() {
        grouped.entry(key).or_default().push(position);
    }
    let unique: Vec<_> = grouped.keys().map(|key| (*key).clone()).collect();
    let values = get_sorted(view, &unique, seq, stats, budget)?;
    let mut result = vec![None; keys.len()];
    for (positions, value) in grouped.into_values().zip(values) {
        for position in positions {
            result[position] = value.clone();
        }
    }
    Ok(result)
}
fn get_sorted(
    view: &View,
    keys: &[Vec<u8>],
    seq: u64,
    stats: &mut ReadStats,
    budget: Option<&crate::read::ReadBudget>,
) -> Result<Vec<Option<Record>>> {
    let mut out: Vec<_> = keys.iter().map(|key| view.mem.get(key, seq)).collect();
    for mem in view.frozen.iter().rev() {
        for (i, key) in keys.iter().enumerate() {
            if let Some(record) = mem.get(key, seq)
                && out[i].as_ref().is_none_or(|old| old.seq < record.seq)
            {
                out[i] = Some(record);
            }
        }
    }
    let tables = view.tables.candidates(keys);
    for table in tables {
        if let Some(budget) = budget {
            budget.check()?;
        }
        let begin = keys.partition_point(|key| *key < table.info.first);
        let end = keys.partition_point(|key| *key <= table.info.last);
        let indexes: Vec<_> = (begin..end)
            .filter(|i| out[*i].as_ref().is_none_or(|r| r.seq < table.info.max_seq))
            .collect();
        if indexes.is_empty() {
            continue;
        }
        let subset: Vec<_> = indexes.iter().map(|i| keys[*i].clone()).collect();
        for (i, record) in indexes
            .into_iter()
            .zip(table.multi_get_controlled(&subset, seq, stats, budget)?)
        {
            if let Some(record) = record
                && out[i].as_ref().is_none_or(|old| old.seq < record.seq)
            {
                out[i] = Some(record);
            }
        }
    }
    Ok(out)
}

struct LevelCursor<'a> {
    tables: &'a [Arc<Table>],
    index: usize,
    cursor: Option<TableCursor<'a>>,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
    filter: Option<Vec<u8>>,
    read_budget: Option<crate::read::ReadBudget>,
}
impl LevelCursor<'_> {
    fn next(&mut self, stats: &mut ReadStats) -> Result<Option<Record>> {
        loop {
            if let Some(cursor) = &mut self.cursor {
                if let Some(record) = cursor.next(stats)? {
                    return Ok(Some(record));
                }
                if self
                    .end
                    .as_ref()
                    .is_some_and(|end| self.tables[self.index].info.last >= *end)
                {
                    return Ok(None);
                }
                self.cursor = None;
                self.index += 1;
            }
            let Some(table) = self.tables.get(self.index) else {
                return Ok(None);
            };
            if self.end.as_ref().is_some_and(|e| table.info.first >= *e) {
                return Ok(None);
            }
            self.cursor = Some(table.cursor(
                Some(self.start.clone()),
                self.end.clone(),
                false,
                self.filter.clone(),
                stats,
            )?);
            if let (Some(cursor), Some(budget)) = (&mut self.cursor, &self.read_budget) {
                cursor.set_read_budget(budget);
            }
        }
    }
}
enum Source<'a> {
    Level(LevelCursor<'a>),
    Mem {
        mem: &'a MemTable,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        last: Option<(Vec<u8>, u64)>,
    },
    Table(TableCursor<'a>),
}
impl Source<'_> {
    fn seek(&mut self, range: &ScanRange) -> Result<()> {
        match self {
            Source::Table(cursor) => {
                cursor.seek(&range.start, range.end.clone(), range.prefix.clone())
            }
            Source::Mem {
                start, end, last, ..
            } => {
                start.clone_from(&range.start);
                end.clone_from(&range.end);
                *last = None;
                Ok(())
            }
            Source::Level(level) => {
                let index = level.tables.partition_point(|t| t.info.last < range.start);
                if index != level.index {
                    level.cursor = None;
                }
                level.index = index;
                level.start.clone_from(&range.start);
                level.end.clone_from(&range.end);
                level.filter.clone_from(&range.prefix);
                if let Some(cursor) = &mut level.cursor {
                    cursor.seek(&range.start, range.end.clone(), range.prefix.clone())?;
                }
                Ok(())
            }
        }
    }
    fn next(&mut self, stats: &mut ReadStats) -> Result<Option<Record>> {
        match self {
            Source::Table(cursor) => cursor.next(stats),
            Source::Level(cursor) => cursor.next(stats),
            Source::Mem {
                mem,
                start,
                end,
                last,
            } => {
                let data = mem.data.read();
                let lower = last
                    .as_ref()
                    .map_or(start.as_slice(), |(k, _)| k.as_slice());
                for (key, versions) in data.range::<[u8], _>((
                    std::ops::Bound::Included(lower),
                    std::ops::Bound::Unbounded,
                )) {
                    if end.as_ref().is_some_and(|e| key >= e) {
                        break;
                    }
                    let end = if let Some((_, seq)) = last.as_ref().filter(|(k, _)| key == k) {
                        versions.partition_point(|r| r.seq < *seq)
                    } else {
                        versions.len()
                    };
                    if let Some(i) = end.checked_sub(1) {
                        let record = &versions[i];
                        *last = Some((key.clone(), record.seq));
                        return Ok(Some(record.clone()));
                    }
                }
                Ok(None)
            }
        }
    }
}
pub(super) struct HeapRecord {
    pub(super) record: Record,
    pub(super) source: usize,
}
impl PartialEq for HeapRecord {
    fn eq(&self, other: &Self) -> bool {
        self.record.key == other.record.key
            && self.record.seq == other.record.seq
            && self.source == other.source
    }
}
impl Eq for HeapRecord {}
impl Ord for HeapRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .record
            .key
            .cmp(&self.record.key)
            .then(self.record.seq.cmp(&other.record.seq))
            .then(self.source.cmp(&other.source))
    }
}
impl PartialOrd for HeapRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
pub struct Scan<'a> {
    sources: Vec<Source<'a>>,
    heap: BinaryHeap<HeapRecord>,
    seq: u64,
    last: Option<Vec<u8>>,
    snapshot: Option<&'a Snapshot>,
    limits: Option<(usize, Instant)>,
    cancellation: Option<crate::ReadCancellation>,
}
impl<'a> Scan<'a> {
    pub(super) fn new(
        view: &'a View,
        seq: u64,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        filter_prefix: Option<Vec<u8>>,
        snapshot: Option<&'a Snapshot>,
        stats: &mut ReadStats,
    ) -> Result<Self> {
        let range = ScanRange {
            start: start.clone(),
            end: end.clone(),
            prefix: filter_prefix,
        };
        let mut scan = Self::unstarted(view, seq, start, end, snapshot, stats)?;
        scan.seek(&range, stats)?;
        Ok(scan)
    }
    fn unstarted(
        view: &'a View,
        seq: u64,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        snapshot: Option<&'a Snapshot>,
        stats: &mut ReadStats,
    ) -> Result<Self> {
        let mut sources = vec![Source::Mem {
            mem: &view.mem,
            start: start.clone(),
            end: end.clone(),
            last: None,
        }];
        for mem in &view.frozen {
            sources.push(Source::Mem {
                mem,
                start: start.clone(),
                end: end.clone(),
                last: None,
            });
        }
        for (level, tables) in view.tables.levels.iter().enumerate() {
            if level == 0 {
                for table in tables {
                    if table.info.last < start
                        || end.as_ref().is_some_and(|e| table.info.first >= *e)
                    {
                        continue;
                    }
                    sources.push(Source::Table(table.cursor(
                        Some(start.clone()),
                        end.clone(),
                        false,
                        None,
                        stats,
                    )?));
                }
            } else {
                let index = tables.partition_point(|t| t.info.last < start);
                sources.push(Source::Level(LevelCursor {
                    tables,
                    index,
                    cursor: None,
                    start: start.clone(),
                    end: end.clone(),
                    filter: None,
                    read_budget: None,
                }));
            }
        }
        Ok(Self {
            sources,
            heap: BinaryHeap::new(),
            seq,
            last: None,
            snapshot,
            limits: None,
            cancellation: None,
        })
    }
    fn seek(&mut self, range: &ScanRange, stats: &mut ReadStats) -> Result<()> {
        self.heap.clear();
        self.last = None;
        if range.end.as_ref().is_some_and(|end| range.start >= *end) {
            return Ok(());
        }
        for (i, source) in self.sources.iter_mut().enumerate() {
            source.seek(range)?;
            if let Some(record) = source.next(stats)? {
                self.heap.push(HeapRecord { record, source: i });
            }
        }
        Ok(())
    }
    pub(crate) fn set_read_budget(&mut self, budget: &crate::read::ReadBudget) {
        self.set_budget(budget.max_scan, budget.deadline);
        self.cancellation = budget.cancellation.clone();
        for source in &mut self.sources {
            match source {
                Source::Table(cursor) => cursor.set_read_budget(budget),
                Source::Level(level) => {
                    level.read_budget = Some(budget.clone());
                    if let Some(cursor) = &mut level.cursor {
                        cursor.set_read_budget(budget);
                    }
                }
                Source::Mem { .. } => {}
            }
        }
    }
    pub fn set_budget(&mut self, entries: usize, deadline: Instant) {
        self.limits = Some((entries, deadline));
    }
    fn check_cancellation(&self) -> Result<()> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(crate::ReadCancellation::is_cancelled)
        {
            return Err(Error::new("CANCELLED", "scan cancelled"));
        }
        Ok(())
    }
    pub fn next(&mut self, stats: &mut ReadStats) -> Result<Option<Record>> {
        if let Some(snapshot) = self.snapshot {
            snapshot.check()?;
        }
        self.check_cancellation()?;
        while let Some(item) = self.heap.pop() {
            self.check_cancellation()?;
            stats.scanned_entries += 1;
            if let Some((limit, deadline)) = self.limits {
                if stats.scanned_entries > limit as u64 {
                    return Err(Error::limit("scan budget exceeded"));
                }
                if Instant::now() > deadline {
                    return Err(Error::new("DEADLINE_EXCEEDED", "scan deadline"));
                }
            }
            let record = item.record;
            if let Some(next) = self.sources[item.source].next(stats)? {
                self.heap.push(HeapRecord {
                    record: next,
                    source: item.source,
                });
            }
            if record.seq > self.seq || self.last.as_ref() == Some(&record.key) {
                continue;
            }
            self.last = Some(record.key.clone());
            if record.value.is_some() {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disjoint_ranges_preserve_spanning_versions_and_tombstones() {
        let root = tempfile::tempdir().unwrap();
        let store = crate::Store::open(
            root.path(),
            Options {
                block_bytes: 1024,
                target_sst_bytes: 32 << 10,
                memtable_bytes: 8 << 20,
                ..Options::default()
            },
        )
        .unwrap();
        let tenant = store
            .tenant(store.create_tenant("ranges").unwrap().id)
            .unwrap();
        let key = |i| format!("Z{i:04}").into_bytes();
        for version in 0..12u8 {
            tenant
                .engine()
                .transact(|tx| {
                    for i in 0..32 {
                        tx.put(key(i), vec![version; 2000])?;
                    }
                    Ok(())
                })
                .unwrap();
        }
        let old = tenant.engine().snapshot().unwrap();
        for version in 12..24u8 {
            tenant
                .engine()
                .transact(|tx| {
                    for i in 0..32 {
                        tx.put(key(i), vec![version; 2000])?;
                    }
                    Ok(())
                })
                .unwrap();
        }
        tenant.engine().flush().unwrap();
        tenant
            .engine()
            .transact(|tx| {
                for i in (0..32).step_by(2) {
                    tx.delete(key(i))?;
                }
                Ok(())
            })
            .unwrap();
        tenant.engine().flush().unwrap();
        tenant.engine().compact(true).unwrap();
        let current = tenant.engine().snapshot().unwrap();
        for (snapshot, version, count) in [(&old, 11, 32), (&current, 23, 16)] {
            let ranges = (0..40)
                .map(|i| ScanRange {
                    start: key(i),
                    end: codec::prefix_end(&key(i)),
                    prefix: None,
                })
                .collect();
            let mut stats = ReadStats::default();
            let mut scan = snapshot.scan_ranges(ranges, &mut stats, None).unwrap();
            scan.scan
                .set_budget(100_000, Instant::now() + Duration::from_secs(10));
            let mut found = Vec::new();
            while let Some(record) = scan.next(&mut stats).unwrap() {
                assert_eq!(record.value.unwrap().as_slice(), vec![version; 2000]);
                assert_eq!(record.key, key(scan.range_index()));
                found.push(record.key);
            }
            assert_eq!(found.len(), count);
            if version == 23 {
                assert_eq!(found, (1..32).step_by(2).map(key).collect::<Vec<_>>());
            }
        }
        let ranges = vec![ScanRange {
            start: key(0),
            end: codec::prefix_end(&key(31)),
            prefix: None,
        }];
        let mut stats = ReadStats::default();
        let mut scan = old.scan_ranges(ranges, &mut stats, None).unwrap();
        scan.scan
            .set_budget(1, Instant::now() + Duration::from_secs(10));
        assert_eq!(
            scan.next(&mut stats).unwrap_err().code,
            "RESOURCE_EXHAUSTED"
        );

        let cancellation = crate::ReadCancellation::default();
        let budget = crate::read::ReadBudget::new(
            &crate::ReadOptions {
                cancellation: Some(cancellation.clone()),
                ..Default::default()
            },
            &tenant.engine().options,
        )
        .unwrap();
        let mut stats = ReadStats::default();
        let mut scan = old
            .scan(key(0), codec::prefix_end(&key(31)), None, &mut stats)
            .unwrap();
        scan.set_read_budget(&budget);
        assert!(scan.next(&mut stats).unwrap().is_some());
        cancellation.cancel();
        let visited = stats.scanned_entries;
        assert_eq!(scan.next(&mut stats).unwrap_err().code, "CANCELLED");
        assert_eq!(stats.scanned_entries, visited);
        assert!(
            old.get(&key(0), &mut ReadStats::default())
                .unwrap()
                .is_some()
        );
    }
}
