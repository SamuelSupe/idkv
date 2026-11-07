mod writer;
use super::{
    cache::{Cache, CacheKey, Contents, Frame},
    files::{reject_symlink, sync_dir},
};
use crate::resources::Bytes;
use crate::{
    Error, Result,
    codec::{self, Reader, put_bytes, put_u32, put_u64},
    model::ReadStats,
    resources::{Budget, IoScheduler, IoTask, Reservation},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use uuid::Uuid;
pub use writer::TableWriter;

const HEADER: usize = 56;
const FOOTER: usize = 20;
const MAX_FRAME: usize = 32 << 20;

#[cfg(test)]
#[test]
fn point_get_preserves_versions_and_tombstones_across_index_partitions() {
    use crate::resources::Counter;
    let dir = tempfile::tempdir().unwrap();
    let id = Uuid::new_v4();
    let generation = Uuid::new_v4();
    let budget = Budget {
        global: Counter::new(128 << 20),
        tenant: Counter::new(128 << 20),
    };
    let io = IoScheduler::new(2);
    let mut writer = TableWriter::new(
        dir.path(),
        id,
        generation,
        0,
        1024,
        io.clone(),
        budget.clone(),
    )
    .unwrap();
    for seq in (1..=300).rev() {
        writer
            .add(Record {
                key: b"same".to_vec(),
                seq,
                value: if seq == 177 {
                    None
                } else {
                    Some(Arc::new(Bytes::new(vec![(seq % 256) as u8; 1200])))
                },
                prefix_len: 0,
            })
            .unwrap();
    }
    writer
        .add(Record {
            key: b"tail".to_vec(),
            seq: 1,
            value: Some(Arc::new(Bytes::new(vec![1; 1200]))),
            prefix_len: 0,
        })
        .unwrap();
    let info = writer.finish().unwrap();
    let table = Table::open(
        dir.path(),
        info,
        id,
        generation,
        io,
        budget,
        Arc::new(Cache::new(16 << 20)),
    )
    .unwrap();
    assert!(table.top.len() > 1);
    let mut latest_stats = ReadStats::default();
    assert_eq!(
        table
            .get(b"same", 300, &mut latest_stats)
            .unwrap()
            .unwrap()
            .seq,
        300
    );
    assert!(
        latest_stats.blocks_read <= 2,
        "latest point read must not load the retained history"
    );
    for seq in [0, 1, 63, 64, 65, 128, 177, 178, 299, 300, 1000] {
        let point = table.get(b"same", seq, &mut ReadStats::default()).unwrap();
        let batch = table
            .multi_get_controlled(
                &[b"same".to_vec(), b"absent".to_vec()],
                seq,
                &mut ReadStats::default(),
                None,
            )
            .unwrap();
        assert_eq!(
            batch[0].as_ref().map(|r| r.seq),
            point.as_ref().map(|r| r.seq)
        );
        assert_eq!(
            batch[0].as_ref().map(|r| r.value.is_some()),
            point.as_ref().map(|r| r.value.is_some())
        );
        assert!(batch[1].is_none());
        if seq == 0 {
            assert!(point.is_none());
        } else {
            let record = point.unwrap();
            assert_eq!(record.seq, seq.min(300));
            assert_eq!(record.value.is_none(), seq == 177);
            if let Some(value) = record.value {
                assert_eq!(value[0], (seq.min(300) % 256) as u8);
            }
        }
    }
    assert!(
        table
            .get(b"absent", 300, &mut ReadStats::default())
            .unwrap()
            .is_none()
    );
    // Budget enough for the largest single decode, but not four decoded blocks
    // plus another decode. Range scans must make progress without skipping a
    // block when optional lookahead cannot fit, including partition boundaries.
    table.cache.clear();
    let mut decode_bytes = 0;
    for part in &table.top {
        let frame = table
            .frame_controlled(part, true, true, &mut ReadStats::default(), None)
            .unwrap();
        let Contents::Index(blocks) = &frame.contents else {
            unreachable!()
        };
        for block in blocks {
            let mut header = [0; 12];
            table.file.read_exact_at(&mut header, block.offset).unwrap();
            let raw = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
            decode_bytes = decode_bytes.max(raw * 12 + block.len as usize + 1024);
        }
    }
    let limit = table.budget.tenant.used() + decode_bytes + 512;
    table.budget.tenant.set_limit(limit).unwrap();
    let mut stats = ReadStats::default();
    let mut cursor = table
        .cursor(
            Some(b"same".to_vec()),
            Some(b"tail".to_vec()),
            false,
            None,
            &mut stats,
        )
        .unwrap();
    let mut expected = 300;
    while let Some(record) = cursor.next(&mut stats).unwrap() {
        assert_eq!(record.key, b"same");
        assert_eq!(record.seq, expected);
        assert_eq!(record.value.is_none(), expected == 177);
        expected -= 1;
    }
    assert_eq!(expected, 0);
}

#[derive(Clone, Debug)]
pub struct Record {
    pub key: Vec<u8>,
    pub seq: u64,
    pub value: Option<Arc<Bytes>>,
    pub prefix_len: usize,
}

#[cfg(test)]
#[test]
fn prefix_compressed_keys_cannot_bypass_the_decoded_frame_budget() {
    use crate::resources::Counter;
    let dir = tempfile::tempdir().unwrap();
    let tenant = Uuid::new_v4();
    let generation = Uuid::new_v4();
    let budget = Budget {
        global: Counter::new(64 << 20),
        tenant: Counter::new(64 << 20),
    };
    let io = IoScheduler::new(2);
    let mut writer = TableWriter::new(
        dir.path(),
        tenant,
        generation,
        0,
        2 << 20,
        io.clone(),
        budget,
    )
    .unwrap();
    let key = vec![7; 6000];
    for seq in (1..=128).rev() {
        writer
            .add(Record {
                key: key.clone(),
                seq,
                value: Some(Arc::new(Bytes::new(vec![42]))),
                prefix_len: 0,
            })
            .unwrap();
    }
    let info = writer.finish().unwrap();
    for (limit, succeeds) in [(512 << 10, false), (2 << 20, true)] {
        let budget = Budget {
            global: Counter::new(64 << 20),
            tenant: Counter::new(limit),
        };
        let table = Table::open(
            dir.path(),
            info.clone(),
            tenant,
            generation,
            io.clone(),
            budget.clone(),
            Arc::new(Cache::new(0)),
        )
        .unwrap();
        let read = table.get(&key, 128, &mut ReadStats::default());
        if succeeds {
            assert_eq!(read.unwrap().unwrap().seq, 128);
        } else {
            assert_eq!(read.unwrap_err().code, "RESOURCE_EXHAUSTED");
        }
        drop(table);
        assert_eq!(budget.tenant.used(), 0);
        assert_eq!(budget.global.used(), 0);
    }
}
impl Record {
    pub fn size(&self) -> usize {
        self.key.len() + self.value.as_ref().map_or(0, |v| v.len()) + 128
    }
    pub fn internal_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        codec::escape(&self.key, &mut key);
        key.extend((!self.seq).to_be_bytes());
        key
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableInfo {
    pub id: Uuid,
    pub level: u32,
    pub size: u64,
    pub first: Vec<u8>,
    pub last: Vec<u8>,
    pub max_seq: u64,
    pub entries: u64,
}
#[derive(Clone, Debug)]
pub struct BlockMeta {
    pub first: Vec<u8>,
    pub last: Vec<u8>,
    pub offset: u64,
    pub len: u32,
    pub bloom: Vec<u8>,
}
pub struct Table {
    pub info: TableInfo,
    path: PathBuf,
    file: Arc<File>,
    top: Vec<BlockMeta>,
    tenant: Uuid,
    io: Arc<IoScheduler>,
    budget: Budget,
    cache: Arc<Cache>,
    _memory: Reservation,
    lifetime: Arc<FileLifetime>,
}
#[derive(Clone, Copy)]
struct FrameRead {
    key: CacheKey,
    len: u32,
    file_size: u64,
    index: bool,
    admit: bool,
    evict: bool,
}
struct FileLifetime {
    path: PathBuf,
    obsolete: AtomicBool,
}
impl Drop for FileLifetime {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::Acquire) {
            let _ = fs::remove_file(&self.path);
        }
    }
}
impl Table {
    pub(crate) fn checkpoint_file(&self) -> Arc<File> {
        self.file.clone()
    }
    pub fn promoted(&self, level: u32) -> Result<Arc<Self>> {
        let memory = self.budget.reserve(self._memory.size())?;
        let mut info = self.info.clone();
        info.level = level;
        Ok(Arc::new(Self {
            info,
            path: self.path.clone(),
            file: self.file.clone(),
            top: self.top.clone(),
            tenant: self.tenant,
            io: self.io.clone(),
            budget: self.budget.clone(),
            cache: self.cache.clone(),
            _memory: memory,
            lifetime: self.lifetime.clone(),
        }))
    }
    pub fn mark_obsolete(&self) {
        self.lifetime.obsolete.store(true, Ordering::Release);
    }
    pub fn open(
        dir: &Path,
        info: TableInfo,
        tenant: Uuid,
        generation: Uuid,
        io: Arc<IoScheduler>,
        budget: Budget,
        cache: Arc<Cache>,
    ) -> Result<Arc<Self>> {
        let path = dir.join(format!("{}.sst", info.id));
        reject_symlink(&path)?;
        let file = Arc::new(File::open(&path)?);
        if file.metadata()?.len() != info.size || info.size < (HEADER + FOOTER) as u64 {
            return Err(Error::corrupt("SST file size"));
        }
        let mut header = [0; HEADER];
        file.read_exact_at(&mut header, 0)?;
        if &header[..8] != b"TKSST001"
            || &header[8..24] != tenant.as_bytes()
            || &header[24..40] != generation.as_bytes()
            || &header[40..56] != info.id.as_bytes()
        {
            return Err(Error::corrupt("SST identity or version mismatch"));
        }
        let mut footer = [0; FOOTER];
        file.read_exact_at(&mut footer, info.size - FOOTER as u64)?;
        if &footer[12..] != b"TKEND001" {
            return Err(Error::corrupt("SST footer"));
        }
        let offset = u64::from_le_bytes(footer[..8].try_into().unwrap());
        let len = u32::from_le_bytes(footer[8..12].try_into().unwrap());
        validate_extent(offset, len, info.size)?;
        let mut frame_header = [0; 12];
        file.read_exact_at(&mut frame_header, offset)?;
        let raw_len = u32::from_le_bytes(frame_header[4..8].try_into().unwrap()) as usize;
        if raw_len > MAX_FRAME {
            return Err(Error::corrupt("SST top directory length"));
        }
        let input_memory = budget.reserve(raw_len + len as usize + 1024)?;
        let raw = read_frame(&file, offset, len)?;
        let memory = budget.reserve(decoded_frame_memory(&raw, true)?)?;
        let top = decode_meta(&raw, info.size)?;
        drop(raw);
        drop(input_memory);
        if top.is_empty() || top[0].first != info.first || top.last().unwrap().last != info.last {
            return Err(Error::corrupt("SST key boundaries"));
        }
        Ok(Arc::new(Self {
            info,
            lifetime: Arc::new(FileLifetime {
                path: path.clone(),
                obsolete: AtomicBool::new(false),
            }),
            path,
            file,
            top,
            tenant,
            io,
            budget,
            cache,
            _memory: memory,
        }))
    }
    fn frame_read(&self, meta: &BlockMeta, index: bool, admit: bool) -> FrameRead {
        FrameRead {
            key: CacheKey(self.info.id, meta.offset),
            len: meta.len,
            file_size: self.info.size,
            index,
            admit,
            evict: true,
        }
    }
    fn pending_frame(
        &self,
        meta: &BlockMeta,
        index: bool,
        admit: bool,
        read_budget: Option<&crate::read::ReadBudget>,
    ) -> Result<PendingFrame> {
        let key = CacheKey(self.info.id, meta.offset);
        if let Some(frame) = self.cache.get(&key) {
            return Ok(PendingFrame::Ready(frame));
        }
        let file = self.file.clone();
        let read = self.frame_read(meta, index, admit);
        let budget = self.budget.clone();
        let cache = self.cache.clone();
        let task = self.io.submit_read(self.tenant, read_budget, move || {
            load_frame(&file, read, &budget, &cache)
        })?;
        Ok(PendingFrame::Loading(task))
    }
    fn scan_frames(
        &self,
        metas: &[BlockMeta],
        admit: bool,
        stats: &mut ReadStats,
        read_budget: Option<&crate::read::ReadBudget>,
    ) -> Result<VecDeque<Arc<Frame>>> {
        if let Some(frames) = metas
            .iter()
            .map(|meta| self.cache.get(&CacheKey(self.info.id, meta.offset)))
            .collect::<Option<VecDeque<_>>>()
        {
            stats.cache_hits += metas.len() as u64;
            return Ok(frames);
        }
        let file = self.file.clone();
        let reads: Vec<_> = metas
            .iter()
            .map(|meta| self.frame_read(meta, false, admit))
            .collect();
        let budget = self.budget.clone();
        let cache = self.cache.clone();
        let task_budget = read_budget.cloned();
        let (frames, delta) = self
            .io
            .submit_read(self.tenant, read_budget, move || {
                let mut frames = VecDeque::new();
                let mut stats = ReadStats::default();
                for mut read in reads {
                    if let Some(budget) = &task_budget {
                        budget.check()?;
                    }
                    read.evict = frames.is_empty();
                    match load_frame(&file, read, &budget, &cache) {
                        Ok((frame, delta)) => {
                            frames.push_back(frame);
                            stats.add(&delta);
                        }
                        // Additional blocks are optional. Retry them after consuming
                        // this prefix, when its charged memory has been released.
                        Err(error) if error.code == "RESOURCE_EXHAUSTED" && !frames.is_empty() => {
                            break;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Ok((frames, stats))
            })?
            .wait()?;
        stats.add(&delta);
        Ok(frames)
    }
    fn frame_controlled(
        &self,
        meta: &BlockMeta,
        index: bool,
        admit: bool,
        stats: &mut ReadStats,
        budget: Option<&crate::read::ReadBudget>,
    ) -> Result<Arc<Frame>> {
        self.pending_frame(meta, index, admit, budget)?.wait(stats)
    }
    pub fn get(&self, key: &[u8], seq: u64, stats: &mut ReadStats) -> Result<Option<Record>> {
        self.get_controlled(key, seq, stats, None)
    }
    pub(crate) fn get_controlled(
        &self,
        key: &[u8],
        seq: u64,
        stats: &mut ReadStats,
        budget: Option<&crate::read::ReadBudget>,
    ) -> Result<Option<Record>> {
        if key < self.info.first.as_slice() || key > self.info.last.as_slice() {
            return Ok(None);
        }
        stats.candidate_files += 1;
        let mut part = self.top.partition_point(|m| m.last.as_slice() < key);
        // A retained MVCC chain may span blocks and index partitions. Search all
        // overlapping partitions, including tombstones and older snapshot versions.
        while part < self.top.len() && self.top[part].first.as_slice() <= key {
            let frame = self.frame_controlled(&self.top[part], true, true, stats, budget)?;
            let Contents::Index(blocks) = &frame.contents else {
                return Err(Error::corrupt("index frame type"));
            };
            let mut block = blocks.partition_point(|m| m.last.as_slice() < key);
            while block < blocks.len() && blocks[block].first.as_slice() <= key {
                if bloom_contains(&blocks[block].bloom, key) {
                    let frame =
                        self.frame_controlled(&blocks[block], false, true, stats, budget)?;
                    let Contents::Data(records) = &frame.contents else {
                        return Err(Error::corrupt("data frame type"));
                    };
                    let start = records.partition_point(|r| {
                        r.key.as_slice() < key || (r.key.as_slice() == key && r.seq > seq)
                    });
                    if let Some(record) = records.get(start).filter(|r| r.key == key) {
                        // Internal keys are sorted by descending sequence across
                        // blocks as well: the first visible version is final.
                        return Ok(Some(record.clone()));
                    }
                }
                block += 1;
            }
            part += 1;
        }
        Ok(None)
    }
    pub(crate) fn multi_get_controlled(
        &self,
        keys: &[Vec<u8>],
        seq: u64,
        stats: &mut ReadStats,
        budget: Option<&crate::read::ReadBudget>,
    ) -> Result<Vec<Option<Record>>> {
        let mut out = vec![None; keys.len()];
        let mut groups = std::collections::BTreeMap::<usize, Vec<usize>>::new();
        for (i, key) in keys.iter().enumerate() {
            if key < &self.info.first || key > &self.info.last {
                continue;
            }
            let part = self.top.partition_point(|m| m.last < *key);
            if part < self.top.len() {
                groups.entry(part).or_default().push(i);
            }
        }
        if !groups.is_empty() {
            stats.candidate_files += 1;
        }
        for (part, indexes) in groups {
            if let Some(budget) = budget {
                budget.check()?;
            }
            let partition = self.frame_controlled(&self.top[part], true, true, stats, budget)?;
            let Contents::Index(blocks) = &partition.contents else {
                return Err(Error::corrupt("index frame type"));
            };
            let mut data_groups = std::collections::BTreeMap::<usize, Vec<usize>>::new();
            for &i in &indexes {
                let mut block = blocks.partition_point(|m| m.last < keys[i]);
                while block < blocks.len() && blocks[block].first <= keys[i] {
                    if bloom_contains(&blocks[block].bloom, &keys[i]) {
                        data_groups.entry(block).or_default().push(i);
                    }
                    block += 1;
                }
            }
            let groups: Vec<_> = data_groups.into_iter().collect();
            for chunk in groups.chunks(8) {
                if let Some(budget) = budget {
                    budget.check()?;
                }
                let pending = chunk
                    .iter()
                    .map(|(block, indexes)| {
                        self.pending_frame(&blocks[*block], false, true, budget)
                            .map(|frame| (indexes, frame))
                    })
                    .collect::<Result<Vec<_>>>()?;
                for (indexes, frame) in pending {
                    if let Some(budget) = budget {
                        budget.check()?;
                    }
                    let data = frame.wait(stats)?;
                    let Contents::Data(records) = &data.contents else {
                        return Err(Error::corrupt("data frame type"));
                    };
                    for &i in indexes {
                        let start = records.partition_point(|r| {
                            r.key < keys[i] || (r.key == keys[i] && r.seq > seq)
                        });
                        if let Some(record) = records.get(start).filter(|r| r.key == keys[i])
                            && out[i]
                                .as_ref()
                                .is_none_or(|old: &Record| old.seq < record.seq)
                        {
                            out[i] = Some(record.clone());
                        }
                    }
                }
            }
            // A version chain can span index partitions.
            for &i in &indexes {
                if out[i].is_none()
                    && self.top[part].last == keys[i]
                    && part + 1 < self.top.len()
                    && self.top[part + 1].first == keys[i]
                {
                    let mut cursor =
                        self.cursor(Some(keys[i].clone()), None, false, None, stats)?;
                    if let Some(budget) = budget {
                        cursor.set_read_budget(budget);
                    }
                    while let Some(record) = cursor.next(stats)? {
                        if let Some(budget) = budget {
                            budget.check()?;
                        }
                        if record.key != keys[i] {
                            break;
                        }
                        if record.seq <= seq
                            && out[i].as_ref().is_none_or(|old| old.seq < record.seq)
                        {
                            out[i] = Some(record);
                            break;
                        }
                    }
                }
            }
        }
        Ok(out)
    }
    pub fn cursor(
        &self,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        admit: bool,
        filter_prefix: Option<Vec<u8>>,
        stats: &mut ReadStats,
    ) -> Result<TableCursor<'_>> {
        stats.candidate_files += 1;
        let part = start
            .as_ref()
            .map_or(0, |s| self.top.partition_point(|m| m.last < *s));
        Ok(TableCursor {
            table: self,
            part,
            block: 0,
            record: 0,
            partition: None,
            data: None,
            buffered: Default::default(),
            sequential: false,
            read_budget: None,
            start,
            end,
            admit,
            filter_prefix,
        })
    }
    pub fn verify(&self) -> Result<()> {
        let mut stats = ReadStats::default();
        let mut iter = self.cursor(None, None, false, None, &mut stats)?;
        let mut previous: Option<Vec<u8>> = None;
        let mut count = 0;
        while let Some(record) = iter.next(&mut stats)? {
            let key = record.internal_key();
            if previous.as_ref().is_some_and(|p| p >= &key) {
                return Err(Error::corrupt("SST record ordering"));
            }
            previous = Some(key);
            count += 1;
        }
        if count != self.info.entries {
            return Err(Error::corrupt("SST entry count"));
        }
        Ok(())
    }
}
fn load_frame(
    file: &File,
    read: FrameRead,
    budget: &Budget,
    cache: &Cache,
) -> Result<(Arc<Frame>, ReadStats)> {
    let FrameRead {
        key,
        len,
        file_size,
        index,
        admit,
        evict,
    } = read;
    let offset = key.1;
    if let Some(frame) = cache.get(&key) {
        return Ok((
            frame,
            ReadStats {
                cache_hits: 1,
                ..ReadStats::default()
            },
        ));
    }
    let mut header = [0; 12];
    file.read_exact_at(&mut header, offset)?;
    let raw_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    if raw_len > MAX_FRAME {
        return Err(Error::corrupt("SST decompressed length"));
    }
    let reserve = |size| {
        if evict {
            cache.reserve(budget, size)
        } else {
            budget.reserve(size)
        }
    };
    let input_memory = reserve(raw_len + len as usize + 1024)?;
    let raw = read_frame(file, offset, len)?;
    let mut memory = reserve(decoded_frame_memory(&raw, index)?)?;
    let contents = if index {
        Contents::Index(decode_meta(&raw, file_size)?)
    } else {
        Contents::Data(decode_records(&raw)?)
    };
    let weight = match &contents {
        Contents::Data(records) => {
            records.capacity() * std::mem::size_of::<Record>()
                + records
                    .iter()
                    .map(|r| r.key.capacity() + r.value.as_ref().map_or(0, |v| v.capacity() + 80))
                    .sum::<usize>()
                + 256
        }
        Contents::Index(blocks) => {
            blocks.capacity() * std::mem::size_of::<BlockMeta>()
                + blocks
                    .iter()
                    .map(|b| b.first.capacity() + b.last.capacity() + b.bloom.capacity())
                    .sum::<usize>()
                + 256
        }
    };
    drop(raw);
    drop(input_memory);
    memory.shrink(weight);
    let memory = Arc::new(memory);
    let mut contents = contents;
    if let Contents::Data(records) = &mut contents {
        for record in records {
            if let Some(value) = &mut record.value {
                Arc::make_mut(value).charge = Some(memory.clone());
            }
        }
    }
    let frame = Arc::new(Frame {
        contents,
        referenced: AtomicBool::new(index),
        reservation: memory,
    });
    if admit || index {
        cache.insert(key, frame.clone());
    }
    Ok((
        frame,
        ReadStats {
            blocks_read: 1,
            decompressed_bytes: raw_len as u64,
            ..ReadStats::default()
        },
    ))
}

enum PendingFrame {
    Ready(Arc<Frame>),
    Loading(IoTask<(Arc<Frame>, ReadStats)>),
}
impl PendingFrame {
    fn wait(self, stats: &mut ReadStats) -> Result<Arc<Frame>> {
        match self {
            Self::Ready(frame) => {
                stats.cache_hits += 1;
                Ok(frame)
            }
            Self::Loading(task) => {
                let (frame, delta) = task.wait()?;
                stats.add(&delta);
                Ok(frame)
            }
        }
    }
}
pub struct TableCursor<'a> {
    table: &'a Table,
    part: usize,
    block: usize,
    record: usize,
    partition: Option<Arc<Frame>>,
    data: Option<Arc<Frame>>,
    buffered: VecDeque<Arc<Frame>>,
    sequential: bool,
    read_budget: Option<crate::read::ReadBudget>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    admit: bool,
    filter_prefix: Option<Vec<u8>>,
}
impl TableCursor<'_> {
    pub(crate) fn set_read_budget(&mut self, budget: &crate::read::ReadBudget) {
        self.read_budget = Some(budget.clone());
    }
    pub(crate) fn seek(
        &mut self,
        start: &[u8],
        end: Option<Vec<u8>>,
        prefix: Option<Vec<u8>>,
    ) -> Result<()> {
        self.start = Some(start.to_vec());
        self.end = end;
        self.filter_prefix = prefix;
        // Strict lower bound avoids reusing a later block of an MVCC chain
        // whose earlier versions may live in the preceding block/partition.
        if let Some(data) = &self.data
            && let Contents::Data(records) = &data.contents
            && records.first().is_some_and(|r| r.key.as_slice() < start)
            && records.last().is_some_and(|r| r.key.as_slice() >= start)
        {
            self.record = records.partition_point(|r| r.key.as_slice() < start);
            return Ok(());
        }
        let part = self
            .table
            .top
            .partition_point(|m| m.last.as_slice() < start);
        if part != self.part {
            self.partition = None;
        }
        self.part = part;
        self.block = match self.partition.as_ref().map(|p| &p.contents) {
            Some(Contents::Index(blocks)) => blocks.partition_point(|m| m.last.as_slice() < start),
            _ => 0,
        };
        self.data = None;
        self.buffered.clear();
        self.sequential = false;
        self.record = 0;
        Ok(())
    }
    pub fn next(&mut self, stats: &mut ReadStats) -> Result<Option<Record>> {
        loop {
            if let Some(data) = &self.data {
                let Contents::Data(records) = &data.contents else {
                    return Err(Error::corrupt("data type"));
                };
                if let Some(record) = records.get(self.record) {
                    self.record += 1;
                    if self.start.as_ref().is_some_and(|s| record.key < *s) {
                        continue;
                    }
                    if self.end.as_ref().is_some_and(|e| record.key >= *e) {
                        return Ok(None);
                    }
                    return Ok(Some(record.clone()));
                }
                self.data = None;
            }
            if let Some(frame) = self.buffered.pop_front() {
                self.data = Some(frame);
                self.record = 0;
                continue;
            }
            if self.part >= self.table.top.len() {
                return Ok(None);
            }
            if self.partition.is_none() {
                self.partition = Some(self.table.frame_controlled(
                    &self.table.top[self.part],
                    true,
                    self.admit,
                    stats,
                    self.read_budget.as_ref(),
                )?);
                self.block = 0;
            }
            let Contents::Index(blocks) = &self.partition.as_ref().unwrap().contents else {
                return Err(Error::corrupt("index type"));
            };
            if self.block >= blocks.len() {
                self.part += 1;
                self.partition = None;
                continue;
            }
            let meta = &blocks[self.block];
            if self.start.as_ref().is_some_and(|s| meta.last < *s) {
                self.block += 1;
                continue;
            }
            if self.end.as_ref().is_some_and(|e| meta.first >= *e) {
                return Ok(None);
            }
            if self
                .filter_prefix
                .as_ref()
                .is_some_and(|p| !bloom_contains(&meta.bloom, p))
            {
                self.block += 1;
                continue;
            }
            // The first block stays demand-only for short queries. Long range
            // scans amortize queue handoffs in batches of at most four blocks.
            // Prefix-filtered lookups retain single-block reads.
            let count = if self.sequential && self.filter_prefix.is_none() {
                4
            } else {
                1
            };
            let end = blocks[self.block..]
                .iter()
                .take(count)
                .take_while(|meta| self.end.as_ref().is_none_or(|end| meta.first < *end))
                .count()
                + self.block;
            if end == self.block + 1 {
                self.data = Some(self.table.frame_controlled(
                    meta,
                    false,
                    self.admit,
                    stats,
                    self.read_budget.as_ref(),
                )?);
                self.block += 1;
            } else {
                let mut frames = self.table.scan_frames(
                    &blocks[self.block..end],
                    self.admit,
                    stats,
                    self.read_budget.as_ref(),
                )?;
                self.block += frames.len();
                self.data = frames.pop_front();
                self.buffered = frames;
            }
            self.sequential = true;
            self.record = 0;
        }
    }
}

// Inspect lengths before allocating decoded records. Prefix compression can
// expand tiny input into many long keys; payload bytes need only one copy.
fn decoded_frame_memory(raw: &[u8], index: bool) -> Result<usize> {
    let mut reader = Reader::new(raw);
    let count = reader.u32()? as usize;
    if index {
        if count > raw.len() / 24 {
            return Err(Error::corrupt("SST metadata count"));
        }
        return Ok(raw.len() + count * std::mem::size_of::<BlockMeta>() + 256);
    }
    if count > raw.len() / 16 {
        return Err(Error::corrupt("SST record count"));
    }
    let mut size = count * (std::mem::size_of::<Record>() + 80) + 256;
    let mut previous_len = 0;
    let mut max_internal = 0;
    for _ in 0..count {
        let shared = reader.u32()? as usize;
        let internal_len = shared + reader.bytes()?.len();
        if shared > previous_len || internal_len > codec::MAX_KEY * 2 + 16 {
            return Err(Error::corrupt("SST prefix length or internal key"));
        }
        // Include spare Vec capacity for unescaped keys and both the current
        // and previous reconstructed internal key used by the decoder.
        size += internal_len * 2 + 8;
        max_internal = max_internal.max(internal_len);
        previous_len = internal_len;
        reader.u32()?;
        let len = reader.u32()?;
        if len != u32::MAX {
            size += reader.take(len as usize)?.len();
        }
    }
    if !reader.remaining().is_empty() {
        return Err(Error::corrupt("SST block trailing bytes"));
    }
    Ok(size + max_internal * 4 + 16)
}

fn read_frame(file: &File, offset: u64, len: u32) -> Result<Vec<u8>> {
    if !(12..=(MAX_FRAME as u32 + 65536)).contains(&len) {
        return Err(Error::corrupt("SST frame size"));
    }
    let mut header = [0; 12];
    file.read_exact_at(&mut header, offset)?;
    let compressed = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
    let raw_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    if compressed + 12 != len as usize || raw_len > MAX_FRAME {
        return Err(Error::corrupt("SST frame lengths"));
    }
    let mut data = vec![0; compressed];
    file.read_exact_at(&mut data, offset + 12)?;
    let mut hash = crc32fast::Hasher::new();
    hash.update(&header[..8]);
    hash.update(&data);
    if hash.finalize() != u32::from_le_bytes(header[8..12].try_into().unwrap()) {
        return Err(Error::corrupt("SST checksum"));
    }
    lz4_flex::block::decompress(&data, raw_len)
        .map_err(|e| Error::corrupt(format!("SST compression: {e}")))
}
fn validate_extent(offset: u64, len: u32, size: u64) -> Result<()> {
    if offset < HEADER as u64
        || offset
            .checked_add(len as u64)
            .is_none_or(|end| end > size - FOOTER as u64)
    {
        return Err(Error::corrupt("SST extent"));
    }
    Ok(())
}
fn encode_meta(blocks: &[BlockMeta]) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, blocks.len() as u32);
    for b in blocks {
        put_bytes(&mut out, &b.first);
        put_bytes(&mut out, &b.last);
        put_u64(&mut out, b.offset);
        put_u32(&mut out, b.len);
        put_bytes(&mut out, &b.bloom);
    }
    out
}
fn decode_meta(raw: &[u8], size: u64) -> Result<Vec<BlockMeta>> {
    let mut r = Reader::new(raw);
    let n = r.u32()? as usize;
    if n > raw.len() / 24 {
        return Err(Error::corrupt("SST metadata count"));
    }
    let mut out: Vec<BlockMeta> = Vec::with_capacity(n);
    for _ in 0..n {
        let b = BlockMeta {
            first: r.bytes()?.to_vec(),
            last: r.bytes()?.to_vec(),
            offset: r.u64()?,
            len: r.u32()?,
            bloom: r.bytes()?.to_vec(),
        };
        validate_extent(b.offset, b.len, size)?;
        if b.first > b.last || out.last().is_some_and(|old| old.last > b.first) {
            return Err(Error::corrupt("SST metadata ordering"));
        }
        out.push(b);
    }
    if !r.remaining().is_empty() {
        return Err(Error::corrupt("SST metadata trailing bytes"));
    }
    Ok(out)
}
fn encode_records(records: &[Record]) -> Vec<u8> {
    let mut raw = Vec::new();
    put_u32(&mut raw, records.len() as u32);
    let mut previous = Vec::new();
    for record in records {
        let key = record.internal_key();
        let shared = key
            .iter()
            .zip(&previous)
            .take_while(|(a, b)| a == b)
            .count();
        put_u32(&mut raw, shared as u32);
        put_bytes(&mut raw, &key[shared..]);
        put_u32(&mut raw, record.prefix_len as u32);
        if let Some(value) = &record.value {
            put_bytes(&mut raw, value);
        } else {
            put_u32(&mut raw, u32::MAX);
        }
        previous = key;
    }
    raw
}
fn decode_records(raw: &[u8]) -> Result<Vec<Record>> {
    let mut r = Reader::new(raw);
    let n = r.u32()? as usize;
    if n > raw.len() / 16 {
        return Err(Error::corrupt("SST record count"));
    }
    let mut out = Vec::with_capacity(n);
    let mut previous = Vec::new();
    for _ in 0..n {
        let shared = r.u32()? as usize;
        if shared > previous.len() {
            return Err(Error::corrupt("SST prefix length"));
        }
        let mut internal = previous[..shared].to_vec();
        internal.extend(r.bytes()?);
        if internal.len() > codec::MAX_KEY * 2 + 16
            || (!previous.is_empty() && internal <= previous)
        {
            return Err(Error::corrupt("SST internal key"));
        }
        let (key, consumed) = codec::unescape(&internal)?;
        if consumed + 8 != internal.len() {
            return Err(Error::corrupt("SST sequence suffix"));
        }
        let seq = !u64::from_be_bytes(internal[consumed..].try_into().unwrap());
        let prefix_len = r.u32()? as usize;
        if prefix_len > key.len() {
            return Err(Error::corrupt("SST filter prefix"));
        }
        let len = r.u32()?;
        let value = if len == u32::MAX {
            None
        } else {
            Some(Arc::new(Bytes::new(r.take(len as usize)?.to_vec())))
        };
        out.push(Record {
            key,
            seq,
            value,
            prefix_len,
        });
        previous = internal;
    }
    if !r.remaining().is_empty() {
        return Err(Error::corrupt("SST block trailing bytes"));
    }
    Ok(out)
}
fn hash(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for &b in bytes {
        h = (h ^ b as u64).wrapping_mul(0x100000001b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^ (h >> 33)
}
fn bloom_make(keys: &[&[u8]]) -> Vec<u8> {
    let mut bits = vec![0u8; (keys.len() * 10).max(64).div_ceil(8)];
    let n = bits.len() * 8;
    for key in keys {
        let h = hash(key);
        let delta = h.rotate_left(31) | 1;
        for i in 0..7 {
            let bit = h.wrapping_add((i as u64).wrapping_mul(delta)) as usize % n;
            bits[bit / 8] |= 1 << (bit % 8);
        }
    }
    bits
}
fn bloom_contains(bits: &[u8], key: &[u8]) -> bool {
    if bits.is_empty() {
        return true;
    }
    let h = hash(key);
    let delta = h.rotate_left(31) | 1;
    let n = bits.len() * 8;
    (0..7).all(|i| {
        let bit = h.wrapping_add((i as u64).wrapping_mul(delta)) as usize % n;
        bits[bit / 8] & (1 << (bit % 8)) != 0
    })
}
