use super::*;
pub struct TableWriter {
    file: Arc<File>,
    io: Arc<IoScheduler>,
    tenant: Uuid,
    temp: PathBuf,
    final_path: PathBuf,
    id: Uuid,
    level: u32,
    offset: u64,
    block_target: usize,
    pending: Vec<Record>,
    pending_bytes: usize,
    partition: Vec<BlockMeta>,
    top: Vec<BlockMeta>,
    first: Vec<u8>,
    last: Vec<u8>,
    previous: Option<(Vec<u8>, u64)>,
    output: Vec<u8>,
    budget: Budget,
    _output_memory: Reservation,
    max_seq: u64,
    count: u64,
}
impl TableWriter {
    pub fn new(
        dir: &Path,
        tenant: Uuid,
        generation: Uuid,
        level: u32,
        block_target: usize,
        io: Arc<IoScheduler>,
        budget: Budget,
    ) -> Result<Self> {
        let output_memory = budget.reserve(1 << 20)?;
        let id = Uuid::new_v4();
        let temp = dir.join(format!("{id}.building"));
        let final_path = dir.join(format!("{id}.sst"));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        file.write_all(b"TKSST001")?;
        file.write_all(tenant.as_bytes())?;
        file.write_all(generation.as_bytes())?;
        file.write_all(id.as_bytes())?;
        Ok(Self {
            file: Arc::new(file),
            io,
            tenant,
            temp,
            final_path,
            id,
            level,
            offset: HEADER as u64,
            block_target,
            pending: vec![],
            pending_bytes: 0,
            partition: vec![],
            top: vec![],
            first: vec![],
            last: vec![],
            previous: None,
            output: Vec::with_capacity(256 << 10),
            budget,
            _output_memory: output_memory,
            max_seq: 0,
            count: 0,
        })
    }
    pub fn add(&mut self, record: Record) -> Result<()> {
        if self.previous.as_ref().is_some_and(|(key, seq)| {
            key > &record.key || (key == &record.key && *seq <= record.seq)
        }) {
            return Err(Error::corrupt("unsorted SST builder input"));
        }
        if self.count == 0 {
            self.first = record.key.clone();
        }
        self.last = record.key.clone();
        self.max_seq = self.max_seq.max(record.seq);
        self.count += 1;
        self.previous = Some((record.key.clone(), record.seq));
        self.pending_bytes += record.size();
        self.pending.push(record);
        if self.pending_bytes >= self.block_target {
            self.flush_block()?;
        }
        Ok(())
    }
    pub fn size(&self) -> u64 {
        self.offset + self.pending_bytes as u64
    }
    fn write_frame(&mut self, raw: &[u8]) -> Result<(u64, u32)> {
        if raw.len() > MAX_FRAME {
            return Err(Error::limit("SST frame exceeds bound"));
        }
        let _temporary = self.budget.reserve(raw.len() * 3 + 4096)?;
        let payload = lz4_flex::block::compress(raw);
        let mut header = Vec::new();
        put_u32(&mut header, payload.len() as u32);
        put_u32(&mut header, raw.len() as u32);
        let mut checksum = crc32fast::Hasher::new();
        checksum.update(&header);
        checksum.update(&payload);
        put_u32(&mut header, checksum.finalize());
        let offset = self.offset;
        let len = (header.len() + payload.len()) as u32;
        header.extend(payload);
        self.output.extend(header);
        self.offset += len as u64;
        if self.output.len() >= 256 << 10 {
            self.flush_output()?;
        }
        Ok((offset, len))
    }
    fn flush_output(&mut self) -> Result<()> {
        if self.output.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.output);
        let position = self.offset - bytes.len() as u64;
        let file = self.file.clone();
        self.io.run(self.tenant, move || {
            file.write_all_at(&bytes, position)?;
            Ok(())
        })?;
        self.output = Vec::with_capacity(256 << 10);
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let raw = encode_records(&self.pending);
        let mut bloom_keys: Vec<&[u8]> = self.pending.iter().map(|r| r.key.as_slice()).collect();
        for r in &self.pending {
            if r.prefix_len > 0 {
                bloom_keys.push(&r.key[..r.prefix_len]);
            }
        }
        let bloom = bloom_make(&bloom_keys);
        let first = self.pending.first().unwrap().key.clone();
        let last = self.pending.last().unwrap().key.clone();
        let (offset, len) = self.write_frame(&raw)?;
        self.partition.push(BlockMeta {
            first,
            last,
            offset,
            len,
            bloom,
        });
        self.pending.clear();
        self.pending_bytes = 0;
        if self.partition.len() >= 64 {
            self.flush_partition()?;
        }
        Ok(())
    }
    fn flush_partition(&mut self) -> Result<()> {
        if self.partition.is_empty() {
            return Ok(());
        }
        let raw = encode_meta(&self.partition);
        let first = self.partition.first().unwrap().first.clone();
        let last = self.partition.last().unwrap().last.clone();
        let (offset, len) = self.write_frame(&raw)?;
        self.top.push(BlockMeta {
            first,
            last,
            offset,
            len,
            bloom: vec![],
        });
        self.partition.clear();
        Ok(())
    }
    pub fn finish(mut self) -> Result<TableInfo> {
        if self.count == 0 {
            return Err(Error::invalid("empty SST"));
        }
        self.flush_block()?;
        self.flush_partition()?;
        let raw = encode_meta(&self.top);
        let (offset, len) = self.write_frame(&raw)?;
        self.flush_output()?;
        let mut footer = Vec::new();
        footer.extend(offset.to_le_bytes());
        footer.extend(len.to_le_bytes());
        footer.extend(b"TKEND001");
        let file = self.file.clone();
        let position = self.offset;
        self.io.run(self.tenant, move || {
            file.write_all_at(&footer, position)?;
            file.sync_all()?;
            Ok(())
        })?;
        fs::rename(&self.temp, &self.final_path)?;
        sync_dir(self.final_path.parent().unwrap())?;
        Ok(TableInfo {
            id: self.id,
            level: self.level,
            size: self.offset + FOOTER as u64,
            first: self.first.clone(),
            last: self.last.clone(),
            max_seq: self.max_seq,
            entries: self.count,
        })
    }
}
impl Drop for TableWriter {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.temp);
    }
}
