use super::*;
use crate::replication::{LogCursor, LogPage, LogRequest, PAGE_BYTES, Source};
use base64::{Engine as _, engine::general_purpose::STANDARD};

impl Engine {
    fn reserve_replica_decode(
        &self,
        bytes: usize,
        held: usize,
        wire_memory: usize,
    ) -> Result<Reservation> {
        if held.saturating_add(bytes) > self.budget.tenant.limit()
            || held.saturating_add(bytes).saturating_add(wire_memory)
                > self.budget.global.foreground_limit()
        {
            return Err(Error::new(
                "REPLICA_SNAPSHOT_REQUIRED",
                "log page decode exceeds memory quota; install a checkpoint",
            ));
        }
        self.cache.reserve(&self.budget, bytes).map_err(|error| {
            if self.budget.tenant.used().saturating_add(bytes) > self.budget.tenant.limit() {
                Error::new("REPLICA_MEMORY_PRESSURE", error.to_string())
            } else {
                error
            }
        })
    }
    pub(crate) fn log_page(&self, request: &LogRequest) -> Result<LogPage> {
        self.ensure_durable()?;
        if request.tenant != self.info.id || request.generation != self.info.generation {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "tenant identity mismatch",
            ));
        }
        let writer = self.writer.lock();
        if let Some(error) = &writer.poison {
            return Err(error.clone());
        }
        let through = self.seq();
        if request.after > through {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "replica is ahead of source",
            ));
        }
        let mut page = LogPage {
            epoch: request.epoch,
            through: request.after,
            source_position: through,
            frames: vec![],
            cursor: request.cursor.clone(),
        };
        if request.after == through {
            return Ok(page);
        }
        let files: Vec<_> = writer
            .manifest
            .retained_wals
            .iter()
            .chain(&writer.manifest.wals)
            .collect();
        let start = files
            .partition_point(|w| w.start_seq <= request.after + 1)
            .saturating_sub(1);
        if files[start].start_seq > request.after + 1 {
            return Err(Error::new(
                "LOG_GONE",
                "replication history expired; install a checkpoint",
            ));
        }
        // Open under the writer lock: retention may unlink names after this point,
        // but these descriptors still refer to the same immutable byte prefixes.
        let mut opened = Vec::new();
        for info in files[start..].iter().take(128) {
            if info.start_seq > through || info.start_seq > request.after.saturating_add(256) {
                break;
            }
            let file = File::open(self.path.join("wal").join(format!("{}.wal", info.id)))?;
            let len = if info.id == writer.wal.info.id {
                Some(writer.wal.len)
            } else {
                None
            };
            opened.push(((*info).clone(), file, len));
        }
        drop(writer);
        let mut page_memory = Vec::new();
        let mut size = 0;
        for (info, file, len) in opened {
            let mut header = [0; 64];
            std::os::unix::fs::FileExt::read_exact_at(&file, &mut header, 0)?;
            if &header[..8] != b"TKWAL001"
                || &header[8..24] != self.info.id.as_bytes()
                || &header[24..40] != self.info.generation.as_bytes()
                || &header[40..56] != info.id.as_bytes()
                || u64::from_le_bytes(header[56..].try_into().unwrap()) != info.start_seq
            {
                return Err(Error::corrupt("replication WAL identity"));
            }
            let len = match len {
                Some(len) => len,
                None => file.metadata()?.len(),
            };
            let id = info.id;
            let mut offset = request
                .cursor
                .as_ref()
                .filter(|c| c.file == id)
                .map_or(64, |c| c.offset);
            if offset < 64 {
                return Err(Error::invalid("replication cursor offset"));
            }
            let mut frames = wal::FrameReader::new(file, offset, len, &self.budget)?;
            while let Some((seq, bytes, _memory)) = frames.next()? {
                if seq > through {
                    return Ok(page);
                }
                offset += bytes.len() as u64;
                if seq <= request.after {
                    continue;
                }
                if seq != page.through + 1 {
                    return Err(Error::new("LOG_GONE", "replication sequence gap"));
                }
                size += bytes.len();
                let wire_bytes = bytes.len().div_ceil(3) * 4 + 64;
                page_memory.push(self.budget.reserve(wire_bytes).or_else(|_| {
                    frames.unbuffer()?;
                    self.budget.reserve(wire_bytes)
                })?);
                page.frames.push(STANDARD.encode(&bytes));
                page.through = seq;
                page.cursor = Some(LogCursor { file: id, offset });
                if size >= PAGE_BYTES || page.frames.len() >= 256 || seq == through {
                    return Ok(page);
                }
            }
        }
        if page.through > request.after {
            Ok(page)
        } else {
            Err(Error::corrupt("replication source history is incomplete"))
        }
    }

    pub(crate) fn apply_replica_page(
        self: &Arc<Self>,
        source: &Source,
        after: u64,
        page: &LogPage,
        wire_memory: usize,
    ) -> Result<()> {
        let authority = self
            .authority
            .read()
            .clone()
            .ok_or_else(|| Error::invalid("managed replica required"))?;
        let _permission = authority.replica_write(source)?;
        if page.epoch != source.epoch
            || page.frames.len() > 256
            || page.through > page.source_position
        {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "replication page identity or bounds",
            ));
        }
        let encoded_bytes = page.frames.iter().try_fold(0usize, |n, s| {
            n.checked_add(s.len())
                .ok_or_else(|| Error::limit("replication page size"))
        })?;
        if encoded_bytes > (wal::MAX_BATCH + PAGE_BYTES + 24).div_ceil(3) * 4 {
            return Err(Error::limit("replication page size"));
        }
        // The wire response already has a global reservation. Charge this
        // tenant for its raw decode buffer, not two more copies of base64.
        let decode_memory = self.reserve_replica_decode(
            encoded_bytes / 4 * 3
                + 4096
                + page.frames.len() * std::mem::size_of::<(u64, usize, Vec<Record>, Reservation)>(),
            0,
            wire_memory,
        )?;
        let mut held = decode_memory.size();
        self.ensure_durable()?;
        let _transaction = self.transaction.lock();
        let mut writer = self.writer.lock();
        if let Some(error) = &writer.poison {
            return Err(error.clone());
        }
        if self.view.read().tables.l0_pressure >= self.options.l0_stop {
            return Err(Error::limit("replica L0 backlog; retry after compaction"));
        }
        if writer.staged != after {
            return Err(Error::new(
                "REPLICATION_CONFLICT",
                "replication position changed; retry from durable position",
            ));
        }
        let mut prepared = Vec::with_capacity(page.frames.len());
        let mut bytes = Vec::with_capacity(encoded_bytes / 4 * 3);
        let mut expected = after;
        let mut disk_bytes = 0u64;
        for encoded in &page.frames {
            if encoded.len() > (wal::MAX_BATCH + 24).div_ceil(3) * 4 {
                return Err(Error::limit("replication frame size"));
            }
            let start = bytes.len();
            STANDARD
                .decode_vec(encoded, &mut bytes)
                .map_err(|_| Error::corrupt("replication frame encoding"))?;
            let frame = &bytes[start..];
            let seq = wal::frame(frame)?;
            if Some(seq) != expected.checked_add(1) || seq > page.through {
                return Err(Error::corrupt("replication sequence gap"));
            }
            let mut memory =
                self.reserve_replica_decode(wal::decoded_memory(&frame[24..])?, held, wire_memory)?;
            let records = wal::decode(&frame[24..], seq)?;
            memory.shrink(records.iter().map(Record::size).sum::<usize>() * 2);
            held += memory.size();
            expected = seq;
            disk_bytes += frame.len() as u64 + 4096;
            prepared.push((seq, frame.len(), records, memory));
        }
        if expected != page.through {
            return Err(Error::corrupt("replication page tail mismatch"));
        }
        if prepared.is_empty() {
            return Ok(());
        }
        let _insert_memory = self.reserve_replica_decode(
            REPLICA_INSERT_CHUNK * std::mem::size_of::<Record>(),
            held,
            wire_memory,
        )?;
        // These writes are already committed on the source. Replay must share
        // maintenance headroom so the foreground threshold cannot block the
        // very cleanup records needed to reclaim space. The total quota remains.
        self.reserve_disk(disk_bytes, true)?;
        // Keep each original TXN1 frame intact but submit one contiguous write to
        // the fair I/O queue. A torn page still recovers complete atomic batches.
        if let Err(error) = writer.wal.append(bytes, &self.io, self.info.id) {
            if error.code == "COMMIT_UNKNOWN" {
                self.poison_writer(&mut writer, error.clone());
                self.notify_durable();
            } else {
                self.disk.fetch_sub(disk_bytes, Ordering::AcqRel);
            }
            return Err(error);
        }
        // The writer guard prevents memtable rotation for the entire page.
        // Snapshot readers need not contend on the view lock during insertion.
        let mem = self.view.read().mem.clone();
        // Small source transactions otherwise hand the memtable write lock to
        // readers after every few records. Share bounded 8-record chunks across
        // transactions; sequence visibility still advances only after page fsync.
        mem.apply_page(
            prepared
                .into_iter()
                .map(|(seq, frame_len, records, memory)| {
                    self.record_staged_commit(&mut writer, seq, &records, frame_len as u64);
                    (records, memory)
                }),
        );
        writer.staged = page.through;
        // A crash replays the same source sequence numbers from this WAL. There is
        // no separately advanced receiver cursor that could skip unapplied data.
        self.sync_writer(&mut writer)?;
        if writer.staged != page.through {
            return Err(Error::corrupt("replication page tail mismatch"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Options, Store,
        replication::{Config, Role},
    };

    #[test]
    fn malformed_page_is_rejected_before_mutation_and_valid_replay_survives_restart() {
        let primary_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();
        let primary = Store::open(primary_dir.path(), Options::default()).unwrap();
        let replica = Store::open(replica_dir.path(), Options::default()).unwrap();
        let cluster = Uuid::new_v4();
        let config = |role| Config {
            cluster,
            role,
            peer_url: "http://127.0.0.1:1".into(),
            token_file: "unused".into(),
            poll_ms: 10,
            log_retention_bytes: 1 << 20,
        };
        primary
            .configure_replication(&config(Role::Primary))
            .unwrap();
        replica
            .configure_replication(&config(Role::Replica))
            .unwrap();
        let info = primary.create_tenant("atomic").unwrap();
        let tenant = primary.tenant(info.id).unwrap();
        tenant.create_collection("docs").unwrap();
        let checkpoint = replica_dir.path().join("incoming");
        tenant.engine().checkpoint(&checkpoint).unwrap();
        let source = replica.bind_source(&primary.replication_state()).unwrap();
        replica
            .install_replica(&source, &checkpoint, info.clone())
            .unwrap();
        let after = tenant.engine().seq();
        tenant
            .put("docs", serde_json::json!({"_key":"a"}), None)
            .unwrap();
        tenant
            .put("docs", serde_json::json!({"_key":"b"}), None)
            .unwrap();
        for value in 0..64 {
            tenant
                .put("docs", serde_json::json!({"_key":"a","value":value}), None)
                .unwrap();
        }
        tenant
            .write_batch(
                "docs",
                (0..100)
                    .map(|i| crate::WriteOp::Put {
                        document: serde_json::json!({"_key":format!("bulk-{i:03}"),"value":i}),
                        if_rev: None,
                    })
                    .collect(),
            )
            .unwrap();
        let request = LogRequest {
            protocol: crate::replication::PROTOCOL,
            cluster,
            epoch: source.epoch,
            tenant: info.id,
            generation: info.generation,
            after,
            cursor: None,
        };
        let page = primary.export_logs(&request).unwrap();
        assert_eq!(page.frames.len(), 67);
        let mut bad = page.clone();
        bad.frames[1] = "invalid".into();
        let handle = replica.tenant(info.id).unwrap();
        let catalog_seq = handle.engine().catalog_seq();
        let old_snapshot = handle.engine().snapshot().unwrap();
        let docs = crate::tenant::catalog::collection_snapshot(&old_snapshot, "docs").unwrap();
        assert_eq!(
            handle
                .engine()
                .apply_replica_page(&source, after, &bad, 0)
                .unwrap_err()
                .code,
            "CORRUPTION"
        );
        assert_eq!(handle.engine().seq(), after);
        assert!(handle.get("docs", "a").unwrap().is_none());
        handle
            .engine()
            .apply_replica_page(&source, after, &page, 0)
            .unwrap();
        assert_eq!(handle.engine().catalog_seq(), catalog_seq);
        assert_eq!(handle.engine().metrics()["commits"], 67);
        assert!(
            old_snapshot
                .get(
                    &crate::tenant::catalog::document_key(docs.id, "a"),
                    &mut ReadStats::default()
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(handle.get("docs", "a").unwrap().unwrap()["value"], 63);
        drop(old_snapshot);
        assert!(handle.engine().metrics()["wal_bytes"].as_u64().unwrap() > 0);
        assert_eq!(
            handle
                .engine()
                .apply_replica_page(&source, after, &page, 0)
                .unwrap_err()
                .code,
            "REPLICATION_CONFLICT"
        );
        let catalog_after = tenant.engine().seq();
        tenant.create_collection("other").unwrap();
        let metadata = primary
            .export_logs(&LogRequest {
                protocol: crate::replication::PROTOCOL,
                after: catalog_after,
                cursor: page.cursor.clone(),
                ..request
            })
            .unwrap();
        handle
            .engine()
            .apply_replica_page(&source, catalog_after, &metadata, 0)
            .unwrap();
        assert_eq!(handle.engine().catalog_seq(), metadata.through);
        let before_delete = handle.engine().snapshot().unwrap();
        tenant.delete("docs", "a", None).unwrap();
        let cleanup = primary
            .export_logs(&LogRequest {
                protocol: crate::replication::PROTOCOL,
                cluster,
                epoch: source.epoch,
                tenant: info.id,
                generation: info.generation,
                after: metadata.through,
                cursor: metadata.cursor.clone(),
            })
            .unwrap();
        {
            let engine = handle.engine();
            let _maintenance = engine.maintenance.lock();
            let actual = engine.disk.load(Ordering::Acquire);
            engine.disk.store(engine.info.disk_bytes, Ordering::Release);
            assert_eq!(
                engine
                    .apply_replica_page(&source, metadata.through, &cleanup, 0)
                    .unwrap_err()
                    .code,
                "RESOURCE_EXHAUSTED"
            );
            assert_eq!(engine.seq(), metadata.through);
            engine.disk.store(
                engine.info.disk_bytes - engine.info.disk_bytes / 5,
                Ordering::Release,
            );
            // A replica must be able to replay deletions after crossing the
            // foreground write threshold, without exceeding its total quota.
            engine
                .apply_replica_page(&source, metadata.through, &cleanup, 0)
                .unwrap();
            engine.disk.store(actual, Ordering::Release);
        }
        assert!(handle.get("docs", "a").unwrap().is_none());
        assert!(
            before_delete
                .get(
                    &crate::tenant::catalog::document_key(docs.id, "a"),
                    &mut ReadStats::default()
                )
                .unwrap()
                .is_some()
        );
        drop(before_delete);
        drop(handle);
        drop(replica);
        let replica = Store::open(replica_dir.path(), Options::default()).unwrap();
        let handle = replica.tenant(info.id).unwrap();
        assert_eq!(handle.engine().seq(), cleanup.through);
        assert!(handle.get("docs", "a").unwrap().is_none());
        assert!(handle.get("docs", "b").unwrap().is_some());
        assert_eq!(
            handle.get("docs", "bulk-099").unwrap().unwrap()["value"],
            99
        );
        assert_eq!(
            handle
                .put("docs", serde_json::json!({"_key":"c"}), None)
                .unwrap_err()
                .code,
            "READ_ONLY"
        );
    }
}
