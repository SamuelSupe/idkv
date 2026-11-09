use crate::{Error, Result, TenantInfo};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::Error as _,
    ser::{SerializeMap, SerializeSeq, SerializeStruct},
};
use std::collections::BTreeMap;
use uuid::Uuid;

pub(super) use crate::tenant::STORAGE_REGISTRY_VERSION as VERSION;
pub(super) const MAX_BYTES: usize = 16 << 20;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(super) enum Lifecycle {
    Active,
    Deleting,
    Deleted,
    Restoring { stage: Uuid, old: Uuid },
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Registration {
    pub(super) info: TenantInfo,
    pub(super) lifecycle: Lifecycle,
}
#[derive(Clone)]
pub(super) struct Registry {
    pub(super) version: u32,
    pub(super) replication_enabled: bool,
    pub(super) tenants: BTreeMap<Uuid, Registration>,
}
impl Registry {
    pub(super) fn prepare_publish(&mut self) -> Result<()> {
        self.version = VERSION;
        if crate::json::encoded_len(self)? > MAX_BYTES - 8 {
            return Err(Error::limit("tenant registry metadata capacity exhausted"));
        }
        Ok(())
    }
}

// Keep all historical status information and UUID reuse protection. Only the
// on-disk representation of terminal entries changes; live/journal rows retain
// their old shape. Version 5 makes older binaries reject the compact registry.
type DeletedRow = (Uuid, String, Uuid, Uuid, usize, u64);
#[derive(Deserialize)]
struct DiskRegistry {
    version: u32,
    #[serde(default)]
    replication_enabled: bool,
    tenants: BTreeMap<Uuid, Registration>,
    #[serde(default)]
    deleted: Vec<DeletedRow>,
}
impl<'de> Deserialize<'de> for Registry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let mut disk = DiskRegistry::deserialize(deserializer)?;
        if disk.version < 5 && !disk.deleted.is_empty() {
            return Err(D::Error::custom(
                "compact deleted tenants require registry v5",
            ));
        }
        for (id, name, generation, history, memory_bytes, disk_bytes) in disk.deleted {
            let registration = Registration {
                info: TenantInfo {
                    id,
                    name,
                    generation,
                    history,
                    memory_bytes,
                    disk_bytes,
                },
                lifecycle: Lifecycle::Deleted,
            };
            if disk.tenants.insert(id, registration).is_some() {
                return Err(D::Error::custom("duplicate tenant UUID in registry"));
            }
        }
        if disk.tenants.iter().any(|(id, row)| *id != row.info.id) {
            return Err(D::Error::custom("registry tenant UUID mismatch"));
        }
        Ok(Self {
            version: disk.version,
            replication_enabled: disk.replication_enabled,
            tenants: disk.tenants,
        })
    }
}
impl Serialize for Registry {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let compact = self.version >= 5;
        let mut state = serializer.serialize_struct("Registry", if compact { 4 } else { 3 })?;
        state.serialize_field("version", &self.version)?;
        state.serialize_field("replication_enabled", &self.replication_enabled)?;
        if compact {
            state.serialize_field("tenants", &LiveRows(&self.tenants))?;
            state.serialize_field("deleted", &DeletedRows(&self.tenants))?;
        } else {
            state.serialize_field("tenants", &self.tenants)?;
        }
        state.end()
    }
}
struct LiveRows<'a>(&'a BTreeMap<Uuid, Registration>);
impl Serialize for LiveRows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut rows = serializer.serialize_map(None)?;
        for (id, row) in self.0 {
            if !matches!(row.lifecycle, Lifecycle::Deleted) {
                rows.serialize_entry(id, row)?;
            }
        }
        rows.end()
    }
}
struct DeletedRows<'a>(&'a BTreeMap<Uuid, Registration>);
impl Serialize for DeletedRows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut rows = serializer.serialize_seq(None)?;
        for (id, row) in self.0 {
            if matches!(row.lifecycle, Lifecycle::Deleted) {
                let info = &row.info;
                rows.serialize_element(&(
                    id,
                    &info.name,
                    info.generation,
                    info.history,
                    info.memory_bytes,
                    info.disk_bytes,
                ))?;
            }
        }
        rows.end()
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Store, atomic_json, read_json};
    use super::*;
    use crate::Options;
    use serde_json::json;
    use std::fs;

    fn fill_history(registry: &mut Registry, bytes: usize) -> TenantInfo {
        let mut add = || {
            let info = TenantInfo {
                id: Uuid::new_v4(),
                name: "deleted".into(),
                generation: Uuid::new_v4(),
                history: Uuid::new_v4(),
                memory_bytes: Options::default().tenant_memory_bytes,
                disk_bytes: Options::default().tenant_disk_bytes,
            };
            registry.tenants.insert(
                info.id,
                Registration {
                    info: info.clone(),
                    lifecycle: Lifecycle::Deleted,
                },
            );
            info
        };
        let first = add();
        let previous = crate::json::encoded_len(registry).unwrap();
        let second = TenantInfo {
            id: Uuid::new_v4(),
            ..first.clone()
        };
        registry.tenants.insert(
            second.id,
            Registration {
                info: second,
                lifecycle: Lifecycle::Deleted,
            },
        );
        let current = crate::json::encoded_len(registry).unwrap();
        let row_bytes = current - previous;
        let count = (bytes - current) / row_bytes;
        let mut tail = Vec::new();
        for _ in 0..count {
            let info = TenantInfo {
                id: Uuid::new_v4(),
                generation: Uuid::new_v4(),
                history: Uuid::new_v4(),
                ..first.clone()
            };
            tail.push(info.id);
            registry.tenants.insert(
                info.id,
                Registration {
                    info,
                    lifecycle: Lifecycle::Deleted,
                },
            );
        }
        let mut padding = bytes - crate::json::encoded_len(registry).unwrap();
        for id in tail.iter().rev().take(10) {
            let name = &mut registry.tenants.get_mut(id).unwrap().info.name;
            let extra = padding.min(128 - name.len());
            name.push_str(&"x".repeat(extra));
            padding -= extra;
        }
        assert_eq!(padding, 0);
        first
    }

    #[test]
    fn legacy_startup_fences_older_identity_writers_before_serving() {
        let dir = tempfile::tempdir().unwrap();
        let (id, ack) = {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            let id = store.create_tenant("legacy-reader").unwrap().id;
            let tenant = store.tenant(id).unwrap();
            tenant.create_collection("docs").unwrap();
            let ack = tenant
                .put("docs", json!({"_key":"ack","value":7}), None)
                .unwrap();
            (id, ack)
        };
        let path = dir.path().join("control/tenants.json");
        let mut registry: Registry = read_json(&path).unwrap();
        registry.version = 4;
        atomic_json(&path, &registry).unwrap();
        {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            assert_eq!(
                store
                    .tenant(id)
                    .unwrap()
                    .get("docs", "ack")
                    .unwrap()
                    .unwrap()["_rev"],
                ack.rev
            );
            let upgraded: serde_json::Value = read_json(&path).unwrap();
            assert_eq!(upgraded["version"], VERSION);
            store.create_tenant("upgrades-on-publication").unwrap();
            let disk: serde_json::Value = read_json(&path).unwrap();
            assert_eq!(disk["version"], VERSION);
        }
        let store = Store::open(dir.path(), Options::default()).unwrap();
        assert_eq!(store.tenants().len(), 2);
        assert_eq!(
            store
                .tenant(id)
                .unwrap()
                .get("docs", "ack")
                .unwrap()
                .unwrap()["_rev"],
            ack.rev
        );
    }

    #[test]
    fn legacy_full_history_migrates_without_losing_status_or_uuid_protection() {
        let dir = tempfile::tempdir().unwrap();
        let (id, ack) = {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            let id = store.create_tenant("sentinel").unwrap().id;
            let tenant = store.tenant(id).unwrap();
            tenant.create_collection("docs").unwrap();
            let ack = tenant
                .put("docs", json!({"_key":"ack","value":7}), None)
                .unwrap();
            (id, ack)
        };
        let path = dir.path().join("control/tenants.json");
        let mut registry: Registry = read_json(&path).unwrap();
        registry.version = 4;
        let deleted = fill_history(&mut registry, MAX_BYTES - 8 - 64);
        let deleted_count = registry.tenants.len() - 1;
        atomic_json(&path, &registry).unwrap();
        let old_bytes = fs::metadata(&path).unwrap().len();
        drop(registry);
        for round in 0..3 {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            store.create_tenant(&format!("fresh-{round}")).unwrap();
            assert_eq!(
                store
                    .tenant(id)
                    .unwrap()
                    .get("docs", "ack")
                    .unwrap()
                    .unwrap()["_rev"],
                ack.rev
            );
            assert_eq!(
                store.status(deleted.id).unwrap()["tenant"],
                serde_json::to_value(&deleted).unwrap()
            );
            assert_eq!(
                store
                    .import_checkpoint_inner(&dir.path().join("absent"), deleted.clone())
                    .unwrap_err()
                    .code,
                "ALREADY_EXISTS"
            );
            assert_eq!(
                store.open_engine_metrics()["registry_deleted_tenants"],
                deleted_count as u64
            );
        }
        let store = Store::open(dir.path(), Options::default()).unwrap();
        for tenant in store.tenants() {
            store.delete_tenant(tenant.id).unwrap();
        }
        assert!(store.tenants().is_empty());
        drop(store);
        let store = Store::open(dir.path(), Options::default()).unwrap();
        store.create_tenant("after-all-deleted").unwrap();
        let new_bytes = fs::metadata(&path).unwrap().len();
        assert!(new_bytes < old_bytes * 3 / 5);
        let disk: serde_json::Value = read_json(&path).unwrap();
        assert_eq!(disk["version"], VERSION);
        println!(
            "REGISTRY_JSON {}",
            json!({"legacy_bytes":old_bytes,"compact_bytes":new_bytes,"legacy_deleted_tenants":deleted_count,"restarts_with_successful_create":3,"confirmed_checks":3,"uuid_reuse_rejected":true})
        );
    }

    #[test]
    fn capacity_rejection_keeps_registry_and_staging_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let (id, ack) = {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            let id = store.create_tenant("sentinel").unwrap().id;
            let tenant = store.tenant(id).unwrap();
            tenant.create_collection("docs").unwrap();
            let ack = tenant
                .put("docs", json!({"_key":"ack","value":7}), None)
                .unwrap();
            (id, ack)
        };
        let path = dir.path().join("control/tenants.json");
        let mut registry: Registry = read_json(&path).unwrap();
        fill_history(&mut registry, MAX_BYTES - 8 - 32);
        atomic_json(&path, &registry).unwrap();
        drop(registry);
        let before = fs::read(&path).unwrap();
        {
            let store = Store::open(dir.path(), Options::default()).unwrap();
            assert_eq!(
                store.create_tenant("cannot-fit").unwrap_err().code,
                "RESOURCE_EXHAUSTED"
            );
            assert_eq!(store.tenants().len(), 1);
            assert_eq!(fs::read(&path).unwrap(), before);
            assert_eq!(fs::read_dir(dir.path().join("staging")).unwrap().count(), 0);
            assert_eq!(
                store
                    .tenant(id)
                    .unwrap()
                    .get("docs", "ack")
                    .unwrap()
                    .unwrap()["_rev"],
                ack.rev
            );
        }
        let store = Store::open(dir.path(), Options::default()).unwrap();
        assert_eq!(store.tenants().len(), 1);
        assert_eq!(
            store
                .tenant(id)
                .unwrap()
                .get("docs", "ack")
                .unwrap()
                .unwrap()["_rev"],
            ack.rev
        );
    }
}
