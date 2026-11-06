use super::{
    TenantHandle,
    datasets::{PublishedLookup, published},
};
use crate::{Error, Result, model::*};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishedReadBatch {
    pub version: Uuid,
    pub result: ReadBatchResult,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishedQuery {
    pub version: Uuid,
    pub history: String,
    pub result: QueryResult,
}

impl TenantHandle {
    pub fn published_dataset(
        &self,
        resource: &str,
        version: Option<Uuid>,
    ) -> Result<PublishedLookup> {
        self.published_lookup(resource, version)
    }

    /// Pin the resource version and all reads in one snapshot. Query cursors
    /// returned here must be resumed with `query_published` on this resource.
    pub fn read_published(
        &self,
        resource: &str,
        version: Option<Uuid>,
        requests: &[ReadRequest],
    ) -> Result<PublishedReadBatch> {
        self.read_with(&crate::ReadOptions::default())?
            .read_published(resource, version, requests)
    }

    pub(crate) fn read_published_controlled(
        &self,
        resource: &str,
        version: Option<Uuid>,
        requests: &[ReadRequest],
        budget: &crate::read::ReadBudget,
    ) -> Result<crate::PublishedReadBatch> {
        budget.check()?;
        super::catalog::name(resource)?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        let history = self.history();
        let snapshot = self.engine().snapshot()?;
        let pinned = published(&snapshot, resource, version, true)?;
        let input_bytes = crate::json::encoded_len(requests)?;
        if requests.len() > self.engine().options.max_batch_operations
            || input_bytes > self.engine().options.max_batch_bytes
        {
            return Err(Error::limit("published read input limit"));
        }
        let _memory = self
            .engine()
            .budget
            .reserve(input_bytes * 4 + requests.len() * 256)?;
        let operations: Vec<_> = requests
            .iter()
            .map(|request| ReadOperation {
                collection: pinned.publication.collection.clone(),
                request: request.clone(),
            })
            .collect();
        let result = self.read_batch_snapshot(
            &operations,
            snapshot,
            history,
            Some((resource.into(), pinned.version)),
            budget,
        )?;
        let out = PublishedReadBatch {
            version: pinned.version,
            result,
        };
        if crate::json::encoded_len(&out)? > self.engine().options.max_response_bytes {
            for result in &out.result.results {
                if let ReadResult::Query { result } = result
                    && let Some(cursor) = &result.cursor
                {
                    let _ = self.release_cursor(cursor);
                }
            }
            return Err(Error::limit("published read response bytes"));
        }
        Ok(out)
    }

    /// Pagination stays on the original dataset even if latest is republished.
    /// An admitted cursor can finish within its snapshot lease after version
    /// expiry; a fresh query for an expired version is rejected.
    pub fn query_published(
        &self,
        resource: &str,
        version: Option<Uuid>,
        query: Query,
    ) -> Result<PublishedQuery> {
        self.read_with(&crate::ReadOptions::default())?
            .query_published(resource, version, query)
    }

    pub(crate) fn query_published_controlled(
        &self,
        resource: &str,
        version: Option<Uuid>,
        query: Query,
        budget: &crate::read::ReadBudget,
    ) -> Result<crate::PublishedQuery> {
        budget.check()?;
        super::catalog::name(resource)?;
        let _permit = self.core.gate.enter()?;
        self.touch();
        let history = self.history();
        let (snapshot, version) = if let Some(token) = &query.cursor {
            let id = Uuid::parse_str(token)
                .map_err(|_| Error::new("CURSOR_EXPIRED", "invalid cursor"))?;
            let cursors = self.core.cursors.lock();
            let cursor = cursors
                .get(&id)
                .filter(|c| c.valid())
                .ok_or_else(|| Error::new("CURSOR_EXPIRED", "cursor absent or expired"))?;
            let (owner, pinned) = cursor
                .publication
                .as_ref()
                .ok_or_else(|| Error::invalid("cursor does not belong to a published dataset"))?;
            if owner != resource || version.is_some_and(|v| v != *pinned) {
                return Err(Error::invalid("cursor resource or version mismatch"));
            }
            (cursor.snapshot.clone(), Some(*pinned))
        } else {
            (self.engine().snapshot()?, version)
        };
        let pinned = published(&snapshot, resource, version, query.cursor.is_none())?;
        let result = self.query_view(
            &pinned.publication.collection,
            query,
            Some(snapshot),
            Some((resource.into(), pinned.version)),
            budget,
            0,
        )?;
        if history != self.history() {
            if let Some(cursor) = &result.cursor {
                let _ = self.release_cursor(cursor);
            }
            return Err(Error::new(
                "HISTORY_CONFLICT",
                "published query crossed a history change",
            ));
        }
        let out = PublishedQuery {
            version: pinned.version,
            history,
            result,
        };
        if crate::json::encoded_len(&out)? > self.engine().options.max_response_bytes {
            if let Some(cursor) = &out.result.cursor {
                let _ = self.release_cursor(cursor);
            }
            return Err(Error::limit("published query response bytes"));
        }
        Ok(out)
    }
}
