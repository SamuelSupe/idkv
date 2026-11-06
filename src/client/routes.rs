use super::{Client, Request, reads::ReadRuntime};
use crate::{Error, ReadContext, Result};
use parking_lot::Mutex;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const ROUTE_TTL: Duration = Duration::from_secs(1);

#[derive(Default)]
pub(super) struct RouteCache {
    current: Mutex<Option<Arc<Routes>>>,
    refresh: tokio::sync::Mutex<()>,
}
pub(super) struct Routes {
    expires: Instant,
    pub(super) nodes: Vec<(usize, ReadContext)>,
}
impl RouteCache {
    fn fresh(&self, runtime: &ReadRuntime<'_>) -> Option<Arc<Routes>> {
        self.current
            .lock()
            .as_ref()
            .filter(|routes| {
                routes.expires > Instant::now()
                    && runtime.options.minimum.as_ref().is_none_or(|minimum| {
                        routes
                            .nodes
                            .iter()
                            .all(|(_, context)| context.history == minimum.history)
                    })
            })
            .cloned()
    }
    pub(super) fn invalidate(&self, routes: &Arc<Routes>) {
        let mut current = self.current.lock();
        if current
            .as_ref()
            .is_some_and(|cached| Arc::ptr_eq(cached, routes))
        {
            *current = None;
        }
    }
}
impl Client {
    /// Cached positions only choose an address. Every data request still sends
    /// its original history/position and checks the server's capability response.
    pub(super) async fn read_routes(&self, runtime: &ReadRuntime<'_>) -> Result<Arc<Routes>> {
        if let Some(routes) = self.routes.fresh(runtime) {
            return Ok(routes);
        }
        let _refresh = self.routes.refresh.lock().await;
        runtime.check()?;
        if let Some(routes) = self.routes.fresh(runtime) {
            return Ok(routes);
        }
        let tenant = self.tenant.to_string();
        let probe = async |node: &reqwest::Url| {
            self.send::<ReadContext>(
                node,
                &["v1", "tenants", &tenant, "context"],
                Request {
                    method: reqwest::Method::GET,
                    body: None,
                    minimum: None,
                    runtime: Some(runtime),
                    read_headers: false,
                },
            )
            .await
        };
        let responses = if self.nodes.len() == 2 {
            let (first, second) = tokio::join!(probe(&self.nodes[0]), probe(&self.nodes[1]));
            vec![first, second]
        } else {
            vec![probe(&self.nodes[0]).await]
        };
        runtime.check()?;
        let mut nodes = Vec::new();
        let mut last = Error::new("UNAVAILABLE", "no readable node");
        for (index, response) in responses.into_iter().enumerate() {
            match response {
                Ok(context) if context.tenant == self.tenant => nodes.push((index, context)),
                Ok(_) => {
                    return Err(Error::new(
                        "INCOMPATIBLE_VERSION",
                        "server returned wrong tenant context",
                    ));
                }
                Err(error) if super::reads::retryable(&error) => last = error,
                Err(error) => return Err(error),
            }
        }
        if nodes.is_empty() {
            return Err(last);
        }
        let routes = Arc::new(Routes {
            expires: Instant::now() + ROUTE_TTL,
            nodes,
        });
        *self.routes.current.lock() = Some(routes.clone());
        Ok(routes)
    }
}
