//! Explicit, instance-owned telemetry. No global logger or tracer is installed.
//! Export uses bounded SDK batch queues; overflow drops telemetry, never writes.
use crate::{Error, Result, Store};
use opentelemetry::{
    Context, KeyValue,
    logs::{LogRecord, Logger, LoggerProvider, Severity},
    propagation::{Extractor, TextMapPropagator},
    trace::{SpanKind, Status, TraceContextExt, Tracer, TracerProvider},
};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    logs::{BatchLogProcessor, SdkLoggerProvider},
    propagation::TraceContextPropagator,
    trace::{BatchSpanProcessor, Sampler, SdkTracerProvider},
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub metrics: bool,
    pub traces: bool,
    pub logs: bool,
    pub service_name: String,
    /// Base URL; the signal paths /v1/traces and /v1/logs are appended.
    pub endpoint: String,
    pub sample_ratio: f64,
    pub queue_size: usize,
    pub export_timeout_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            metrics: true,
            traces: false,
            logs: false,
            service_name: "tenant-kv".into(),
            endpoint: "http://127.0.0.1:4318".into(),
            sample_ratio: 0.1,
            queue_size: 1024,
            export_timeout_ms: 2000,
        }
    }
}

const BUCKETS: [f64; 8] = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];
const READ_FIELDS: [&str; 6] = [
    "candidate_files",
    "scanned_entries",
    "blocks_read",
    "decompressed_bytes",
    "cache_hits",
    "document_reads",
];
#[derive(Default)]
struct Measurement {
    count: u64,
    errors: u64,
    sum: f64,
    buckets: [u64; 8],
}
struct Inner {
    config: Config,
    traces: Option<SdkTracerProvider>,
    logs: Option<SdkLoggerProvider>,
    measurements: Mutex<BTreeMap<String, Measurement>>,
    active: AtomicU64,
    reads: [AtomicU64; 6],
}
#[derive(Clone)]
pub struct Telemetry(Arc<Inner>);
impl Default for Telemetry {
    fn default() -> Self {
        Self(Arc::new(Inner {
            config: Config::default(),
            traces: None,
            logs: None,
            measurements: Mutex::new(BTreeMap::new()),
            active: AtomicU64::new(0),
            reads: Default::default(),
        }))
    }
}
#[derive(Clone, Default)]
struct Principal(Arc<Mutex<Option<String>>>);

impl Telemetry {
    /// Build on a blocking thread when called from an async runtime. Explicit
    /// endpoint and timeout override OTEL environment defaults. Standard OTEL
    /// header environment variables support collector authentication. Redirects
    /// are disabled; credentials and exporter errors are never logged here.
    pub fn new(config: Config) -> Result<Self> {
        if config.service_name.is_empty()
            || config.service_name.len() > 128
            || config.service_name.chars().any(char::is_control)
            || !config.sample_ratio.is_finite()
            || !(0.0..=1.0).contains(&config.sample_ratio)
            || !(16..=65536).contains(&config.queue_size)
            || !(100..=10000).contains(&config.export_timeout_ms)
        {
            return Err(Error::invalid("telemetry configuration bounds"));
        }
        let resource = Resource::builder_empty()
            .with_attributes([
                KeyValue::new("service.name", config.service_name.clone()),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            ])
            .build();
        let timeout = Duration::from_millis(config.export_timeout_ms);
        let exporter_error = |_| Error::invalid("cannot initialize OTLP exporter");
        let mut traces = None;
        let mut logs = None;
        if config.traces || config.logs {
            let url = reqwest::Url::parse(&config.endpoint)
                .map_err(|_| Error::invalid("OTLP endpoint URL"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(Error::invalid(
                    "OTLP endpoint must be an HTTP(S) base URL without credentials or query",
                ));
            }
            let client = reqwest::blocking::Client::builder()
                .timeout(timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| Error::invalid("OTLP HTTP client"))?;
            if config.traces {
                let exporter = opentelemetry_otlp::SpanExporter::builder()
                    .with_http()
                    .with_protocol(opentelemetry_otlp::Protocol::HttpJson)
                    .with_endpoint(format!(
                        "{}/v1/traces",
                        config.endpoint.trim_end_matches('/')
                    ))
                    .with_timeout(timeout)
                    .with_headers(Default::default())
                    .with_http_client(client.clone())
                    .build()
                    .map_err(exporter_error)?;
                let batch = opentelemetry_sdk::trace::BatchConfigBuilder::default()
                    .with_max_queue_size(config.queue_size)
                    .with_max_export_batch_size(config.queue_size.min(256))
                    .with_scheduled_delay(Duration::from_millis(500))
                    .build();
                traces = Some(
                    SdkTracerProvider::builder()
                        .with_resource(resource.clone())
                        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                            config.sample_ratio,
                        ))))
                        .with_span_processor(
                            BatchSpanProcessor::builder(exporter)
                                .with_batch_config(batch)
                                .build(),
                        )
                        .build(),
                );
            }
            if config.logs {
                let exporter = opentelemetry_otlp::LogExporter::builder()
                    .with_http()
                    .with_protocol(opentelemetry_otlp::Protocol::HttpJson)
                    .with_endpoint(format!("{}/v1/logs", config.endpoint.trim_end_matches('/')))
                    .with_timeout(timeout)
                    .with_headers(Default::default())
                    .with_http_client(client)
                    .build()
                    .map_err(exporter_error)?;
                let batch = opentelemetry_sdk::logs::BatchConfigBuilder::default()
                    .with_max_queue_size(config.queue_size)
                    .with_max_export_batch_size(config.queue_size.min(256))
                    .with_scheduled_delay(Duration::from_millis(500))
                    .build();
                logs = Some(
                    SdkLoggerProvider::builder()
                        .with_resource(resource)
                        .with_log_processor(
                            BatchLogProcessor::builder(exporter)
                                .with_batch_config(batch)
                                .build(),
                        )
                        .build(),
                );
            }
        }
        Ok(Self(Arc::new(Inner {
            config,
            traces,
            logs,
            measurements: Mutex::new(BTreeMap::new()),
            active: AtomicU64::new(0),
            reads: Default::default(),
        })))
    }

    pub fn metrics_enabled(&self) -> bool {
        self.0.config.metrics
    }

    pub(crate) fn record_read(&self, value: &serde_json::Value) {
        let Some(stats) = value
            .get("stats")
            .or_else(|| value.get("result").and_then(|r| r.get("stats")))
        else {
            return;
        };
        let mut attributes = Vec::new();
        for (i, field) in READ_FIELDS.iter().enumerate() {
            if let Some(count) = stats.get(field).and_then(serde_json::Value::as_u64) {
                if self.metrics_enabled() {
                    self.0.reads[i].fetch_add(count, Ordering::Relaxed);
                }
                if self.0.traces.is_some() {
                    attributes.push(KeyValue::new(
                        format!("db.{field}"),
                        count.min(i64::MAX as u64) as i64,
                    ));
                }
            }
        }
        if self.0.traces.is_some() {
            Context::current().span().add_event("db.read", attributes);
        }
    }

    /// Start an operation using a route template, never a raw URL or key. W3C
    /// trace context is extracted, but baggage and request contents are omitted.
    pub fn begin(&self, route: &str, method: &str, headers: &axum::http::HeaderMap) -> Observation {
        let principal = Principal::default();
        let parent = TraceContextPropagator::new()
            .extract(&Headers(headers))
            .with_value(principal.clone());
        let context = if let Some(provider) = &self.0.traces {
            let span = provider
                .tracer("tenant-kv.http")
                .span_builder(format!("{method} {route}"))
                .with_kind(SpanKind::Server)
                .with_attributes([
                    KeyValue::new("http.request.method", method.to_owned()),
                    KeyValue::new("http.route", route.to_owned()),
                ])
                .start_with_context(&provider.tracer("tenant-kv.http"), &parent);
            parent.with_span(span)
        } else {
            parent
        };
        if self.metrics_enabled() {
            self.0.active.fetch_add(1, Ordering::Relaxed);
        }
        Observation {
            telemetry: self.clone(),
            context,
            route: route.into(),
            principal,
            started: Instant::now(),
            finished: false,
        }
    }

    pub fn render_metrics(&self, store: &Store) -> String {
        if !self.metrics_enabled() {
            return String::new();
        }
        let mut out = format!(
            "# HELP tkv_memory_reserved_bytes Accounted engine and HTTP memory.\n# TYPE tkv_memory_reserved_bytes gauge\ntkv_memory_reserved_bytes {}\n# TYPE tkv_http_in_flight gauge\ntkv_http_in_flight {}\n# TYPE tkv_tenants gauge\ntkv_tenants {}\n# TYPE tkv_node_writable gauge\ntkv_node_writable {}\n# TYPE tkv_http_requests_total counter\n# TYPE tkv_http_errors_total counter\n# TYPE tkv_http_request_duration_seconds histogram\n",
            store.memory_bytes(),
            self.0.active.load(Ordering::Relaxed),
            store.tenants().len(),
            u8::from(store.writable())
        );
        for (i, field) in READ_FIELDS.iter().enumerate() {
            out.push_str(&format!(
                "# TYPE tkv_query_{field}_total counter\ntkv_query_{field}_total {}\n",
                self.0.reads[i].load(Ordering::Relaxed)
            ));
        }
        for (name, value) in store.open_engine_metrics() {
            let family = if name.starts_with("registry_") {
                format!("tkv_{name}")
            } else {
                format!("tkv_open_engines_{name}")
            };
            out.push_str(&format!("# TYPE {family} gauge\n{family} {value}\n"));
        }
        for (route, value) in self.0.measurements.lock().iter() {
            let route = route
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");
            out.push_str(&format!("tkv_http_requests_total{{route=\"{route}\"}} {}\ntkv_http_errors_total{{route=\"{route}\"}} {}\n", value.count, value.errors));
            for (i, bound) in BUCKETS.iter().enumerate() {
                out.push_str(&format!("tkv_http_request_duration_seconds_bucket{{route=\"{route}\",le=\"{bound}\"}} {}\n", value.buckets[i]));
            }
            out.push_str(&format!("tkv_http_request_duration_seconds_bucket{{route=\"{route}\",le=\"+Inf\"}} {}\ntkv_http_request_duration_seconds_count{{route=\"{route}\"}} {}\ntkv_http_request_duration_seconds_sum{{route=\"{route}\"}} {}\n", value.count, value.count, value.sum));
        }
        out
    }

    /// Drain exporters after requests finish. Run outside the async executor.
    pub fn shutdown(&self) -> Result<()> {
        let timeout = Duration::from_millis(self.0.config.export_timeout_ms + 1000);
        let traces = self
            .0
            .traces
            .as_ref()
            .map(|p| p.shutdown_with_timeout(timeout));
        let logs = self
            .0
            .logs
            .as_ref()
            .map(|p| p.shutdown_with_timeout(timeout));
        if traces.is_some_and(|r| r.is_err()) || logs.is_some_and(|r| r.is_err()) {
            return Err(Error::new(
                "TELEMETRY_UNAVAILABLE",
                "telemetry shutdown could not flush all records",
            ));
        }
        Ok(())
    }
}

pub(crate) fn authenticated(context: &Context, subject: &str) {
    if let Some(principal) = context.get::<Principal>() {
        *principal.0.lock() = Some(subject.into());
    }
}

pub struct Observation {
    telemetry: Telemetry,
    context: Context,
    route: String,
    principal: Principal,
    started: Instant,
    finished: bool,
}
impl Observation {
    pub fn context(&self) -> Context {
        self.context.clone()
    }
    pub fn finish(mut self, status: u16) {
        self.complete(status);
    }
    fn complete(&mut self, status: u16) {
        if self.finished {
            return;
        }
        self.finished = true;
        let elapsed = self.started.elapsed().as_secs_f64();
        if self.telemetry.metrics_enabled() {
            self.telemetry.0.active.fetch_sub(1, Ordering::Relaxed);
            let mut measurements = self.telemetry.0.measurements.lock();
            let route = if self.route.len() <= 256
                && (measurements.len() < 256 || measurements.contains_key(&self.route))
            {
                self.route.clone()
            } else {
                "other".into()
            };
            let value = measurements.entry(route).or_default();
            value.count += 1;
            value.errors += u64::from(status >= 400);
            value.sum += elapsed;
            for (i, bound) in BUCKETS.iter().enumerate() {
                value.buckets[i] += u64::from(elapsed <= *bound);
            }
        }
        if self.telemetry.0.traces.is_some() {
            let span = self.context.span();
            span.set_attribute(KeyValue::new(
                "http.response.status_code",
                i64::from(status),
            ));
            if status >= 500 {
                span.set_status(Status::error("request failed"));
            }
        }
        if let Some(provider) = &self.telemetry.0.logs {
            let logger = provider.logger("tenant-kv.audit");
            let mut record = logger.create_log_record();
            record.set_event_name("http.request.completed");
            record.set_severity_number(if status >= 500 {
                Severity::Error
            } else {
                Severity::Info
            });
            record.set_body("HTTP request completed".into());
            record.add_attribute("http.route", self.route.clone());
            record.add_attribute("http.response.status_code", i64::from(status));
            record.add_attribute("duration_ms", elapsed * 1000.0);
            if let Some(principal) = &*self.principal.0.lock() {
                record.add_attribute("enduser.id", principal.clone());
            }
            let span = self.context.span();
            let sc = span.span_context();
            if sc.is_valid() {
                record.set_trace_context(sc.trace_id(), sc.span_id(), Some(sc.trace_flags()));
            }
            logger.emit(record);
        }
        if self.telemetry.0.traces.is_some() {
            self.context.span().end();
        }
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        self.complete(499);
    }
}
struct Headers<'a>(&'a axum::http::HeaderMap);
impl Extractor for Headers<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}
