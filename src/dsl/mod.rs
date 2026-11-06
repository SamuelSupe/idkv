//! One-statement DSL. Prepared syntax contains no tenant, snapshot, resolved
//! index, or bound parameter values and can be reused with fresh bindings.
mod cache;
mod parser;
use crate::{Error, Result, TenantHandle, model::*, resources::Reservation};
pub(crate) use cache::Cache;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;

pub const MAX_STATEMENT_BYTES: usize = 64 << 10;
pub type Params = Map<String, Value>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub statement: String,
    #[serde(default)]
    pub params: Params,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub write_options: WriteOptions,
}

/// Serialized exactly as the corresponding native operation. A missing Get is
/// null; queries and lookups retain their snapshot, plan and execution statistics.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Output {
    Query(QueryResult),
    Explain(Explain),
    Get(Option<Value>),
    MultiGet(Vec<Option<Value>>),
    Lookup(LookupResult),
    Write(WriteResult),
}

#[derive(Debug)]
enum Operand {
    Literal(Value),
    Param(usize),
}
#[derive(Debug)]
enum Predicate {
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    Compare {
        field: String,
        op: CompareOp,
        value: Operand,
    },
}
#[derive(Debug)]
struct Selection {
    filter: Option<Predicate>,
    sort: Vec<IndexField>,
    fields: Vec<String>,
    limit: Operand,
}
#[derive(Debug)]
enum Command {
    Query(Selection),
    Explain(Selection),
    Get(Operand),
    MultiGet(Operand),
    Lookup {
        name: String,
        inputs: Operand,
    },
    Put {
        document: Operand,
        revision: Option<Operand>,
    },
    Delete {
        key: Operand,
        revision: Option<Operand>,
    },
}

#[derive(Debug)]
pub struct Prepared {
    collection: String,
    command: Command,
    parameters: Vec<String>,
    _charge: Option<Reservation>,
}
impl Prepared {
    /// Parse once for embedded use. The caller owns this allocation. Use
    /// TenantHandle::prepare_dsl for an engine-budgeted, cached preparation.
    pub fn parse(statement: &str) -> Result<Self> {
        parser::parse(statement)
    }
    pub fn is_write(&self) -> bool {
        matches!(self.command, Command::Put { .. } | Command::Delete { .. })
    }
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// Execute against this authorized tenant handle. Bindings are values only;
    /// collection names and field paths cannot be supplied as parameters.
    pub fn execute<'a>(
        &'a self,
        tenant: &TenantHandle,
        params: &'a Params,
        cursor: Option<&str>,
    ) -> Result<Output> {
        self.execute_with(tenant, params, cursor, &WriteOptions::default(), "embedded")
    }

    pub fn execute_with<'a>(
        &'a self,
        tenant: &TenantHandle,
        params: &'a Params,
        cursor: Option<&str>,
        options: &WriteOptions,
        principal: &str,
    ) -> Result<Output> {
        self.execute_controlled(tenant, params, cursor, (options, principal), None)
    }

    /// Execute a read-only statement with one deadline shared by binding and execution.
    pub fn execute_read<'a>(
        &'a self,
        reader: &crate::TenantReader<'_>,
        params: &'a Params,
        cursor: Option<&str>,
    ) -> Result<Output> {
        if self.is_write() {
            return Err(Error::invalid("execute_read requires a read statement"));
        }
        reader.budget.check()?;
        self.execute_controlled(
            reader.tenant,
            params,
            cursor,
            (&WriteOptions::default(), "embedded"),
            Some(reader),
        )
    }

    fn execute_controlled<'a>(
        &'a self,
        tenant: &TenantHandle,
        params: &'a Params,
        cursor: Option<&str>,
        write: (&WriteOptions, &str),
        reader: Option<&crate::TenantReader<'_>>,
    ) -> Result<Output> {
        let (options, principal) = write;
        let default_reader;
        let reader = match reader {
            Some(reader) => reader,
            None => {
                default_reader = tenant.read_with(&crate::ReadOptions::default())?;
                &default_reader
            }
        };
        if !self.is_write()
            && (options.operation_id.is_some() || options.expected_history.is_some())
        {
            return Err(Error::invalid("write_options require a write statement"));
        }
        if cursor.is_some() && !matches!(self.command, Command::Query(_)) {
            return Err(Error::invalid("cursor is only valid for from queries"));
        }
        if params.len() != self.parameters.len() {
            return Err(Error::invalid(
                "bindings must contain exactly the statement parameters",
            ));
        }
        let bindings: Vec<&Value> = self
            .parameters
            .iter()
            .map(|name| {
                params
                    .get(name)
                    .ok_or_else(|| Error::invalid(format!("missing parameter ${name}")))
            })
            .collect::<Result<_>>()?;
        let binding_sizes = bindings
            .iter()
            .map(|v| crate::json::value_memory(v))
            .collect::<Result<Vec<_>>>()?;
        let expanded = match &self.command {
            Command::Query(s) | Command::Explain(s) => s
                .filter
                .as_ref()
                .map(|f| f.binding_memory(&binding_sizes))
                .transpose()?
                .unwrap_or(0),
            Command::Put { document, .. } => document.binding_memory(&binding_sizes)?,
            Command::MultiGet(keys) => keys.binding_memory(&binding_sizes)?,
            _ => 0,
        };
        if expanded > tenant.engine().options.max_query_bytes {
            return Err(Error::limit(
                "expanded DSL bindings exceed temporary memory limit",
            ));
        }
        let size = crate::json::encoded_len(params)?;
        if size > tenant.engine().options.max_batch_bytes {
            return Err(Error::limit("DSL parameter byte limit"));
        }
        // Reservations cover clones made by binding and the query/put APIs. Input
        // JSON trees supplied by embedded callers remain owned by those callers.
        let _memory = tenant
            .engine()
            .budget
            .reserve(expanded.max(size.saturating_mul(8)).saturating_add(4096))?;
        let value = |operand: &'a Operand| match operand {
            Operand::Literal(v) => v,
            Operand::Param(i) => bindings[*i],
        };
        let string = |operand: &'a Operand| {
            value(operand)
                .as_str()
                .ok_or_else(|| Error::invalid("key must be a string"))
        };
        let revision = |rev: &'a Option<Operand>| {
            rev.as_ref()
                .map(|v| unsigned(value(v), "if_rev"))
                .transpose()
        };
        let collection = &self.collection;
        Ok(match &self.command {
            Command::Get(key) => Output::Get(reader.get(collection, string(key)?)?),
            Command::MultiGet(keys) => {
                let keys = value(keys)
                    .as_array()
                    .ok_or_else(|| Error::invalid("mget requires an array of strings"))?;
                if keys.len() > tenant.engine().options.max_read_keys {
                    return Err(Error::limit("multi_get input limit"));
                }
                let keys = keys
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(str::to_owned)
                            .ok_or_else(|| Error::invalid("mget keys must be strings"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Output::MultiGet(reader.multi_get(collection, &keys)?)
            }
            Command::Lookup { name, inputs } => {
                let inputs = value(inputs)
                    .as_array()
                    .ok_or_else(|| Error::invalid("lookup requires an array of objects"))?;
                Output::Lookup(reader.lookup_many(collection, name, inputs)?)
            }
            Command::Put {
                document,
                revision: rev,
            } => Output::Write(
                tenant
                    .write_batch_with(
                        collection,
                        vec![WriteOp::Put {
                            document: value(document).clone(),
                            if_rev: revision(rev)?,
                        }],
                        options,
                        principal,
                    )?
                    .remove(0),
            ),
            Command::Delete { key, revision: rev } => Output::Write(
                tenant
                    .write_batch_with(
                        collection,
                        vec![WriteOp::Delete {
                            key: string(key)?.into(),
                            if_rev: revision(rev)?,
                        }],
                        options,
                        principal,
                    )?
                    .remove(0),
            ),
            Command::Query(selection) | Command::Explain(selection) => {
                let query = Query {
                    filter: selection
                        .filter
                        .as_ref()
                        .map(|f| f.bind(&bindings))
                        .transpose()?,
                    sort: selection.sort.clone(),
                    select: selection.fields.clone(),
                    limit: usize::try_from(unsigned(value(&selection.limit), "limit")?)
                        .map_err(|_| Error::invalid("limit overflow"))?,
                    cursor: cursor.map(str::to_owned),
                };
                if matches!(self.command, Command::Explain(_)) {
                    Output::Explain(reader.explain(collection, &query)?)
                } else {
                    Output::Query(reader.query(collection, query)?)
                }
            }
        })
    }
}
fn unsigned(value: &Value, name: &str) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| Error::invalid(format!("{name} must be an unsigned integer")))
}
impl Predicate {
    fn binding_memory(&self, sizes: &[usize]) -> Result<usize> {
        Ok(128usize.saturating_add(match self {
            Self::And(parts) | Self::Or(parts) => parts.iter().try_fold(0usize, |sum, p| {
                Ok::<_, Error>(sum.saturating_add(p.binding_memory(sizes)?))
            })?,
            Self::Not(p) => p.binding_memory(sizes)?,
            Self::Compare { field, value, .. } => field.len() * 2 + value.binding_memory(sizes)?,
        }))
    }
    fn bind(&self, bindings: &[&Value]) -> Result<Filter> {
        Ok(match self {
            Self::And(parts) => Filter::And {
                and: parts
                    .iter()
                    .map(|p| p.bind(bindings))
                    .collect::<Result<_>>()?,
            },
            Self::Or(parts) => Filter::Or {
                or: parts
                    .iter()
                    .map(|p| p.bind(bindings))
                    .collect::<Result<_>>()?,
            },
            Self::Not(inner) => Filter::Not {
                not: Box::new(inner.bind(bindings)?),
            },
            Self::Compare { field, op, value } => Filter::Compare {
                field: field.clone(),
                op: op.clone(),
                value: match value {
                    Operand::Literal(v) => v.clone(),
                    Operand::Param(i) => (*bindings[*i]).clone(),
                },
            },
        })
    }
}
impl Operand {
    fn binding_memory(&self, sizes: &[usize]) -> Result<usize> {
        match self {
            Self::Literal(v) => crate::json::value_memory(v),
            Self::Param(i) => Ok(sizes[*i]),
        }
    }
}

/// Cheap authorization preflight using the same command lexer as the full parser.
/// HTTP checks the fully prepared command again before execution.
pub(crate) fn is_write(statement: &str) -> Result<bool> {
    parser::is_write(statement)
}
impl TenantHandle {
    pub fn prepare_dsl(&self, statement: &str) -> Result<Arc<Prepared>> {
        self.core.dsl.prepare(statement, &self.engine().budget)
    }
    pub fn execute_dsl(&self, request: &Request) -> Result<Output> {
        self.prepare_dsl(&request.statement)?.execute_with(
            self,
            &request.params,
            request.cursor.as_deref(),
            &request.write_options,
            "embedded",
        )
    }
    pub fn dsl_cache_stats(&self) -> Value {
        self.core.dsl.stats()
    }
}
