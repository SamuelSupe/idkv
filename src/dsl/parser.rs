use super::*;
use crate::codec;

struct Parser<'a> {
    source: &'a str,
    pos: usize,
    parameters: Vec<String>,
    nodes: usize,
}
pub(super) fn is_write(source: &str) -> Result<bool> {
    let mut p = Parser::new(source)?;
    Ok(match p.ident()? {
        "put" | "delete" => true,
        "from" | "explain" | "get" | "mget" | "lookup" | "resolve" | "resolvemany" | "ids"
        | "history" => false,
        _ => {
            return Err(p.error(
                "expected from, explain, get, mget, lookup, resolve, resolvemany, ids, history, put or delete",
            ));
        }
    })
}
pub(super) fn parse(source: &str) -> Result<Prepared> {
    let mut p = Parser::new(source)?;
    let kind = p.ident()?;
    if kind == "explain" {
        p.expect_word("from")?;
    }
    if !matches!(
        kind,
        "from"
            | "explain"
            | "get"
            | "mget"
            | "lookup"
            | "resolve"
            | "resolvemany"
            | "ids"
            | "history"
            | "put"
            | "delete"
    ) {
        return Err(p.error("unknown command"));
    }
    let collection = if matches!(kind, "resolve" | "resolvemany" | "ids" | "history") {
        crate::identity::COLLECTION.to_owned()
    } else {
        p.name()?
    };
    let command = match kind {
        "from" | "explain" => {
            let selection = p.selection()?;
            if kind == "from" {
                Command::Query(selection)
            } else {
                Command::Explain(selection)
            }
        }
        "resolve" | "resolvemany" | "ids" | "history" => {
            p.expect_word("with")?;
            let request = p.operand()?;
            if kind == "resolve" {
                Command::Resolve(request)
            } else if kind == "ids" {
                Command::SubjectIds(request)
            } else if kind == "history" {
                Command::SubjectHistory(request)
            } else {
                Command::ResolveMany(request)
            }
        }
        "get" => Command::Get(p.operand()?),
        "mget" => Command::MultiGet(p.operand()?),
        "lookup" => {
            p.expect_word("using")?;
            let name = p.name()?;
            p.expect_word("with")?;
            Command::Lookup {
                name,
                inputs: p.operand()?,
            }
        }
        "put" => {
            let document = p.operand()?;
            Command::Put {
                document,
                revision: p.revision()?,
            }
        }
        "delete" => {
            let key = p.operand()?;
            Command::Delete {
                key,
                revision: p.revision()?,
            }
        }
        _ => unreachable!(),
    };
    p.skip();
    if p.pos != source.len() {
        return Err(p.error("unexpected input; exactly one statement is allowed"));
    }
    Ok(Prepared {
        collection,
        command,
        parameters: p.parameters,
        _charge: None,
    })
}
impl<'a> Parser<'a> {
    fn new(source: &'a str) -> Result<Self> {
        if source.len() > MAX_STATEMENT_BYTES {
            return Err(Error::limit("DSL statement exceeds 64 KiB"));
        }
        Ok(Self {
            source,
            pos: 0,
            parameters: Vec::new(),
            nodes: 0,
        })
    }
    fn error(&self, message: &str) -> Error {
        Error::invalid(message).at(self.source, self.pos)
    }
    fn skip(&mut self) {
        loop {
            while self
                .source
                .as_bytes()
                .get(self.pos)
                .is_some_and(u8::is_ascii_whitespace)
            {
                self.pos += 1;
            }
            if self.source.as_bytes().get(self.pos) == Some(&b'#') {
                while self.pos < self.source.len() && self.source.as_bytes()[self.pos] != b'\n' {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }
    fn take(&mut self, byte: u8) -> bool {
        self.skip();
        if self.source.as_bytes().get(self.pos) == Some(&byte) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, byte: u8) -> Result<()> {
        if self.take(byte) {
            Ok(())
        } else {
            Err(self.error(&format!("expected '{}'", char::from(byte))))
        }
    }
    fn ident(&mut self) -> Result<&'a str> {
        self.skip();
        let start = self.pos;
        let bytes = self.source.as_bytes();
        if !bytes
            .get(start)
            .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
        {
            return Err(self.error("expected identifier"));
        }
        self.pos += 1;
        while bytes
            .get(self.pos)
            .is_some_and(|b| b.is_ascii_alphanumeric() || matches!(*b, b'_' | b'-'))
        {
            self.pos += 1;
        }
        Ok(&self.source[start..self.pos])
    }
    fn word(&mut self, word: &str) -> bool {
        self.skip();
        let end = self.pos + word.len();
        if self.source.get(self.pos..end) == Some(word)
            && !self
                .source
                .as_bytes()
                .get(end)
                .is_some_and(|b| b.is_ascii_alphanumeric() || matches!(*b, b'_' | b'-'))
        {
            self.pos = end;
            true
        } else {
            false
        }
    }
    fn expect_word(&mut self, word: &str) -> Result<()> {
        if self.word(word) {
            Ok(())
        } else {
            Err(self.error(&format!("expected {word}")))
        }
    }
    fn name(&mut self) -> Result<String> {
        self.skip();
        let name = if self.source.as_bytes().get(self.pos) == Some(&b'"') {
            self.json()?.as_str().unwrap().to_owned()
        } else {
            self.ident()?.to_owned()
        };
        crate::tenant::catalog::name(&name).map_err(|e| e.at(self.source, self.pos))?;
        Ok(name)
    }
    fn field(&mut self) -> Result<String> {
        let first = self.ident()?;
        let path = if first == "field" && self.take(b'(') {
            let v = self.json()?;
            let path = v
                .as_str()
                .ok_or_else(|| self.error("field() requires a JSON Pointer string"))?
                .to_owned();
            self.expect(b')')?;
            path
        } else {
            let mut path = format!("/{first}");
            while self.take(b'.') {
                path.push('/');
                path.push_str(self.ident()?);
            }
            path
        };
        codec::validate_pointer(&path).map_err(|e| e.at(self.source, self.pos))?;
        Ok(path)
    }
    fn operand(&mut self) -> Result<Operand> {
        if self.take(b'$') {
            let name = self.ident()?.to_owned();
            let index = if let Some(i) = self.parameters.iter().position(|p| *p == name) {
                i
            } else {
                if self.parameters.len() >= 128 {
                    return Err(self.error("parameter count exceeds 128"));
                }
                self.parameters.push(name);
                self.parameters.len() - 1
            };
            Ok(Operand::Param(index))
        } else {
            Ok(Operand::Literal(self.json()?))
        }
    }
    fn json(&mut self) -> Result<Value> {
        self.skip();
        let start = self.pos;
        let bytes = self.source.as_bytes();
        let Some(first) = bytes.get(start).copied() else {
            return Err(self.error("expected JSON value or parameter"));
        };
        let mut string = false;
        let mut escaped = false;
        let mut depth = 0usize;
        if matches!(first, b'{' | b'[' | b'"') {
            while self.pos < bytes.len() {
                let b = bytes[self.pos];
                self.pos += 1;
                if string {
                    if escaped {
                        escaped = false;
                    } else if b == b'\\' {
                        escaped = true;
                    } else if b == b'"' {
                        string = false;
                        if depth == 0 {
                            break;
                        }
                    }
                } else {
                    match b {
                        b'"' => string = true,
                        b'{' | b'[' => {
                            depth += 1;
                            if depth > 64 {
                                return Err(self.error("JSON nesting exceeds 64"));
                            }
                        }
                        b'}' | b']' => {
                            if depth == 0 {
                                return Err(self.error("unexpected JSON closing delimiter"));
                            }
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
        } else {
            while bytes.get(self.pos).is_some_and(|b| {
                !b.is_ascii_whitespace() && !matches!(*b, b'|' | b')' | b',' | b'#' | b';')
            }) {
                self.pos += 1;
            }
        }
        serde_json::from_str(&self.source[start..self.pos])
            .map_err(|_| Error::invalid("invalid JSON literal").at(self.source, start))
    }
    fn revision(&mut self) -> Result<Option<Operand>> {
        if self.word("if_rev") {
            Ok(Some(self.operand()?))
        } else {
            Ok(None)
        }
    }
    fn selection(&mut self) -> Result<Selection> {
        let mut s = Selection {
            filter: None,
            sort: Vec::new(),
            fields: Vec::new(),
            limit: Operand::Literal(Value::from(100)),
        };
        let mut last = 0;
        while self.take(b'|') {
            let clause = self.ident()?;
            let stage = match clause {
                "where" => 1,
                "sort" => 2,
                "fields" => 3,
                "limit" => 4,
                _ => return Err(self.error("expected where, sort, fields or limit")),
            };
            if stage <= last {
                return Err(
                    self.error("clauses must occur once in where, sort, fields, limit order")
                );
            }
            last = stage;
            match stage {
                1 => s.filter = Some(self.or(0)?),
                2 => loop {
                    let field = self.field()?;
                    let order = if self.word("desc") {
                        Order::Desc
                    } else {
                        self.word("asc");
                        Order::Asc
                    };
                    if s.sort.iter().any(|f| f.field == field) {
                        return Err(self.error("duplicate sort field"));
                    }
                    s.sort.push(IndexField { field, order });
                    if s.sort.len() > 8 {
                        return Err(self.error("sort field limit"));
                    }
                    if !self.take(b',') {
                        break;
                    }
                },
                3 => loop {
                    s.fields.push(self.field()?);
                    if s.fields.len() > 64 {
                        return Err(self.error("projection field limit"));
                    }
                    if !self.take(b',') {
                        break;
                    }
                },
                _ => s.limit = self.operand()?,
            }
        }
        Ok(s)
    }
    fn node(&mut self) -> Result<()> {
        self.nodes += 1;
        if self.nodes > 128 {
            Err(self.error("filter exceeds 128 nodes"))
        } else {
            Ok(())
        }
    }
    fn or(&mut self, depth: usize) -> Result<Predicate> {
        let first = self.and(depth)?;
        if !self.word("or") {
            return Ok(first);
        }
        self.node()?;
        let mut parts = vec![first];
        loop {
            parts.push(self.and(depth + 1)?);
            if !self.word("or") {
                break;
            }
        }
        Ok(Predicate::Or(parts))
    }
    fn and(&mut self, depth: usize) -> Result<Predicate> {
        let first = self.unary(depth)?;
        if !self.word("and") {
            return Ok(first);
        }
        self.node()?;
        let mut parts = vec![first];
        loop {
            parts.push(self.unary(depth + 1)?);
            if !self.word("and") {
                break;
            }
        }
        Ok(Predicate::And(parts))
    }
    fn unary(&mut self, depth: usize) -> Result<Predicate> {
        if depth > 16 {
            return Err(self.error("filter nesting exceeds 16"));
        }
        if self.word("not") {
            self.node()?;
            return Ok(Predicate::Not(Box::new(self.unary(depth + 1)?)));
        }
        if self.take(b'(') {
            let value = self.or(depth + 1)?;
            self.expect(b')')?;
            return Ok(value);
        }
        self.node()?;
        if self.word("exists") {
            self.expect(b'(')?;
            let field = self.field()?;
            self.expect(b')')?;
            return Ok(Predicate::Compare {
                field,
                op: CompareOp::Exists,
                value: Operand::Literal(Value::Bool(true)),
            });
        }
        let field = self.field()?;
        self.skip();
        let op = if self.word("in") {
            CompareOp::In
        } else {
            let rest = &self.source[self.pos..];
            let (length, op) = if rest.starts_with("==") {
                (2, CompareOp::Eq)
            } else if rest.starts_with("!=") {
                (2, CompareOp::Ne)
            } else if rest.starts_with(">=") {
                (2, CompareOp::Gte)
            } else if rest.starts_with("<=") {
                (2, CompareOp::Lte)
            } else if rest.starts_with('>') {
                (1, CompareOp::Gt)
            } else if rest.starts_with('<') {
                (1, CompareOp::Lt)
            } else {
                return Err(self.error("expected comparison operator"));
            };
            self.pos += length;
            op
        };
        Ok(Predicate::Compare {
            field,
            op,
            value: self.operand()?,
        })
    }
}
