use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Error {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Location {
    pub byte_offset: usize,
    pub line: usize,
    pub column: usize,
}

impl Error {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            location: None,
        }
    }
    pub(crate) fn at(mut self, source: &str, offset: usize) -> Self {
        let mut offset = offset.min(source.len());
        while !source.is_char_boundary(offset) {
            offset -= 1;
        }
        let before = &source[..offset];
        self.location = Some(Location {
            byte_offset: offset,
            line: before.bytes().filter(|b| *b == b'\n').count() + 1,
            column: before.rsplit('\n').next().unwrap_or("").chars().count() + 1,
        });
        self
    }
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new("INVALID_ARGUMENT", message)
    }
    pub fn corrupt(message: impl Into<String>) -> Self {
        Self::new("CORRUPTION", message)
    }
    pub fn limit(message: impl Into<String>) -> Self {
        Self::new("RESOURCE_EXHAUSTED", message)
    }
    pub fn missing(message: impl Into<String>) -> Self {
        Self::new("NOT_FOUND", message)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::new("IO_ERROR", e.to_string())
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::invalid(e.to_string())
    }
}
