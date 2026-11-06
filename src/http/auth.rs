use super::*;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Read,
    Write,
    Admin,
    Replication,
    IdentityWrite,
    IdentityCorrect,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Token {
    pub sha256: String,
    pub subject: String,
    pub tenant: Option<Uuid>,
    pub role: Role,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    pub tokens: Vec<Token>,
}
pub fn token_hash(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
impl AuthConfig {
    pub fn validate(&self) -> Result<()> {
        if self.tokens.is_empty() || self.tokens.len() > 4096 {
            return Err(Error::invalid("configure 1..4096 API token hashes"));
        }
        let mut seen = std::collections::HashSet::new();
        for token in &self.tokens {
            if token.subject.is_empty()
                || token.subject.len() > 128
                || token.subject.chars().any(char::is_control)
                || token.sha256.len() != 64
                || !token
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                || (token.tenant.is_none()
                    && !matches!(token.role, Role::Admin | Role::Replication))
                || (token.tenant.is_some() && token.role == Role::Replication)
                || !seen.insert(&token.sha256)
            {
                return Err(Error::invalid(
                    "invalid or duplicate token hash or global role",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn authenticate(&self, headers: &HeaderMap) -> Result<Token> {
        let token = headers
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .filter(|s| s.len() >= 16 && s.len() <= 512)
            .ok_or_else(|| Error::new("UNAUTHENTICATED", "Bearer token required"))?;
        let digest = token_hash(token);
        let mut matched = None;
        for candidate in &self.tokens {
            let diff = candidate
                .sha256
                .as_bytes()
                .iter()
                .zip(digest.as_bytes())
                .fold(0u8, |n, (a, b)| n | (a ^ b));
            if diff == 0 {
                matched = Some(candidate.clone());
            }
        }
        matched.ok_or_else(|| Error::new("UNAUTHENTICATED", "invalid token"))
    }
}

pub(super) fn allows(role: &Role, required: &Role) -> bool {
    role == &Role::Admin
        || role == required
        || (required == &Role::Read
            && matches!(
                role,
                Role::Write | Role::IdentityWrite | Role::IdentityCorrect
            ))
        || (required == &Role::IdentityWrite && role == &Role::IdentityCorrect)
}
