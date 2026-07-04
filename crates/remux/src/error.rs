use serde::Serialize;
use serde_json::{json, Value};
use std::fmt;

pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const DOMAIN_ERROR: i64 = -32000;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(METHOD_NOT_FOUND, format!("method not found: {method}"))
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(INVALID_PARAMS, message)
    }

    pub fn invalid_object_id(id: &str, message: impl Into<String>) -> Self {
        Self::invalid_params(message).with_data(json!({ "id": id }))
    }

    pub fn object_not_found(id: &str) -> Self {
        Self::new(DOMAIN_ERROR, "objectNotFound").with_data(json!({ "id": id }))
    }

    pub fn domain(message: impl Into<String>) -> Self {
        Self::new(DOMAIN_ERROR, message)
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

impl std::error::Error for RpcError {}

impl From<anyhow::Error> for RpcError {
    fn from(value: anyhow::Error) -> Self {
        Self::domain(value.to_string())
    }
}
