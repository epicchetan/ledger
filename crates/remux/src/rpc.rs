use crate::error::RpcError;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

pub type DispatchFuture = Pin<Box<dyn Future<Output = Result<Value, RpcError>> + Send>>;
pub type DispatchFn = Arc<dyn Fn(Request) -> DispatchFuture + Send + Sync>;
pub type OutboundSender = mpsc::Sender<OutboundMessage>;

#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    pub method: String,
    pub params: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InboundMessage {
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    Notification {
        method: String,
        params: Value,
    },
    InvalidRequest {
        id: Value,
        error: RpcError,
    },
}

#[derive(Debug)]
pub enum OutboundMessage {
    Json(Value),
    Flush(oneshot::Sender<()>),
}

#[derive(Debug, Deserialize)]
struct InboundEnvelope {
    id: Option<Value>,
    jsonrpc: Option<String>,
    method: Option<String>,
    #[serde(default)]
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcNotification {
    jsonrpc: &'static str,
    method: String,
    params: Value,
}

pub async fn serve_stdio(make_dispatch: impl FnOnce(OutboundSender) -> DispatchFn) -> Result<()> {
    let (output_tx, output_rx) = mpsc::channel::<OutboundMessage>(1024);
    let writer = tokio::spawn(stdout_writer(output_rx));
    let dispatch = make_dispatch(output_tx.clone());
    let mut tasks = JoinSet::new();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut sigterm = Box::pin(wait_sigterm());
    let mut terminating = false;

    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else {
                    break;
                };
                handle_line(&line, &dispatch, &output_tx, &mut tasks);
            }
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = result {
                    eprintln!("[ledger-remux] request supervisor failed: {error}");
                }
            }
            _ = &mut sigterm => {
                terminating = true;
                break;
            }
        }
    }

    if terminating {
        tasks.abort_all();
        writer.abort();
        return Ok(());
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            eprintln!("[ledger-remux] request supervisor failed: {error}");
        }
    }
    flush_output(&output_tx).await;
    writer.abort();
    Ok(())
}

fn handle_line(
    line: &str,
    dispatch: &DispatchFn,
    output_tx: &OutboundSender,
    tasks: &mut JoinSet<()>,
) {
    match parse_inbound_line(line) {
        Ok(Some(InboundMessage::Request { id, method, params })) => {
            spawn_request(
                tasks,
                dispatch.clone(),
                output_tx.clone(),
                id,
                Request { method, params },
            );
        }
        Ok(Some(InboundMessage::InvalidRequest { id, error })) => {
            let output_tx = output_tx.clone();
            tasks.spawn(async move {
                if let Err(error) = send_error(&output_tx, id, error).await {
                    eprintln!("[ledger-remux] failed to send invalid request response: {error}");
                }
            });
        }
        Ok(Some(InboundMessage::Notification { .. })) | Ok(None) => {}
        Err(error) => {
            eprintln!("[ledger-remux] ignored invalid protocol line: {error}");
        }
    }
}

fn spawn_request(
    tasks: &mut JoinSet<()>,
    dispatch: DispatchFn,
    output_tx: OutboundSender,
    id: Value,
    request: Request,
) {
    tasks.spawn(async move {
        let handler = tokio::spawn(async move { (dispatch)(request).await });
        let response = match handler.await {
            Ok(Ok(result)) => response_result(id.clone(), result),
            Ok(Err(error)) => response_error(id.clone(), error),
            Err(error) => response_error(
                id.clone(),
                RpcError::domain(format!("request handler failed: {error}")),
            ),
        };
        if let Err(error) = send_json(&output_tx, response).await {
            eprintln!("[ledger-remux] failed to send response: {error}");
        }
    });
}

pub fn parse_inbound_line(line: &str) -> serde_json::Result<Option<InboundMessage>> {
    if line.trim().is_empty() {
        return Ok(None);
    }
    let envelope = serde_json::from_str::<InboundEnvelope>(line)?;
    let params = envelope.params.unwrap_or(Value::Null);
    let jsonrpc_valid = envelope
        .jsonrpc
        .as_deref()
        .is_none_or(|value| value == "2.0");

    match (envelope.id, envelope.method) {
        (Some(id), Some(method)) if jsonrpc_valid => {
            Ok(Some(InboundMessage::Request { id, method, params }))
        }
        (Some(id), Some(_)) => Ok(Some(InboundMessage::InvalidRequest {
            id,
            error: RpcError::invalid_params("jsonrpc must be 2.0"),
        })),
        (Some(id), None) => Ok(Some(InboundMessage::InvalidRequest {
            id,
            error: RpcError::invalid_params("request method missing"),
        })),
        (None, Some(method)) if jsonrpc_valid => {
            Ok(Some(InboundMessage::Notification { method, params }))
        }
        (None, _) => Ok(None),
    }
}

pub fn response_result(id: Value, result: Value) -> Value {
    serde_json::to_value(JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: Some(result),
        error: None,
    })
    .expect("JSON-RPC response serializes")
}

pub fn response_error(id: Value, error: RpcError) -> Value {
    serde_json::to_value(JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: None,
        error: Some(error),
    })
    .expect("JSON-RPC error serializes")
}

pub fn notification(method: impl Into<String>, params: Value) -> Value {
    serde_json::to_value(JsonRpcNotification {
        jsonrpc: "2.0",
        method: method.into(),
        params,
    })
    .expect("JSON-RPC notification serializes")
}

pub fn protocol_line(value: &Value) -> serde_json::Result<String> {
    let mut line = serde_json::to_string(value)?;
    line.push('\n');
    Ok(line)
}

pub async fn send_json(
    output_tx: &OutboundSender,
    value: Value,
) -> std::result::Result<(), mpsc::error::SendError<OutboundMessage>> {
    output_tx.send(OutboundMessage::Json(value)).await
}

pub async fn send_error(
    output_tx: &OutboundSender,
    id: Value,
    error: RpcError,
) -> std::result::Result<(), mpsc::error::SendError<OutboundMessage>> {
    send_json(output_tx, response_error(id, error)).await
}

pub async fn send_notification(
    output_tx: &OutboundSender,
    method: impl Into<String>,
    params: Value,
) -> std::result::Result<(), mpsc::error::SendError<OutboundMessage>> {
    send_json(output_tx, notification(method, params)).await
}

async fn flush_output(output_tx: &OutboundSender) {
    let (tx, rx) = oneshot::channel();
    if output_tx.send(OutboundMessage::Flush(tx)).await.is_ok() {
        let _ = rx.await;
    }
}

async fn stdout_writer(mut output_rx: mpsc::Receiver<OutboundMessage>) {
    let mut stdout = tokio::io::stdout();
    while let Some(message) = output_rx.recv().await {
        match message {
            OutboundMessage::Json(value) => match protocol_line(&value) {
                Ok(line) => {
                    if let Err(error) = stdout.write_all(line.as_bytes()).await {
                        eprintln!("[ledger-remux] failed to write protocol line: {error}");
                        break;
                    }
                    if let Err(error) = stdout.flush().await {
                        eprintln!("[ledger-remux] failed to flush protocol line: {error}");
                        break;
                    }
                }
                Err(error) => {
                    eprintln!("[ledger-remux] failed to serialize protocol line: {error}");
                }
            },
            OutboundMessage::Flush(done) => {
                let _ = stdout.flush().await;
                let _ = done.send(());
            }
        }
    }
}

async fn wait_sigterm() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                eprintln!("[ledger-remux] failed to install SIGTERM handler: {error}");
                std::future::pending::<()>().await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_requests() {
        let message = parse_inbound_line(
            r#"{"jsonrpc":"2.0","id":1,"method":"remux/ledger/ping","params":{"x":1}}"#,
        )
        .unwrap();

        assert_eq!(
            message,
            Some(InboundMessage::Request {
                id: json!(1),
                method: "remux/ledger/ping".to_string(),
                params: json!({"x":1}),
            })
        );
    }

    #[test]
    fn parses_notifications() {
        let message =
            parse_inbound_line(r#"{"jsonrpc":"2.0","method":"remux/viewer/ready"}"#).unwrap();

        assert_eq!(
            message,
            Some(InboundMessage::Notification {
                method: "remux/viewer/ready".to_string(),
                params: Value::Null,
            })
        );
    }

    #[test]
    fn rejects_invalid_lines() {
        assert!(parse_inbound_line("not json").is_err());
    }

    #[test]
    fn serializes_responses_as_single_protocol_lines() {
        let response = response_result(json!("abc"), json!({"ok":true}));
        let line = protocol_line(&response).unwrap();

        assert_eq!(
            line,
            r#"{"id":"abc","jsonrpc":"2.0","result":{"ok":true}}"#.to_string() + "\n"
        );
    }

    #[test]
    fn serializes_errors_as_single_protocol_lines() {
        let response = response_error(json!(7), RpcError::method_not_found("missing"));
        let line = protocol_line(&response).unwrap();

        assert!(line.ends_with('\n'));
        assert!(line.contains(r#""code":-32601"#));
        assert!(line.contains(r#""message":"method not found: missing""#));
    }
}
