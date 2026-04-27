//! Slack Socket Mode WebSocket pump.
//!
//! Fetches a one-shot WSS URL via `POST apps.connections.open` (authorised
//! with the `xapp-…` app-level token), dials it, then processes incoming
//! envelopes. Every `events_api` / `slash_commands` / `interactive` envelope
//! must be acked by echoing its `envelope_id` back on the WebSocket within
//! 3 seconds — otherwise Slack retries.
//!
//! Reference: <https://api.slack.com/apis/socket-mode>

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::ChannelError;
use crate::inbound::{InboundEmitter, InboundEvent, InboundEventKind};

const CONNECTIONS_OPEN_URL: &str = "https://slack.com/api/apps.connections.open";

#[derive(Debug, Deserialize)]
struct ConnectionsOpenResp {
    ok: bool,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

async fn fetch_socket_url(http: &reqwest::Client, app_token: &str) -> Result<String, ChannelError> {
    let resp = http
        .post(CONNECTIONS_OPEN_URL)
        .bearer_auth(app_token)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("")
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if !(200..300).contains(&status) {
        return Err(ChannelError::ChannelRejected { status, body });
    }
    let parsed: ConnectionsOpenResp = serde_json::from_str(&body)
        .map_err(|e| ChannelError::Other(format!("apps.connections.open decode: {e}: {body}")))?;
    if !parsed.ok {
        return Err(ChannelError::ChannelRejected {
            status,
            body: format!("apps.connections.open error: {:?}", parsed.error),
        });
    }
    parsed
        .url
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ChannelError::Other("apps.connections.open missing url".into()))
}

pub(crate) async fn run(
    http: reqwest::Client,
    app_token: String,
    channel_id: Uuid,
    emit: InboundEmitter,
    cancel: CancellationToken,
) {
    let mut backoff = Duration::from_secs(2);
    const MAX_BACKOFF: Duration = Duration::from_mins(1);

    loop {
        if cancel.is_cancelled() {
            debug!(%channel_id, "slack ws pump cancelled");
            return;
        }

        let url = match fetch_socket_url(&http, &app_token).await {
            Ok(u) => u,
            Err(e) => {
                // invalid_auth / not_authed etc. are configuration errors — log
                // and back off, but don't abandon the pump because operators may
                // fix the token at runtime.
                warn!(%channel_id, error = %e, backoff_secs = backoff.as_secs(), "slack apps.connections.open failed");
                sleep_or_cancel(backoff, &cancel).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        info!(%channel_id, "slack ws connecting");
        match connect_and_pump(&url, channel_id, &emit, &cancel).await {
            Ok(()) => {
                backoff = Duration::from_secs(2);
                debug!(%channel_id, "slack ws disconnected cleanly, reconnecting");
            }
            Err(e) => {
                warn!(%channel_id, error = %e, backoff_secs = backoff.as_secs(), "slack ws error; backing off");
                sleep_or_cancel(backoff, &cancel).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

async fn sleep_or_cancel(dur: Duration, cancel: &CancellationToken) {
    tokio::select! {
        () = tokio::time::sleep(dur) => {}
        () = cancel.cancelled() => {}
    }
}

type WsSink = Arc<
    Mutex<
        futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
            WsMessage,
        >,
    >,
>;

async fn connect_and_pump(
    url: &str,
    channel_id: Uuid,
    emit: &InboundEmitter,
    cancel: &CancellationToken,
) -> Result<(), ChannelError> {
    let request = url
        .into_client_request()
        .map_err(|e| ChannelError::Other(format!("build ws request: {e}")))?;
    let (ws, _resp) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| ChannelError::Other(format!("slack ws dial failed: {e}")))?;

    let (writer, mut reader) = ws.split();
    let writer: WsSink = Arc::new(Mutex::new(writer));

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            msg = reader.next() => {
                let Some(msg) = msg else { return Ok(()); };
                let msg = match msg {
                    Ok(m) => m,
                    Err(e) => return Err(ChannelError::Other(format!("ws read: {e}"))),
                };
                match msg {
                    WsMessage::Text(text) => {
                        let payload: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(%channel_id, error = %e, "slack ws: non-JSON frame");
                                continue;
                            }
                        };
                        handle_envelope(&payload, channel_id, emit, &writer).await;
                    }
                    WsMessage::Ping(payload) => {
                        let mut w = writer.lock().await;
                        let _ = w.send(WsMessage::Pong(payload)).await;
                    }
                    WsMessage::Close(_) => {
                        debug!(%channel_id, "slack ws closed");
                        return Ok(());
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn handle_envelope(payload: &Value, channel_id: Uuid, emit: &InboundEmitter, writer: &WsSink) {
    let kind = payload.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "hello" => {
            info!(%channel_id, "slack ws hello");
        }
        "disconnect" => {
            let reason = payload.get("reason").and_then(Value::as_str).unwrap_or("");
            debug!(%channel_id, %reason, "slack ws disconnect requested");
            // Close from our side; outer loop will re-fetch URL and reconnect.
            let mut w = writer.lock().await;
            let _ = w.send(WsMessage::Close(None)).await;
        }
        "events_api" => {
            ack(payload, writer).await;
            emit_events_api(payload, channel_id, emit);
        }
        "slash_commands" | "interactive" => {
            // Ack with empty body — upstream handler can post follow-ups via
            // chat.postMessage using the standard reply path.
            ack(payload, writer).await;
            // We currently don't forward these as InboundEvents (the channel
            // router is text-message oriented). Extend here when needed.
        }
        other => {
            debug!(%channel_id, %other, "slack ws: unhandled envelope type");
        }
    }
}

async fn ack(envelope: &Value, writer: &WsSink) {
    let Some(envelope_id) = envelope.get("envelope_id").and_then(Value::as_str) else {
        return;
    };
    let frame = json!({ "envelope_id": envelope_id });
    let mut w = writer.lock().await;
    if let Err(e) = w.send(WsMessage::Text(frame.to_string().into())).await {
        error!(error = %e, "slack ws: ack send failed");
    }
}

fn emit_events_api(envelope: &Value, channel_id: Uuid, emit: &InboundEmitter) {
    let Some(inner) = envelope.get("payload") else { return };
    if inner.get("type").and_then(Value::as_str) != Some("event_callback") {
        return;
    }
    let Some(event) = inner.get("event") else { return };

    // Skip bot echoes.
    if event.get("bot_id").is_some() {
        return;
    }
    let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
    if event_type != "app_mention" && event_type != "message" {
        return;
    }
    if event_type == "message" && event.get("subtype").is_some() {
        return;
    }

    let text_raw = event.get("text").and_then(Value::as_str).unwrap_or("");
    let text = strip_leading_mention(text_raw);
    let channel_ext = event.get("channel").and_then(Value::as_str).unwrap_or("").to_string();
    let user = event.get("user").and_then(Value::as_str).map(str::to_string);
    let ts = event.get("ts").and_then(Value::as_str).unwrap_or("").to_string();

    if text.is_empty() || channel_ext.is_empty() {
        return;
    }

    let kind = if let Some(stripped) = text.strip_prefix('/') {
        let (name, args) = stripped
            .split_once(char::is_whitespace)
            .map_or((stripped, ""), |(a, b)| (a, b));
        InboundEventKind::Command {
            name: name.to_string(),
            args: args.to_string(),
        }
    } else {
        InboundEventKind::Message {
            text: text.clone(),
            attachments: Vec::new(),
        }
    };

    emit.send(InboundEvent {
        channel_id,
        channel_type: "slack".into(),
        external_thread_id: channel_ext,
        external_user_id: user,
        kind,
        received_at: Utc::now(),
        raw: json!({ "ts": ts, "event_type": event_type }),
    });
}

/// Strip a leading `<@Uxxxx>` mention (possibly followed by whitespace) that
/// Slack prefixes to app-mention events.
fn strip_leading_mention(text: &str) -> String {
    let trimmed = text.trim_start();
    if let Some(rest) = trimmed.strip_prefix('<')
        && rest.as_bytes().first() == Some(&b'@')
        && let Some(end) = rest.find('>')
    {
        return rest[end + 1..].trim_start().to_string();
    }
    text.trim().to_string()
}
