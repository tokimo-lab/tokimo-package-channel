//! DingTalk Stream Mode WebSocket pump.
//!
//! Flow:
//! 1. `POST https://api.dingtalk.com/v1.0/gateway/connections/open` with
//!    `{clientId, clientSecret, subscriptions, ua, localIp}`. The response
//!    carries `{endpoint, ticket}`.
//! 2. Dial `{endpoint}?ticket={ticket}`.
//! 3. Receive JSON frames of shape
//!    `{ specVersion, type, headers{topic,messageId,…}, data: "<json-string>" }`.
//! 4. Ack each data frame within a few seconds by sending a JSON response
//!    `{ code: 200, headers: { messageId, contentType }, message: "OK", data: "{}" }`.
//! 5. Reconnect on disconnect — each connection requires a fresh ticket.
//!
//! Reference: <https://open.dingtalk.com/document/orgapp/stream-mode-data-encryption-implementation>

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
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::error::ChannelError;
use crate::inbound::{InboundEmitter, InboundEvent, InboundEventKind};

const CONNECTIONS_OPEN_URL: &str = "https://api.dingtalk.com/v1.0/gateway/connections/open";
const BOT_MESSAGE_TOPIC: &str = "/v1.0/im/bot/messages/get";

#[derive(Debug, Deserialize)]
struct ConnectionsOpenResp {
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    ticket: Option<String>,
}

async fn open_connection(
    http: &reqwest::Client,
    client_id: &str,
    client_secret: &str,
) -> Result<(String, String), ChannelError> {
    let body = json!({
        "clientId": client_id,
        "clientSecret": client_secret,
        "subscriptions": [
            { "type": "CALLBACK", "topic": BOT_MESSAGE_TOPIC },
            { "type": "EVENT",    "topic": "*" }
        ],
        "ua": "tokimo/1.0",
        "localIp": "127.0.0.1"
    });
    let resp = http.post(CONNECTIONS_OPEN_URL).json(&body).send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    if !(200..300).contains(&status) {
        return Err(ChannelError::ChannelRejected { status, body: text });
    }
    let parsed: ConnectionsOpenResp = serde_json::from_str(&text)
        .map_err(|e| ChannelError::Other(format!("connections/open decode: {e}: {text}")))?;
    let endpoint = parsed
        .endpoint
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ChannelError::Other("connections/open missing endpoint".into()))?;
    let ticket = parsed
        .ticket
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ChannelError::Other("connections/open missing ticket".into()))?;
    Ok((endpoint, ticket))
}

pub(crate) async fn run(
    http: reqwest::Client,
    client_id: String,
    client_secret: String,
    channel_id: Uuid,
    emit: InboundEmitter,
    cancel: CancellationToken,
) {
    let mut backoff = Duration::from_secs(2);
    const MAX_BACKOFF: Duration = Duration::from_mins(1);

    loop {
        if cancel.is_cancelled() {
            debug!(%channel_id, "dingtalk ws pump cancelled");
            return;
        }

        let (endpoint, ticket) = match open_connection(&http, &client_id, &client_secret).await {
            Ok(v) => v,
            Err(e) => {
                warn!(%channel_id, error = %e, backoff_secs = backoff.as_secs(), "dingtalk connections/open failed");
                sleep_or_cancel(backoff, &cancel).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        let sep = if endpoint.contains('?') { '&' } else { '?' };
        let dial_url = format!("{endpoint}{sep}ticket={ticket}");
        info!(%channel_id, "dingtalk ws connecting");

        match connect_and_pump(&dial_url, channel_id, &emit, &cancel).await {
            Ok(()) => {
                backoff = Duration::from_secs(2);
                debug!(%channel_id, "dingtalk ws disconnected cleanly, reconnecting");
            }
            Err(e) => {
                warn!(%channel_id, error = %e, backoff_secs = backoff.as_secs(), "dingtalk ws error; backing off");
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
        .map_err(|e| ChannelError::Other(format!("dingtalk ws dial failed: {e}")))?;

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
                        let frame: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(%channel_id, error = %e, "dingtalk ws: non-JSON frame");
                                continue;
                            }
                        };
                        handle_frame(&frame, channel_id, emit, &writer).await;
                    }
                    WsMessage::Ping(payload) => {
                        let mut w = writer.lock().await;
                        let _ = w.send(WsMessage::Pong(payload)).await;
                    }
                    WsMessage::Close(_) => {
                        debug!(%channel_id, "dingtalk ws closed");
                        return Ok(());
                    }
                    _ => {}
                }
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_frame(frame: &Value, channel_id: Uuid, emit: &InboundEmitter, writer: &WsSink) {
    let frame_type = frame.get("type").and_then(Value::as_str).unwrap_or("");
    let headers = frame.get("headers").cloned().unwrap_or(Value::Null);
    let topic = headers.get("topic").and_then(Value::as_str).unwrap_or("").to_string();
    let message_id = headers
        .get("messageId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    // Always ack data-carrying frames.
    let should_ack = matches!(frame_type, "CALLBACK" | "EVENT");
    if should_ack && !message_id.is_empty() {
        let ack = json!({
            "code": 200,
            "headers": {
                "contentType": "application/json",
                "messageId": message_id,
            },
            "message": "OK",
            "data": "{}"
        });
        let mut w = writer.lock().await;
        if let Err(e) = w.send(WsMessage::Text(ack.to_string().into())).await {
            warn!(%channel_id, error = %e, "dingtalk ws: ack send failed");
        }
    }

    if frame_type == "SYSTEM" {
        debug!(%channel_id, ?topic, "dingtalk ws: system frame");
        return;
    }

    // `data` is a JSON-encoded string for CALLBACK frames.
    if topic != BOT_MESSAGE_TOPIC {
        debug!(%channel_id, %topic, "dingtalk ws: ignoring topic");
        return;
    }
    let Some(data_str) = frame.get("data").and_then(Value::as_str) else {
        return;
    };
    let payload: Value = match serde_json::from_str(data_str) {
        Ok(v) => v,
        Err(e) => {
            warn!(%channel_id, error = %e, "dingtalk ws: data JSON decode failed");
            return;
        }
    };

    let msgtype = payload.get("msgtype").and_then(Value::as_str).unwrap_or("");
    if msgtype != "text" {
        return;
    }
    let content_raw = payload
        .get("text")
        .and_then(|t| t.get("content"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let content = strip_at_mention(&content_raw);
    if content.is_empty() {
        return;
    }

    let user_id = payload
        .get("senderStaffId")
        .and_then(Value::as_str)
        .or_else(|| payload.get("senderId").and_then(Value::as_str))
        .map(str::to_string);
    let conversation_id = payload
        .get("conversationId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let session_webhook = payload
        .get("sessionWebhook")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    let thread_id = if session_webhook.is_empty() {
        conversation_id.clone()
    } else {
        format!("{conversation_id}|{session_webhook}")
    };

    let kind = if let Some(stripped) = content.strip_prefix('/') {
        let (name, args) = stripped
            .split_once(char::is_whitespace)
            .map_or((stripped, ""), |(a, b)| (a, b));
        InboundEventKind::Command {
            name: name.to_string(),
            args: args.to_string(),
        }
    } else {
        InboundEventKind::Message {
            text: content,
            attachments: Vec::new(),
        }
    };

    emit.send(InboundEvent {
        channel_id,
        channel_type: "dingtalk".into(),
        external_thread_id: thread_id,
        external_user_id: user_id,
        kind,
        received_at: Utc::now(),
        raw: json!({
            "conversationId": conversation_id,
            "sessionWebhook": session_webhook,
            "msgId": payload.get("msgId").cloned().unwrap_or(Value::Null),
        }),
    });
}

fn strip_at_mention(text: &str) -> String {
    let trimmed = text.trim_start();
    if let Some(rest) = trimmed.strip_prefix('@')
        && let Some(idx) = rest.find(char::is_whitespace)
    {
        return rest[idx..].trim_start().to_string();
    }
    text.to_string()
}
