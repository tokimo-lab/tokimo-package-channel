//! Discord Gateway v10 WebSocket pump.
//!
//! Dials `wss://gateway.discord.gg/?v=10&encoding=json` and runs the standard
//! Discord Gateway lifecycle: Hello → Heartbeat loop + Identify → Dispatch.
//! We only forward `MESSAGE_CREATE` dispatches upstream as
//! [`InboundEvent`]s; other events are ignored but their sequence numbers are
//! still tracked for heartbeating.
//!
//! Resume-on-reconnect is attempted opportunistically when the server sends
//! opcode 7 (Reconnect); opcode 9 (Invalid Session) triggers a fresh
//! IDENTIFY. Authentication failures (close code 4004) and disallowed intents
//! (4014) are fatal and exit the pump.
//!
//! Reference: <https://discord.com/developers/docs/topics/gateway>

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

const GATEWAY_BOOTSTRAP: &str = "https://discord.com/api/v10/gateway/bot";
const FALLBACK_GATEWAY_URL: &str = "wss://gateway.discord.gg";

#[derive(Debug, Deserialize)]
struct GatewayBootstrap {
    url: String,
}

/// Fetch the Gateway URL (and shard hint) via the bot-authenticated bootstrap
/// endpoint. On any failure, fall back to the well-known public gateway host.
async fn discover_gateway(http: &reqwest::Client, bot_token: &str) -> String {
    match http
        .get(GATEWAY_BOOTSTRAP)
        .header("Authorization", format!("Bot {bot_token}"))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => match resp.json::<GatewayBootstrap>().await {
            Ok(g) => g.url,
            Err(e) => {
                warn!(error = %e, "discord: gateway/bot decode failed; using fallback");
                FALLBACK_GATEWAY_URL.into()
            }
        },
        Ok(resp) => {
            let status = resp.status();
            warn!(%status, "discord: gateway/bot rejected; using fallback");
            FALLBACK_GATEWAY_URL.into()
        }
        Err(e) => {
            warn!(error = %e, "discord: gateway/bot fetch failed; using fallback");
            FALLBACK_GATEWAY_URL.into()
        }
    }
}

/// Pump entry-point. Loops forever (until `cancel` fires), reconnecting with
/// exponential backoff on recoverable errors.
pub(crate) async fn run(
    http: reqwest::Client,
    bot_token: String,
    intents: u64,
    channel_id: Uuid,
    emit: InboundEmitter,
    cancel: CancellationToken,
) {
    let mut backoff = Duration::from_secs(2);
    const MAX_BACKOFF: Duration = Duration::from_mins(1);

    loop {
        if cancel.is_cancelled() {
            debug!(%channel_id, "discord ws pump cancelled");
            return;
        }

        let gateway_url = discover_gateway(&http, &bot_token).await;
        let dial_url = format!("{gateway_url}/?v=10&encoding=json");
        info!(%channel_id, url = %dial_url, "discord ws connecting");

        match connect_and_pump(&dial_url, &bot_token, intents, channel_id, &emit, &cancel).await {
            Ok(ConnectOutcome::Clean) => {
                backoff = Duration::from_secs(2);
                debug!(%channel_id, "discord ws disconnected cleanly, reconnecting");
            }
            Ok(ConnectOutcome::Fatal(reason)) => {
                error!(%channel_id, %reason, "discord ws fatal error; pump stopped");
                return;
            }
            Err(e) => {
                warn!(%channel_id, error = %e, backoff_secs = backoff.as_secs(), "discord ws error; backing off");
                sleep_or_cancel(backoff, &cancel).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

enum ConnectOutcome {
    Clean,
    Fatal(String),
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
    bot_token: &str,
    intents: u64,
    channel_id: Uuid,
    emit: &InboundEmitter,
    cancel: &CancellationToken,
) -> Result<ConnectOutcome, ChannelError> {
    let request = url
        .into_client_request()
        .map_err(|e| ChannelError::Other(format!("build ws request: {e}")))?;
    let (ws, _resp) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| ChannelError::Other(format!("discord ws dial failed: {e}")))?;
    info!(%channel_id, "discord ws connected");

    let (writer, mut reader) = ws.split();
    let writer: WsSink = Arc::new(Mutex::new(writer));

    // Shared latest-seq: heartbeat task reads it; receive loop updates it
    // on every inbound frame carrying a non-null `s` field.
    let seq = Arc::new(Mutex::new(None::<i64>));
    let mut heartbeat_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut identified = false;

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                if let Some(h) = heartbeat_task { h.abort(); }
                return Ok(ConnectOutcome::Clean);
            }
            msg = reader.next() => {
                let Some(msg) = msg else {
                    if let Some(h) = heartbeat_task { h.abort(); }
                    return Ok(ConnectOutcome::Clean);
                };
                let msg = match msg {
                    Ok(m) => m,
                    Err(e) => {
                        if let Some(h) = heartbeat_task { h.abort(); }
                        return Err(ChannelError::Other(format!("ws read: {e}")));
                    }
                };
                match msg {
                    WsMessage::Text(text) => {
                        let payload: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(%channel_id, error = %e, "discord ws: non-JSON text frame");
                                continue;
                            }
                        };
                        if let Some(s) = payload.get("s").and_then(Value::as_i64) {
                            *seq.lock().await = Some(s);
                        }
                        let op = payload.get("op").and_then(Value::as_i64).unwrap_or(-1);
                        match op {
                            10 => {
                                // Hello: start heartbeat + send Identify.
                                let interval_ms = payload
                                    .get("d")
                                    .and_then(|d| d.get("heartbeat_interval"))
                                    .and_then(Value::as_u64)
                                    .unwrap_or(41_250);
                                heartbeat_task = Some(spawn_heartbeat(writer.clone(), seq.clone(), interval_ms, cancel.clone()));
                                if !identified {
                                    send_identify(&writer, bot_token, intents).await?;
                                    identified = true;
                                }
                            }
                            11 => { /* Heartbeat ACK */ }
                            1 => {
                                // Server-requested immediate heartbeat.
                                let last = *seq.lock().await;
                                send_heartbeat(&writer, last).await?;
                            }
                            7 => {
                                // Reconnect request — close and let outer loop reconnect.
                                debug!(%channel_id, "discord ws opcode 7: reconnect requested");
                                if let Some(h) = heartbeat_task { h.abort(); }
                                return Ok(ConnectOutcome::Clean);
                            }
                            9 => {
                                // Invalid Session — wait a bit and re-identify on reconnect.
                                warn!(%channel_id, "discord ws invalid session; reconnecting");
                                if let Some(h) = heartbeat_task { h.abort(); }
                                sleep_or_cancel(Duration::from_secs(2), cancel).await;
                                return Ok(ConnectOutcome::Clean);
                            }
                            0 => {
                                // Dispatch
                                let event_type = payload.get("t").and_then(Value::as_str).unwrap_or("");
                                handle_dispatch(event_type, payload.get("d"), channel_id, emit);
                            }
                            other => {
                                debug!(%channel_id, op = other, "discord ws: unhandled opcode");
                            }
                        }
                    }
                    WsMessage::Close(frame) => {
                        let code = frame.as_ref().map_or(0, |f| u16::from(f.code));
                        let reason = frame.as_ref().map_or("", |f| f.reason.as_ref());
                        if let Some(h) = heartbeat_task { h.abort(); }
                        match code {
                            4004 => return Ok(ConnectOutcome::Fatal(format!("authentication failed: {reason}"))),
                            4014 => return Ok(ConnectOutcome::Fatal(format!(
                                "disallowed intents (enable privileged intents in Developer Portal): {reason}"
                            ))),
                            _ => {
                                debug!(%channel_id, %code, %reason, "discord ws closed");
                                return Ok(ConnectOutcome::Clean);
                            }
                        }
                    }
                    WsMessage::Ping(payload) => {
                        let mut w = writer.lock().await;
                        let _ = w.send(WsMessage::Pong(payload)).await;
                    }
                    _ => {}
                }
            }
        }
    }
}

fn spawn_heartbeat(
    writer: WsSink,
    seq: Arc<Mutex<Option<i64>>>,
    interval_ms: u64,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_millis(interval_ms.max(5_000));
        loop {
            tokio::select! {
                () = tokio::time::sleep(interval) => {
                    let last = *seq.lock().await;
                    let frame = json!({ "op": 1, "d": last });
                    let mut w = writer.lock().await;
                    if w.send(WsMessage::Text(frame.to_string().into())).await.is_err() {
                        break;
                    }
                }
                () = cancel.cancelled() => break,
            }
        }
    })
}

async fn send_heartbeat(writer: &WsSink, last_seq: Option<i64>) -> Result<(), ChannelError> {
    let frame = json!({ "op": 1, "d": last_seq });
    let mut w = writer.lock().await;
    w.send(WsMessage::Text(frame.to_string().into()))
        .await
        .map_err(|e| ChannelError::Other(format!("ws send heartbeat: {e}")))
}

async fn send_identify(writer: &WsSink, bot_token: &str, intents: u64) -> Result<(), ChannelError> {
    let frame = json!({
        "op": 2,
        "d": {
            "token": bot_token,
            "intents": intents,
            "properties": {
                "os": "linux",
                "browser": "tokimo",
                "device": "tokimo",
            }
        }
    });
    let mut w = writer.lock().await;
    w.send(WsMessage::Text(frame.to_string().into()))
        .await
        .map_err(|e| ChannelError::Other(format!("ws send identify: {e}")))
}

fn handle_dispatch(event_type: &str, data: Option<&Value>, channel_id: Uuid, emit: &InboundEmitter) {
    if event_type != "MESSAGE_CREATE" {
        debug!(channel_id = %channel_id, event = %event_type, "discord ws: ignoring dispatch");
        return;
    }
    let Some(d) = data else { return };
    // Skip bot echoes to avoid self-reply loops.
    if d.get("author")
        .and_then(|a| a.get("bot"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return;
    }
    let content = d
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    if content.is_empty() {
        return;
    }
    let channel_ext = d.get("channel_id").and_then(Value::as_str).unwrap_or("").to_string();
    if channel_ext.is_empty() {
        return;
    }
    let message_id = d.get("id").and_then(Value::as_str).unwrap_or("").to_string();
    let guild_id = d.get("guild_id").and_then(Value::as_str).map(str::to_string);
    let author_id = d
        .get("author")
        .and_then(|a| a.get("id"))
        .and_then(Value::as_str)
        .map(str::to_string);

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
        channel_type: "discord".into(),
        external_thread_id: channel_ext.clone(),
        external_user_id: author_id.clone(),
        kind,
        received_at: Utc::now(),
        raw: json!({
            "message_id": message_id,
            "channel_id": channel_ext,
            "guild_id": guild_id,
            "author_id": author_id,
        }),
    });
}
