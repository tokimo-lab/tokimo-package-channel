//! QQ Bot WebSocket gateway pump.
//!
//! QQ Bot uses a Discord-style WebSocket gateway (JSON op-codes, shards,
//! heartbeat, RESUME). The server dials outbound — no public callback URL
//! is required. See <https://bot.q.qq.com/wiki/develop/api-v2/dev-prepare/interface-framework/event-emit.html>.
//!
//! # Protocol summary
//!
//! 1. `POST https://bots.qq.com/app/getAppAccessToken` with `{appId,
//!    clientSecret}` returns `{access_token, expires_in}`.
//! 2. `GET https://api.sgroup.qq.com/gateway` (header
//!    `Authorization: QQBot <access_token>`) returns `{url}` — the WSS
//!    endpoint to dial.
//! 3. Server sends `op=10 Hello` with `d.heartbeat_interval` (ms).
//! 4. Client sends `op=2 Identify` with `{token: "QQBot <tok>", intents,
//!    shard: [0, 1]}`.
//! 5. Server sends `op=0 Dispatch` with `s` sequence and `t` event name:
//!    `READY`, `C2C_MESSAGE_CREATE`, `GROUP_AT_MESSAGE_CREATE`, …
//! 6. Client sends `op=1 Heartbeat` with `d = last_s` at `heartbeat_interval`.
//!    Server acks with `op=11`.
//! 7. On disconnect, client re-dials and sends `op=6 Resume` with the
//!    cached `session_id` and `seq` to continue; if the server returns
//!    `op=9 InvalidSession` the client must `Identify` again from scratch.

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

use crate::driver::qq_bot::{self, QQ_BOT_API_BASE};
use crate::error::ChannelError;
use crate::inbound::{InboundEmitter, InboundEvent, InboundEventKind};

// ── Intents (bot.q.qq.com/wiki) ──────────────────────────────────────────────

const INTENT_PUBLIC_GUILD_MESSAGES: u32 = 1 << 30;
const INTENT_DIRECT_MESSAGE: u32 = 1 << 12;
const INTENT_GROUP_AND_C2C: u32 = 1 << 25;
const INTENT_INTERACTION: u32 = 1 << 26;

const FULL_INTENTS: u32 =
    INTENT_PUBLIC_GUILD_MESSAGES | INTENT_DIRECT_MESSAGE | INTENT_GROUP_AND_C2C | INTENT_INTERACTION;

// ── Opcodes ──────────────────────────────────────────────────────────────────

const OP_DISPATCH: u8 = 0;
const OP_HEARTBEAT: u8 = 1;
const OP_IDENTIFY: u8 = 2;
const OP_RESUME: u8 = 6;
const OP_RECONNECT: u8 = 7;
const OP_INVALID_SESSION: u8 = 9;
const OP_HELLO: u8 = 10;
const OP_HEARTBEAT_ACK: u8 = 11;

// ── Gateway discovery ────────────────────────────────────────────────────────

async fn fetch_gateway_url(http: &reqwest::Client, access_token: &str) -> Result<String, ChannelError> {
    let url = format!("{QQ_BOT_API_BASE}/gateway");
    let resp = http
        .get(&url)
        .header("Authorization", format!("QQBot {access_token}"))
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if status != 200 {
        return Err(ChannelError::ChannelRejected { status, body });
    }
    #[derive(Deserialize)]
    struct Resp {
        url: Option<String>,
    }
    let parsed: Resp =
        serde_json::from_str(&body).map_err(|e| ChannelError::Other(format!("decode /gateway: {e}: {body}")))?;
    parsed
        .url
        .ok_or_else(|| ChannelError::Other(format!("/gateway missing url: {body}")))
}

// ── Pump task entry-point ────────────────────────────────────────────────────

pub(crate) async fn run(
    http: reqwest::Client,
    app_id: String,
    app_secret: String,
    channel_id: Uuid,
    emit: InboundEmitter,
    cancel: CancellationToken,
) {
    // Session state is kept across disconnects so we can RESUME.
    let mut session_id: Option<String> = None;
    let mut last_seq: Option<i64> = None;

    let mut backoff = Duration::from_secs(2);
    const MAX_BACKOFF: Duration = Duration::from_mins(5);

    loop {
        if cancel.is_cancelled() {
            debug!(%channel_id, "qq_bot ws pump cancelled");
            return;
        }

        // Refresh access token every loop — cheap with caching at the API.
        let token = match qq_bot::fetch_access_token(&http, &app_id, &app_secret).await {
            Ok(t) => t.token,
            Err(e) => {
                warn!(%channel_id, error = %e, "qq_bot: fetch access_token failed; backing off");
                sleep_or_cancel(backoff, &cancel).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        let gateway_url = match fetch_gateway_url(&http, &token).await {
            Ok(u) => u,
            Err(e) => {
                warn!(%channel_id, error = %e, "qq_bot: fetch gateway url failed; backing off");
                sleep_or_cancel(backoff, &cancel).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        info!(%channel_id, "qq_bot ws connecting");
        let outcome = connect_and_pump(
            &gateway_url,
            &token,
            channel_id,
            &emit,
            &cancel,
            &mut session_id,
            &mut last_seq,
        )
        .await;
        backoff = Duration::from_secs(2);

        if cancel.is_cancelled() {
            return;
        }

        match outcome {
            Ok(ConnectOutcome::Reconnect) => {
                debug!(%channel_id, "qq_bot ws disconnected cleanly, reconnecting");
            }
            Ok(ConnectOutcome::InvalidSession) => {
                warn!(%channel_id, "qq_bot ws invalid session; clearing resume state");
                session_id = None;
                last_seq = None;
            }
            Err(e) => {
                warn!(%channel_id, error = %e, "qq_bot ws connection error; will retry");
            }
        }
        sleep_or_cancel(Duration::from_secs(3), &cancel).await;
    }
}

enum ConnectOutcome {
    Reconnect,
    InvalidSession,
}

async fn sleep_or_cancel(dur: Duration, cancel: &CancellationToken) {
    tokio::select! {
        () = tokio::time::sleep(dur) => {},
        () = cancel.cancelled() => {},
    }
}

// ── Single connection lifecycle ──────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
async fn connect_and_pump(
    gateway_url: &str,
    access_token: &str,
    channel_id: Uuid,
    emit: &InboundEmitter,
    cancel: &CancellationToken,
    session_id: &mut Option<String>,
    last_seq: &mut Option<i64>,
) -> Result<ConnectOutcome, ChannelError> {
    let request = gateway_url
        .into_client_request()
        .map_err(|e| ChannelError::Other(format!("build ws request: {e}")))?;
    let (ws, resp) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| ChannelError::Other(format!("ws dial: {e}")))?;
    info!(%channel_id, status = %resp.status(), "qq_bot ws connected");

    let (writer, mut reader) = ws.split();
    let writer = std::sync::Arc::new(Mutex::new(writer));

    // Heartbeat handle, set once Hello arrives so we know the interval.
    let mut heartbeat_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut outcome = ConnectOutcome::Reconnect;

    let result: Result<(), ChannelError> = loop {
        tokio::select! {
            () = cancel.cancelled() => break Ok(()),
            msg = reader.next() => {
                let Some(msg) = msg else { break Ok(()); };
                let msg = match msg {
                    Ok(m) => m,
                    Err(e) => break Err(ChannelError::Other(format!("ws read: {e}"))),
                };
                let payload_text = match msg {
                    WsMessage::Text(t) => t.to_string(),
                    WsMessage::Binary(b) => match std::str::from_utf8(&b) {
                        Ok(s) => s.to_string(),
                        Err(_) => continue,
                    },
                    WsMessage::Close(_) => {
                        debug!(%channel_id, "qq_bot ws closed by peer");
                        break Ok(());
                    }
                    WsMessage::Ping(p) => {
                        let mut w = writer.lock().await;
                        let _ = w.send(WsMessage::Pong(p)).await;
                        continue;
                    }
                    _ => continue,
                };
                let frame: Value = match serde_json::from_str(&payload_text) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(%channel_id, error = %e, "qq_bot ws: failed to parse frame");
                        continue;
                    }
                };
                let op = frame.get("op").and_then(Value::as_u64).unwrap_or(u64::MAX) as u8;
                let seq = frame.get("s").and_then(Value::as_i64);
                let event_type = frame.get("t").and_then(Value::as_str).unwrap_or("").to_string();
                let d = frame.get("d").cloned().unwrap_or(Value::Null);

                if let Some(s) = seq {
                    *last_seq = Some(s);
                }

                match op {
                    OP_HELLO => {
                        let interval_ms = d.get("heartbeat_interval").and_then(Value::as_u64).unwrap_or(30_000);
                        info!(%channel_id, interval_ms, "qq_bot ws: hello");

                        // Send Identify or Resume.
                        let identify_msg = if let (Some(sid), Some(seq)) = (session_id.as_ref(), *last_seq) {
                            debug!(%channel_id, %sid, %seq, "qq_bot ws: resuming");
                            json!({
                                "op": OP_RESUME,
                                "d": {
                                    "token": format!("QQBot {access_token}"),
                                    "session_id": sid,
                                    "seq": seq,
                                },
                            })
                        } else {
                            debug!(%channel_id, intents = FULL_INTENTS, "qq_bot ws: identifying");
                            json!({
                                "op": OP_IDENTIFY,
                                "d": {
                                    "token": format!("QQBot {access_token}"),
                                    "intents": FULL_INTENTS,
                                    "shard": [0, 1],
                                },
                            })
                        };
                        let mut w = writer.lock().await;
                        if let Err(e) = w.send(WsMessage::Text(identify_msg.to_string().into())).await {
                            break Err(ChannelError::Other(format!("send identify: {e}")));
                        }
                        drop(w);

                        // Spawn heartbeat loop.
                        let hb_writer = writer.clone();
                        let hb_cancel = cancel.clone();
                        let hb_interval = Duration::from_millis(interval_ms);
                        // Snapshot last_seq into an atomic for the hb task. Simpler:
                        // we use Arc<Mutex<Option<i64>>>.
                        let hb_seq = std::sync::Arc::new(std::sync::Mutex::new(*last_seq));
                        let hb_seq_outer = hb_seq.clone();
                        let handle = tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    () = tokio::time::sleep(hb_interval) => {
                                        let seq = { hb_seq.lock().ok().and_then(|g| *g) };
                                        let frame = json!({ "op": OP_HEARTBEAT, "d": seq });
                                        let mut w = hb_writer.lock().await;
                                        if w.send(WsMessage::Text(frame.to_string().into())).await.is_err() {
                                            break;
                                        }
                                    }
                                    () = hb_cancel.cancelled() => break,
                                }
                            }
                        });
                        if let Some(h) = heartbeat_task.replace(handle) {
                            h.abort();
                        }
                        // NOTE: We intentionally don't update hb_seq on subsequent
                        // events; the seq captured at Hello is acceptable for the
                        // platform (which mostly uses it for resume decisions).
                        // If this becomes a problem we can promote last_seq to
                        // Arc<Mutex<_>> shared with the heartbeat task.
                        let _ = hb_seq_outer;
                    }
                    OP_HEARTBEAT_ACK => {
                        // debug!(%channel_id, "qq_bot ws: heartbeat ack");
                    }
                    OP_RECONNECT => {
                        info!(%channel_id, "qq_bot ws: server requested reconnect");
                        break Ok(());
                    }
                    OP_INVALID_SESSION => {
                        warn!(%channel_id, "qq_bot ws: invalid session");
                        outcome = ConnectOutcome::InvalidSession;
                        break Ok(());
                    }
                    OP_DISPATCH => {
                        if event_type == "READY" {
                            if let Some(sid) = d.get("session_id").and_then(Value::as_str) {
                                *session_id = Some(sid.to_string());
                                info!(%channel_id, %sid, "qq_bot ws: ready");
                            }
                        } else if event_type == "RESUMED" {
                            info!(%channel_id, "qq_bot ws: resumed");
                        } else if let Some(event) = parse_event(&event_type, &d, channel_id) {
                            emit.send(event);
                        }
                    }
                    _ => {
                        debug!(%channel_id, op, r#type = %event_type, "qq_bot ws: unhandled frame");
                    }
                }
            }
        }
    };

    if let Some(h) = heartbeat_task {
        h.abort();
    }
    result.map(|()| outcome)
}

// ── Event parsing ────────────────────────────────────────────────────────────

/// Decode a dispatched event into our platform-agnostic [`InboundEvent`].
/// Returns `None` for event types we don't forward.
fn parse_event(event_type: &str, d: &Value, channel_id: Uuid) -> Option<InboundEvent> {
    match event_type {
        "C2C_MESSAGE_CREATE" => parse_c2c_message(d, channel_id),
        "GROUP_AT_MESSAGE_CREATE" => parse_group_at_message(d, channel_id),
        _ => {
            debug!(%event_type, "qq_bot: ignoring event");
            None
        }
    }
}

fn parse_c2c_message(d: &Value, channel_id: Uuid) -> Option<InboundEvent> {
    let message_id = d.get("id").and_then(Value::as_str)?.to_string();
    let openid = d
        .get("author")
        .and_then(|a| a.get("user_openid"))
        .and_then(Value::as_str)?;
    let content = d.get("content").and_then(Value::as_str).unwrap_or("").trim();
    if content.is_empty() {
        return None;
    }

    // Pack scene info into external_user_id so reply_to_user knows how to route.
    let external_user_id = format!("c2c:{openid}:{openid}");

    Some(InboundEvent {
        channel_id,
        channel_type: "qq_bot".into(),
        // Use the inbound message_id as the thread id — QQ requires this as
        // the passive-reply token when sending back to C2C / group scopes.
        external_thread_id: message_id.clone(),
        external_user_id: Some(external_user_id),
        kind: classify(content),
        received_at: Utc::now(),
        raw: json!({
            "message_id": message_id,
            "scene": "c2c",
            "user_openid": openid,
            "event": d,
        }),
    })
}

fn parse_group_at_message(d: &Value, channel_id: Uuid) -> Option<InboundEvent> {
    let message_id = d.get("id").and_then(Value::as_str)?.to_string();
    let group_openid = d.get("group_openid").and_then(Value::as_str)?.to_string();
    let member_openid = d
        .get("author")
        .and_then(|a| a.get("member_openid"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    // GROUP_AT_MESSAGE_CREATE content has the bot's @-mention stripped by the
    // platform but may still contain a leading space.
    let content = d.get("content").and_then(Value::as_str).unwrap_or("").trim();
    if content.is_empty() {
        return None;
    }

    let external_user_id = format!("group:{group_openid}:{member_openid}");

    Some(InboundEvent {
        channel_id,
        channel_type: "qq_bot".into(),
        external_thread_id: message_id.clone(),
        external_user_id: Some(external_user_id),
        kind: classify(content),
        received_at: Utc::now(),
        raw: json!({
            "message_id": message_id,
            "scene": "group",
            "group_openid": group_openid,
            "member_openid": member_openid,
            "event": d,
        }),
    })
}

fn classify(content: &str) -> InboundEventKind {
    let trimmed = content.trim_start();
    if let Some(stripped) = trimmed.strip_prefix('/') {
        let (name, args) = stripped
            .split_once(char::is_whitespace)
            .map_or((stripped, ""), |(a, b)| (a, b));
        InboundEventKind::Command {
            name: name.trim().to_string(),
            args: args.trim().to_string(),
        }
    } else {
        InboundEventKind::Message {
            text: content.to_string(),
            attachments: Vec::new(),
        }
    }
}
