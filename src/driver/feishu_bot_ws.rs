//! Feishu / Lark open-platform WebSocket long-connection pump.
//!
//! Replaces the old event-subscription webhook: the server dials an outbound
//! WebSocket to `open.feishu.cn` and receives events on that persistent
//! connection. No public HTTPS endpoint is required. Ported from the
//! reference Go SDK [`larksuite/oapi-sdk-go/ws`].
//!
//! # Wire protocol
//!
//! 1. `POST https://open.feishu.cn/callback/ws/endpoint` with `{AppID, AppSecret}`
//!    returns a signed `wss://` URL plus a `ClientConfig` carrying
//!    reconnect/ping timings.
//! 2. Dial that URL. Non-101 handshake responses carry error detail in the
//!    headers `Handshake-Status`, `Handshake-Msg`, `Handshake-Autherrcode`.
//! 3. Binary frames are protobuf-encoded [`Frame`] messages. `method`
//!    distinguishes `Control` (0) from `Data` (1). Headers carry `type`
//!    (event/card/ping/pong), `message_id`, `sum`, `seq`.
//! 4. Large payloads are split across multiple data frames — reassemble using
//!    `message_id + seq` until `sum` pieces are collected.
//! 5. Each received event must be acked with a data frame whose payload is a
//!    JSON `{code: 200, headers: {...}, data: []}` response (the outer server
//!    fires a timeout otherwise).
//! 6. Control frames of type `ping` are expected every `PingInterval` seconds
//!    — reply with a `pong` control frame.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::error::ChannelError;
use crate::inbound::{InboundEmitter, InboundEvent, InboundEventKind};

// Generated from `proto/pbbp2.proto` by `build.rs`.
#[allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs, non_snake_case)]
pub mod pbbp2 {
    include!(concat!(env!("OUT_DIR"), "/pbbp2.rs"));
}

use pbbp2::{Frame, Header};

// ── Protocol constants (larksuite/oapi-sdk-go/ws/const.go) ───────────────────

const ENDPOINT_URL: &str = "https://open.feishu.cn/callback/ws/endpoint";

// FrameType
const FRAME_TYPE_CONTROL: i32 = 0;
const FRAME_TYPE_DATA: i32 = 1;

// Header keys on frames
const H_TYPE: &str = "type";
const H_MESSAGE_ID: &str = "message_id";
const H_SUM: &str = "sum";
const H_SEQ: &str = "seq";
const H_BIZ_RT: &str = "biz_rt";

// Values for `type`. Client-initiated heartbeats are `ping`; server replies to
// our pings come back as `pong` (mirrors the Go SDK's `NewPingFrame`).
const T_PING: &str = "ping";
const T_EVENT: &str = "event";

// Known server error codes
const CODE_OK: i64 = 0;

// ── Endpoint discovery ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct EndpointResp {
    code: i64,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: Option<EndpointData>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct EndpointData {
    #[serde(rename = "URL")]
    url: String,
    #[serde(rename = "ClientConfig", default)]
    client_config: ClientConfig,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case)]
pub struct ClientConfig {
    #[serde(default = "default_reconnect_interval")]
    pub ReconnectInterval: u32,
    #[serde(default = "default_reconnect_nonce")]
    pub ReconnectNonce: u32,
    #[serde(default = "default_ping_interval")]
    pub PingInterval: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            ReconnectInterval: default_reconnect_interval(),
            ReconnectNonce: default_reconnect_nonce(),
            PingInterval: default_ping_interval(),
        }
    }
}

const fn default_reconnect_interval() -> u32 {
    120
}
const fn default_reconnect_nonce() -> u32 {
    30
}
const fn default_ping_interval() -> u32 {
    120
}

pub(crate) async fn discover(
    http: &reqwest::Client,
    app_id: &str,
    app_secret: &str,
) -> Result<EndpointData, ChannelError> {
    let resp = http
        .post(ENDPOINT_URL)
        .header("locale", "zh")
        .json(&json!({ "AppID": app_id, "AppSecret": app_secret }))
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if status != 200 {
        return Err(ChannelError::ChannelRejected { status, body });
    }
    let parsed: EndpointResp =
        serde_json::from_str(&body).map_err(|e| ChannelError::Other(format!("decode ws endpoint: {e}: {body}")))?;
    if parsed.code != CODE_OK {
        return Err(ChannelError::ChannelRejected {
            status,
            body: format!("code={} msg={}", parsed.code, parsed.msg),
        });
    }
    parsed
        .data
        .ok_or_else(|| ChannelError::Other("ws endpoint response missing `data`".into()))
}

// ── Tenant-token + bot-info (used for chat-type filtering) ──────────────────

const FEISHU_API_BASE: &str = "https://open.feishu.cn";

#[derive(Default)]
struct TokenCell {
    token: String,
    expires_at: Option<Instant>,
}

/// Fetch a `tenant_access_token`, cached in-process until a minute before expiry.
async fn tenant_token(
    http: &reqwest::Client,
    cell: &Mutex<TokenCell>,
    app_id: &str,
    app_secret: &str,
) -> Result<String, ChannelError> {
    {
        let guard = cell.lock().await;
        if let Some(exp) = guard.expires_at
            && exp > Instant::now()
            && !guard.token.is_empty()
        {
            return Ok(guard.token.clone());
        }
    }

    let url = format!("{FEISHU_API_BASE}/open-apis/auth/v3/tenant_access_token/internal");
    let resp = http
        .post(&url)
        .json(&json!({ "app_id": app_id, "app_secret": app_secret }))
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if status != 200 {
        return Err(ChannelError::ChannelRejected { status, body });
    }
    #[derive(Deserialize)]
    struct TokenResp {
        code: i64,
        #[serde(default)]
        msg: Option<String>,
        #[serde(default)]
        tenant_access_token: Option<String>,
        #[serde(default)]
        expire: Option<u64>,
    }
    let parsed: TokenResp =
        serde_json::from_str(&body).map_err(|e| ChannelError::Other(format!("decode token response: {e}")))?;
    if parsed.code != 0 {
        return Err(ChannelError::ChannelRejected {
            status,
            body: format!("code={} msg={:?}", parsed.code, parsed.msg),
        });
    }
    let token = parsed
        .tenant_access_token
        .ok_or_else(|| ChannelError::Other("missing tenant_access_token".into()))?;
    let expire = parsed.expire.unwrap_or(7200).saturating_sub(60);

    let mut guard = cell.lock().await;
    guard.token.clone_from(&token);
    guard.expires_at = Some(Instant::now() + Duration::from_secs(expire));
    Ok(token)
}

/// Fetch the bot's own `open_id` via `GET /open-apis/bot/v3/info`.
async fn fetch_bot_open_id(http: &reqwest::Client, token: &str) -> Result<String, ChannelError> {
    let url = format!("{FEISHU_API_BASE}/open-apis/bot/v3/info");
    let resp = http.get(&url).bearer_auth(token).send().await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if status != 200 {
        return Err(ChannelError::ChannelRejected { status, body });
    }
    let parsed: Value =
        serde_json::from_str(&body).map_err(|e| ChannelError::Other(format!("decode bot/v3/info: {e}")))?;
    let code = parsed.get("code").and_then(Value::as_i64).unwrap_or(-1);
    if code != 0 {
        return Err(ChannelError::ChannelRejected {
            status,
            body: format!("bot/v3/info code={code} body={body}"),
        });
    }
    parsed
        .get("bot")
        .and_then(|b| b.get("open_id"))
        .and_then(Value::as_str)
        .map(String::from)
        .ok_or_else(|| ChannelError::Other("bot/v3/info missing bot.open_id".into()))
}

/// Fetch the sender `open_id` of a specific message (used to detect whether a
/// group reply is replying to the bot). Returns None if lookup fails or the
/// message has no sender.
async fn fetch_message_sender_open_id(http: &reqwest::Client, token: &str, message_id: &str) -> Option<String> {
    let url = format!("{FEISHU_API_BASE}/open-apis/im/v1/messages/{message_id}");
    let resp = http.get(&url).bearer_auth(token).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body = resp.text().await.ok()?;
    let parsed: Value = serde_json::from_str(&body).ok()?;
    // Response: { code, data: { items: [{ sender: { id, id_type, sender_type } }] } }
    // `id` is open_id when id_type == "open_id". Bot senders usually have id_type == "app_id"
    // pointing to app_id, so we prefer the open_id stored in message.sender.id field.
    parsed
        .get("data")?
        .get("items")?
        .get(0)?
        .get("sender")
        .and_then(|s| s.get("id"))
        .and_then(Value::as_str)
        .map(String::from)
}

// ── Frame helpers ───────────────────────────────────────────────────────────

fn find_header<'a>(frame: &'a Frame, key: &str) -> Option<&'a str> {
    frame.headers.iter().find(|h| h.key == key).map(|h| h.value.as_str())
}

/// `service` field in the Frame is taken from `?service_id=` on the WS URL.
fn service_id_from_url(url: &str) -> i32 {
    let Some(query) = url.split_once('?').map(|(_, q)| q) else {
        return 0;
    };
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=')
            && k == "service_id"
            && let Ok(n) = v.parse::<i32>()
        {
            return n;
        }
    }
    0
}

fn build_response_frame(incoming: &Frame, response_body: &Value) -> Vec<u8> {
    // Mirror the Go SDK: reuse the incoming frame's headers (type, message_id,
    // sum, seq, trace_id, timestamp), append biz_rt, replace the payload with
    // the JSON response. Without this header echo the server does not match
    // the ACK to the pending event and keeps retransmitting.
    let payload = serde_json::to_vec(response_body).unwrap_or_default();
    let mut headers = incoming.headers.clone();
    headers.push(Header {
        key: H_BIZ_RT.into(),
        value: "0".into(),
    });
    let frame = Frame {
        seq_id: 0,
        log_id: 0,
        service: incoming.service,
        method: FRAME_TYPE_DATA,
        headers,
        payload_encoding: None,
        payload_type: None,
        payload: Some(payload),
        log_id_new: None,
    };
    frame.encode_to_vec()
}

fn build_ping_frame(service: i32) -> Vec<u8> {
    let frame = Frame {
        seq_id: 0,
        log_id: 0,
        service,
        method: FRAME_TYPE_CONTROL,
        headers: vec![Header {
            key: H_TYPE.into(),
            value: T_PING.into(),
        }],
        payload_encoding: None,
        payload_type: None,
        payload: None,
        log_id_new: None,
    };
    frame.encode_to_vec()
}

// ── Packet reassembly ────────────────────────────────────────────────────────

#[derive(Default)]
struct Reassembler {
    // message_id -> (sum, pieces[seq] = Some(payload))
    #[allow(clippy::type_complexity)]
    pending: HashMap<String, (usize, Vec<Option<Vec<u8>>>)>,
}

impl Reassembler {
    /// Returns Some(assembled_payload) when all chunks arrived, else None.
    fn push(&mut self, msg_id: &str, seq: usize, sum: usize, payload: Vec<u8>) -> Option<Vec<u8>> {
        if sum <= 1 {
            return Some(payload);
        }
        let entry = self
            .pending
            .entry(msg_id.to_string())
            .or_insert_with(|| (sum, vec![None; sum]));
        if seq < entry.1.len() {
            entry.1[seq] = Some(payload);
        }
        if entry.1.iter().all(Option::is_some) {
            let (_, pieces) = self.pending.remove(msg_id)?;
            let mut out = Vec::new();
            for p in pieces.into_iter().flatten() {
                out.extend_from_slice(&p);
            }
            Some(out)
        } else {
            None
        }
    }
}

// ── Event parsing ───────────────────────────────────────────────────────────

/// Outcome of parsing a Feishu event payload.
pub(crate) struct ParsedEvent {
    pub event: InboundEvent,
    pub chat_type: String,
    /// True if any entry in `message.mentions` matches the bot's own open_id.
    pub bot_mentioned: bool,
    /// Non-empty when the message is a reply (`message.parent_id`). Used to
    /// decide whether to treat the reply as an implicit @bot.
    pub parent_id: String,
}

/// Decode a v2 Feishu event JSON body into our platform-agnostic event.
///
/// Shape matches the old webhook mode: `{ schema: "2.0", header, event }`.
/// Returns None for events we do not forward (e.g. card callbacks, empty text,
/// unsupported event types). Filtering by chat_type / mentions is done by the
/// caller based on the returned [`ParsedEvent`] metadata.
pub(crate) fn parse_event_payload(
    payload: &[u8],
    channel_id: Uuid,
    ack_emoji: &str,
    bot_open_id: Option<&str>,
) -> Option<ParsedEvent> {
    let json: Value = serde_json::from_slice(payload).ok()?;
    let header = json.get("header")?;
    let event_type = header.get("event_type").and_then(Value::as_str)?;
    if event_type != "im.message.receive_v1" {
        debug!("feishu_bot ws: ignoring event_type {event_type}");
        return None;
    }
    let event = json.get("event")?;
    let message = event.get("message")?;
    let sender = event.get("sender").cloned().unwrap_or(Value::Null);

    let message_id = message.get("message_id").and_then(Value::as_str).unwrap_or("");
    let chat_id = message.get("chat_id").and_then(Value::as_str).unwrap_or("");
    let chat_type = message
        .get("chat_type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let parent_id = message
        .get("parent_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let content_str = message.get("content").and_then(Value::as_str).unwrap_or("");
    let content_json: Value = serde_json::from_str(content_str).unwrap_or(Value::Null);
    let raw_text = content_json.get("text").and_then(Value::as_str).unwrap_or("");

    // Find the bot's mention entry (if any) and strip its `@_user_N` placeholder.
    let empty_vec = Vec::new();
    let mentions = message.get("mentions").and_then(Value::as_array).unwrap_or(&empty_vec);
    let mut bot_mention_key: Option<String> = None;
    let mut bot_mentioned = false;
    if let Some(bot_id) = bot_open_id {
        for m in mentions {
            let m_open_id = m
                .get("id")
                .and_then(|id| id.get("open_id"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if m_open_id == bot_id {
                bot_mentioned = true;
                if let Some(k) = m.get("key").and_then(Value::as_str) {
                    bot_mention_key = Some(k.to_string());
                }
                break;
            }
        }
    }

    let text = if let Some(key) = bot_mention_key.as_deref() {
        raw_text.replacen(key, "", 1).trim().to_string()
    } else {
        raw_text.trim().to_string()
    };

    if text.is_empty() {
        return None;
    }

    let sender_open_id = sender
        .get("sender_id")
        .and_then(|s| s.get("open_id"))
        .and_then(Value::as_str)
        .map(String::from);

    let trimmed = text.trim_start();
    let kind = if let Some(stripped) = trimmed.strip_prefix('/') {
        let (name, args) = stripped
            .split_once(char::is_whitespace)
            .map_or((stripped, ""), |(a, b)| (a, b));
        InboundEventKind::Command {
            name: name.trim().to_string(),
            args: args.trim().to_string(),
        }
    } else {
        InboundEventKind::Message {
            text: text.clone(),
            attachments: Vec::new(),
        }
    };

    let raw = json!({
        "message_id": message_id,
        "chat_id": chat_id,
        "chat_type": chat_type,
        "ack_emoji": ack_emoji,
        "sender": sender,
    });

    // Scope the conversation by (chat_type, chat_id, sender_open_id) so that:
    //   - p2p DM and a user's group messages live on separate conversation
    //     threads (different chat_id)
    //   - each user in a group has an independent thread
    //   - the same user across different groups is also kept separate
    // The chat_type prefix lets `reply_to_user` know whether to @ the
    // original sender (group only) without carrying extra state.
    // `external_user_id` is the conversation mapping key in
    // `channel_user_conversations`; encoding this into it achieves
    // per-chat isolation without a schema change.
    let scoped_user_id = match (chat_id, sender_open_id.as_deref()) {
        (c, Some(uid)) if !c.is_empty() => Some(format!("{chat_type}:{c}:{uid}")),
        (_, other) => other.map(String::from),
    };

    let inbound = InboundEvent {
        channel_id,
        channel_type: "feishu_bot".into(),
        external_thread_id: if chat_id.is_empty() {
            sender_open_id.clone().unwrap_or_default()
        } else {
            chat_id.to_string()
        },
        external_user_id: scoped_user_id,
        kind,
        received_at: Utc::now(),
        raw,
    };

    Some(ParsedEvent {
        event: inbound,
        chat_type,
        bot_mentioned,
        parent_id,
    })
}

// ── Pump task ───────────────────────────────────────────────────────────────

/// Derive a low-precision jitter in `[0, max_secs)` without pulling in `rand`.
fn jitter_secs(max_secs: u32) -> u64 {
    if max_secs == 0 {
        return 0;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    u64::from(nanos % max_secs)
}

/// Shared context held across pump reconnects. Carries HTTP client, app
/// credentials, cached tenant token and (if obtainable) the bot's own open_id
/// used to decide whether group messages should be forwarded.
struct PumpCtx {
    http: reqwest::Client,
    app_id: String,
    app_secret: String,
    ack_emoji: String,
    channel_id: Uuid,
    token: Mutex<TokenCell>,
    /// Bot's own open_id. Populated lazily on first successful lookup; None
    /// means we have no bot identity yet and must fail-open (forward all).
    bot_open_id: Mutex<Option<String>>,
}

impl PumpCtx {
    async fn ensure_bot_open_id(&self) -> Option<String> {
        {
            let guard = self.bot_open_id.lock().await;
            if let Some(id) = guard.clone() {
                return Some(id);
            }
        }
        let token = match tenant_token(&self.http, &self.token, &self.app_id, &self.app_secret).await {
            Ok(t) => t,
            Err(e) => {
                warn!(channel_id = %self.channel_id, error = %e, "feishu_bot: tenant_access_token lookup failed; group filter disabled");
                return None;
            }
        };
        match fetch_bot_open_id(&self.http, &token).await {
            Ok(id) => {
                info!(channel_id = %self.channel_id, bot_open_id = %id, "feishu_bot: resolved bot open_id");
                let mut guard = self.bot_open_id.lock().await;
                *guard = Some(id.clone());
                Some(id)
            }
            Err(e) => {
                warn!(channel_id = %self.channel_id, error = %e, "feishu_bot: bot/v3/info failed; group filter disabled");
                None
            }
        }
    }

    async fn token(&self) -> Option<String> {
        tenant_token(&self.http, &self.token, &self.app_id, &self.app_secret)
            .await
            .ok()
    }
}

/// Main pump entry-point. Loops forever (until `cancel` fires), re-dialling
/// on disconnect. Configured with the reconnect cadence returned by the
/// discovery endpoint.
pub(crate) async fn run(
    http: reqwest::Client,
    app_id: String,
    app_secret: String,
    ack_emoji: String,
    channel_id: Uuid,
    emit: InboundEmitter,
    cancel: CancellationToken,
) {
    let ctx = Arc::new(PumpCtx {
        http,
        app_id,
        app_secret,
        ack_emoji,
        channel_id,
        token: Mutex::new(TokenCell::default()),
        bot_open_id: Mutex::new(None),
    });

    // Resolve bot open_id up-front so group filtering works on the very first
    // event. Failure is non-fatal: we fall back to forwarding all events.
    let _ = ctx.ensure_bot_open_id().await;

    // Exponential backoff for *failures to discover / dial*; once connected
    // successfully we reset to the server-provided reconnect interval.
    let mut backoff = Duration::from_secs(2);
    const MAX_BACKOFF: Duration = Duration::from_mins(5);

    loop {
        if cancel.is_cancelled() {
            debug!(%channel_id, "feishu_bot ws pump cancelled");
            return;
        }

        let endpoint = match discover(&ctx.http, &ctx.app_id, &ctx.app_secret).await {
            Ok(e) => e,
            Err(e) => {
                warn!(%channel_id, error = %e, "feishu_bot ws discover failed; backing off");
                sleep_or_cancel(backoff, &cancel).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        info!(%channel_id, "feishu_bot ws connecting");
        let connected = connect_and_pump(&endpoint, &ctx, &emit, &cancel).await;
        backoff = Duration::from_secs(2); // reset on (attempted) connect

        if cancel.is_cancelled() {
            return;
        }

        // Wait the configured reconnect interval (+ random jitter) before
        // re-dialling. This matches the server-side expectations.
        let base = u64::from(endpoint.client_config.ReconnectInterval.max(1));
        let jitter = jitter_secs(endpoint.client_config.ReconnectNonce.max(1));
        let wait = Duration::from_secs(base.min(30) + jitter.min(10));
        // Clamp sleep to something reasonable — the Feishu default is 120s
        // but for the user's perception we reconnect faster than that.
        match connected {
            Ok(()) => {
                debug!(%channel_id, "feishu_bot ws disconnected cleanly, reconnecting in {}s", wait.as_secs());
                sleep_or_cancel(wait, &cancel).await;
            }
            Err(e) => {
                warn!(%channel_id, error = %e, "feishu_bot ws connection error; reconnecting in {}s", wait.as_secs());
                sleep_or_cancel(wait, &cancel).await;
            }
        }
    }
}

async fn sleep_or_cancel(dur: Duration, cancel: &CancellationToken) {
    tokio::select! {
        () = tokio::time::sleep(dur) => {},
        () = cancel.cancelled() => {},
    }
}

/// Dial the WebSocket, run receive + ping loops until it closes or cancels.
async fn connect_and_pump(
    endpoint: &EndpointData,
    ctx: &Arc<PumpCtx>,
    emit: &InboundEmitter,
    cancel: &CancellationToken,
) -> Result<(), ChannelError> {
    let channel_id = ctx.channel_id;
    let service = service_id_from_url(&endpoint.url);

    let request = endpoint
        .url
        .as_str()
        .into_client_request()
        .map_err(|e| ChannelError::Other(format!("build ws request: {e}")))?;

    let (ws, resp) = match tokio_tungstenite::connect_async(request).await {
        Ok(v) => v,
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
            return Err(handshake_error(*resp));
        }
        Err(e) => {
            return Err(ChannelError::Other(format!("ws dial failed: {e}")));
        }
    };
    info!(%channel_id, status = %resp.status(), "feishu_bot ws connected");

    let (writer, mut reader) = ws.split();
    let writer = Arc::new(Mutex::new(writer));
    let ping_interval = Duration::from_secs(u64::from(endpoint.client_config.PingInterval.max(30)));

    // Ping loop — periodic control frames so the server knows we are alive.
    let ping_writer = writer.clone();
    let ping_cancel = cancel.clone();
    let ping_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = tokio::time::sleep(ping_interval) => {
                    let frame = build_ping_frame(service);
                    let mut guard = ping_writer.lock().await;
                    if guard.send(WsMessage::Binary(frame.into())).await.is_err() {
                        break;
                    }
                }
                () = ping_cancel.cancelled() => break,
            }
        }
    });

    let mut reassembler = Reassembler::default();

    // Receive loop.
    let result: Result<(), ChannelError> = loop {
        tokio::select! {
            () = cancel.cancelled() => {
                break Ok(());
            }
            msg = reader.next() => {
                let Some(msg) = msg else {
                    break Ok(()); // stream ended
                };
                let msg = match msg {
                    Ok(m) => m,
                    Err(e) => break Err(ChannelError::Other(format!("ws read: {e}"))),
                };
                match msg {
                    WsMessage::Binary(bytes) => {
                        if let Err(e) = handle_binary(
                            &bytes,
                            ctx,
                            &mut reassembler,
                            emit,
                            &writer,
                        ).await {
                            warn!(%channel_id, error = %e, "feishu_bot ws frame handler error");
                        }
                    }
                    WsMessage::Ping(payload) => {
                        let mut w = writer.lock().await;
                        let _ = w.send(WsMessage::Pong(payload)).await;
                    }
                    WsMessage::Close(_) => {
                        debug!(%channel_id, "feishu_bot ws closed by peer");
                        break Ok(());
                    }
                    _ => { /* ignore text / pong */ }
                }
            }
        }
    };

    ping_task.abort();
    result
}

/// Pull per-handshake diagnostic headers out of a 4xx/5xx handshake response.
fn handshake_error(resp: Response) -> ChannelError {
    let status = resp.status().as_u16();
    let h = resp.headers();
    let hs_status = h.get("Handshake-Status").and_then(|v| v.to_str().ok()).unwrap_or("");
    let hs_msg = h.get("Handshake-Msg").and_then(|v| v.to_str().ok()).unwrap_or("");
    let hs_auth = h
        .get("Handshake-Autherrcode")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    ChannelError::ChannelRejected {
        status,
        body: format!("ws handshake rejected (HandshakeStatus={hs_status}, HandshakeMsg={hs_msg}, Auth={hs_auth})"),
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

async fn handle_binary(
    bytes: &[u8],
    ctx: &Arc<PumpCtx>,
    reassembler: &mut Reassembler,
    emit: &InboundEmitter,
    writer: &WsSink,
) -> Result<(), ChannelError> {
    let channel_id = ctx.channel_id;
    let frame = Frame::decode(bytes).map_err(|e| ChannelError::Other(format!("decode ws frame: {e}")))?;
    let msg_type = find_header(&frame, H_TYPE).unwrap_or("").to_string();

    match frame.method {
        FRAME_TYPE_CONTROL => {
            // Control frames carry pong/config from the server — advisory only;
            // the periodic ping task keeps us alive.
            debug!(%channel_id, r#type = %msg_type, "feishu_bot ws control frame");
            Ok(())
        }
        FRAME_TYPE_DATA => {
            if msg_type != T_EVENT {
                debug!(%channel_id, "feishu_bot ws ignoring data frame type={msg_type}");
                return Ok(());
            }

            let msg_id = find_header(&frame, H_MESSAGE_ID).unwrap_or("").to_string();
            let sum = find_header(&frame, H_SUM)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(1);
            let seq = find_header(&frame, H_SEQ)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);

            let payload = frame.payload.clone().unwrap_or_default();
            let Some(full) = reassembler.push(&msg_id, seq, sum, payload) else {
                return Ok(()); // waiting for more pieces
            };

            // Always ack so the server doesn't resend. The Go SDK sends
            // `{"code":200,"headers":null,"data":null}` with the incoming
            // frame's headers echoed back (+ biz_rt).
            let response_body = json!({
                "code": 200,
                "headers": Value::Null,
                "data": Value::Null,
            });
            let ack_frame = build_response_frame(&frame, &response_body);
            {
                let mut w = writer.lock().await;
                let _ = w.send(WsMessage::Binary(ack_frame.into())).await;
            }

            // Parse, then filter by chat_type / mentions before emitting.
            let bot_open_id = ctx.ensure_bot_open_id().await;
            let Some(parsed) = parse_event_payload(&full, channel_id, &ctx.ack_emoji, bot_open_id.as_deref()) else {
                return Ok(());
            };

            let should_forward = should_forward_event(&parsed, bot_open_id.as_deref(), ctx).await;
            if !should_forward {
                debug!(
                    %channel_id,
                    chat_type = %parsed.chat_type,
                    "feishu_bot: dropping group message (no @bot and not a reply to bot)"
                );
                return Ok(());
            }

            emit.send(parsed.event);
            Ok(())
        }
        other => {
            debug!(%channel_id, "feishu_bot ws unknown frame method={other}");
            Ok(())
        }
    }
}

/// Apply the user's routing rule:
/// * p2p → always forward
/// * group → forward only if bot was @-mentioned, or if the message is a reply
///   to one of the bot's own messages (treated as an implicit @bot)
/// * unknown chat_type / bot open_id unavailable → fail-open (forward)
async fn should_forward_event(parsed: &ParsedEvent, bot_open_id: Option<&str>, ctx: &Arc<PumpCtx>) -> bool {
    match parsed.chat_type.as_str() {
        "group" | "topic" => {
            let Some(bot_id) = bot_open_id else {
                // Can't tell — don't silently drop user messages.
                return true;
            };
            if parsed.bot_mentioned {
                return true;
            }
            if parsed.parent_id.is_empty() {
                return false;
            }
            // Reply: fetch the parent message's sender and compare to the bot.
            let Some(token) = ctx.token().await else {
                return false;
            };
            match fetch_message_sender_open_id(&ctx.http, &token, &parsed.parent_id).await {
                Some(sender_id) => sender_id == bot_id,
                None => false,
            }
        }
        // "p2p" and any unrecognised chat_type fall through to fail-open — we
        // explicitly do not want to silently drop direct messages.
        _ => true,
    }
}
