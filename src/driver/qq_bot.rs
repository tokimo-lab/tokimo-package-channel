//! QQ Bot driver — bidirectional.
//!
//! Connects to the official Tencent QQ Open Platform bot API
//! (<https://bot.q.qq.com/wiki/>). Events arrive over a WebSocket gateway
//! (see [`qq_bot_ws`](super::qq_bot_ws)); outbound messages are posted via
//! the v2 REST API.
//!
//! Config shape (stored in `channels.config`):
//! ```jsonc
//! {
//!   "appId":              "1023456789",
//!   "appSecret":          "xxxxxxxxxxxx",
//!   "defaultUserOpenid":  "Uxxxx"        // optional — outbound DM target
//! }
//! ```
//!
//! Inbound `external_user_id` is encoded as `"{scene}:{scope_id}:{openid}"`
//! where `scene` is `"c2c"` or `"group"`, `scope_id` is the `group_openid`
//! for group messages or the user's `openid` for DMs. This preserves
//! per-chat routing in `reply_to_user` without any schema change — mirroring
//! the `feishu_bot` pattern.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::http::HeaderMap;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::capability::{ChannelCapabilities, InboundKind};
use crate::config_store::ConfigWriter;
use crate::direction::ChannelDirection;
use crate::driver::ChannelDriver;
use crate::driver::qq_bot_ws;
use crate::error::ChannelError;
use crate::inbound::{InboundDriver, InboundEmitter, InboundEvent, PumpHandle, WebhookOutcome};
use crate::template::RenderedMessage;

pub(crate) const QQ_BOT_API_BASE: &str = "https://api.sgroup.qq.com";
pub(crate) const QQ_BOT_TOKEN_URL: &str = "https://bots.qq.com/app/getAppAccessToken";

/// `msg_type` constants from the QQ Bot v2 API.
const MSG_TYPE_TEXT: i32 = 0;
const MSG_TYPE_MARKDOWN: i32 = 2;

/// Passive-reply window on QQ is ~5 minutes; we keep session state a bit
/// longer so late-arriving final chunks can still flush cleanly.
const SESSION_TTL: Duration = Duration::from_mins(15);

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QqBotConfig {
    pub app_id: String,
    pub app_secret: String,
    /// Optional DM recipient openid used by outbound `send` when no explicit
    /// target is supplied by the caller.
    #[serde(default)]
    pub default_user_openid: Option<String>,
}

pub(crate) struct CachedToken {
    pub(crate) token: String,
    pub(crate) expires_at: Instant,
}

pub struct QqBotDriver {
    client: reqwest::Client,
    token_cache: Mutex<Vec<((String, String), CachedToken)>>,
    /// Per `reply_msg_id` session state — tracks `msg_seq` and (for C2C
    /// markdown) the in-flight stream session. Cleaned up on terminal chunks
    /// and lazily GC'd by `purge_expired`.
    reply_sessions: Mutex<HashMap<String, ReplySession>>,
}

struct ReplySession {
    /// Active streaming state, if any. We hold onto the entry briefly after
    /// the stream is `done` so any late arriving non-streaming send tied to
    /// the same inbound msg_id keeps using a fresh msg_seq (cleaned up by
    /// `purge_expired` after `SESSION_TTL`).
    stream: Option<StreamState>,
    last_activity: Instant,
}

struct StreamState {
    /// Captured on the first chunk and reused for every subsequent chunk
    /// (including the DONE terminator). All chunks of one stream session
    /// MUST share the same `msg_seq`; only `index` increments. Sending
    /// different `msg_seq` values per chunk causes the QQ platform to treat
    /// each chunk as a fresh passive reply, producing duplicate messages.
    msg_seq: i32,
    /// Populated after the first chunk succeeds; required on subsequent
    /// chunks so the platform knows which message to update.
    stream_msg_id: Option<String>,
    /// Monotonic index within this stream session, starting at 0.
    next_index: i32,
    /// Set after a DONE chunk is sent — further chunks are rejected.
    done: bool,
}

/// Stream state for `stream_c2c_chunk`.
#[derive(Debug, Clone, Copy)]
pub(crate) enum StreamInputState {
    Generating,
    Done,
}

impl StreamInputState {
    fn as_i32(self) -> i32 {
        match self {
            Self::Generating => 1,
            Self::Done => 10,
        }
    }
}

impl QqBotDriver {
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            token_cache: Mutex::new(Vec::new()),
            reply_sessions: Mutex::new(HashMap::new()),
        }
    }

    fn extract_config(config: &Value) -> Result<QqBotConfig, ChannelError> {
        serde_json::from_value::<QqBotConfig>(config.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid qq_bot config: {e}")))
    }

    /// Fetch an `access_token`, cached in-process until a minute before expiry.
    pub(crate) async fn access_token(&self, cfg: &QqBotConfig) -> Result<String, ChannelError> {
        let key = (cfg.app_id.clone(), cfg.app_secret.clone());
        {
            let cache = self.token_cache.lock().expect("token cache poisoned");
            if let Some((_, hit)) = cache.iter().find(|(k, _)| *k == key)
                && hit.expires_at > Instant::now()
            {
                return Ok(hit.token.clone());
            }
        }

        let fetched = fetch_access_token(&self.client, &cfg.app_id, &cfg.app_secret).await?;
        let token_str = fetched.token.clone();

        let mut cache = self.token_cache.lock().expect("token cache poisoned");
        cache.retain(|(k, _)| *k != key);
        cache.push((key, fetched));
        Ok(token_str)
    }

    /// POST a v2 message envelope, returning the decoded API response. Honours
    /// the platform's `code != 0` error semantics even on HTTP 200.
    async fn post_v2_message(&self, cfg: &QqBotConfig, path: &str, body: &Value) -> Result<Value, ChannelError> {
        let token = self.access_token(cfg).await?;
        let url = format!("{QQ_BOT_API_BASE}{path}");
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("QQBot {token}"))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        if status != 200 {
            return Err(ChannelError::ChannelRejected { status, body: text });
        }
        let parsed: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        let code = parsed.get("code").and_then(Value::as_i64).unwrap_or(0);
        if code != 0 {
            return Err(ChannelError::ChannelRejected {
                status,
                body: format!("code={code} body={text}"),
            });
        }
        Ok(parsed)
    }

    /// Generate a fresh `msg_seq` for a new send. QQ requires `msg_seq` to be
    /// unique within a passive-reply window (5 min) tied to the same inbound
    /// `msg_id`; per the official openclaw-qqbot reference implementation,
    /// the recommended approach is a random 16-bit value (timestamp ^ counter).
    /// Using a per-window monotonic counter is *not* required and conflicts
    /// with how stream chunks must share a single `msg_seq`.
    fn gen_msg_seq() -> i32 {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let bump = COUNTER.fetch_add(1, Ordering::Relaxed);
        ((nanos ^ bump) % 65536) as i32
    }

    fn purge_expired(&self) {
        let mut sessions = self.reply_sessions.lock().expect("reply sessions poisoned");
        let now = Instant::now();
        sessions.retain(|_, s| now.duration_since(s.last_activity) < SESSION_TTL);
    }

    /// Push a chunk into a C2C streaming markdown session. The first call
    /// for a given `reply_msg_id` creates a platform stream and captures the
    /// returned `stream_msg_id`; subsequent calls carry it so the platform
    /// can update the in-flight message. Uses `input_mode=replace` — each
    /// chunk carries the *full accumulated text*, not a delta.
    pub(crate) async fn stream_c2c_chunk(
        &self,
        cfg: &QqBotConfig,
        user_openid: &str,
        reply_msg_id: &str,
        accumulated_text: &str,
        state: StreamInputState,
    ) -> Result<(), ChannelError> {
        let (msg_seq, stream_msg_id, index) = {
            self.purge_expired();
            let mut sessions = self.reply_sessions.lock().expect("reply sessions poisoned");
            let entry = sessions
                .entry(reply_msg_id.to_string())
                .or_insert_with(|| ReplySession {
                    stream: None,
                    last_activity: Instant::now(),
                });
            if entry.stream.is_none() {
                entry.stream = Some(StreamState {
                    msg_seq: Self::gen_msg_seq(),
                    stream_msg_id: None,
                    next_index: 0,
                    done: false,
                });
            }
            let stream = entry.stream.as_mut().expect("stream created above");
            if stream.done {
                return Err(ChannelError::Other(
                    "qq_bot stream already terminated for this reply".into(),
                ));
            }
            entry.last_activity = Instant::now();
            let idx = stream.next_index;
            stream.next_index = stream.next_index.saturating_add(1);
            (stream.msg_seq, stream.stream_msg_id.clone(), idx)
        };

        let mut body = json!({
            "input_mode": "replace",
            "input_state": state.as_i32(),
            "content_type": "markdown",
            "content_raw": accumulated_text,
            "event_id": reply_msg_id,
            "msg_id": reply_msg_id,
            "msg_seq": msg_seq,
            "index": index,
        });
        if let Some(ref smid) = stream_msg_id {
            body.as_object_mut()
                .expect("json object")
                .insert("stream_msg_id".into(), Value::String(smid.clone()));
        }

        let path = format!("/v2/users/{}/stream_messages", urlencoding::encode(user_openid));
        tracing::info!(
            "[qq_chunk] send reply_msg_id={} idx={} state={} text_len={} stream_msg_id={:?} msg_seq={}",
            reply_msg_id,
            index,
            state.as_i32(),
            accumulated_text.len(),
            stream_msg_id,
            msg_seq
        );
        let resp = match self.post_v2_message(cfg, &path, &body).await {
            Ok(r) => {
                tracing::info!("[qq_chunk] resp reply_msg_id={} idx={} body={}", reply_msg_id, index, r);
                r
            }
            Err(e) => {
                tracing::warn!(
                    "[qq_chunk] FAIL reply_msg_id={} idx={} state={} err={}",
                    reply_msg_id,
                    index,
                    state.as_i32(),
                    e
                );
                return Err(e);
            }
        };

        // Capture / update stream_msg_id from the first successful response.
        // Per the official openclaw-qqbot reference (`src/streaming.ts:896`),
        // the platform returns the stream id in the top-level `id` field of
        // the response, NOT in `data.stream_msg_id` or `stream_msg_id`.
        let returned_smid = resp
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                resp.get("data")
                    .and_then(|d| d.get("stream_msg_id"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .or_else(|| resp.get("stream_msg_id").and_then(Value::as_str).map(str::to_string));

        {
            let mut sessions = self.reply_sessions.lock().expect("reply sessions poisoned");
            if let Some(entry) = sessions.get_mut(reply_msg_id) {
                if matches!(state, StreamInputState::Done) {
                    // Clear the stream slot so a follow-up AI message can
                    // start a brand-new QQ stream (fresh msg_seq, null
                    // stream_msg_id, index=0) on the same passive-reply
                    // token. Passive tokens allow up to ~5 replies; multi-
                    // message turns rely on this reset to surface each
                    // assistant message as its own bubble.
                    entry.stream = None;
                    entry.last_activity = Instant::now();
                } else if let Some(stream) = entry.stream.as_mut() {
                    if stream.stream_msg_id.is_none()
                        && let Some(smid) = returned_smid
                    {
                        stream.stream_msg_id = Some(smid);
                    }
                    entry.last_activity = Instant::now();
                }
            }
        }
        Ok(())
    }

    /// Fallback used when streaming isn't supported (e.g. group markdown):
    /// drain the receiver, keeping only the final accumulated text, then
    /// send a single non-streaming reply.
    pub(crate) async fn drain_stream_as_oneshot(
        &self,
        config: &Value,
        external_user_id: &str,
        external_thread_id: &str,
        mut rx: tokio::sync::mpsc::Receiver<crate::inbound::StreamReplyChunk>,
    ) -> Result<(), ChannelError> {
        let mut final_text = String::new();
        while let Some(chunk) = rx.recv().await {
            final_text = chunk.accumulated_text;
            if chunk.terminal {
                break;
            }
        }
        if final_text.is_empty() {
            return Ok(());
        }
        self.reply_to_user(config, external_user_id, external_thread_id, &final_text)
            .await
    }
}

/// Raw token fetch (no caching). Shared with the ws pump so the gateway
/// discovery / Identify flow can request tokens independently of the driver
/// instance.
pub(crate) async fn fetch_access_token(
    http: &reqwest::Client,
    app_id: &str,
    app_secret: &str,
) -> Result<CachedToken, ChannelError> {
    let resp = http
        .post(QQ_BOT_TOKEN_URL)
        .json(&json!({ "appId": app_id, "clientSecret": app_secret }))
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if status != 200 {
        return Err(ChannelError::ChannelRejected { status, body });
    }
    #[derive(Deserialize)]
    struct TokenResp {
        #[serde(default)]
        access_token: Option<String>,
        /// QQ returns `expires_in` as a *string* containing seconds. Accept
        /// either numeric or string encodings.
        #[serde(default)]
        expires_in: Option<StringOrInt>,
        #[serde(default)]
        code: Option<i64>,
        #[serde(default)]
        message: Option<String>,
    }
    let parsed: TokenResp =
        serde_json::from_str(&body).map_err(|e| ChannelError::Other(format!("decode token response: {e}: {body}")))?;
    if let Some(code) = parsed.code
        && code != 0
    {
        return Err(ChannelError::ChannelRejected {
            status,
            body: format!("code={code} msg={:?}", parsed.message),
        });
    }
    let token = parsed
        .access_token
        .ok_or_else(|| ChannelError::Other(format!("missing access_token in response: {body}")))?;
    let expire_secs = parsed.expires_in.map_or(7200, StringOrInt::into_u64).saturating_sub(60);
    Ok(CachedToken {
        token,
        expires_at: Instant::now() + std::time::Duration::from_secs(expire_secs),
    })
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StringOrInt {
    Int(u64),
    Str(String),
}

impl StringOrInt {
    fn into_u64(self) -> u64 {
        match self {
            Self::Int(n) => n,
            Self::Str(s) => s.trim().parse::<u64>().unwrap_or(7200),
        }
    }
}

#[async_trait]
impl ChannelDriver for QqBotDriver {
    fn channel_type(&self) -> &'static str {
        "qq_bot"
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            supports_markdown: true,
            supports_card: false,
            supports_image: true,
            max_text_length: 0,
        }
    }

    async fn send(&self, config: &Value, message: &RenderedMessage) -> Result<(), ChannelError> {
        let cfg = Self::extract_config(config)?;
        let openid = cfg
            .default_user_openid
            .as_deref()
            .ok_or_else(|| ChannelError::ConfigError("defaultUserOpenid required for outbound send".into()))?;
        let text = message.markdown.as_deref().unwrap_or(&message.text);
        // QQ Bot is now always sent as markdown (msg_type=2) — the platform
        // requires the bot to have the "native markdown" capability approved,
        // which is the standard onboarding for AI bots.
        let seq = Self::gen_msg_seq();
        let body = build_message_body(text, None, true, seq);
        let path = format!("/v2/users/{}/messages", urlencoding::encode(openid));
        self.post_v2_message(&cfg, &path, &body).await?;
        Ok(())
    }

    fn inbound(&self) -> Option<&dyn InboundDriver> {
        Some(self)
    }

    fn connectivity_probes(&self, _config: &Value) -> Vec<(String, u16)> {
        vec![("api.sgroup.qq.com".to_string(), 443), ("bots.qq.com".to_string(), 443)]
    }
}

/// Build the `{content|markdown, msg_type, msg_seq, msg_id?}` envelope used
/// by `/v2/users/*/messages` and `/v2/groups/*/messages`.
///
/// Passing `Some(msg_id)` sends as a *passive reply* — the only way to reach
/// a user without an established active-message quota. `msg_id` must come
/// from a recently-received inbound event (<5 min old).
fn build_message_body(content: &str, reply_to_msg_id: Option<&str>, as_markdown: bool, msg_seq: i32) -> Value {
    let mut body = if as_markdown {
        json!({
            "markdown": { "content": content },
            "msg_type": MSG_TYPE_MARKDOWN,
            "msg_seq": msg_seq,
        })
    } else {
        json!({
            "content": content,
            "msg_type": MSG_TYPE_TEXT,
            "msg_seq": msg_seq,
        })
    };
    if let Some(id) = reply_to_msg_id {
        body.as_object_mut()
            .expect("json object")
            .insert("msg_id".into(), Value::String(id.to_string()));
    }
    body
}

#[async_trait]
impl InboundDriver for QqBotDriver {
    fn kind(&self) -> InboundKind {
        InboundKind::Pump
    }

    async fn parse_webhook(
        &self,
        _config: &Value,
        _channel_id: Uuid,
        _headers: &HeaderMap,
        _body: Bytes,
    ) -> Result<WebhookOutcome, ChannelError> {
        Err(ChannelError::Unsupported(
            "qq_bot uses WebSocket gateway; webhook mode is not supported".into(),
        ))
    }

    async fn start_pump(
        &self,
        config: &Value,
        channel_id: Uuid,
        emit: InboundEmitter,
        _writer: ConfigWriter,
    ) -> Result<PumpHandle, ChannelError> {
        let cfg = Self::extract_config(config)?;
        let http = self.client.clone();
        let cancel = CancellationToken::new();
        let cancel_child = cancel.clone();

        let task = tokio::spawn(qq_bot_ws::run(
            http,
            cfg.app_id,
            cfg.app_secret,
            channel_id,
            emit,
            cancel_child,
        ));

        Ok(PumpHandle { cancel, task })
    }

    async fn ack_inbound(&self, _config: &Value, _event: &InboundEvent) -> Result<(), ChannelError> {
        // QQ has no universal "reaction" primitive; the inbound message is
        // acknowledged by replying to it (happens in reply_to_user).
        Ok(())
    }

    async fn reply_to_user(
        &self,
        config: &Value,
        external_user_id: &str,
        external_thread_id: &str,
        text: &str,
    ) -> Result<(), ChannelError> {
        let cfg = Self::extract_config(config)?;

        // external_user_id is encoded by qq_bot_ws as "{scene}:{scope_id}:{openid}".
        // Fall back to treating it as a bare openid for legacy callers.
        let (scene, _scope_id, user_openid) = {
            let parts: Vec<&str> = external_user_id.splitn(3, ':').collect();
            if parts.len() == 3 {
                (parts[0], parts[1], parts[2])
            } else {
                ("c2c", "", external_user_id)
            }
        };

        // external_thread_id carries the inbound message_id (set by the pump).
        // QQ treats this as a passive-reply token; without it the send will
        // be rejected with "active-message quota exceeded" for most bots.
        let reply_msg_id = if external_thread_id.is_empty() {
            None
        } else {
            Some(external_thread_id)
        };
        let seq = Self::gen_msg_seq();
        let body = build_message_body(text, reply_msg_id, true, seq);

        let (path, target_label) = match scene {
            "group" => {
                let group_id = _scope_id;
                if group_id.is_empty() {
                    return Err(ChannelError::ConfigError(
                        "qq_bot group reply missing group_openid".into(),
                    ));
                }
                (
                    format!("/v2/groups/{}/messages", urlencoding::encode(group_id)),
                    "group",
                )
            }
            _ => (
                format!("/v2/users/{}/messages", urlencoding::encode(user_openid)),
                "c2c",
            ),
        };

        debug!(%target_label, path = %path, "qq_bot: sending reply");
        match self.post_v2_message(&cfg, &path, &body).await {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!(error = %e, "qq_bot: reply failed");
                Err(e)
            }
        }
    }

    async fn reply_to_user_streaming(
        &self,
        config: &Value,
        external_user_id: &str,
        external_thread_id: &str,
        mut rx: tokio::sync::mpsc::Receiver<crate::inbound::StreamReplyChunk>,
    ) -> Result<(), ChannelError> {
        let cfg = Self::extract_config(config)?;

        let (scene, _scope_id, user_openid) = {
            let parts: Vec<&str> = external_user_id.splitn(3, ':').collect();
            if parts.len() == 3 {
                (parts[0], parts[1], parts[2])
            } else {
                ("c2c", "", external_user_id)
            }
        };

        // Streaming is only supported for C2C markdown messages by QQ. Group
        // chats fall back to a buffered one-shot send.
        let streaming_ok = scene == "c2c" && !external_thread_id.is_empty();
        if !streaming_ok {
            return self
                .drain_stream_as_oneshot(config, external_user_id, external_thread_id, rx)
                .await;
        }

        let reply_msg_id = external_thread_id.to_string();
        let mut last_sent: String = String::new();
        let mut pending: Option<(String, bool)> = None;
        let mut flushed_any = false;

        // Throttle between non-terminal flushes (matches openclaw-qqbot
        // `THROTTLE_CONSTANTS.DEFAULT_MS = 500`). The first flush is also
        // delayed by this window so the first chunk can gather a useful
        // amount of text instead of being a single character.
        const THROTTLE: std::time::Duration = std::time::Duration::from_millis(500);
        let mut last_flush_at: Option<tokio::time::Instant> = None;

        loop {
            // Block until we have at least one chunk to consider.
            if pending.is_none() {
                match rx.recv().await {
                    Some(c) => pending = Some((c.accumulated_text, c.terminal)),
                    None => break,
                }
            }

            // Drain any immediately available chunks (coalesce to latest).
            while let Ok(next) = rx.try_recv() {
                pending = Some((next.accumulated_text, next.terminal));
            }

            let mut is_terminal = pending.as_ref().is_some_and(|p| p.1);

            // Throttle: for non-terminal chunks, wait until the throttle
            // window elapses since last flush (or from now for the first
            // flush). During the wait, keep coalescing newly arriving
            // chunks. A terminal chunk arriving during the wait short-
            // circuits the delay.
            if !is_terminal {
                let target = match last_flush_at {
                    Some(t) => t + THROTTLE,
                    None => tokio::time::Instant::now() + THROTTLE,
                };
                while tokio::time::Instant::now() < target {
                    let wait = target - tokio::time::Instant::now();
                    tokio::select! {
                        () = tokio::time::sleep(wait) => break,
                        recv = rx.recv() => match recv {
                            Some(c) => {
                                let term = c.terminal;
                                pending = Some((c.accumulated_text, term));
                                if term {
                                    is_terminal = true;
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
                // Final drain after wait.
                while let Ok(next) = rx.try_recv() {
                    pending = Some((next.accumulated_text, next.terminal));
                }
            }

            let (send_text, is_terminal) = pending.take().expect("pending set above");
            tracing::info!(
                "[qq_drv] chunk recv reply_msg_id={} text_len={} is_done={} last_sent_len={}",
                reply_msg_id,
                send_text.len(),
                is_terminal,
                last_sent.len()
            );
            if send_text == last_sent && !is_terminal {
                tracing::info!("[qq_drv] skip duplicate chunk reply_msg_id={}", reply_msg_id);
                continue;
            }
            let state = if is_terminal {
                StreamInputState::Done
            } else {
                StreamInputState::Generating
            };
            self.stream_c2c_chunk(&cfg, user_openid, &reply_msg_id, &send_text, state)
                .await?;
            last_sent = send_text;
            flushed_any = true;
            last_flush_at = Some(tokio::time::Instant::now());
            if is_terminal {
                return Ok(());
            }
        }

        // Sender dropped without a terminal chunk — close out the session
        // with whatever we last sent (or the final pending text).
        if let Some((send_text, _)) = pending {
            self.stream_c2c_chunk(&cfg, user_openid, &reply_msg_id, &send_text, StreamInputState::Done)
                .await?;
        } else if flushed_any {
            self.stream_c2c_chunk(&cfg, user_openid, &reply_msg_id, &last_sent, StreamInputState::Done)
                .await?;
        }
        Ok(())
    }
}
