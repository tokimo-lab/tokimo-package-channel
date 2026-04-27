//! Feishu application bot driver — bidirectional.
//!
//! This driver is distinct from the legacy custom-webhook `feishu` driver:
//!
//! * `feishu`      — outbound only, posts to a group's inbound custom webhook.
//! * `feishu_bot`  — bidirectional, acts as a proper Feishu app bot:
//!   * outbound: `im/v1/messages` using `tenant_access_token`
//!   * inbound:  WebSocket long-connection to `open.feishu.cn` (see
//!     [`feishu_bot_ws`](super::feishu_bot_ws)). No public HTTPS callback
//!     needs to be exposed — the server dials outbound.
//!   * ack:      adds a reaction (emoji) to the incoming message
//!
//! Config shape (stored in `channels.config`):
//! ```jsonc
//! {
//!   "appId":     "cli_xxxxxxxx",
//!   "appSecret": "xxxxxxxxxxxxxxxx",
//!   "ackEmoji":  "OK"                // optional — reaction emoji key (default "OK")
//! }
//! ```
//!
//! Inbound `event.external_user_id` is the sender's `open_id`, which is also
//! used as the outbound `receive_id` when replying.

use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;
use axum::http::HeaderMap;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::capability::{ChannelCapabilities, InboundKind};
use crate::config_store::ConfigWriter;
use crate::direction::ChannelDirection;
use crate::driver::ChannelDriver;
use crate::driver::feishu_bot_ws;
use crate::error::ChannelError;
use crate::inbound::{InboundDriver, InboundEmitter, InboundEvent, PumpHandle, WebhookOutcome};
use crate::template::RenderedMessage;

const FEISHU_API_BASE: &str = "https://open.feishu.cn";

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct FeishuBotConfig {
    app_id: String,
    app_secret: String,
    #[serde(default)]
    ack_emoji: Option<String>,
}

struct CachedToken {
    token: String,
    expires_at: Instant,
}

pub struct FeishuBotDriver {
    client: reqwest::Client,
    token_cache: Mutex<Vec<((String, String), CachedToken)>>,
}

impl FeishuBotDriver {
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            token_cache: Mutex::new(Vec::new()),
        }
    }

    fn extract_config(config: &Value) -> Result<FeishuBotConfig, ChannelError> {
        serde_json::from_value::<FeishuBotConfig>(config.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid feishu_bot config: {e}")))
    }

    /// Fetch a tenant_access_token, cached in-process until a minute before expiry.
    async fn tenant_token(&self, cfg: &FeishuBotConfig) -> Result<String, ChannelError> {
        let key = (cfg.app_id.clone(), cfg.app_secret.clone());
        {
            let cache = self.token_cache.lock().expect("token cache poisoned");
            if let Some((_, hit)) = cache.iter().find(|(k, _)| *k == key)
                && hit.expires_at > Instant::now()
            {
                return Ok(hit.token.clone());
            }
        }

        let url = format!("{FEISHU_API_BASE}/open-apis/auth/v3/tenant_access_token/internal");
        let resp = self
            .client
            .post(&url)
            .json(&json!({ "app_id": cfg.app_id, "app_secret": cfg.app_secret }))
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
        let expire_secs = parsed.expire.unwrap_or(7200).saturating_sub(60);
        let expires_at = Instant::now() + std::time::Duration::from_secs(expire_secs);

        let mut cache = self.token_cache.lock().expect("token cache poisoned");
        cache.retain(|(k, _)| *k != key);
        cache.push((
            key,
            CachedToken {
                token: token.clone(),
                expires_at,
            },
        ));
        Ok(token)
    }

    /// POST `im/v1/messages?receive_id_type={id_type}` — text/interactive message.
    async fn post_message(
        &self,
        cfg: &FeishuBotConfig,
        receive_id_type: &str,
        receive_id: &str,
        msg_type: &str,
        content: &Value,
    ) -> Result<(), ChannelError> {
        let token = self.tenant_token(cfg).await?;
        let url = format!("{FEISHU_API_BASE}/open-apis/im/v1/messages?receive_id_type={receive_id_type}");
        let content_str =
            serde_json::to_string(content).map_err(|e| ChannelError::Other(format!("encode content: {e}")))?;
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&json!({
                "receive_id": receive_id,
                "msg_type": msg_type,
                "content": content_str,
            }))
            .send()
            .await?;
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        if status != 200 {
            return Err(ChannelError::ChannelRejected { status, body });
        }
        if let Ok(parsed) = serde_json::from_str::<Value>(&body) {
            let code = parsed.get("code").and_then(Value::as_i64).unwrap_or(0);
            if code != 0 {
                return Err(ChannelError::ChannelRejected {
                    status,
                    body: format!("code={code} body={body}"),
                });
            }
        }
        Ok(())
    }

    /// POST im/v1/messages/:id/reactions — add reaction to a message.
    async fn add_reaction(&self, cfg: &FeishuBotConfig, message_id: &str, emoji: &str) -> Result<(), ChannelError> {
        let token = self.tenant_token(cfg).await?;
        let url = format!("{FEISHU_API_BASE}/open-apis/im/v1/messages/{message_id}/reactions");
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&json!({ "reaction_type": { "emoji_type": emoji } }))
            .send()
            .await?;
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        if status != 200 {
            warn!(%status, %body, "feishu_bot reaction failed");
            return Err(ChannelError::ChannelRejected { status, body });
        }
        Ok(())
    }
}

#[async_trait]
impl ChannelDriver for FeishuBotDriver {
    fn channel_type(&self) -> &'static str {
        "feishu_bot"
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            supports_markdown: true,
            supports_card: true,
            supports_image: true,
            max_text_length: 0,
        }
    }

    async fn send(&self, config: &Value, message: &RenderedMessage) -> Result<(), ChannelError> {
        let cfg = Self::extract_config(config)?;
        let receive_id = config
            .get("defaultOpenId")
            .and_then(Value::as_str)
            .ok_or_else(|| ChannelError::ConfigError("defaultOpenId required for outbound send".into()))?;
        if let Some(card) = message.card_payloads.get("feishu") {
            self.post_message(&cfg, "open_id", receive_id, "interactive", card)
                .await
        } else {
            let text = message.markdown.as_deref().unwrap_or(&message.text);
            self.post_message(&cfg, "open_id", receive_id, "text", &json!({ "text": text }))
                .await
        }
    }

    fn inbound(&self) -> Option<&dyn InboundDriver> {
        Some(self)
    }

    fn connectivity_probes(&self, _config: &Value) -> Vec<(String, u16)> {
        vec![("open.feishu.cn".to_string(), 443)]
    }
}

/// Send a text message to a specific Feishu user via this driver.
pub async fn reply_text(
    driver: &FeishuBotDriver,
    config: &Value,
    open_id: &str,
    text: &str,
) -> Result<(), ChannelError> {
    let cfg = FeishuBotDriver::extract_config(config)?;
    driver
        .post_message(&cfg, "open_id", open_id, "text", &json!({ "text": text }))
        .await
}

#[async_trait]
impl InboundDriver for FeishuBotDriver {
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
            "feishu_bot uses WebSocket long-connection; webhook mode is disabled".into(),
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
        let ack_emoji = cfg.ack_emoji.clone().unwrap_or_else(|| "OK".to_string());
        let http = self.client.clone();
        let cancel = CancellationToken::new();
        let cancel_child = cancel.clone();

        let task = tokio::spawn(feishu_bot_ws::run(
            http,
            cfg.app_id,
            cfg.app_secret,
            ack_emoji,
            channel_id,
            emit,
            cancel_child,
        ));

        Ok(PumpHandle { cancel, task })
    }

    async fn ack_inbound(&self, config: &Value, event: &InboundEvent) -> Result<(), ChannelError> {
        let cfg = Self::extract_config(config)?;
        let Some(message_id) = event.raw.get("message_id").and_then(Value::as_str) else {
            return Ok(());
        };
        if message_id.is_empty() {
            return Ok(());
        }
        let emoji = event.raw.get("ack_emoji").and_then(Value::as_str).unwrap_or("OK");
        self.add_reaction(&cfg, message_id, emoji).await
    }

    async fn reply_to_user(
        &self,
        config: &Value,
        external_user_id: &str,
        external_thread_id: &str,
        text: &str,
    ) -> Result<(), ChannelError> {
        let cfg = Self::extract_config(config)?;

        // external_user_id is encoded as "{chat_type}:{chat_id}:{open_id}"
        // by feishu_bot_ws (see scoped_user_id). Fall back to treating
        // it as a plain open_id for other code paths.
        let (chat_type, sender_open_id) = {
            let parts: Vec<&str> = external_user_id.splitn(3, ':').collect();
            if parts.len() == 3 {
                (Some(parts[0]), parts[2])
            } else {
                (None, external_user_id)
            }
        };

        // Agent output is markdown. Feishu's `text` msg_type renders
        // plain text only; to get proper markdown rendering we must send
        // an interactive card (v2) with a `markdown` element. In group
        // chats, prepend `<at user_id="ou_xxx">` so the reply pings the
        // original sender.
        let is_group = matches!(chat_type, Some("group" | "topic"));
        let content = if is_group && sender_open_id.starts_with("ou_") {
            // Feishu card markdown @-mention: <at id=ou_xxx></at>
            // (note the attribute name is `id`, not `user_id`, and the
            // value is NOT quoted — that is the IM text-message syntax).
            format!("<at id={sender_open_id}></at> {text}")
        } else {
            text.to_string()
        };
        let card = json!({
            "schema": "2.0",
            "config": { "streaming_mode": false },
            "body": {
                "elements": [
                    { "tag": "markdown", "content": content }
                ]
            }
        });

        // Prefer replying to the originating chat (works for both p2p and
        // group — Feishu assigns an `oc_*` chat_id to both). This ensures
        // group @mentions get answered in the group, not the user's DM.
        if external_thread_id.starts_with("oc_") {
            return self
                .post_message(&cfg, "chat_id", external_thread_id, "interactive", &card)
                .await;
        }

        // Fallback: no chat_id — DM the sender by open_id.
        self.post_message(&cfg, "open_id", sender_open_id, "interactive", &card)
            .await
    }
}
