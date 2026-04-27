//! Telegram Bot driver — bidirectional (outbound sendMessage + inbound long-poll pump).
//!
//! Chosen the long-poll pump (getUpdates) over webhook so the server does not
//! require a public HTTPS endpoint during development. Webhook mode can be
//! added later by implementing `parse_webhook`.

use async_trait::async_trait;
use axum::http::HeaderMap;
use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::capability::{ChannelCapabilities, InboundKind};
use crate::direction::ChannelDirection;
use crate::driver::ChannelDriver;
use crate::error::ChannelError;
use crate::inbound::{InboundDriver, InboundEmitter, InboundEvent, InboundEventKind, PumpHandle, WebhookOutcome};
use crate::template::RenderedMessage;

pub struct TelegramBotDriver {
    client: reqwest::Client,
}

impl TelegramBotDriver {
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    fn api_url(token: &str, method: &str) -> String {
        format!("https://api.telegram.org/bot{token}/{method}")
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TelegramConfig {
    bot_token: String,
    /// Optional chat ID for outbound-only uses. When absent, `send()` fails.
    default_chat_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TgApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

#[async_trait]
impl ChannelDriver for TelegramBotDriver {
    fn channel_type(&self) -> &'static str {
        "telegram_bot"
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            supports_markdown: true,
            supports_card: false,
            supports_image: true,
            max_text_length: 4096,
        }
    }

    async fn send(&self, config: &Value, message: &RenderedMessage) -> Result<(), ChannelError> {
        let cfg: TelegramConfig = serde_json::from_value(config.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid telegram_bot config: {e}")))?;
        let chat_id = cfg
            .default_chat_id
            .ok_or_else(|| ChannelError::ConfigError("defaultChatId required for outbound send".into()))?;

        let text = message.markdown.as_deref().unwrap_or(&message.text);
        let body = json!({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": if message.markdown.is_some() { "MarkdownV2" } else { "HTML" },
        });

        let url = Self::api_url(&cfg.bot_token, "sendMessage");
        debug!(%chat_id, "sending telegram message");
        let resp = self.client.post(&url).json(&body).send().await?;
        let status = resp.status().as_u16();
        let resp_body = resp.text().await.unwrap_or_default();
        if status != 200 {
            return Err(ChannelError::ChannelRejected {
                status,
                body: resp_body,
            });
        }
        Ok(())
    }

    fn inbound(&self) -> Option<&dyn InboundDriver> {
        Some(self)
    }

    fn connectivity_probes(&self, _config: &Value) -> Vec<(String, u16)> {
        vec![("api.telegram.org".to_string(), 443)]
    }
}

// ── Inbound: long-poll pump ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct TgUpdate {
    update_id: i64,
    message: Option<TgMessage>,
    callback_query: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct TgMessage {
    #[serde(default)]
    text: Option<String>,
    chat: TgChat,
    from: Option<TgUser>,
}

#[derive(Debug, Deserialize)]
struct TgChat {
    id: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct TgUser {
    id: i64,
    #[serde(default)]
    username: Option<String>,
}

#[async_trait]
impl InboundDriver for TelegramBotDriver {
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
        // Webhook mode is not implemented yet. Prefer pump for dev.
        Err(ChannelError::Unsupported(
            "telegram_bot uses long-poll; webhook mode not yet implemented".into(),
        ))
    }

    async fn start_pump(
        &self,
        config: &Value,
        channel_id: Uuid,
        emit: InboundEmitter,
        _writer: crate::config_store::ConfigWriter,
    ) -> Result<PumpHandle, ChannelError> {
        let cfg: TelegramConfig = serde_json::from_value(config.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid telegram_bot config: {e}")))?;
        let token = cfg.bot_token;
        let client = self.client.clone();
        let cancel = CancellationToken::new();
        let cancel_child = cancel.clone();

        let task = tokio::spawn(async move {
            let mut offset: i64 = 0;
            loop {
                if cancel_child.is_cancelled() {
                    debug!("telegram_bot pump cancelled (channel {channel_id})");
                    break;
                }

                let url = TelegramBotDriver::api_url(&token, "getUpdates");
                let body = json!({ "offset": offset, "timeout": 25 });
                let poll = async {
                    let resp = client.post(&url).json(&body).send().await.ok()?;
                    resp.json::<TgApiResponse<Vec<TgUpdate>>>().await.ok()
                };

                match tokio::select! {
                    r = poll => Some(r),
                    () = cancel_child.cancelled() => None,
                } {
                    Some(Some(api)) if api.ok => {
                        let updates = api.result.unwrap_or_default();
                        for u in updates {
                            offset = u.update_id + 1;
                            if let Some(msg) = u.message {
                                let text = msg.text.unwrap_or_default();
                                if text.is_empty() {
                                    continue;
                                }
                                let external_user_id = msg.from.as_ref().map(|f| f.id.to_string());
                                let kind = if let Some(stripped) = text.strip_prefix('/') {
                                    let (name, args) = stripped.split_once(' ').map_or((stripped, ""), |(a, b)| (a, b));
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
                                    channel_type: "telegram_bot".into(),
                                    external_thread_id: msg.chat.id.to_string(),
                                    external_user_id,
                                    kind,
                                    received_at: Utc::now(),
                                    raw: serde_json::to_value(&msg.from).unwrap_or_default(),
                                });
                            } else if let Some(cb) = u.callback_query {
                                emit.send(InboundEvent {
                                    channel_id,
                                    channel_type: "telegram_bot".into(),
                                    external_thread_id: String::new(),
                                    external_user_id: None,
                                    kind: InboundEventKind::Callback { data: cb.clone() },
                                    received_at: Utc::now(),
                                    raw: cb,
                                });
                            }
                        }
                    }
                    Some(Some(api)) => {
                        warn!("telegram_bot api error: {:?}", api.description);
                        sleep(Duration::from_secs(5)).await;
                    }
                    Some(None) => {
                        sleep(Duration::from_secs(2)).await;
                    }
                    None => break,
                }
            }
        });

        Ok(PumpHandle { cancel, task })
    }
}
