//! Discord driver — bidirectional via Gateway WebSocket.
//!
//! * **Outbound** (optional): legacy `webhookUrl` still supported for
//!   one-way notifications. Independent of the bot credentials below; users
//!   who only want to push alerts can keep using just the webhook URL.
//! * **Inbound**: Discord Gateway v10 WebSocket. The server dials outbound
//!   to `wss://gateway.discord.gg` — no public HTTPS endpoint required.
//!   See [`discord_ws`](super::discord_ws) for the protocol implementation.
//!
//! Config (camelCase JSON in `channels.config`):
//! ```jsonc
//! {
//!   "webhookUrl":    "https://discord.com/api/webhooks/...",   // optional outbound
//!   "username":      "Tokimo",                                 // optional display name
//!   "botToken":      "MTA1...",                                // required for inbound + replies
//!   "intents":       37376,                                    // optional, default GUILD_MESSAGES|MESSAGE_CONTENT|DIRECT_MESSAGES
//!   "applicationId": "...",                                    // legacy, unused in ws mode
//!   "publicKey":     "..."                                     // legacy, unused in ws mode
//! }
//! ```
//!
//! > **Privileged intent**: the default intent mask includes
//! > `MESSAGE_CONTENT` (1 << 15), which is a *privileged* intent and must
//! > be enabled in the Discord Developer Portal → Bot → Privileged Gateway
//! > Intents. Without it the bot will receive empty `content` fields.
//!
//! `external_thread_id` is the Discord `channel_id` of the originating
//! message; replies go back via `POST /channels/{channel_id}/messages` with
//! the bot token.

use async_trait::async_trait;
use axum::http::HeaderMap;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use uuid::Uuid;

use crate::capability::{ChannelCapabilities, InboundKind};
use crate::config_store::ConfigWriter;
use crate::direction::ChannelDirection;
use crate::driver::ChannelDriver;
use crate::driver::discord_ws;
use crate::error::ChannelError;
use crate::inbound::{InboundDriver, InboundEmitter, PumpHandle, WebhookOutcome};
use crate::template::RenderedMessage;

pub(crate) const DISCORD_API_BASE: &str = "https://discord.com/api/v10";

/// Default intents: GUILD_MESSAGES (1<<9=512) | DIRECT_MESSAGES (1<<12=4096)
/// | MESSAGE_CONTENT (1<<15=32768) = 37376. `MESSAGE_CONTENT` is privileged.
pub(crate) const DEFAULT_INTENTS: u64 = 37376;

pub struct DiscordDriver {
    client: reqwest::Client,
}

impl DiscordDriver {
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct DiscordConfig {
    #[serde(default)]
    pub webhook_url: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub bot_token: Option<String>,
    #[serde(default)]
    pub intents: Option<u64>,
    // Legacy fields retained for backward compatibility with HTTP Interactions
    // config; unused in WebSocket mode.
    #[serde(default)]
    pub application_id: Option<String>,
    #[serde(default)]
    pub public_key: Option<String>,
}

impl DiscordConfig {
    pub(crate) fn from_value(v: &Value) -> Result<Self, ChannelError> {
        serde_json::from_value::<Self>(v.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid discord config: {e}")))
    }
}

#[async_trait]
impl ChannelDriver for DiscordDriver {
    fn channel_type(&self) -> &'static str {
        "discord"
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            supports_markdown: true,
            supports_card: false,
            supports_image: true,
            max_text_length: 2000,
        }
    }

    async fn send(&self, config: &Value, message: &RenderedMessage) -> Result<(), ChannelError> {
        let webhook_url = config
            .get("webhookUrl")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ChannelError::ConfigError("missing webhookUrl".into()))?;

        let content = message.markdown.clone().unwrap_or_else(|| message.text.clone());
        let mut body = json!({ "content": content });
        if let Some(username) = config.get("username").and_then(Value::as_str).filter(|s| !s.is_empty()) {
            body["username"] = json!(username);
        }

        debug!(url = %webhook_url, "sending discord notification");
        let resp = self.client.post(webhook_url).json(&body).send().await?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let body = resp.text().await.unwrap_or_default();
            return Err(ChannelError::ChannelRejected { status, body });
        }
        Ok(())
    }

    fn inbound(&self) -> Option<&dyn InboundDriver> {
        Some(self)
    }

    fn connectivity_probes(&self, _config: &Value) -> Vec<(String, u16)> {
        vec![("discord.com".to_string(), 443)]
    }
}

#[async_trait]
impl InboundDriver for DiscordDriver {
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
            "discord uses WebSocket Gateway; webhook inbound is disabled".into(),
        ))
    }

    async fn start_pump(
        &self,
        config: &Value,
        channel_id: Uuid,
        emit: InboundEmitter,
        _writer: ConfigWriter,
    ) -> Result<PumpHandle, ChannelError> {
        let cfg = DiscordConfig::from_value(config)?;
        let bot_token = cfg
            .bot_token
            .clone()
            .ok_or_else(|| ChannelError::ConfigError("discord botToken required for inbound".into()))?;
        let intents = cfg.intents.unwrap_or(DEFAULT_INTENTS);

        let cancel = CancellationToken::new();
        let cancel_child = cancel.clone();
        let http = self.client.clone();

        let task = tokio::spawn(discord_ws::run(
            http,
            bot_token,
            intents,
            channel_id,
            emit,
            cancel_child,
        ));
        Ok(PumpHandle { cancel, task })
    }

    async fn reply_to_user(
        &self,
        config: &Value,
        _external_user_id: &str,
        external_thread_id: &str,
        text: &str,
    ) -> Result<(), ChannelError> {
        let cfg = DiscordConfig::from_value(config)?;
        let bot_token = cfg
            .bot_token
            .as_deref()
            .ok_or_else(|| ChannelError::ConfigError("discord botToken required to reply".into()))?;
        if external_thread_id.is_empty() {
            return Err(ChannelError::Other(
                "discord external_thread_id (channel_id) missing; cannot reply".into(),
            ));
        }

        let url = format!("{DISCORD_API_BASE}/channels/{external_thread_id}/messages");
        let body = json!({ "content": text });
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {bot_token}"))
            .json(&body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let body = resp.text().await.unwrap_or_default();
            return Err(ChannelError::ChannelRejected { status, body });
        }
        Ok(())
    }

    async fn reply_file_to_user(
        &self,
        config: &Value,
        _external_user_id: &str,
        external_thread_id: &str,
        file: &crate::file::FilePayload,
        caption: Option<&str>,
    ) -> Result<(), ChannelError> {
        let cfg = DiscordConfig::from_value(config)?;
        let bot_token = cfg
            .bot_token
            .as_deref()
            .ok_or_else(|| ChannelError::ConfigError("discord botToken required to reply_file_to_user".into()))?;
        if external_thread_id.is_empty() {
            return Err(ChannelError::Other(
                "discord external_thread_id (channel_id) missing".into(),
            ));
        }

        let (data, filename, content_type) = crate::file::resolve_to_bytes(&self.client, file).await?;
        let content_type = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
        let part = reqwest::multipart::Part::bytes(data.to_vec())
            .file_name(filename)
            .mime_str(&content_type)
            .map_err(|e| ChannelError::Other(format!("invalid mime: {e}")))?;
        let mut payload = serde_json::Map::new();
        if let Some(caption) = caption {
            payload.insert("content".to_string(), Value::String(caption.to_string()));
        }
        let form = reqwest::multipart::Form::new()
            .part("files[0]", part)
            .text("payload_json", Value::Object(payload).to_string());

        let url = format!("{DISCORD_API_BASE}/channels/{external_thread_id}/messages");
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {bot_token}"))
            .multipart(form)
            .send()
            .await?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let body = resp.text().await.unwrap_or_default();
            return Err(ChannelError::ChannelRejected { status, body });
        }
        Ok(())
    }
}
