//! DingTalk driver — bidirectional via Stream Mode.
//!
//! * **Outbound** (optional): legacy custom-robot webhook (`webhookUrl` +
//!   optional `secret`) stays supported for one-way notifications.
//!   Independent of the stream-mode credentials below.
//! * **Inbound**: Stream Mode. The server opens a WebSocket via
//!   `POST https://api.dingtalk.com/v1.0/gateway/connections/open` authorised
//!   with the app's `clientId` / `clientSecret` (AppKey / AppSecret).
//!   No public HTTPS endpoint is required.
//!
//! Config:
//! ```jsonc
//! {
//!   "webhookUrl":    "https://oapi.dingtalk.com/robot/send?access_token=...", // optional outbound
//!   "secret":        "SEC...",  // optional outbound HMAC secret
//!   "clientId":      "...",     // required for inbound: app AppKey
//!   "clientSecret":  "...",     // required for inbound: app AppSecret
//!   "robotCode":     "...",     // optional; default = clientId. Used when replying via OAuth API.
//!   "signingSecret": "..."      // legacy outgoing-robot signing secret, unused in stream mode
//! }
//! ```
//!
//! Reply path: each stream callback carries a `sessionWebhook` that is valid
//! for a few hours — we pack it into `external_thread_id` as
//! `"{conversationId}|{sessionWebhook}"` so `reply_to_user` can recover it
//! without touching `raw`.

use async_trait::async_trait;
use axum::http::HeaderMap;
use base64::Engine;
use bytes::Bytes;
use hmac::{KeyInit, Mac};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use uuid::Uuid;

use crate::capability::{ChannelCapabilities, InboundKind};
use crate::config_store::ConfigWriter;
use crate::direction::ChannelDirection;
use crate::driver::ChannelDriver;
use crate::driver::dingtalk_ws;
use crate::error::ChannelError;
use crate::inbound::{InboundDriver, InboundEmitter, PumpHandle, WebhookOutcome};
use crate::template::RenderedMessage;

pub struct DingtalkDriver {
    client: reqwest::Client,
}

impl DingtalkDriver {
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    fn sign(timestamp: i64, secret: &str) -> Result<String, ChannelError> {
        let string_to_sign = format!("{timestamp}\n{secret}");
        type HmacSha256 = hmac::Hmac<sha2::Sha256>;
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|e| ChannelError::Other(format!("HMAC init: {e}")))?;
        mac.update(string_to_sign.as_bytes());
        let sig = mac.finalize().into_bytes();
        let b64 = base64::engine::general_purpose::STANDARD.encode(sig);
        Ok(urlencoding::encode(&b64).into_owned())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct DingtalkConfig {
    #[serde(default)]
    pub webhook_url: Option<String>,
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_secret: Option<String>,
    #[serde(default)]
    pub robot_code: Option<String>,
    #[serde(default)]
    pub signing_secret: Option<String>,
}

impl DingtalkConfig {
    pub(crate) fn from_value(v: &Value) -> Result<Self, ChannelError> {
        serde_json::from_value::<Self>(v.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid dingtalk config: {e}")))
    }
}

#[async_trait]
impl ChannelDriver for DingtalkDriver {
    fn channel_type(&self) -> &'static str {
        "dingtalk"
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            supports_markdown: true,
            supports_card: false,
            supports_image: false,
            max_text_length: 20_000,
        }
    }

    async fn send(&self, config: &Value, message: &RenderedMessage) -> Result<(), ChannelError> {
        let base_url = config
            .get("webhookUrl")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ChannelError::ConfigError("missing webhookUrl".into()))?
            .to_string();

        let secret = config.get("secret").and_then(Value::as_str).filter(|s| !s.is_empty());

        let final_url = if let Some(secret) = secret {
            let ts = chrono::Utc::now().timestamp_millis();
            let sign = Self::sign(ts, secret)?;
            let sep = if base_url.contains('?') { '&' } else { '?' };
            format!("{base_url}{sep}timestamp={ts}&sign={sign}")
        } else {
            base_url
        };

        let body = build_message_body(message);

        debug!(url = %final_url, "sending dingtalk notification");
        let resp = self.client.post(&final_url).json(&body).send().await?;
        let status = resp.status().as_u16();
        let resp_body = resp.text().await.unwrap_or_default();
        if status != 200 {
            return Err(ChannelError::ChannelRejected {
                status,
                body: resp_body,
            });
        }
        if let Ok(parsed) = serde_json::from_str::<Value>(&resp_body) {
            let code = parsed.get("errcode").and_then(Value::as_i64).unwrap_or(0);
            if code != 0 {
                let msg = parsed.get("errmsg").and_then(Value::as_str).unwrap_or("unknown");
                return Err(ChannelError::ChannelRejected {
                    status,
                    body: format!("errcode={code}, errmsg={msg}"),
                });
            }
        }
        Ok(())
    }

    fn inbound(&self) -> Option<&dyn InboundDriver> {
        Some(self)
    }

    fn connectivity_probes(&self, _config: &Value) -> Vec<(String, u16)> {
        vec![("api.dingtalk.com".to_string(), 443)]
    }
}

fn build_message_body(message: &RenderedMessage) -> Value {
    if let Some(md) = &message.markdown {
        json!({
            "msgtype": "markdown",
            "markdown": { "title": "Tokimo", "text": md },
        })
    } else {
        json!({
            "msgtype": "text",
            "text": { "content": &message.text },
        })
    }
}

#[async_trait]
impl InboundDriver for DingtalkDriver {
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
            "dingtalk uses Stream Mode WebSocket; webhook inbound is disabled".into(),
        ))
    }

    async fn start_pump(
        &self,
        config: &Value,
        channel_id: Uuid,
        emit: InboundEmitter,
        _writer: ConfigWriter,
    ) -> Result<PumpHandle, ChannelError> {
        let cfg = DingtalkConfig::from_value(config)?;
        let client_id = cfg
            .client_id
            .clone()
            .ok_or_else(|| ChannelError::ConfigError("dingtalk clientId required for inbound".into()))?;
        let client_secret = cfg
            .client_secret
            .clone()
            .ok_or_else(|| ChannelError::ConfigError("dingtalk clientSecret required for inbound".into()))?;

        let cancel = CancellationToken::new();
        let cancel_child = cancel.clone();
        let http = self.client.clone();

        let task = tokio::spawn(dingtalk_ws::run(
            http,
            client_id,
            client_secret,
            channel_id,
            emit,
            cancel_child,
        ));
        Ok(PumpHandle { cancel, task })
    }

    async fn reply_to_user(
        &self,
        _config: &Value,
        _external_user_id: &str,
        external_thread_id: &str,
        text: &str,
    ) -> Result<(), ChannelError> {
        let (_, session_webhook) = external_thread_id.split_once('|').ok_or_else(|| {
            ChannelError::Other("dingtalk external_thread_id missing sessionWebhook; cannot reply".into())
        })?;
        if session_webhook.is_empty() {
            return Err(ChannelError::Other("dingtalk sessionWebhook is empty".into()));
        }

        let body = json!({
            "msgtype": "text",
            "text": { "content": text },
        });
        let resp = self.client.post(session_webhook).json(&body).send().await?;
        let status = resp.status().as_u16();
        let resp_body = resp.text().await.unwrap_or_default();
        if !(200..300).contains(&status) {
            return Err(ChannelError::ChannelRejected {
                status,
                body: resp_body,
            });
        }
        if let Ok(parsed) = serde_json::from_str::<Value>(&resp_body) {
            let code = parsed.get("errcode").and_then(Value::as_i64).unwrap_or(0);
            if code != 0 {
                return Err(ChannelError::ChannelRejected {
                    status,
                    body: resp_body,
                });
            }
        }
        Ok(())
    }
}
