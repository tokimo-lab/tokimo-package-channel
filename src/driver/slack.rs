//! Slack driver — bidirectional via Socket Mode.
//!
//! * **Outbound** (optional): legacy incoming-webhook URL (`webhookUrl`)
//!   remains supported for one-way notifications. Independent from the
//!   bot credentials — users who only want outbound keep using the webhook.
//! * **Inbound**: Socket Mode. The server dials Slack via an `appToken`
//!   (xapp-…), opens a short-lived WebSocket, and receives `events_api`
//!   envelopes on that connection. No public HTTPS endpoint is required.
//!
//! Config:
//! ```jsonc
//! {
//!   "webhookUrl":    "https://hooks.slack.com/services/...",  // optional outbound
//!   "appToken":      "xapp-...",   // required for Socket Mode inbound
//!   "botToken":      "xoxb-...",   // required for replies via chat.postMessage
//!   "signingSecret": "..."         // optional / unused in Socket Mode (kept for backward compat)
//! }
//! ```

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
use crate::driver::slack_ws;
use crate::error::ChannelError;
use crate::inbound::{InboundDriver, InboundEmitter, PumpHandle, WebhookOutcome};
use crate::template::RenderedMessage;

pub(crate) const SLACK_API_BASE: &str = "https://slack.com/api";

pub struct SlackDriver {
    client: reqwest::Client,
}

impl SlackDriver {
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct SlackConfig {
    #[serde(default)]
    pub webhook_url: Option<String>,
    #[serde(default)]
    pub app_token: Option<String>,
    #[serde(default)]
    pub bot_token: Option<String>,
    #[serde(default)]
    pub signing_secret: Option<String>,
}

impl SlackConfig {
    pub(crate) fn from_value(v: &Value) -> Result<Self, ChannelError> {
        serde_json::from_value::<Self>(v.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid slack config: {e}")))
    }
}

#[async_trait]
impl ChannelDriver for SlackDriver {
    fn channel_type(&self) -> &'static str {
        "slack"
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            supports_markdown: true,
            supports_card: false,
            supports_image: true,
            max_text_length: 40_000,
        }
    }

    async fn send(&self, config: &Value, message: &RenderedMessage) -> Result<(), ChannelError> {
        let webhook_url = config
            .get("webhookUrl")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ChannelError::ConfigError("missing webhookUrl".into()))?;

        let text = message.markdown.clone().unwrap_or_else(|| message.text.clone());
        let body = json!({ "text": text });

        debug!(url = %webhook_url, "sending slack notification");
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
        vec![("slack.com".to_string(), 443)]
    }
}

#[async_trait]
impl InboundDriver for SlackDriver {
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
            "slack uses Socket Mode WebSocket; webhook inbound is disabled".into(),
        ))
    }

    async fn start_pump(
        &self,
        config: &Value,
        channel_id: Uuid,
        emit: InboundEmitter,
        _writer: ConfigWriter,
    ) -> Result<PumpHandle, ChannelError> {
        let cfg = SlackConfig::from_value(config)?;
        let app_token = cfg
            .app_token
            .clone()
            .ok_or_else(|| ChannelError::ConfigError("slack appToken (xapp-…) required for inbound".into()))?;

        let cancel = CancellationToken::new();
        let cancel_child = cancel.clone();
        let http = self.client.clone();

        let task = tokio::spawn(slack_ws::run(http, app_token, channel_id, emit, cancel_child));
        Ok(PumpHandle { cancel, task })
    }

    async fn reply_to_user(
        &self,
        config: &Value,
        _external_user_id: &str,
        external_thread_id: &str,
        text: &str,
    ) -> Result<(), ChannelError> {
        let cfg = SlackConfig::from_value(config)?;
        let bot_token = cfg
            .bot_token
            .as_deref()
            .ok_or_else(|| ChannelError::ConfigError("slack botToken required to reply".into()))?;

        let url = format!("{SLACK_API_BASE}/chat.postMessage");
        let body = json!({ "channel": external_thread_id, "text": text });
        let resp = self.client.post(&url).bearer_auth(bot_token).json(&body).send().await?;
        let status = resp.status().as_u16();
        let resp_body = resp.text().await.unwrap_or_default();
        if !(200..300).contains(&status) {
            return Err(ChannelError::ChannelRejected {
                status,
                body: resp_body,
            });
        }
        if let Ok(parsed) = serde_json::from_str::<Value>(&resp_body) {
            let ok = parsed.get("ok").and_then(Value::as_bool).unwrap_or(false);
            if !ok {
                return Err(ChannelError::ChannelRejected {
                    status,
                    body: resp_body,
                });
            }
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
        let cfg = SlackConfig::from_value(config)?;
        let bot_token = cfg
            .bot_token
            .as_deref()
            .ok_or_else(|| ChannelError::ConfigError("slack botToken required for reply_file_to_user".into()))?;
        let (data, filename, _content_type) = crate::file::resolve_to_bytes(&self.client, file).await?;

        let upload_url_api = format!("{SLACK_API_BASE}/files.getUploadURLExternal");
        let length = data.len().to_string();
        let resp = self
            .client
            .get(&upload_url_api)
            .bearer_auth(bot_token)
            .query(&[("filename", filename.as_str()), ("length", length.as_str())])
            .send()
            .await?;
        let status = resp.status().as_u16();
        let resp_body = resp.text().await.unwrap_or_default();
        if !(200..300).contains(&status) {
            return Err(ChannelError::ChannelRejected {
                status,
                body: resp_body,
            });
        }
        let parsed: Value = serde_json::from_str(&resp_body)
            .map_err(|e| ChannelError::Other(format!("slack upload URL response JSON decode failed: {e}")))?;
        let ok = parsed.get("ok").and_then(Value::as_bool).unwrap_or(false);
        if !ok {
            return Err(ChannelError::ChannelRejected {
                status,
                body: resp_body,
            });
        }
        let upload_url = parsed
            .get("upload_url")
            .and_then(Value::as_str)
            .ok_or_else(|| ChannelError::Other("slack upload URL response missing upload_url".into()))?;
        let file_id = parsed
            .get("file_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ChannelError::Other("slack upload URL response missing file_id".into()))?;

        let upload_resp = self
            .client
            .put(upload_url)
            .header("Content-Type", "application/octet-stream")
            .body(data.to_vec())
            .send()
            .await?;
        let upload_status = upload_resp.status().as_u16();
        if !(200..300).contains(&upload_status) {
            let body = upload_resp.text().await.unwrap_or_default();
            return Err(ChannelError::ChannelRejected {
                status: upload_status,
                body,
            });
        }

        let complete_url = format!("{SLACK_API_BASE}/files.completeUploadExternal");
        let mut complete_body = json!({
            "files": [{ "id": file_id }],
            "channel_id": external_thread_id,
        });
        if let Some(caption) = caption {
            complete_body["initial_comment"] = json!(caption);
        }
        let complete_resp = self
            .client
            .post(&complete_url)
            .bearer_auth(bot_token)
            .json(&complete_body)
            .send()
            .await?;
        let complete_status = complete_resp.status().as_u16();
        let complete_resp_body = complete_resp.text().await.unwrap_or_default();
        if !(200..300).contains(&complete_status) {
            return Err(ChannelError::ChannelRejected {
                status: complete_status,
                body: complete_resp_body,
            });
        }
        let complete_parsed: Value = serde_json::from_str(&complete_resp_body)
            .map_err(|e| ChannelError::Other(format!("slack complete upload response JSON decode failed: {e}")))?;
        let complete_ok = complete_parsed.get("ok").and_then(Value::as_bool).unwrap_or(false);
        if !complete_ok {
            return Err(ChannelError::ChannelRejected {
                status: complete_status,
                body: complete_resp_body,
            });
        }
        Ok(())
    }
}
