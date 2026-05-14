use async_trait::async_trait;
use base64::Engine;
use hmac::{KeyInit, Mac};
use serde_json::{Value, json};
use tracing::debug;

use crate::capability::ChannelCapabilities;
use crate::direction::ChannelDirection;
use crate::driver::ChannelDriver;
use crate::error::ChannelError;
use crate::template::RenderedMessage;

/// Outbound-only driver for Feishu custom webhook bots (text + card + signature).
pub struct FeishuDriver {
    client: reqwest::Client,
}

impl FeishuDriver {
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    fn extract_config(config: &Value) -> Result<(String, Option<String>), ChannelError> {
        let webhook_url = config
            .get("webhookUrl")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ChannelError::ConfigError("missing webhookUrl".into()))?
            .to_string();

        let secret = config
            .get("secret")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(String::from);

        Ok((webhook_url, secret))
    }

    /// Compute Feishu webhook signature.
    /// `sign = Base64(HMAC-SHA256(timestamp + "\n" + secret, secret))`
    fn sign(timestamp: i64, secret: &str) -> Result<String, ChannelError> {
        let string_to_sign = format!("{timestamp}\n{secret}");

        type HmacSha256 = hmac::Hmac<sha2::Sha256>;
        let mut mac = HmacSha256::new_from_slice(string_to_sign.as_bytes())
            .map_err(|e| ChannelError::Other(format!("HMAC init failed: {e}")))?;
        mac.update(&[]);
        let result = mac.finalize();

        Ok(base64::engine::general_purpose::STANDARD.encode(result.into_bytes()))
    }
}

#[async_trait]
impl ChannelDriver for FeishuDriver {
    fn channel_type(&self) -> &'static str {
        "feishu"
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Outbound
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
        let (webhook_url, secret) = Self::extract_config(config)?;

        let mut body = if let Some(card) = message.card_payloads.get("feishu") {
            json!({
                "msg_type": "interactive",
                "card": card,
            })
        } else {
            json!({
                "msg_type": "text",
                "content": { "text": &message.text },
            })
        };

        if let Some(ref secret) = secret {
            let timestamp = chrono::Utc::now().timestamp();
            let sign = Self::sign(timestamp, secret)?;
            body["timestamp"] = json!(timestamp.to_string());
            body["sign"] = json!(sign);
        }

        debug!(url = %webhook_url, "sending feishu notification");

        let resp = self.client.post(&webhook_url).json(&body).send().await?;

        let status = resp.status().as_u16();
        let resp_body = resp.text().await.unwrap_or_default();

        if status != 200 {
            return Err(ChannelError::ChannelRejected {
                status,
                body: resp_body,
            });
        }

        if let Ok(parsed) = serde_json::from_str::<Value>(&resp_body) {
            let code = parsed.get("code").and_then(Value::as_i64).unwrap_or(0);
            if code != 0 {
                let msg = parsed.get("msg").and_then(Value::as_str).unwrap_or("unknown error");
                return Err(ChannelError::ChannelRejected {
                    status,
                    body: format!("code={code}, msg={msg}"),
                });
            }
        }

        Ok(())
    }

    fn connectivity_probes(&self, config: &Value) -> Vec<(String, u16)> {
        let Some(url) = config.get("webhookUrl").and_then(Value::as_str) else {
            return Vec::new();
        };
        parse_host_port(url).map(|hp| vec![hp]).unwrap_or_default()
    }
}

fn parse_host_port(url: &str) -> Option<(String, u16)> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?.to_string();
    let port = parsed.port_or_known_default().unwrap_or(443);
    Some((host, port))
}
