//! Generic webhook driver — outbound only.
//!
//! POST a simple JSON body to any URL. Useful as an integration fallback.
//!
//! Config:
//! ```jsonc
//! {
//!   "url":     "https://example.com/hook",
//!   "method":  "POST"                // optional, default POST
//!   "headers": { "X-Api-Key": "…" }  // optional extra headers
//! }
//! ```
//!
//! Body shape:
//! ```json
//! { "title": "...", "text": "...", "markdown": "..." }
//! ```

use async_trait::async_trait;
use serde_json::{Value, json};
use tracing::debug;

use crate::capability::ChannelCapabilities;
use crate::direction::ChannelDirection;
use crate::driver::ChannelDriver;
use crate::error::ChannelError;
use crate::template::RenderedMessage;

pub struct WebhookDriver {
    client: reqwest::Client,
}

impl WebhookDriver {
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ChannelDriver for WebhookDriver {
    fn channel_type(&self) -> &'static str {
        "webhook"
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Outbound
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            supports_markdown: true,
            supports_card: false,
            supports_image: false,
            max_text_length: 0,
            supports_file: false,
            max_file_size: 0,
        }
    }

    async fn send(&self, config: &Value, message: &RenderedMessage) -> Result<(), ChannelError> {
        let url = config
            .get("url")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ChannelError::ConfigError("missing url".into()))?;

        let method = config
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("POST")
            .to_uppercase();

        let body = json!({
            "text":     message.text,
            "markdown": message.markdown,
        });

        let mut req = match method.as_str() {
            "PUT" => self.client.put(url),
            "PATCH" => self.client.patch(url),
            _ => self.client.post(url),
        };

        if let Some(headers) = config.get("headers").and_then(Value::as_object) {
            for (k, v) in headers {
                if let Some(sv) = v.as_str() {
                    req = req.header(k, sv);
                }
            }
        }

        debug!(%url, %method, "sending webhook notification");
        let resp = req.json(&body).send().await?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let body = resp.text().await.unwrap_or_default();
            return Err(ChannelError::ChannelRejected { status, body });
        }
        Ok(())
    }

    fn connectivity_probes(&self, config: &Value) -> Vec<(String, u16)> {
        config
            .get("url")
            .and_then(Value::as_str)
            .and_then(parse_host_port)
            .map(|hp| vec![hp])
            .unwrap_or_default()
    }
}

fn parse_host_port(url: &str) -> Option<(String, u16)> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?.to_string();
    let port = parsed.port_or_known_default().unwrap_or(443);
    Some((host, port))
}
