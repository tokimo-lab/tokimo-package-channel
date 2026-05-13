//! WeClaw (iLink / ClawBot) driver — bidirectional:
//!
//! * **Outbound** — renders text messages to the user via `sendmessage`.
//! * **Inbound**  — a long-poll pump against `getupdates`. Every response
//!   carries an updated `get_updates_buf` cursor and any fresh messages the
//!   user sent to the bot. Messages include a `context_token` which is
//!   required for outbound; the pump persists refreshed credentials back to
//!   DB via [`ConfigWriter`] so subsequent sends can use them immediately.
//!
//! The pump replaces the previous ad-hoc "spawn a 5-minute one-shot task after
//! activate" mechanism, and means the server will pick up `context_token`
//! whenever the user messages the bot, even across restarts.

use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::capability::{ChannelCapabilities, InboundKind};
use crate::config_store::ConfigWriter;
use crate::direction::ChannelDirection;
use crate::driver::ChannelDriver;
use crate::error::ChannelError;
use crate::inbound::{InboundDriver, InboundEmitter, InboundEvent, InboundEventKind, PumpHandle};
use crate::template::RenderedMessage;

pub struct WeclawDriver {
    client: reqwest::Client,
}

impl WeclawDriver {
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ChannelDriver for WeclawDriver {
    fn channel_type(&self) -> &'static str {
        "weclaw"
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            supports_markdown: false,
            supports_card: false,
            supports_image: false,
            max_text_length: 0,
            supports_file: false,
            max_file_size: 0,
        }
    }

    async fn send(&self, config: &Value, message: &RenderedMessage) -> Result<(), ChannelError> {
        let creds: rust_client_api::weclaw::WeclawCredentials = serde_json::from_value(config.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid weclaw config: {e}")))?;

        debug!("sending weclaw message to user {}", creds.user_id);

        rust_client_api::weclaw::send_message(&self.client, &creds, &message.text)
            .await
            .map_err(|e| ChannelError::ChannelRejected { status: 0, body: e })
    }

    fn inbound(&self) -> Option<&dyn InboundDriver> {
        Some(self)
    }

    fn connectivity_probes(&self, _config: &Value) -> Vec<(String, u16)> {
        vec![("ilinkai.weixin.qq.com".to_string(), 443)]
    }
}

#[async_trait]
impl InboundDriver for WeclawDriver {
    fn kind(&self) -> InboundKind {
        InboundKind::Pump
    }

    async fn reply_to_user(
        &self,
        config: &Value,
        _external_user_id: &str,
        _external_thread_id: &str,
        text: &str,
    ) -> Result<(), ChannelError> {
        // iLink bots are 1:1-bound to a single WeChat user (stored in
        // `creds.user_id`), and `send_message` already targets that user
        // using the latest `context_token`. The external ids are therefore
        // redundant here — we delegate to the normal send path.
        let creds: rust_client_api::weclaw::WeclawCredentials = serde_json::from_value(config.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid weclaw config: {e}")))?;

        rust_client_api::weclaw::send_message(&self.client, &creds, text)
            .await
            .map_err(|e| ChannelError::ChannelRejected { status: 0, body: e })
    }

    async fn start_pump(
        &self,
        config: &Value,
        channel_id: Uuid,
        emit: InboundEmitter,
        writer: ConfigWriter,
    ) -> Result<PumpHandle, ChannelError> {
        let mut creds: rust_client_api::weclaw::WeclawCredentials = serde_json::from_value(config.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid weclaw config: {e}")))?;

        let client = self.client.clone();
        let cancel = CancellationToken::new();
        let cancel_child = cancel.clone();

        let task = tokio::spawn(async move {
            info!(%channel_id, "weclaw pump started");
            loop {
                if cancel_child.is_cancelled() {
                    debug!(%channel_id, "weclaw pump cancelled");
                    break;
                }

                let poll = rust_client_api::weclaw::poll_updates(&client, &creds);
                let outcome = tokio::select! {
                    res = poll => Some(res),
                    () = cancel_child.cancelled() => None,
                };

                let Some(res) = outcome else {
                    break;
                };

                match res {
                    Ok((updated, inbound_msgs)) => {
                        let buf_changed = updated.get_updates_buf != creds.get_updates_buf;
                        let token_changed =
                            updated.context_token.is_some() && updated.context_token != creds.context_token;

                        // Adopt new state in-memory first so the next poll uses
                        // the updated cursor, then persist so outbound sends
                        // can see the fresh context_token.
                        creds = updated.clone();

                        if buf_changed || token_changed {
                            match serde_json::to_value(&creds) {
                                Ok(new_config) => {
                                    if let Err(e) = writer.write(new_config).await {
                                        warn!(%channel_id, "weclaw persist creds failed: {e}");
                                    } else if token_changed {
                                        info!(%channel_id, "weclaw context_token refreshed");
                                    }
                                }
                                Err(e) => warn!(%channel_id, "weclaw serialize creds failed: {e}"),
                            }
                        }

                        // Forward each inbound user message as a Message event
                        // so the AI router can pick it up. Non-text messages
                        // (images/voice/etc.) arrive as Message with empty
                        // text for now — upstream can filter.
                        for msg in inbound_msgs {
                            let Some(text) = msg.text else {
                                debug!(%channel_id, "weclaw: skipping non-text inbound item");
                                continue;
                            };

                            // `/new` etc. → Command. Anything else → Message.
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
                                    text,
                                    attachments: Vec::new(),
                                }
                            };

                            emit.send(InboundEvent {
                                channel_id,
                                channel_type: "weclaw".into(),
                                external_thread_id: msg.from_user_id.clone(),
                                external_user_id: Some(msg.from_user_id.clone()),
                                kind,
                                received_at: Utc::now(),
                                raw: Value::Null,
                            });
                        }
                    }
                    Err(e) => {
                        warn!(%channel_id, "weclaw poll_updates error: {e}");
                        // Back off on transient errors so we don't hot-loop
                        // against iLink.
                        tokio::select! {
                            () = sleep(Duration::from_secs(5)) => {}
                            () = cancel_child.cancelled() => break,
                        }
                    }
                }
            }
            info!(%channel_id, "weclaw pump stopped");
        });

        Ok(PumpHandle { cancel, task })
    }
}
