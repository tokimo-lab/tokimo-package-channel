//! Inbound event ingress — webhook parsing + active pump task management.

use std::fmt;

use async_trait::async_trait;
use axum::http::HeaderMap;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::capability::InboundKind;
use crate::config_store::ConfigWriter;
use crate::error::ChannelError;

/// Unified inbound event produced by any inbound-capable driver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundEvent {
    pub channel_id: Uuid,
    pub channel_type: String,
    /// External-platform thread/chat identifier (e.g. Telegram `chat_id`).
    pub external_thread_id: String,
    /// Optional external user identifier (who sent it).
    pub external_user_id: Option<String>,
    pub kind: InboundEventKind,
    pub received_at: DateTime<Utc>,
    /// Raw platform payload for debugging / advanced use cases.
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InboundEventKind {
    Message {
        text: String,
        #[serde(default)]
        attachments: Vec<InboundAttachment>,
    },
    /// `@bot /command args` style — driver extracts the command name.
    Command {
        name: String,
        args: String,
    },
    /// Interactive button / callback data.
    Callback {
        data: Value,
    },
    Other {
        tag: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundAttachment {
    pub kind: String,
    pub url: Option<String>,
    pub name: Option<String>,
    pub size: Option<u64>,
}

/// Broadcaster passed to pump tasks so they can forward decoded events to
/// the [`crate::ChannelHub`] subscribers.
#[derive(Clone)]
pub struct InboundEmitter(pub(crate) broadcast::Sender<InboundEvent>);

impl InboundEmitter {
    pub fn send(&self, event: InboundEvent) {
        // Broadcast channel returns Err when no subscribers — that is fine.
        let _ = self.0.send(event);
    }
}

impl fmt::Debug for InboundEmitter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboundEmitter").finish()
    }
}

/// Handle returned by [`InboundDriver::start_pump`]. Dropping or calling
/// `stop()` cancels the pump task.
pub struct PumpHandle {
    pub cancel: CancellationToken,
    pub task: tokio::task::JoinHandle<()>,
}

impl PumpHandle {
    pub fn stop(self) {
        self.cancel.cancel();
        // Best-effort: detach the task; the cancellation token lets it exit cleanly.
        drop(self.task);
    }
}

/// Optional companion to [`ChannelDriver`](crate::driver::ChannelDriver) that
/// adds inbound message handling.
#[async_trait]
pub trait InboundDriver: Send + Sync {
    fn kind(&self) -> InboundKind;

    /// Validate signature + parse a webhook POST from the platform. Returns
    /// a [`WebhookOutcome`] with an optional decoded event and an optional
    /// JSON body to echo back to the platform (used for handshakes like
    /// Feishu's `url_verification` challenge).
    async fn parse_webhook(
        &self,
        _config: &Value,
        _channel_id: Uuid,
        _headers: &HeaderMap,
        _body: Bytes,
    ) -> Result<WebhookOutcome, ChannelError> {
        Err(ChannelError::Unsupported("webhook inbound not implemented".into()))
    }

    /// Start a background task that actively ingests inbound events (e.g.
    /// Telegram long-poll). Must honour `cancel` on the returned
    /// [`PumpHandle`] for shutdown.
    ///
    /// `writer` can be used by drivers that need to persist refreshed
    /// credentials / cursors back to their channel config (e.g. WeClaw's
    /// `context_token`). Drivers that have no such need should ignore it.
    async fn start_pump(
        &self,
        _config: &Value,
        _channel_id: Uuid,
        _emit: InboundEmitter,
        _writer: ConfigWriter,
    ) -> Result<PumpHandle, ChannelError> {
        Err(ChannelError::Unsupported("pump inbound not implemented".into()))
    }

    /// Optional hook called when the application has acknowledged an inbound
    /// event (e.g. Feishu: add a reaction to the original message so the
    /// user knows their message was received). Default: no-op.
    async fn ack_inbound(&self, _config: &Value, _event: &InboundEvent) -> Result<(), ChannelError> {
        Ok(())
    }

    /// Send a plain-text reply directly to the external user that triggered
    /// an inbound event. Used by the AI inbound router to post assistant
    /// responses back to the originating platform (e.g. Feishu DM, Telegram
    /// private chat). Default: returns [`ChannelError::Unsupported`].
    async fn reply_to_user(
        &self,
        _config: &Value,
        _external_user_id: &str,
        _external_thread_id: &str,
        _text: &str,
    ) -> Result<(), ChannelError> {
        Err(ChannelError::Unsupported("reply_to_user not implemented".into()))
    }

    /// Stream a reply to the external user as it's being generated. Drivers
    /// that have no native streaming primitive should fall back to a
    /// buffered one-shot send by draining `rx` and invoking `reply_to_user`
    /// with the final accumulated text.
    ///
    /// Each [`StreamReplyChunk`] carries the FULL accumulated text so far
    /// (not a delta), matching QQ's `input_mode=replace` semantics; drivers
    /// that need deltas must compute them. The stream ends on the first
    /// chunk with `terminal=true` OR when the sender drops `rx`.
    async fn reply_to_user_streaming(
        &self,
        config: &Value,
        external_user_id: &str,
        external_thread_id: &str,
        mut rx: tokio::sync::mpsc::Receiver<StreamReplyChunk>,
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

/// A chunk in an outbound streaming reply. Carries the *full* accumulated
/// text, not a delta, so drivers using `input_mode=replace` (e.g. QQ Bot
/// streaming markdown) can forward it verbatim.
#[derive(Debug, Clone)]
pub struct StreamReplyChunk {
    pub accumulated_text: String,
    /// Set on the final chunk. After a terminal chunk the driver must close
    /// out the platform stream (e.g. QQ `input_state=10`).
    pub terminal: bool,
}

/// Result of parsing a webhook POST.
#[derive(Debug, Default, Clone)]
pub struct WebhookOutcome {
    /// Decoded event to broadcast. `None` for handshakes or other non-event payloads.
    pub event: Option<InboundEvent>,
    /// Optional JSON body that the webhook endpoint should echo to the platform
    /// (e.g. Feishu's `url_verification` `challenge`).
    pub reply: Option<Value>,
}

impl WebhookOutcome {
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn event(event: InboundEvent) -> Self {
        Self {
            event: Some(event),
            reply: None,
        }
    }

    #[must_use]
    pub fn reply(reply: Value) -> Self {
        Self {
            event: None,
            reply: Some(reply),
        }
    }
}
