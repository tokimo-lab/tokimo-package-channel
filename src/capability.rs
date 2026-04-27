use serde::{Deserialize, Serialize};

/// Declares what outbound message formats a channel can render.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChannelCapabilities {
    pub supports_markdown: bool,
    pub supports_card: bool,
    pub supports_image: bool,
    pub max_text_length: usize,
}

/// How an inbound driver receives messages from the external platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InboundKind {
    /// External platform pushes via HTTP webhook (`POST /api/channels/:id/webhook`).
    Webhook,
    /// Driver runs a background pump (long-poll / WebSocket / IMAP IDLE).
    Pump,
    /// Driver supports both.
    Both,
}
