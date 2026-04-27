pub mod builtin;
pub mod json_template;

use std::collections::HashMap;

/// Semantic status tag for visual styling (card header color, emoji prefix, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageStatus {
    Success,
    Failed,
    Warning,
    Info,
}

impl MessageStatus {
    #[must_use]
    pub fn emoji(self) -> &'static str {
        match self {
            Self::Success => "✅",
            Self::Failed => "❌",
            Self::Warning => "⚠️",
            Self::Info => "ℹ️",
        }
    }

    #[must_use]
    pub fn feishu_card_color(self) -> &'static str {
        match self {
            Self::Success => "green",
            Self::Failed => "red",
            Self::Warning => "orange",
            Self::Info => "blue",
        }
    }
}

/// Input context for template rendering.
///
/// Apps fill in whatever fields they need. If no per-channel template is
/// registered, only `title` and `body` are sent as plain text.
#[derive(Debug, Clone)]
pub struct TemplateContext {
    pub title: String,
    pub body: String,
    /// KV pairs for card field display (used by registered templates only).
    pub fields: Vec<(String, String)>,
    pub image_url: Option<String>,
    pub url: Option<String>,
    pub status: Option<MessageStatus>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Default for TemplateContext {
    fn default() -> Self {
        Self {
            title: String::new(),
            body: String::new(),
            fields: Vec::new(),
            image_url: None,
            url: None,
            status: None,
            timestamp: chrono::Utc::now(),
        }
    }
}

/// Multi-format rendered message. Each [`ChannelDriver`](crate::ChannelDriver)
/// picks the richest format it supports.
#[derive(Debug, Clone)]
pub struct RenderedMessage {
    /// Plain-text fallback (every channel must handle this).
    pub text: String,
    /// Markdown version (Telegram, Feishu, Slack, …).
    pub markdown: Option<String>,
    /// Channel-specific card payloads. Key = channel_type.
    pub card_payloads: HashMap<String, serde_json::Value>,
}

/// Default rendering: title + body as plain text, nothing else.
pub fn render_default(ctx: &TemplateContext) -> RenderedMessage {
    let mut text = ctx.title.clone();
    if !ctx.body.is_empty() {
        text.push('\n');
        text.push_str(&ctx.body);
    }
    RenderedMessage {
        text,
        markdown: None,
        card_payloads: HashMap::new(),
    }
}
