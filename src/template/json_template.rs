//! JSON-driven notification template engine.
//!
//! Each app ships a `notify-template.jsonc` next to its source that declares,
//! per channel type, **how** to render a [`TemplateContext`] into a message.
//!
//! ## Supported template variables
//!
//! | Variable | Description |
//! |---|---|
//! | `{{title}}` | Notification title |
//! | `{{body}}` | Notification body |
//! | `{{status_emoji}}` | ✅ / ❌ / ⚠️ / ℹ️ based on status |
//! | `{{status_color}}` | Feishu card color: `green` / `red` / `orange` / `blue` |
//! | `{{timestamp}}` | `YYYY-MM-DD HH:MM:SS UTC` |
//! | `{{url}}` | Optional action URL (empty string when absent) |
//!
//! ## Block directives
//!
//! | Directive | Description |
//! |---|---|
//! | `{{#if body}}…{{/if}}` | Render block only when field is non-empty |
//! | `{{#each fields}}{{key}}: {{value}}{{/each}}` | Expand per-field |
//!
//! Supported `{{#if}}` field names: `body`, `url`, `fields`, `title`.
//!
//! ## Example JSON
//!
//! ```json
//! {
//!   "app_id": "my-app",
//!   "channels": {
//!     "feishu": {
//!       "format": "card",
//!       "card": {
//!         "header_title": "{{status_emoji}} {{title}}",
//!         "elements": [
//!           { "type": "text",       "content": "{{body}}",  "if": "body" },
//!           { "type": "fields",                             "if": "fields" },
//!           { "type": "hr",                                 "if": "url" },
//!           { "type": "url_button", "text": "查看详情",      "if": "url" },
//!           { "type": "hr" },
//!           { "type": "footer",     "content": "Tokimo · {{timestamp}}" }
//!         ]
//!       }
//!     },
//!     "weclaw": {
//!       "format": "markdown",
//!       "template": "**{{status_emoji}} {{title}}**\n\n{{body}}{{#each fields}}\n- **{{key}}**: {{value}}{{/each}}{{#if url}}\n\n[查看详情]({{url}}){{/if}}\n\n_Tokimo · {{timestamp}}_"
//!     }
//!   }
//! }
//! ```

use std::collections::HashMap;
use std::fmt::Write as _;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::template::{MessageStatus, RenderedMessage, TemplateContext};

// ─── Top-level config ─────────────────────────────────────────────────────────

/// Parsed representation of an app's `notify-template.json`.
#[derive(Debug, Clone, Deserialize)]
pub struct AppNotifyTemplate {
    pub app_id: String,
    pub channels: HashMap<String, ChannelTemplate>,
}

// ─── Per-channel template ─────────────────────────────────────────────────────

/// Declares how to render a notification for one specific channel type.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum ChannelTemplate {
    /// Feishu interactive card.
    /// Also generates markdown + plain-text fallbacks automatically.
    Card { card: CardTemplate },

    /// Markdown message (string template).
    /// Plain-text fallback is `"{{status_emoji}} {{title}}\n{{body}}"`.
    Markdown { template: String },

    /// Plain text message (string template).
    Text { template: String },
}

impl ChannelTemplate {
    pub fn render(&self, ctx: &TemplateContext) -> RenderedMessage {
        let vars = TemplateVars::from_ctx(ctx);
        match self {
            ChannelTemplate::Card { card } => {
                let feishu_card = card.render_feishu(&vars);
                // Auto-generate text + markdown fallbacks from the card's header/body
                let text = vars.render("{{status_emoji}} {{title}}\n{{body}}");
                let md = vars.render(
                    "**{{status_emoji}} {{title}}**\n\n\
                     {{body}}\
                     {{#each fields}}\n- **{{key}}**: {{value}}{{/each}}\
                     {{#if url}}\n\n[查看详情]({{url}}){{/if}}\n\n\
                     _Tokimo · {{timestamp}}_",
                );
                RenderedMessage {
                    text,
                    markdown: Some(md),
                    card_payloads: HashMap::from([("feishu".into(), feishu_card)]),
                }
            }
            ChannelTemplate::Markdown { template } => RenderedMessage {
                text: vars.render("{{status_emoji}} {{title}}\n{{body}}"),
                markdown: Some(vars.render(template)),
                card_payloads: HashMap::new(),
            },
            ChannelTemplate::Text { template } => RenderedMessage {
                text: vars.render(template),
                markdown: None,
                card_payloads: HashMap::new(),
            },
        }
    }
}

// ─── Feishu card template ─────────────────────────────────────────────────────

/// Declarative Feishu interactive card layout.
#[derive(Debug, Clone, Deserialize)]
pub struct CardTemplate {
    /// Header title template string, e.g. `"{{status_emoji}} {{title}}"`.
    pub header_title: String,
    pub elements: Vec<CardElement>,
}

impl CardTemplate {
    fn render_feishu(&self, vars: &TemplateVars<'_>) -> Value {
        let header = json!({
            "title": { "tag": "plain_text", "content": vars.simple_sub(&self.header_title) },
            "template": vars.status_color,
        });

        let mut elements: Vec<Value> = Vec::new();
        for elem in &self.elements {
            if let Some(cond) = elem.condition()
                && !vars.is_truthy(cond)
            {
                continue;
            }
            if let Some(v) = elem.render_feishu(vars) {
                elements.push(v);
            }
        }

        json!({ "header": header, "elements": elements })
    }
}

/// One element in a Feishu card body.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CardElement {
    /// `div` with lark_md content. Supports all template variables.
    Text {
        content: String,
        #[serde(rename = "if")]
        condition: Option<String>,
    },
    /// Expand `ctx.fields` as a two-column field grid.
    Fields {
        #[serde(rename = "if")]
        condition: Option<String>,
    },
    /// Primary action button that links to `{{url}}`.
    UrlButton {
        text: String,
        #[serde(rename = "if")]
        condition: Option<String>,
    },
    /// Horizontal rule divider.
    Hr {
        #[serde(rename = "if")]
        condition: Option<String>,
    },
    /// Footer note at the bottom of the card.
    Footer { content: String },
}

impl CardElement {
    fn condition(&self) -> Option<&str> {
        match self {
            Self::Text { condition, .. }
            | Self::Fields { condition, .. }
            | Self::UrlButton { condition, .. }
            | Self::Hr { condition, .. } => condition.as_deref(),
            Self::Footer { .. } => None,
        }
    }

    fn render_feishu(&self, vars: &TemplateVars<'_>) -> Option<Value> {
        match self {
            Self::Text { content, .. } => Some(json!({
                "tag": "div",
                "text": { "tag": "lark_md", "content": vars.render(content) },
            })),
            Self::Fields { .. } => {
                let items: Vec<Value> = vars
                    .fields
                    .iter()
                    .map(|(k, v)| {
                        json!({
                            "is_short": true,
                            "text": { "tag": "lark_md", "content": format!("**{k}**\n{v}") },
                        })
                    })
                    .collect();
                Some(json!({ "tag": "div", "fields": items }))
            }
            Self::UrlButton { text, .. } => vars.url.map(|url| {
                json!({
                    "tag": "action",
                    "actions": [{
                        "tag": "button",
                        "text": { "tag": "plain_text", "content": vars.simple_sub(text) },
                        "url": url,
                        "type": "primary",
                    }],
                })
            }),
            Self::Hr { .. } => Some(json!({ "tag": "hr" })),
            Self::Footer { content } => Some(json!({
                "tag": "note",
                "elements": [{ "tag": "plain_text", "content": vars.simple_sub(content) }],
            })),
        }
    }
}

// ─── Template variable resolution ─────────────────────────────────────────────

/// Variables resolved from [`TemplateContext`] and available in all templates.
pub struct TemplateVars<'a> {
    title: &'a str,
    body: &'a str,
    status_emoji: &'static str,
    pub status_color: &'static str,
    timestamp: String,
    url: Option<&'a str>,
    fields: &'a [(String, String)],
}

impl<'a> TemplateVars<'a> {
    pub fn from_ctx(ctx: &'a TemplateContext) -> Self {
        let status = ctx.status.unwrap_or(MessageStatus::Info);
        Self {
            title: &ctx.title,
            body: &ctx.body,
            status_emoji: status.emoji(),
            status_color: status.feishu_card_color(),
            timestamp: ctx.timestamp.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
            url: ctx.url.as_deref(),
            fields: &ctx.fields,
        }
    }

    /// `{{#if field}}` — truthy when the named field is non-empty/present.
    pub fn is_truthy(&self, field: &str) -> bool {
        match field {
            "body" => !self.body.is_empty(),
            "url" => self.url.is_some(),
            "fields" => !self.fields.is_empty(),
            "title" => !self.title.is_empty(),
            _ => false,
        }
    }

    /// Simple `{{var}}` substitution with no block processing.
    pub fn simple_sub(&self, s: &str) -> String {
        s.replace("{{title}}", self.title)
            .replace("{{body}}", self.body)
            .replace("{{status_emoji}}", self.status_emoji)
            .replace("{{status_color}}", self.status_color)
            .replace("{{timestamp}}", &self.timestamp)
            .replace("{{url}}", self.url.unwrap_or(""))
    }

    /// Full render: expands `{{#each fields}}`, evaluates `{{#if …}}`, then
    /// substitutes `{{var}}` placeholders.
    pub fn render(&self, template: &str) -> String {
        let s = self.expand_each(template);
        let s = self.eval_if(&s);
        self.simple_sub(&s)
    }

    /// Expand all `{{#each fields}}…{{/each}}` blocks.
    fn expand_each(&self, template: &str) -> String {
        const OPEN: &str = "{{#each fields}}";
        const CLOSE: &str = "{{/each}}";
        let mut result = String::new();
        let mut rest = template;
        while let Some(start) = rest.find(OPEN) {
            result.push_str(&rest[..start]);
            rest = &rest[start + OPEN.len()..];
            if let Some(end) = rest.find(CLOSE) {
                let inner = &rest[..end];
                for (k, v) in self.fields {
                    result.push_str(&inner.replace("{{key}}", k).replace("{{value}}", v));
                }
                rest = &rest[end + CLOSE.len()..];
            } else {
                result.push_str(OPEN);
            }
        }
        result.push_str(rest);
        result
    }

    /// Evaluate all `{{#if field}}…{{/if}}` blocks (supports nesting).
    fn eval_if(&self, template: &str) -> String {
        const OPEN_PREFIX: &str = "{{#if ";
        const CLOSE: &str = "{{/if}}";
        let mut result = String::new();
        let mut rest = template;
        while let Some(start) = rest.find(OPEN_PREFIX) {
            result.push_str(&rest[..start]);
            rest = &rest[start + OPEN_PREFIX.len()..];
            if let Some(end_brace) = rest.find("}}") {
                let field = &rest[..end_brace];
                rest = &rest[end_brace + 2..];
                if let Some(close_pos) = rest.find(CLOSE) {
                    let inner = &rest[..close_pos];
                    if self.is_truthy(field) {
                        result.push_str(&self.eval_if(inner));
                    }
                    rest = &rest[close_pos + CLOSE.len()..];
                } else {
                    let _ = write!(result, "{{{{#if {field}}}}}");
                }
            } else {
                result.push_str(OPEN_PREFIX);
            }
        }
        result.push_str(rest);
        result
    }
}
