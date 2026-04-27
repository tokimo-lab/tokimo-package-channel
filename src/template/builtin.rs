use std::collections::HashMap;
use std::fmt::Write;

use serde_json::json;

use super::{MessageStatus, RenderedMessage, TemplateContext};

/// Rich-text rendering: emoji + title + body + fields + footer.
pub fn render_text(ctx: &TemplateContext) -> String {
    let emoji = ctx.status.map_or("", MessageStatus::emoji);

    let mut text = format!("{emoji} {}", ctx.title);

    if !ctx.body.is_empty() {
        text.push('\n');
        text.push_str(&ctx.body);
    }

    for (k, v) in &ctx.fields {
        let _ = write!(text, "\n{k}: {v}");
    }

    if let Some(ref url) = ctx.url {
        let _ = write!(text, "\n{url}");
    }

    let ts = ctx.timestamp.format("%Y-%m-%d %H:%M:%S UTC");
    let _ = write!(text, "\n— Tokimo · {ts}");

    text
}

/// Markdown rendering (Telegram, Slack, generic).
pub fn render_markdown(ctx: &TemplateContext) -> String {
    let emoji = ctx.status.map_or("", MessageStatus::emoji);

    let mut md = format!("**{emoji} {}**", ctx.title);

    if !ctx.body.is_empty() {
        let _ = write!(md, "\n\n{}", ctx.body);
    }

    if !ctx.fields.is_empty() {
        md.push('\n');
        for (k, v) in &ctx.fields {
            let _ = write!(md, "\n- **{k}**: {v}");
        }
    }

    if let Some(ref url) = ctx.url {
        let _ = write!(md, "\n\n[查看详情]({url})");
    }

    let ts = ctx.timestamp.format("%Y-%m-%d %H:%M:%S UTC");
    let _ = write!(md, "\n\n_Tokimo · {ts}_");

    md
}

/// Feishu Interactive Card JSON.
pub fn render_feishu_card(ctx: &TemplateContext) -> serde_json::Value {
    let status = ctx.status.unwrap_or(MessageStatus::Info);
    let color = status.feishu_card_color();
    let emoji = status.emoji();

    let header = json!({
        "title": { "tag": "plain_text", "content": format!("{emoji} {}", ctx.title) },
        "template": color,
    });

    let mut elements: Vec<serde_json::Value> = Vec::new();

    // Body text
    if !ctx.body.is_empty() {
        elements.push(json!({
            "tag": "div",
            "text": { "tag": "lark_md", "content": &ctx.body },
        }));
    }

    // Fields grid
    if !ctx.fields.is_empty() {
        let field_items: Vec<serde_json::Value> = ctx
            .fields
            .iter()
            .map(|(k, v)| {
                json!({
                    "is_short": true,
                    "text": { "tag": "lark_md", "content": format!("**{k}**\n{v}") },
                })
            })
            .collect();
        elements.push(json!({ "tag": "div", "fields": field_items }));
    }

    // Optional link button
    if let Some(ref url) = ctx.url {
        elements.push(json!({ "tag": "hr" }));
        elements.push(json!({
            "tag": "action",
            "actions": [{
                "tag": "button",
                "text": { "tag": "plain_text", "content": "查看详情" },
                "url": url,
                "type": "primary",
            }],
        }));
    }

    // Footer
    elements.push(json!({ "tag": "hr" }));
    let ts = ctx.timestamp.format("%Y-%m-%d %H:%M:%S UTC");
    elements.push(json!({
        "tag": "note",
        "elements": [
            { "tag": "plain_text", "content": format!("Tokimo · {ts}") },
        ],
    }));

    json!({ "header": header, "elements": elements })
}

/// Markdown + plain-text renderer (no channel-specific card).
///
/// Suitable for channels that support markdown but not interactive cards
/// (Telegram, WeChat Work, generic webhooks, …).
pub fn markdown_renderer(ctx: &TemplateContext) -> RenderedMessage {
    RenderedMessage {
        text: render_text(ctx),
        markdown: Some(render_markdown(ctx)),
        card_payloads: HashMap::new(),
    }
}

/// Convenience: render a full [`RenderedMessage`] with Feishu card + rich text + markdown.
///
/// Apps can use this as a [`TemplateFn`](crate::TemplateFn) when registering
/// a "feishu" channel template.
pub fn feishu_rich_renderer(ctx: &TemplateContext) -> RenderedMessage {
    let text = render_text(ctx);
    let markdown = render_markdown(ctx);
    let card = render_feishu_card(ctx);
    RenderedMessage {
        text,
        markdown: Some(markdown),
        card_payloads: HashMap::from([("feishu".into(), card)]),
    }
}
