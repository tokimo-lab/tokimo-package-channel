//! Tokimo channel system — bidirectional integrations with external platforms
//! (Feishu, WeCom, Telegram, Email, etc).
//!
//! Replaces the older outbound-only `tokimo-notify` crate. See
//! `docs/infra/channel-system.md` for architecture notes.

pub mod capability;
pub mod config_store;
pub mod direction;
pub mod driver;
pub mod error;
pub mod hub;
pub mod inbound;
pub mod template;

pub use capability::{ChannelCapabilities, InboundKind};
pub use config_store::{ChannelConfigStore, ConfigWriter, NoopConfigStore};
pub use direction::ChannelDirection;
pub use driver::ChannelDriver;
pub use error::ChannelError;
pub use hub::{ChannelHub, DriverMetadata, SendTarget, TemplateFn};
pub use inbound::{
    InboundAttachment, InboundDriver, InboundEmitter, InboundEvent, InboundEventKind, PumpHandle, StreamReplyChunk,
    WebhookOutcome,
};
pub use template::{MessageStatus, RenderedMessage, TemplateContext};

/// Install rustls' default `CryptoProvider` once for the whole process.
///
/// All WS-based channel drivers (`feishu_bot`, `qq_bot`, `slack`, `discord`,
/// `dingtalk`) use `tokio_tungstenite::connect_async`, which in turn drives
/// rustls 0.23. The workspace transitively pulls both `ring` (via mongodb's
/// older rustls 0.21) and `aws-lc-rs` (via russh), so rustls 0.23 cannot
/// auto-select a provider and panics on the first `wss://` handshake with
/// "Could not automatically determine the process-level CryptoProvider".
///
/// In production that panic happens inside `tokio::spawn(...pump...)`, where
/// tokio swallows it — the pump task dies immediately and no inbound events
/// are ever delivered, yet the channel still logs "channel activated". This
/// was the root cause of the "Feishu bot doesn't receive messages on
/// Windows" bug (and would have hit every other WS driver eventually).
///
/// Idempotent: re-invoking is a no-op. Safe to call from any thread.
pub fn install_default_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}
