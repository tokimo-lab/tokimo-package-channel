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
