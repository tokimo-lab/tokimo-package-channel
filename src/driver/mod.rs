pub mod dingtalk;
pub mod dingtalk_ws;
pub mod discord;
pub mod discord_ws;
pub mod feishu;
pub mod feishu_bot;
pub mod feishu_bot_ws;
pub mod qq_bot;
pub mod qq_bot_ws;
pub mod slack;
pub mod slack_ws;
pub mod telegram_bot;
pub mod webhook;
pub mod weclaw;
pub mod wecom;

use async_trait::async_trait;
use serde_json::Value;

use crate::capability::ChannelCapabilities;
use crate::direction::ChannelDirection;
use crate::error::ChannelError;
use crate::file::FilePayload;
use crate::inbound::InboundDriver;
use crate::template::RenderedMessage;

/// A channel driver handles send (outbound) and optionally receive (inbound)
/// for a single external integration type (Feishu webhook, Telegram bot, …).
#[async_trait]
pub trait ChannelDriver: Send + Sync + 'static {
    /// Identifier matching `channels.type` in the DB.
    fn channel_type(&self) -> &'static str;

    fn direction(&self) -> ChannelDirection;

    fn capabilities(&self) -> ChannelCapabilities;

    /// Send a rendered message. Drivers without outbound support may return
    /// [`ChannelError::Unsupported`].
    async fn send(&self, _config: &Value, _message: &RenderedMessage) -> Result<(), ChannelError> {
        Err(ChannelError::Unsupported(format!(
            "channel '{}' does not support send",
            self.channel_type()
        )))
    }

    /// Send a file/image to the channel. Drivers without file support return
    /// [`ChannelError::Unsupported`].
    async fn send_file(
        &self,
        _config: &Value,
        _file: &FilePayload,
        _caption: Option<&str>,
    ) -> Result<(), ChannelError> {
        Err(ChannelError::Unsupported(format!(
            "channel '{}' does not support send_file",
            self.channel_type()
        )))
    }

    /// Returns the companion [`InboundDriver`] when this channel supports
    /// receiving. Default: no inbound support.
    fn inbound(&self) -> Option<&dyn InboundDriver> {
        None
    }

    /// Return the list of `(host, port)` pairs this driver needs to reach in order
    /// to function. The default [`check_connection`] implementation probes each
    /// endpoint with a TCP connect. Drivers that want custom behaviour can leave
    /// this empty and override `check_connection` directly.
    fn connectivity_probes(&self, _config: &Value) -> Vec<(String, u16)> {
        Vec::new()
    }

    /// Run a lightweight connectivity check against this channel's external
    /// dependencies. The default implementation TCP-connects to each probe
    /// returned by [`connectivity_probes`] with a short timeout.
    async fn check_connection(&self, config: &Value) -> Result<(), ChannelError> {
        let probes = self.connectivity_probes(config);
        if probes.is_empty() {
            return Err(ChannelError::Unsupported(format!(
                "channel '{}' does not support connectivity check",
                self.channel_type()
            )));
        }
        let timeout = std::time::Duration::from_secs(5);
        for (host, port) in probes {
            let addr = format!("{host}:{port}");
            match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&addr)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    return Err(ChannelError::ConnectionFailed(format!("{addr}: {e}")));
                }
                Err(_) => {
                    return Err(ChannelError::ConnectionFailed(format!("{addr}: timeout")));
                }
            }
        }
        Ok(())
    }
}
