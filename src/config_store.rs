//! Persistence hook for drivers whose inbound pump needs to update their own
//! channel config (e.g. WeClaw must save the `context_token` it learns from
//! `getupdates` back to DB so outbound sends can use it).
//!
//! A concrete implementation lives in `rust-server` (DB-backed). The
//! `ChannelHub` holds an `Arc<dyn ChannelConfigStore>` and hands each active
//! pump a scoped [`ConfigWriter`].

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::error::ChannelError;

/// Abstraction over the channel table: update a channel's JSON config by id.
#[async_trait]
pub trait ChannelConfigStore: Send + Sync {
    async fn update_config(&self, channel_id: Uuid, config: Value) -> Result<(), ChannelError>;
}

/// Scoped handle given to a running pump so it can persist configuration
/// updates (refreshed tokens, cursors, etc.) for *its* channel only.
#[derive(Clone)]
pub struct ConfigWriter {
    store: Arc<dyn ChannelConfigStore>,
    channel_id: Uuid,
}

impl ConfigWriter {
    #[must_use]
    pub fn new(store: Arc<dyn ChannelConfigStore>, channel_id: Uuid) -> Self {
        Self { store, channel_id }
    }

    #[must_use]
    pub fn channel_id(&self) -> Uuid {
        self.channel_id
    }

    /// Persist the given JSON config for this writer's channel.
    pub async fn write(&self, config: Value) -> Result<(), ChannelError> {
        self.store.update_config(self.channel_id, config).await
    }
}

impl std::fmt::Debug for ConfigWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigWriter")
            .field("channel_id", &self.channel_id)
            .finish_non_exhaustive()
    }
}

/// No-op store — useful in tests / situations where no driver needs writeback.
pub struct NoopConfigStore;

#[async_trait]
impl ChannelConfigStore for NoopConfigStore {
    async fn update_config(&self, _channel_id: Uuid, _config: Value) -> Result<(), ChannelError> {
        Ok(())
    }
}
