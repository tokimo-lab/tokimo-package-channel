//! Channel hub — runtime registry of channel driver types + active per-channel
//! lifecycle (pump tasks, inbound broadcaster, outbound template registry).
//!
//! Replaces the old `Notifier` with a bidirectional, dynamically-managed API.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;
use tracing::{info, warn};
use uuid::Uuid;

use crate::capability::ChannelCapabilities;
use crate::config_store::{ChannelConfigStore, ConfigWriter, NoopConfigStore};
use crate::direction::ChannelDirection;
use crate::driver::{
    ChannelDriver, dingtalk::DingtalkDriver, discord::DiscordDriver, feishu::FeishuDriver, feishu_bot::FeishuBotDriver,
    qq_bot::QqBotDriver, slack::SlackDriver, telegram_bot::TelegramBotDriver, webhook::WebhookDriver,
    weclaw::WeclawDriver, wecom::WecomDriver,
};
use crate::error::ChannelError;
use crate::inbound::{InboundEmitter, InboundEvent, PumpHandle, WebhookOutcome};
use crate::template::{RenderedMessage, TemplateContext, render_default};

/// A function that renders a [`TemplateContext`] into a channel-specific
/// [`RenderedMessage`]. Apps register one per `(app_id, channel_type)`.
pub type TemplateFn = Arc<dyn Fn(&TemplateContext) -> RenderedMessage + Send + Sync>;

/// A concrete send target: channel type + its JSON config from DB.
pub struct SendTarget {
    pub channel_type: String,
    pub config: Value,
}

/// Metadata about a registered driver type, exposed via the API so the
/// frontend can render driver-selection UIs with correct direction badges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverMetadata {
    pub channel_type: String,
    pub direction: ChannelDirection,
    pub capabilities: ChannelCapabilities,
}

struct ActiveChannel {
    pump: Option<PumpHandle>,
}

/// Central hub — shared across the app as `Arc<ChannelHub>`.
pub struct ChannelHub {
    drivers: HashMap<String, Arc<dyn ChannelDriver>>,
    templates: Mutex<HashMap<(String, String), TemplateFn>>,
    /// Active runtime state per `channel_id` (pump handle etc.).
    active: Mutex<HashMap<Uuid, ActiveChannel>>,
    /// Broadcast of every inbound event produced by any active pump.
    inbound_tx: broadcast::Sender<InboundEvent>,
    /// Writeback store used by pumps to persist refreshed credentials.
    config_store: Arc<dyn ChannelConfigStore>,
}

impl ChannelHub {
    /// Build a hub with all built-in drivers registered and the given config
    /// store (used by pumps that refresh their own credentials, e.g. WeClaw).
    #[must_use]
    pub fn new(http_client: reqwest::Client, config_store: Arc<dyn ChannelConfigStore>) -> Arc<Self> {
        // Pin rustls' crypto provider once before any WS driver dials out —
        // see `install_default_crypto_provider` for why this matters.
        crate::install_default_crypto_provider();

        let mut drivers: HashMap<String, Arc<dyn ChannelDriver>> = HashMap::new();

        let feishu = Arc::new(FeishuDriver::new(http_client.clone()));
        drivers.insert(feishu.channel_type().to_string(), feishu);

        let feishu_bot = Arc::new(FeishuBotDriver::new(http_client.clone()));
        drivers.insert(feishu_bot.channel_type().to_string(), feishu_bot);

        let qq_bot = Arc::new(QqBotDriver::new(http_client.clone()));
        drivers.insert(qq_bot.channel_type().to_string(), qq_bot);

        let weclaw = Arc::new(WeclawDriver::new(http_client.clone()));
        drivers.insert(weclaw.channel_type().to_string(), weclaw);

        let tg = Arc::new(TelegramBotDriver::new(http_client.clone()));
        drivers.insert(tg.channel_type().to_string(), tg);

        let discord = Arc::new(DiscordDriver::new(http_client.clone()));
        drivers.insert(discord.channel_type().to_string(), discord);

        let slack = Arc::new(SlackDriver::new(http_client.clone()));
        drivers.insert(slack.channel_type().to_string(), slack);

        let wecom = Arc::new(WecomDriver::new(http_client.clone()));
        drivers.insert(wecom.channel_type().to_string(), wecom);

        let dingtalk = Arc::new(DingtalkDriver::new(http_client.clone()));
        drivers.insert(dingtalk.channel_type().to_string(), dingtalk);

        let webhook = Arc::new(WebhookDriver::new(http_client));
        drivers.insert(webhook.channel_type().to_string(), webhook);

        let (inbound_tx, _) = broadcast::channel(512);

        Arc::new(Self {
            drivers,
            templates: Mutex::new(HashMap::new()),
            active: Mutex::new(HashMap::new()),
            inbound_tx,
            config_store,
        })
    }

    /// Convenience for tests: build a hub with a no-op config store.
    #[must_use]
    pub fn new_without_store(http_client: reqwest::Client) -> Arc<Self> {
        Self::new(http_client, Arc::new(NoopConfigStore))
    }

    // ── Driver introspection ─────────────────────────────────────────────────

    #[must_use]
    pub fn list_drivers(&self) -> Vec<DriverMetadata> {
        let mut out: Vec<DriverMetadata> = self
            .drivers
            .values()
            .map(|d| DriverMetadata {
                channel_type: d.channel_type().to_string(),
                direction: d.direction(),
                capabilities: d.capabilities(),
            })
            .collect();
        out.sort_by(|a, b| a.channel_type.cmp(&b.channel_type));
        out
    }

    #[must_use]
    pub fn supports(&self, channel_type: &str) -> bool {
        self.drivers.contains_key(channel_type)
    }

    #[must_use]
    pub fn driver_direction(&self, channel_type: &str) -> Option<ChannelDirection> {
        self.drivers.get(channel_type).map(|d| d.direction())
    }

    // ── Template registry ────────────────────────────────────────────────────

    /// Register per-channel templates for an app from a JSONC string.
    pub fn register_jsonc_template(&self, jsonc: &str) -> Result<(), jsonc_parser::errors::ParseError> {
        use crate::template::json_template::AppNotifyTemplate;

        let config: AppNotifyTemplate =
            jsonc_parser::parse_to_serde_value(jsonc, &jsonc_parser::ParseOptions::default())?;

        let mut map = self.templates.lock().unwrap_or_else(|e| e.into_inner());
        for (channel_type, channel_tmpl) in config.channels {
            let renderer: TemplateFn = Arc::new(move |ctx| channel_tmpl.render(ctx));
            map.insert((config.app_id.clone(), channel_type), renderer);
        }
        Ok(())
    }

    // ── Outbound send ────────────────────────────────────────────────────────

    pub async fn send(
        &self,
        app_id: &str,
        channel_type: &str,
        config: &Value,
        context: TemplateContext,
    ) -> Result<(), ChannelError> {
        let driver = self
            .drivers
            .get(channel_type)
            .ok_or_else(|| ChannelError::UnsupportedChannel(channel_type.to_string()))?;

        if !driver.direction().supports_outbound() {
            return Err(ChannelError::Unsupported(format!(
                "channel '{channel_type}' is inbound-only"
            )));
        }

        let key = (app_id.to_string(), channel_type.to_string());
        let rendered = {
            let map = self.templates.lock().expect("templates mutex poisoned");
            map.get(&key).cloned()
        };
        let rendered = rendered.map_or_else(|| render_default(&context), |tpl| tpl(&context));

        driver.send(config, &rendered).await?;
        info!("channel message sent via {channel_type}");
        Ok(())
    }

    pub async fn send_many(
        &self,
        app_id: &str,
        targets: Vec<SendTarget>,
        context: TemplateContext,
    ) -> Vec<Result<(), ChannelError>> {
        let mut results = Vec::with_capacity(targets.len());
        for target in &targets {
            let res = self
                .send(app_id, &target.channel_type, &target.config, context.clone())
                .await;
            if let Err(ref e) = res {
                warn!("channel send to {} failed: {e}", target.channel_type);
            }
            results.push(res);
        }
        results
    }

    // ── Inbound lifecycle ────────────────────────────────────────────────────

    pub fn subscribe_inbound(&self) -> broadcast::Receiver<InboundEvent> {
        self.inbound_tx.subscribe()
    }

    /// Activate a channel's inbound pump (if the driver has one). No-op for
    /// drivers that only use webhook mode or are outbound-only.
    pub async fn activate(&self, channel_id: Uuid, channel_type: &str, config: &Value) -> Result<(), ChannelError> {
        self.deactivate(channel_id); // idempotent reload

        let driver = self
            .drivers
            .get(channel_type)
            .ok_or_else(|| ChannelError::UnsupportedChannel(channel_type.to_string()))?;
        let Some(inbound) = driver.inbound() else {
            return Ok(());
        };

        let pump = match inbound
            .start_pump(
                config,
                channel_id,
                InboundEmitter(self.inbound_tx.clone()),
                ConfigWriter::new(self.config_store.clone(), channel_id),
            )
            .await
        {
            Ok(handle) => Some(handle),
            Err(ChannelError::Unsupported(_)) => None, // webhook-only driver
            Err(e) => return Err(e),
        };

        self.active
            .lock()
            .expect("active mutex poisoned")
            .insert(channel_id, ActiveChannel { pump });
        info!(%channel_id, %channel_type, "channel activated");
        Ok(())
    }

    /// Stop any active pump for `channel_id`.
    pub fn deactivate(&self, channel_id: Uuid) {
        if let Some(active) = self.active.lock().expect("active mutex poisoned").remove(&channel_id)
            && let Some(pump) = active.pump
        {
            pump.stop();
            info!(%channel_id, "channel deactivated");
        }
    }

    /// Dispatch a received webhook payload for a given channel. Returns the
    /// [`WebhookOutcome`] containing the decoded event (also broadcast to
    /// subscribers) and/or a reply body to echo back to the platform.
    pub async fn dispatch_webhook(
        &self,
        channel_id: Uuid,
        channel_type: &str,
        config: &Value,
        headers: &axum::http::HeaderMap,
        body: bytes::Bytes,
    ) -> Result<WebhookOutcome, ChannelError> {
        let driver = self
            .drivers
            .get(channel_type)
            .ok_or_else(|| ChannelError::UnsupportedChannel(channel_type.to_string()))?;
        let inbound = driver
            .inbound()
            .ok_or_else(|| ChannelError::Unsupported(format!("{channel_type} has no inbound")))?;
        let outcome = inbound.parse_webhook(config, channel_id, headers, body).await?;
        if let Some(ref ev) = outcome.event {
            let _ = self.inbound_tx.send(ev.clone());
        }
        Ok(outcome)
    }

    /// Acknowledge an inbound event via driver-specific feedback (e.g. Feishu
    /// reaction). No-op for drivers that don't implement `ack_inbound`.
    pub async fn ack_inbound(
        &self,
        channel_type: &str,
        config: &Value,
        event: &InboundEvent,
    ) -> Result<(), ChannelError> {
        let driver = self
            .drivers
            .get(channel_type)
            .ok_or_else(|| ChannelError::UnsupportedChannel(channel_type.to_string()))?;
        let Some(inbound) = driver.inbound() else {
            return Ok(());
        };
        inbound.ack_inbound(config, event).await
    }

    /// Send a plain-text reply directly to a user in an inbound-capable
    /// channel. Dispatches to the driver's [`InboundDriver::reply_to_user`]
    /// implementation. Used by the AI inbound router to post assistant
    /// answers back to the originating platform.
    pub async fn reply_to_user(
        &self,
        channel_type: &str,
        config: &Value,
        external_user_id: &str,
        external_thread_id: &str,
        text: &str,
    ) -> Result<(), ChannelError> {
        let driver = self
            .drivers
            .get(channel_type)
            .ok_or_else(|| ChannelError::UnsupportedChannel(channel_type.to_string()))?;
        let inbound = driver
            .inbound()
            .ok_or_else(|| ChannelError::Unsupported(format!("channel '{channel_type}' has no inbound driver")))?;
        inbound
            .reply_to_user(config, external_user_id, external_thread_id, text)
            .await
    }

    /// Stream a reply to a user as tokens are produced. The caller pushes
    /// [`StreamReplyChunk`]s (each carrying the full accumulated text) into
    /// the returned sender; dropping the sender or sending a `terminal=true`
    /// chunk closes the stream. Drivers that don't support native streaming
    /// silently fall back to a buffered one-shot send.
    pub async fn reply_to_user_streaming(
        &self,
        channel_type: &str,
        config: &Value,
        external_user_id: &str,
        external_thread_id: &str,
        rx: tokio::sync::mpsc::Receiver<crate::inbound::StreamReplyChunk>,
    ) -> Result<(), ChannelError> {
        let driver = self
            .drivers
            .get(channel_type)
            .ok_or_else(|| ChannelError::UnsupportedChannel(channel_type.to_string()))?;
        let inbound = driver
            .inbound()
            .ok_or_else(|| ChannelError::Unsupported(format!("channel '{channel_type}' has no inbound driver")))?;
        inbound
            .reply_to_user_streaming(config, external_user_id, external_thread_id, rx)
            .await
    }

    /// Run a connectivity check for the given channel type against its
    /// current config. Delegates to [`ChannelDriver::check_connection`],
    /// which by default TCP-probes each of the driver's declared endpoints.
    pub async fn check_connection(&self, channel_type: &str, config: &Value) -> Result<(), ChannelError> {
        let driver = self
            .drivers
            .get(channel_type)
            .ok_or_else(|| ChannelError::UnsupportedChannel(channel_type.to_string()))?;
        driver.check_connection(config).await
    }
}
