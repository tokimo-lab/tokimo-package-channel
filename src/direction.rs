use serde::{Deserialize, Serialize};

/// Which directions a channel driver supports.
///
/// Persisted to DB (lowercase) and exposed via `ts-rs` to the frontend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelDirection {
    /// Can only send messages out to the external platform.
    Outbound,
    /// Can only receive messages from the external platform.
    Inbound,
    /// Supports both sending and receiving.
    Bidirectional,
}

impl ChannelDirection {
    #[must_use]
    pub fn supports_outbound(self) -> bool {
        matches!(self, Self::Outbound | Self::Bidirectional)
    }

    #[must_use]
    pub fn supports_inbound(self) -> bool {
        matches!(self, Self::Inbound | Self::Bidirectional)
    }

    /// Label for admin UI.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Outbound => "outbound",
            Self::Inbound => "inbound",
            Self::Bidirectional => "bidirectional",
        }
    }
}
