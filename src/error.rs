use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChannelError {
    #[error("unsupported channel type: {0}")]
    UnsupportedChannel(String),

    #[error("operation not supported by this channel: {0}")]
    Unsupported(String),

    #[error("channel config error: {0}")]
    ConfigError(String),

    #[error("http request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("channel returned error: {status} – {body}")]
    ChannelRejected { status: u16, body: String },

    #[error("inbound webhook signature mismatch")]
    SignatureMismatch,

    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("template render error: {0}")]
    TemplateError(String),

    #[error("{0}")]
    Other(String),
}
