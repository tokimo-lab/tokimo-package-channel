//! WeClaw (iLink / ClawBot) driver — bidirectional:
//!
//! * **Outbound** — renders text messages to the user via `sendmessage`.
//! * **Inbound**  — a long-poll pump against `getupdates`. Every response
//!   carries an updated `get_updates_buf` cursor and any fresh messages the
//!   user sent to the bot. Messages include a `context_token` which is
//!   required for outbound; the pump persists refreshed credentials back to
//!   DB via [`ConfigWriter`] so subsequent sends can use them immediately.
//!
//! The pump replaces the previous ad-hoc "spawn a 5-minute one-shot task after
//! activate" mechanism, and means the server will pick up `context_token`
//! whenever the user messages the bot, even across restarts.

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use ecb::cipher::{BlockEncryptMut, KeyInit, block_padding::Pkcs7};
use md5::Digest;
use rand::Rng;
use serde_json::Value;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::capability::{ChannelCapabilities, InboundKind};
use crate::config_store::ConfigWriter;
use crate::direction::ChannelDirection;
use crate::driver::ChannelDriver;
use crate::error::ChannelError;
use crate::inbound::{InboundDriver, InboundEmitter, InboundEvent, InboundEventKind, PumpHandle};
use crate::template::RenderedMessage;

const ILINK_BASE: &str = "https://ilinkai.weixin.qq.com";
const CDN_BASE_URL: &str = "https://novac2c.cdn.weixin.qq.com/c2c";
const CHANNEL_VERSION: &str = "1.0.2";

// iLink message item types.
const ITEM_TYPE_IMAGE: u32 = 2;
const ITEM_TYPE_FILE: u32 = 4;
const ITEM_TYPE_VIDEO: u32 = 5;

// getuploadurl media type values.
const UPLOAD_MEDIA_TYPE_IMAGE: u32 = 1;
const UPLOAD_MEDIA_TYPE_VIDEO: u32 = 2;
const UPLOAD_MEDIA_TYPE_FILE: u32 = 3;

type Aes128EcbEnc = ecb::Encryptor<aes::Aes128>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaKind {
    Image,
    Video,
    File,
}

impl MediaKind {
    fn upload_media_type(self) -> u32 {
        match self {
            Self::Image => UPLOAD_MEDIA_TYPE_IMAGE,
            Self::Video => UPLOAD_MEDIA_TYPE_VIDEO,
            Self::File => UPLOAD_MEDIA_TYPE_FILE,
        }
    }

    fn from_mime(mime: &str) -> Self {
        if mime.starts_with("image/") {
            Self::Image
        } else if mime.starts_with("video/") {
            Self::Video
        } else {
            Self::File
        }
    }
}

pub struct WeclawDriver {
    client: reqwest::Client,
}

impl WeclawDriver {
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ChannelDriver for WeclawDriver {
    fn channel_type(&self) -> &'static str {
        "weclaw"
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            supports_markdown: false,
            supports_card: false,
            supports_image: true,
            max_text_length: 0,
        }
    }

    async fn send(&self, config: &Value, message: &RenderedMessage) -> Result<(), ChannelError> {
        let creds: rust_client_api::weclaw::WeclawCredentials = serde_json::from_value(config.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid weclaw config: {e}")))?;

        debug!("sending weclaw message to user {}", creds.user_id);

        rust_client_api::weclaw::send_message(&self.client, &creds, &message.text)
            .await
            .map_err(|e| ChannelError::ChannelRejected { status: 0, body: e })
    }

    fn inbound(&self) -> Option<&dyn InboundDriver> {
        Some(self)
    }

    fn connectivity_probes(&self, _config: &Value) -> Vec<(String, u16)> {
        vec![("ilinkai.weixin.qq.com".to_string(), 443)]
    }
}

#[async_trait]
impl InboundDriver for WeclawDriver {
    fn kind(&self) -> InboundKind {
        InboundKind::Pump
    }

    async fn reply_to_user(
        &self,
        config: &Value,
        _external_user_id: &str,
        _external_thread_id: &str,
        text: &str,
    ) -> Result<(), ChannelError> {
        // iLink bots are 1:1-bound to a single WeChat user (stored in
        // `creds.user_id`), and `send_message` already targets that user
        // using the latest `context_token`. The external ids are therefore
        // redundant here — we delegate to the normal send path.
        let creds: rust_client_api::weclaw::WeclawCredentials = serde_json::from_value(config.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid weclaw config: {e}")))?;

        rust_client_api::weclaw::send_message(&self.client, &creds, text)
            .await
            .map_err(|e| ChannelError::ChannelRejected { status: 0, body: e })
    }

    async fn reply_file_to_user(
        &self,
        config: &Value,
        _external_user_id: &str,
        _external_thread_id: &str,
        file: &crate::file::FilePayload,
        caption: Option<&str>,
    ) -> Result<(), ChannelError> {
        // iLink bots are 1:1-bound to a single user (creds.user_id) and use the
        // most recent context_token. The external ids are redundant — same as
        // reply_to_user above.
        let creds: rust_client_api::weclaw::WeclawCredentials = serde_json::from_value(config.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid weclaw config: {e}")))?;

        let (data, filename, content_type) = crate::file::resolve_to_bytes(&self.client, file).await?;
        let mime = content_type
            .clone()
            .unwrap_or_else(|| crate::file::guess_content_type(&filename).to_string());
        let kind = MediaKind::from_mime(&mime);

        send_file(&self.client, &creds, kind, &filename, &data)
            .await
            .map_err(|e| ChannelError::ChannelRejected { status: 0, body: e })?;

        if let Some(text) = caption.map(str::trim).filter(|s| !s.is_empty()) {
            rust_client_api::weclaw::send_message(&self.client, &creds, text)
                .await
                .map_err(|e| ChannelError::ChannelRejected { status: 0, body: e })?;
        }

        Ok(())
    }

    async fn start_pump(
        &self,
        config: &Value,
        channel_id: Uuid,
        emit: InboundEmitter,
        writer: ConfigWriter,
    ) -> Result<PumpHandle, ChannelError> {
        let mut creds: rust_client_api::weclaw::WeclawCredentials = serde_json::from_value(config.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid weclaw config: {e}")))?;

        let client = self.client.clone();
        let cancel = CancellationToken::new();
        let cancel_child = cancel.clone();

        let task = tokio::spawn(async move {
            info!(%channel_id, "weclaw pump started");
            loop {
                if cancel_child.is_cancelled() {
                    debug!(%channel_id, "weclaw pump cancelled");
                    break;
                }

                let poll = rust_client_api::weclaw::poll_updates(&client, &creds);
                let outcome = tokio::select! {
                    res = poll => Some(res),
                    () = cancel_child.cancelled() => None,
                };

                let Some(res) = outcome else {
                    break;
                };

                match res {
                    Ok((updated, inbound_msgs)) => {
                        let buf_changed = updated.get_updates_buf != creds.get_updates_buf;
                        let token_changed =
                            updated.context_token.is_some() && updated.context_token != creds.context_token;

                        // Adopt new state in-memory first so the next poll uses
                        // the updated cursor, then persist so outbound sends
                        // can see the fresh context_token.
                        creds = updated.clone();

                        if buf_changed || token_changed {
                            match serde_json::to_value(&creds) {
                                Ok(new_config) => {
                                    if let Err(e) = writer.write(new_config).await {
                                        warn!(%channel_id, "weclaw persist creds failed: {e}");
                                    } else if token_changed {
                                        info!(%channel_id, "weclaw context_token refreshed");
                                    }
                                }
                                Err(e) => warn!(%channel_id, "weclaw serialize creds failed: {e}"),
                            }
                        }

                        // Forward each inbound user message as a Message event
                        // so the AI router can pick it up. Non-text messages
                        // (images/voice/etc.) arrive as Message with empty
                        // text for now — upstream can filter.
                        for msg in inbound_msgs {
                            let Some(text) = msg.text else {
                                debug!(%channel_id, "weclaw: skipping non-text inbound item");
                                continue;
                            };

                            // `/new` etc. → Command. Anything else → Message.
                            let trimmed = text.trim_start();
                            let kind = if let Some(stripped) = trimmed.strip_prefix('/') {
                                let (name, args) = stripped
                                    .split_once(char::is_whitespace)
                                    .map_or((stripped, ""), |(a, b)| (a, b));
                                InboundEventKind::Command {
                                    name: name.trim().to_string(),
                                    args: args.trim().to_string(),
                                }
                            } else {
                                InboundEventKind::Message {
                                    text,
                                    attachments: Vec::new(),
                                }
                            };

                            emit.send(InboundEvent {
                                channel_id,
                                channel_type: "weclaw".into(),
                                external_thread_id: msg.from_user_id.clone(),
                                external_user_id: Some(msg.from_user_id.clone()),
                                kind,
                                received_at: Utc::now(),
                                raw: Value::Null,
                            });
                        }
                    }
                    Err(e) => {
                        warn!(%channel_id, "weclaw poll_updates error: {e}");
                        // Back off on transient errors so we don't hot-loop
                        // against iLink.
                        tokio::select! {
                            () = sleep(Duration::from_secs(5)) => {}
                            () = cancel_child.cancelled() => break,
                        }
                    }
                }
            }
            info!(%channel_id, "weclaw pump stopped");
        });

        Ok(PumpHandle { cancel, task })
    }
}

// --- iLink CDN file upload (encrypt → getuploadurl → upload → sendmessage) ---

fn random_wechat_uin() -> String {
    let n: u32 = rand::thread_rng().r#gen();
    base64::engine::general_purpose::STANDARD.encode(n.to_string())
}

fn aes_padded_size(plain_len: usize) -> usize {
    ((plain_len / 16) + 1) * 16
}

fn encrypt_aes_ecb(plaintext: &[u8], key: &[u8; 16]) -> Vec<u8> {
    let padded_size = aes_padded_size(plaintext.len());
    let mut buffer = vec![0u8; padded_size];
    buffer[..plaintext.len()].copy_from_slice(plaintext);
    let encrypted = Aes128EcbEnc::new(key.into())
        .encrypt_padded_mut::<Pkcs7>(&mut buffer, plaintext.len())
        .expect("AES ECB padded buffer is correctly sized");
    encrypted.to_vec()
}

async fn request_upload_param(
    client: &reqwest::Client,
    creds: &rust_client_api::weclaw::WeclawCredentials,
    kind: MediaKind,
    bytes: &[u8],
    aes_key: &[u8; 16],
    filekey: &str,
) -> Result<String, String> {
    let md5_hex = hex::encode(md5::Md5::digest(bytes));
    let body = serde_json::json!({
        "filekey": filekey,
        "media_type": kind.upload_media_type(),
        "to_user_id": creds.user_id,
        "rawsize": bytes.len(),
        "rawfilemd5": md5_hex,
        "filesize": aes_padded_size(bytes.len()),
        "no_need_thumb": true,
        "aeskey": hex::encode(aes_key),
        "base_info": { "channel_version": CHANNEL_VERSION }
    });

    let url = format!("{ILINK_BASE}/ilink/bot/getuploadurl");
    let resp = client
        .post(&url)
        .header("AuthorizationType", "ilink_bot_token")
        .header("X-WECHAT-UIN", random_wechat_uin())
        .header("Authorization", format!("Bearer {}", creds.bot_token))
        .json(&body)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| format!("iLink getuploadurl request failed: {e}"))?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!("iLink getuploadurl HTTP {}: {text}", status.as_u16()));
    }
    let parsed: Value =
        serde_json::from_str(&text).map_err(|e| format!("iLink getuploadurl parse failed: {e}: {text}"))?;
    let ret = parsed.get("ret").and_then(Value::as_i64).unwrap_or(0);
    let errcode = parsed.get("errcode").and_then(Value::as_i64).unwrap_or(0);
    if ret != 0 || errcode != 0 {
        return Err(format!(
            "iLink getuploadurl error: ret={ret}, errcode={errcode}, body={text}"
        ));
    }
    parsed
        .get("upload_param")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("iLink getuploadurl returned no upload_param: {text}"))
}

async fn upload_to_cdn(
    client: &reqwest::Client,
    upload_param: &str,
    filekey: &str,
    ciphertext: &[u8],
) -> Result<String, String> {
    let url = format!(
        "{CDN_BASE_URL}/upload?encrypted_query_param={}&filekey={}",
        urlencoding::encode(upload_param),
        urlencoding::encode(filekey)
    );

    let mut last_error: Option<String> = None;
    for attempt in 1..=3u32 {
        let resp = client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(ciphertext.to_vec())
            .timeout(Duration::from_mins(1))
            .send()
            .await;

        match resp {
            Ok(r) if r.status().is_success() => {
                return r
                    .headers()
                    .get("x-encrypted-param")
                    .and_then(|v| v.to_str().ok())
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| "iLink CDN upload missing x-encrypted-param header".to_string());
            }
            Ok(r) => {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                let msg = format!("iLink CDN upload attempt {attempt} failed ({status}): {body}");
                if status.is_client_error() {
                    return Err(msg);
                }
                last_error = Some(msg);
            }
            Err(e) => last_error = Some(format!("iLink CDN upload attempt {attempt} request failed: {e}")),
        }

        sleep(Duration::from_millis(500)).await;
    }
    Err(last_error.unwrap_or_else(|| "iLink CDN upload failed".into()))
}

/// Encrypt the payload, upload to WeChat CDN, then send a message referencing
/// the uploaded media via the standard sendmessage endpoint.
async fn send_file(
    client: &reqwest::Client,
    creds: &rust_client_api::weclaw::WeclawCredentials,
    kind: MediaKind,
    filename: &str,
    bytes: &[u8],
) -> Result<(), String> {
    let context_token = creds
        .context_token
        .as_deref()
        .ok_or("context_token not available — user must send a message to ClawBot first")?;

    let filekey = Uuid::new_v4().simple().to_string();
    let aes_key: [u8; 16] = rand::thread_rng().r#gen();

    let upload_param = request_upload_param(client, creds, kind, bytes, &aes_key, &filekey).await?;
    let ciphertext = encrypt_aes_ecb(bytes, &aes_key);
    let encrypted_query_param = upload_to_cdn(client, &upload_param, &filekey, &ciphertext).await?;
    let aes_key_b64 = base64::engine::general_purpose::STANDARD.encode(aes_key);

    let item = match kind {
        MediaKind::Image => serde_json::json!({
            "type": ITEM_TYPE_IMAGE,
            "image_item": {
                "media": {
                    "encrypt_query_param": encrypted_query_param,
                    "aes_key": aes_key_b64,
                    "encrypt_type": 1
                },
                "mid_size": ciphertext.len()
            }
        }),
        MediaKind::Video => serde_json::json!({
            "type": ITEM_TYPE_VIDEO,
            "video_item": {
                "media": {
                    "encrypt_query_param": encrypted_query_param,
                    "aes_key": aes_key_b64,
                    "encrypt_type": 1
                },
                "video_size": ciphertext.len()
            }
        }),
        MediaKind::File => serde_json::json!({
            "type": ITEM_TYPE_FILE,
            "file_item": {
                "media": {
                    "encrypt_query_param": encrypted_query_param,
                    "aes_key": aes_key_b64,
                    "encrypt_type": 1
                },
                "file_name": filename,
                "len": bytes.len().to_string()
            }
        }),
    };

    let client_id = format!(
        "tokimo-weixin:{}-{:08x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        rand::thread_rng().r#gen::<u32>()
    );

    let body = serde_json::json!({
        "msg": {
            "from_user_id": "",
            "to_user_id": creds.user_id,
            "client_id": client_id,
            "context_token": context_token,
            "item_list": [item],
            "message_type": 2,
            "message_state": 2
        },
        "base_info": { "channel_version": CHANNEL_VERSION }
    });

    let url = format!("{ILINK_BASE}/ilink/bot/sendmessage");
    let resp = client
        .post(&url)
        .header("AuthorizationType", "ilink_bot_token")
        .header("X-WECHAT-UIN", random_wechat_uin())
        .header("Authorization", format!("Bearer {}", creds.bot_token))
        .json(&body)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| format!("iLink sendmessage (file) request failed: {e}"))?;

    let status = resp.status();
    let resp_text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!(
            "iLink sendmessage (file) HTTP {}: {resp_text}",
            status.as_u16()
        ));
    }
    if let Ok(parsed) = serde_json::from_str::<Value>(&resp_text) {
        let ret = parsed.get("ret").and_then(Value::as_i64).unwrap_or(0);
        let errcode = parsed.get("errcode").and_then(Value::as_i64).unwrap_or(0);
        if ret != 0 || errcode != 0 {
            let errmsg = parsed
                .get("errmsg")
                .or_else(|| parsed.get("err_msg"))
                .and_then(Value::as_str)
                .unwrap_or("");
            return Err(format!(
                "iLink sendmessage (file) error: ret={ret}, errcode={errcode}, msg={errmsg}"
            ));
        }
    }

    Ok(())
}
