//! WeCom (企业微信) driver — bidirectional.
//!
//! Two inbound paths coexist in the WeCom ecosystem:
//! 1. **Group robot webhook** (`webhookUrl`, key-based) — outbound only. We
//!    keep supporting it for one-way notifications.
//! 2. **Enterprise application callback** — bidirectional. An app receives
//!    user messages via an HTTPS endpoint signed with `token` and encrypted
//!    with `encodingAesKey`, and replies via the `cgi-bin/message/send` API
//!    using an access token derived from `corpId`/`corpSecret`.
//!
//! Config:
//! ```jsonc
//! {
//!   "webhookUrl":      "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...", // optional, outbound only
//!   "corpId":          "wwxxxxxxxx",
//!   "corpSecret":      "xxxxxx",
//!   "agentId":         "1000002",
//!   "token":           "signature-token",
//!   "encodingAesKey":  "43-char-aes-key"
//! }
//! ```
//!
//! Caveats:
//! * The WeCom "URL verification" step uses a GET request with an `echostr`
//!   query param. Our public route is POST-only (`handle_channel_webhook`),
//!   so verification must be completed manually or by temporarily pointing
//!   the callback at a helper tool. Once the app is verified, all further
//!   traffic is POST and handled here.
//! * Replies use the active `message/send` API (encrypted passive reply is
//!   not implemented).

use std::sync::Mutex;
use std::time::Instant;

use aes::Aes256;
use aes::cipher::{BlockDecryptMut, KeyIvInit};
use async_trait::async_trait;
use axum::http::HeaderMap;
use base64::Engine;
use bytes::Bytes;
use cbc::Decryptor;
use chrono::Utc;
use quick_xml::events::Event;
use serde::Deserialize;
use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use tracing::debug;
use uuid::Uuid;

use crate::capability::{ChannelCapabilities, InboundKind};
use crate::direction::ChannelDirection;
use crate::driver::ChannelDriver;
use crate::error::ChannelError;
use crate::inbound::{InboundDriver, InboundEvent, InboundEventKind, WebhookOutcome};
use crate::template::RenderedMessage;

const WECOM_API_BASE: &str = "https://qyapi.weixin.qq.com/cgi-bin";

type Aes256CbcDec = Decryptor<Aes256>;

struct CachedToken {
    token: String,
    expires_at: Instant,
}

pub struct WecomDriver {
    client: reqwest::Client,
    token_cache: Mutex<Vec<((String, String), CachedToken)>>,
}

impl WecomDriver {
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            token_cache: Mutex::new(Vec::new()),
        }
    }

    async fn access_token(&self, corp_id: &str, corp_secret: &str) -> Result<String, ChannelError> {
        let key = (corp_id.to_string(), corp_secret.to_string());
        {
            let cache = self.token_cache.lock().expect("wecom token cache poisoned");
            if let Some((_, hit)) = cache.iter().find(|(k, _)| *k == key)
                && hit.expires_at > Instant::now()
            {
                return Ok(hit.token.clone());
            }
        }

        let url = format!("{WECOM_API_BASE}/gettoken?corpid={corp_id}&corpsecret={corp_secret}");
        let resp = self.client.get(&url).send().await?;
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        if status != 200 {
            return Err(ChannelError::ChannelRejected { status, body });
        }
        #[derive(Deserialize)]
        struct TokenResp {
            errcode: i64,
            #[serde(default)]
            errmsg: Option<String>,
            #[serde(default)]
            access_token: Option<String>,
            #[serde(default)]
            expires_in: Option<u64>,
        }
        let parsed: TokenResp = serde_json::from_str(&body)
            .map_err(|e| ChannelError::Other(format!("decode wecom token response: {e}")))?;
        if parsed.errcode != 0 {
            return Err(ChannelError::ChannelRejected {
                status,
                body: format!("errcode={} errmsg={:?}", parsed.errcode, parsed.errmsg),
            });
        }
        let token = parsed
            .access_token
            .ok_or_else(|| ChannelError::Other("missing access_token".into()))?;
        let ttl = parsed.expires_in.unwrap_or(7200).saturating_sub(60);
        let expires_at = Instant::now() + std::time::Duration::from_secs(ttl);

        let mut cache = self.token_cache.lock().expect("wecom token cache poisoned");
        cache.retain(|(k, _)| *k != key);
        cache.push((
            key,
            CachedToken {
                token: token.clone(),
                expires_at,
            },
        ));
        Ok(token)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct WecomConfig {
    #[serde(default)]
    webhook_url: Option<String>,
    #[serde(default)]
    corp_id: Option<String>,
    #[serde(default)]
    corp_secret: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    encoding_aes_key: Option<String>,
}

impl WecomConfig {
    fn from_value(v: &Value) -> Result<Self, ChannelError> {
        serde_json::from_value::<Self>(v.clone())
            .map_err(|e| ChannelError::ConfigError(format!("invalid wecom config: {e}")))
    }
}

#[async_trait]
impl ChannelDriver for WecomDriver {
    fn channel_type(&self) -> &'static str {
        "wecom"
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            supports_markdown: true,
            supports_card: false,
            supports_image: false,
            max_text_length: 4096,
        }
    }

    async fn send(&self, config: &Value, message: &RenderedMessage) -> Result<(), ChannelError> {
        let webhook_url = config
            .get("webhookUrl")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ChannelError::ConfigError("missing webhookUrl".into()))?;

        let body = if let Some(md) = &message.markdown {
            json!({
                "msgtype": "markdown",
                "markdown": { "content": md },
            })
        } else {
            json!({
                "msgtype": "text",
                "text": { "content": &message.text },
            })
        };

        debug!(url = %webhook_url, "sending wecom notification");
        let resp = self.client.post(webhook_url).json(&body).send().await?;
        let status = resp.status().as_u16();
        let resp_body = resp.text().await.unwrap_or_default();
        if status != 200 {
            return Err(ChannelError::ChannelRejected {
                status,
                body: resp_body,
            });
        }
        if let Ok(parsed) = serde_json::from_str::<Value>(&resp_body) {
            let code = parsed.get("errcode").and_then(Value::as_i64).unwrap_or(0);
            if code != 0 {
                let msg = parsed.get("errmsg").and_then(Value::as_str).unwrap_or("unknown");
                return Err(ChannelError::ChannelRejected {
                    status,
                    body: format!("errcode={code}, errmsg={msg}"),
                });
            }
        }
        Ok(())
    }

    fn inbound(&self) -> Option<&dyn InboundDriver> {
        Some(self)
    }

    fn connectivity_probes(&self, config: &Value) -> Vec<(String, u16)> {
        config
            .get("webhookUrl")
            .and_then(Value::as_str)
            .and_then(parse_host_port)
            .map_or_else(|| vec![("qyapi.weixin.qq.com".to_string(), 443)], |hp| vec![hp])
    }
}

fn parse_host_port(url: &str) -> Option<(String, u16)> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?.to_string();
    let port = parsed.port_or_known_default().unwrap_or(443);
    Some((host, port))
}

// ── Signature / crypto helpers ──────────────────────────────────────────────

fn compute_signature(token: &str, timestamp: &str, nonce: &str, encrypt: &str) -> String {
    let mut parts = [token, timestamp, nonce, encrypt];
    parts.sort_unstable();
    let joined = parts.concat();
    let mut hasher = Sha1::new();
    hasher.update(joined.as_bytes());
    hex::encode(hasher.finalize())
}

fn decode_aes_key(encoding_aes_key: &str) -> Result<[u8; 32], ChannelError> {
    // WeCom's encodingAESKey is 43 chars of URL-safe-ish base64 (no padding).
    // Append a single '=' to make it 44-char standard base64.
    let padded = format!("{encoding_aes_key}=");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(padded.as_bytes())
        .map_err(|e| ChannelError::ConfigError(format!("encodingAesKey base64: {e}")))?;
    let arr: [u8; 32] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| ChannelError::ConfigError("encodingAesKey must decode to 32 bytes".into()))?;
    Ok(arr)
}

/// Decrypt a WeCom AES-encrypted payload. Returns (plaintext_xml, receive_id).
fn decrypt_aes(encrypted_b64: &str, aes_key: &[u8; 32]) -> Result<(String, String), ChannelError> {
    let ciphertext = base64::engine::general_purpose::STANDARD
        .decode(encrypted_b64.trim().as_bytes())
        .map_err(|e| ChannelError::Other(format!("encrypt base64 decode: {e}")))?;
    if ciphertext.len() % 16 != 0 || ciphertext.is_empty() {
        return Err(ChannelError::Other("wecom ciphertext length invalid".into()));
    }

    let mut iv = [0u8; 16];
    iv.copy_from_slice(&aes_key[..16]);

    let mut buf = ciphertext.clone();
    // Decrypt in place without padding validation — WeCom uses PKCS#7 but we
    // strip it manually (see below) because the last byte reliably encodes
    // padding length.
    let decryptor = Aes256CbcDec::new(aes_key.into(), &iv.into());
    let blocks_count = buf.len() / 16;
    {
        // Process each block in place to avoid depending on the
        // block-padding feature of cbc.
        use aes::cipher::generic_array::GenericArray;
        let mut dec = decryptor;
        for chunk in buf.chunks_exact_mut(16).take(blocks_count) {
            let block = GenericArray::from_mut_slice(chunk);
            dec.decrypt_block_mut(block);
        }
    }

    // Strip PKCS#7 padding.
    let pad = *buf
        .last()
        .ok_or_else(|| ChannelError::Other("empty decrypted buffer".into()))? as usize;
    if pad == 0 || pad > 32 || pad > buf.len() {
        return Err(ChannelError::Other(format!("invalid padding: {pad}")));
    }
    buf.truncate(buf.len() - pad);

    // Layout: [16 random bytes][4-byte BE msg_len][msg_bytes][receive_id_bytes]
    if buf.len() < 20 {
        return Err(ChannelError::Other("decrypted buffer too short".into()));
    }
    let msg_len = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]) as usize;
    if buf.len() < 20 + msg_len {
        return Err(ChannelError::Other("msg_len overflows decrypted buffer".into()));
    }
    let msg = String::from_utf8(buf[20..20 + msg_len].to_vec())
        .map_err(|e| ChannelError::Other(format!("wecom plaintext utf8: {e}")))?;
    let receive_id = String::from_utf8(buf[20 + msg_len..].to_vec()).unwrap_or_default();
    Ok((msg, receive_id))
}

/// Extract `<Encrypt>...</Encrypt>` value from an XML body.
fn extract_xml_tag(xml: &[u8], tag: &str) -> Option<String> {
    let mut reader = quick_xml::Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut inside = false;
    let mut collected = String::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) if e.name().as_ref() == tag.as_bytes() => {
                inside = true;
            }
            Ok(Event::End(e)) if e.name().as_ref() == tag.as_bytes() => {
                return Some(collected);
            }
            Ok(Event::Text(t)) if inside => {
                if let Ok(s) = t.unescape() {
                    collected.push_str(&s);
                }
            }
            Ok(Event::CData(c)) if inside => {
                if let Ok(s) = std::str::from_utf8(c.as_ref()) {
                    collected.push_str(s);
                }
            }
            Ok(Event::Eof) | Err(_) => return None,
            _ => {}
        }
        buf.clear();
    }
}

fn extract_xml_fields(xml: &str, tags: &[&str]) -> std::collections::HashMap<String, String> {
    use std::collections::HashMap;
    let mut reader = quick_xml::Reader::from_reader(xml.as_bytes());
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut out: HashMap<String, String> = HashMap::new();
    let mut current: Option<String> = None;
    let mut accum = String::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tags.contains(&name.as_str()) {
                    current = Some(name);
                    accum.clear();
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if let Some(ref cur) = current
                    && *cur == name
                {
                    out.insert(cur.clone(), std::mem::take(&mut accum));
                    current = None;
                }
            }
            Ok(Event::Text(t)) if current.is_some() => {
                if let Ok(s) = t.unescape() {
                    accum.push_str(&s);
                }
            }
            Ok(Event::CData(c)) if current.is_some() => {
                if let Ok(s) = std::str::from_utf8(c.as_ref()) {
                    accum.push_str(s);
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    out
}

#[async_trait]
impl InboundDriver for WecomDriver {
    fn kind(&self) -> InboundKind {
        InboundKind::Webhook
    }

    async fn parse_webhook(
        &self,
        config: &Value,
        channel_id: Uuid,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Result<WebhookOutcome, ChannelError> {
        let cfg = WecomConfig::from_value(config)?;
        let token = cfg
            .token
            .as_deref()
            .ok_or_else(|| ChannelError::ConfigError("wecom token required for inbound".into()))?;
        let aes_key_raw = cfg
            .encoding_aes_key
            .as_deref()
            .ok_or_else(|| ChannelError::ConfigError("wecom encodingAesKey required for inbound".into()))?;
        let aes_key = decode_aes_key(aes_key_raw)?;

        // Signature + timestamp + nonce arrive as query string, which the
        // public route forwards via headers only in some setups. To support
        // both query-string and header placements, look in common header
        // slots first, then fall back to a subset of headers that proxies
        // occasionally surface.
        let msg_signature = header_or_empty(headers, "msg_signature");
        let timestamp = header_or_empty(headers, "timestamp");
        let nonce = header_or_empty(headers, "nonce");
        if msg_signature.is_empty() || timestamp.is_empty() || nonce.is_empty() {
            return Err(ChannelError::ConfigError(
                "wecom callback requires msg_signature/timestamp/nonce; forward them as headers".into(),
            ));
        }

        // Body is XML with <Encrypt>...</Encrypt>.
        let encrypt = extract_xml_tag(&body, "Encrypt")
            .ok_or_else(|| ChannelError::Other("wecom body missing <Encrypt>".into()))?;

        let expected = compute_signature(token, &timestamp, &nonce, &encrypt);
        if !constant_time_eq(expected.as_bytes(), msg_signature.as_bytes()) {
            return Err(ChannelError::SignatureMismatch);
        }

        let (plaintext_xml, _receive_id) = decrypt_aes(&encrypt, &aes_key)?;
        let fields = extract_xml_fields(
            &plaintext_xml,
            &[
                "MsgType",
                "Content",
                "FromUserName",
                "ToUserName",
                "MsgId",
                "AgentID",
                "CreateTime",
            ],
        );

        let msg_type = fields.get("MsgType").map_or("", String::as_str);
        if msg_type != "text" {
            // Event messages (subscribe, click, …) and other message types
            // are ignored for now.
            return Ok(WebhookOutcome::none());
        }

        let content = fields.get("Content").cloned().unwrap_or_default();
        let from_user = fields.get("FromUserName").cloned().unwrap_or_default();
        let msg_id = fields.get("MsgId").cloned().unwrap_or_default();
        if content.is_empty() || from_user.is_empty() {
            return Ok(WebhookOutcome::none());
        }

        // WeCom app DMs are 1:1 between the app and a user; use the user id
        // as both thread id and user id.
        let ev = InboundEvent {
            channel_id,
            channel_type: "wecom".into(),
            external_thread_id: from_user.clone(),
            external_user_id: Some(from_user),
            kind: InboundEventKind::Message {
                text: content,
                attachments: Vec::new(),
            },
            received_at: Utc::now(),
            raw: json!({ "msgId": msg_id }),
        };
        // WeCom expects an empty HTTP 200 body when not using passive reply.
        Ok(WebhookOutcome::event(ev))
    }

    async fn reply_to_user(
        &self,
        config: &Value,
        external_user_id: &str,
        _external_thread_id: &str,
        text: &str,
    ) -> Result<(), ChannelError> {
        let cfg = WecomConfig::from_value(config)?;
        let corp_id = cfg
            .corp_id
            .as_deref()
            .ok_or_else(|| ChannelError::ConfigError("wecom corpId required".into()))?;
        let corp_secret = cfg
            .corp_secret
            .as_deref()
            .ok_or_else(|| ChannelError::ConfigError("wecom corpSecret required".into()))?;
        let agent_id_str = cfg
            .agent_id
            .as_deref()
            .ok_or_else(|| ChannelError::ConfigError("wecom agentId required".into()))?;
        let agent_id: i64 = agent_id_str
            .parse()
            .map_err(|e| ChannelError::ConfigError(format!("wecom agentId must be integer: {e}")))?;

        let token = self.access_token(corp_id, corp_secret).await?;
        let url = format!("{WECOM_API_BASE}/message/send?access_token={token}");
        let body = json!({
            "touser": external_user_id,
            "msgtype": "text",
            "agentid": agent_id,
            "text": { "content": text },
        });
        let resp = self.client.post(&url).json(&body).send().await?;
        let status = resp.status().as_u16();
        let resp_body = resp.text().await.unwrap_or_default();
        if !(200..300).contains(&status) {
            return Err(ChannelError::ChannelRejected {
                status,
                body: resp_body,
            });
        }
        if let Ok(parsed) = serde_json::from_str::<Value>(&resp_body) {
            let code = parsed.get("errcode").and_then(Value::as_i64).unwrap_or(0);
            if code != 0 {
                return Err(ChannelError::ChannelRejected {
                    status,
                    body: resp_body,
                });
            }
        }
        Ok(())
    }
}

fn header_or_empty(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
