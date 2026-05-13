#![allow(
    clippy::print_stdout,
    clippy::panic,
    clippy::match_wild_err_arm,
    clippy::duration_suboptimal_units
)]

//! Repro test for the "Feishu bot channel silently breaks on Windows" bug.
//!
//! Symptom (from production logs on Windows):
//! ```text
//! 13:55:23.717 INFO feishu_bot ws connecting [v2 with 15s timeout] host=msg-frontier.feishu.cn
//!   ...silence for >6 minutes, no success, no `ws dial timed out after 15s` warn...
//! ```
//!
//! That is impossible by inspection of `connect_and_pump` — `tokio::time::timeout(15s, ...)`
//! must resolve. So either the future doesn't get scheduled, or the inner future
//! blocks the runtime in a way that prevents the timer from firing.
//!
//! This test exercises **only** the discover + WS dial steps, with verbose printing,
//! to determine experimentally whether:
//!   (a) connect_async returns Ok / Err / hangs past the timeout,
//!   (b) the timeout itself actually fires,
//!   (c) frames flow after a successful connect when the bot is messaged.
//!
//! Credentials come from the `feishu_bot` channel row in `tokimo_db` and are
//! baked in for reproducibility on the dev box. Override with
//! FEISHU_BOT_APP_ID / FEISHU_BOT_APP_SECRET env vars.
//!
//! Run with:
//!     cargo test -p tokimo-channel --test feishu_bot_ws_connect -- --nocapture --ignored

use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::json;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const DEFAULT_APP_ID: &str = "cli_a944fbe000f91cdd";
const DEFAULT_APP_SECRET: &str = "eSdBQfEKAjZcqnzpf7yOhh6wNnoUqXvo";

/// Install rustls' default crypto provider once. Without this the workspace
/// has both `ring` and `aws-lc-rs` in scope, rustls 0.23 refuses to pick one,
/// and `connect_async` panics with "Could not automatically determine the
/// process-level CryptoProvider" — the actual cause of the Windows symptom.
fn install_rustls_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[derive(Debug, Deserialize)]
struct EndpointResp {
    code: i64,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: Option<EndpointData>,
}

#[derive(Debug, Deserialize, Clone)]
struct EndpointData {
    #[serde(rename = "URL")]
    url: String,
}

fn creds() -> (String, String) {
    let id = std::env::var("FEISHU_BOT_APP_ID").unwrap_or_else(|_| DEFAULT_APP_ID.to_string());
    let secret = std::env::var("FEISHU_BOT_APP_SECRET").unwrap_or_else(|_| DEFAULT_APP_SECRET.to_string());
    (id, secret)
}

async fn discover_ws_url() -> String {
    let (app_id, app_secret) = creds();
    let http = reqwest::Client::builder().build().expect("build reqwest client");
    let body = http
        .post("https://open.feishu.cn/callback/ws/endpoint")
        .header("locale", "zh")
        .json(&json!({ "AppID": app_id, "AppSecret": app_secret }))
        .send()
        .await
        .expect("discover request failed")
        .text()
        .await
        .unwrap_or_default();
    let parsed: EndpointResp = serde_json::from_str(&body).expect("decode endpoint body");
    assert_eq!(parsed.code, 0, "discover business error: {}", parsed.msg);
    parsed.data.expect("data missing").url
}

/// Step 1: just hit the discover endpoint. Confirms outbound HTTPS works.
#[tokio::test]
#[ignore = "hits live Feishu API; run with --ignored"]
async fn discover_endpoint_works() {
    let t0 = Instant::now();
    let ws_url = discover_ws_url().await;
    println!("[discover] elapsed={:?} ws_url={ws_url}", t0.elapsed());
    assert!(ws_url.starts_with("wss://"));
}

/// Step 2: discover, then dial the WS URL with a 20s timeout and print exactly
/// what comes back. This is the closest match to what `connect_and_pump` does
/// in production.
#[tokio::test]
#[ignore = "hits live Feishu API; run with --ignored"]
async fn dial_websocket_with_timeout() {
    install_rustls_provider();
    let ws_url = discover_ws_url().await;
    println!("[dial] ws_url={ws_url}");

    let request = ws_url.as_str().into_client_request().expect("build ws request");

    let t0 = Instant::now();
    let outcome = tokio::time::timeout(Duration::from_secs(20), tokio_tungstenite::connect_async(request)).await;
    let elapsed = t0.elapsed();
    println!("[dial] elapsed={elapsed:?}");

    match outcome {
        Ok(Ok((_ws, resp))) => {
            println!("[dial] SUCCESS status={} headers={:?}", resp.status(), resp.headers());
        }
        Ok(Err(tokio_tungstenite::tungstenite::Error::Http(resp))) => {
            let status = resp.status();
            let hs_status = resp
                .headers()
                .get("Handshake-Status")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let hs_msg = resp
                .headers()
                .get("Handshake-Msg")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            panic!("ws handshake rejected (status={status}, Handshake-Status={hs_status}, Handshake-Msg={hs_msg})");
        }
        Ok(Err(e)) => {
            panic!("ws dial failed: {e} (kind: {e:?})");
        }
        Err(_) => {
            panic!("ws dial hung past 20s — timeout fired (elapsed={elapsed:?})");
        }
    }
}

/// Step 3: dial, then sit on the stream for 60s to see if frames flow when
/// the bot is messaged. Run interactively and send a message to the bot.
#[tokio::test]
#[ignore = "interactive: keep running while you send a message to the bot"]
async fn dial_and_observe_frames() {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;

    install_rustls_provider();
    let ws_url = discover_ws_url().await;
    println!("[observe] ws_url={ws_url}");

    let request = ws_url.as_str().into_client_request().expect("build ws request");
    let (ws, resp) = tokio::time::timeout(Duration::from_secs(20), tokio_tungstenite::connect_async(request))
        .await
        .expect("ws dial hung past 20s")
        .expect("ws dial errored");
    println!("[observe] connected, status={}", resp.status());

    let (_writer, mut reader) = ws.split();
    let observe_for = Duration::from_secs(60);
    println!("[observe] reading frames for {observe_for:?} — send a message to the bot now");
    let started = Instant::now();
    loop {
        let remaining = observe_for.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            println!("[observe] done");
            break;
        }
        match tokio::time::timeout(remaining, reader.next()).await {
            Err(_) => {
                println!("[observe] timeout reached, no more frames");
                break;
            }
            Ok(None) => {
                println!("[observe] stream ended");
                break;
            }
            Ok(Some(Err(e))) => {
                println!("[observe] read error: {e}");
                break;
            }
            Ok(Some(Ok(msg))) => match msg {
                WsMessage::Binary(b) => println!("[observe] binary frame: {} bytes", b.len()),
                WsMessage::Text(t) => println!("[observe] text frame: {t}"),
                WsMessage::Ping(_) => println!("[observe] ping"),
                WsMessage::Pong(_) => println!("[observe] pong"),
                WsMessage::Close(c) => {
                    println!("[observe] close: {c:?}");
                    break;
                }
                WsMessage::Frame(_) => {}
            },
        }
    }
}
