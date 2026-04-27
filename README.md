# tokimo-package-channel

Bidirectional channel abstraction for Tokimo — outbound notifications + inbound event ingress across 15+ platforms.

## Supported platforms

| Platform | Outbound | Inbound (WebSocket) |
|----------|----------|---------------------|
| Feishu / Lark | ✓ | ✓ |
| DingTalk | ✓ | ✓ |
| WeCom | ✓ | — |
| Telegram | ✓ | — |
| Slack | ✓ | ✓ |
| Discord | ✓ | ✓ |
| QQ Bot | ✓ | ✓ |
| Webhook | ✓ | — |
| Weclaw | ✓ | — |

## Usage

```rust
use tokimo_channel::{ChannelHub, ChannelDirection, ChannelCapabilities};

let hub = ChannelHub::new();

// Register drivers
hub.register("feishu", feishu_driver);

// Send a message
hub.send(
    "feishu",
    SendTarget::User("user_id".into()),
    "Hello from Tokimo!",
).await?;
```

## Architecture

- `driver/` — Channel driver implementations (15+ platforms)
- `hub.rs` — Channel hub: registry, routing, message dispatch
- `inbound.rs` — Inbound event abstraction (WebSocket long-connections)
- `template/` — Message templating (JSON template engine + built-in formatters)
- `capability.rs` — Channel capability declaration (supports inbound/outbound)
- `config_store.rs` — Channel configuration persistence trait
- `error.rs` — Unified error type

## License

MIT
