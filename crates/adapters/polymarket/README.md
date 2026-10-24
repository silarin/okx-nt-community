# OKX NT Community: `nautilus-polymarket`

This source crate is part of [OKX NT Community](../../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

[NautilusTrader](https://nautilustrader.io) adapter for the [Polymarket](https://polymarket.com) prediction market.

The `nautilus-polymarket` crate provides client implementations (HTTP & WebSocket), data
models and parsing for the **Polymarket CLOB API** for trading binary option contracts.

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `examples`: Enables the crate's example binaries.
- `extension-module`: Builds as a Python extension module.
- `high-precision` (default): Enables
  [high-precision mode](https://nautilustrader.io/docs/nightly/getting_started/installation/#precision-mode)
  to use 128-bit value types.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).

## API endpoints

The adapter communicates with these Polymarket API surfaces:

| API            | Base URL                                        | Auth                            | Purpose                                     |
| -------------- | ----------------------------------------------- | ------------------------------- | ------------------------------------------- |
| CLOB REST      | `https://clob.polymarket.com`                   | L2 HMAC                         | Orders, trades, balances.                   |
| CLOB WebSocket | `wss://ws-subscriptions-clob.polymarket.com/ws` | L2 HMAC (user channel)          | Streaming orderbook, trades, order updates. |
| PolyBolt       | `wss://ws-live-v2.polymarket.com/ws`            | L2 credentials (auth frame)     | Crypto, equity, and TWAP reference prices.  |
| Gamma          | `https://gamma-api.polymarket.com`              | None                            | Market and event discovery, tags, search.   |
| Data           | `https://data-api.polymarket.com`               | None                            | Trade history and user positions.           |
| Relayer        | `https://relayer-v2.polymarket.com`             | Relayer API key or Builder HMAC | Deposit Wallet positions and session keys.  |

## Authentication

The CLOB uses two-tier authentication:

- **L1 (EIP-712)**: Wallet-level signing for API credential creation and order signing
  via the CTF Exchange contract. Uses `alloy` signer crates.
- **L2 (HMAC-SHA256)**: API key + secret + passphrase for authenticated REST and
  WebSocket requests. Signatures expire after 30 seconds.

Deposit Wallet position operations use a separate Relayer API key, sent in the `RELAYER_API_KEY`
header with its signer address in `RELAYER_API_KEY_ADDRESS`.

Session authorization and revocation use owner signatures and Builder HMAC credentials.
`PolymarketSessionKeyClient` manages access outside the trading runtime; session trading uses
explicit session CLOB credentials and `PolymarketSignerType::Session`.

## Documentation

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-polymarket) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
