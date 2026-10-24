# OKX NT Community: `nautilus-kraken`

This source crate is part of [OKX NT Community](../../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

[NautilusTrader](https://nautilustrader.io) adapter for the [Kraken](https://www.kraken.com) exchange.

The `nautilus-kraken` crate provides client bindings (HTTP & WebSocket) and data models
for the official **Kraken API v2**.

The official Kraken API reference can be found at <https://docs.kraken.com/api/docs/>.

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Features

- HTTP REST API clients for market data (Spot v2, Futures v3).
- WebSocket clients for real-time data feeds (Spot v2, Futures).
- Support for both Spot and Futures markets.
- Instrument, ticker, trade, orderbook, and OHLC data.
- Prepared for execution support (orders, positions, balances) - WIP.

## Architecture

This crate provides **separate HTTP and WebSocket clients for Spot and Futures markets**.
This design reflects fundamental differences between the two APIs:

| Aspect         | Spot                  | Futures                      |
| -------------- | --------------------- | ---------------------------- |
| API Version    | REST API v2           | Derivatives API v3           |
| Base URL       | `api.kraken.com`      | `futures.kraken.com`         |
| Auth Headers   | `API-Key`, `API-Sign` | `APIKey`, `Authent`, `Nonce` |
| Request Format | URL-encoded form      | JSON body                    |
| WebSocket      | v2 protocol           | Futures-specific protocol    |

Kraken Futures was originally a separate platform (Crypto Facilities) acquired by Kraken,
which explains why the APIs remain distinct rather than unified.

### Client Types

- **`KrakenSpotHttpClient`** / **`KrakenSpotWebSocketClient`**: For spot trading pairs (e.g., `BTC/USD`, `ETH/EUR`).
- **`KrakenFuturesHttpClient`** / **`KrakenFuturesWebSocketClient`**: For perpetual and fixed-maturity futures (e.g., `PF_XBTUSD`, `PI_ETHUSD`).

### Bitcoin symbol format

| Market  | Format | Example     | Notes                                           |
| ------- | ------ | ----------- | ----------------------------------------------- |
| Spot    | `BTC`  | `BTC/USD`   | XBT normalized to BTC (base or quote position). |
| Futures | `XBT`  | `PI_XBTUSD` | Uses Kraken's native XBT format.                |

## Examples

See the `bin/` directory for example usage:

```bash
cargo run --bin kraken-http-spot-raw
cargo run --bin kraken-http-spot-public
cargo run --bin kraken-ws-spot-data
```

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `examples`: Enables the crate's example binaries.
- `extension-module`: Builds as a Python extension module.
- `high-precision` (default): Enables
  [high-precision mode](https://nautilustrader.io/docs/nightly/getting_started/installation/#precision-mode)
  to use 128-bit value types. Keep this default for Futures because Kraken may return instrument
  precision above standard-precision mode's nine-decimal limit.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).

## Documentation

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-kraken) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
