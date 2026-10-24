# OKX NT Community: `nautilus-architect-ax`

This source crate is part of [OKX NT Community](../../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

[NautilusTrader](https://nautilustrader.io) adapter for [AX Exchange](https://architect.exchange).

## Overview

[AX Exchange](https://architect.exchange) is a centralized and regulated derivatives exchange for
traditional underlying asset classes. Its production catalog lists perpetual contracts across FX,
equities, energy ETFs, metals, energy, treasuries, and compute. Its sandbox also exposes dated
futures. AX is licensed by the
[Bermuda Monetary Authority (BMA)](https://www.bma.bm).

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `examples`: Enables the crate's example binaries.
- `extension-module`: Builds as a Python extension module.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).

## Documentation

- [Crate docs](https://docs.rs/nautilus-architect-ax)
- [API reference](https://docs.architect.exchange/api-reference)
- [AX Exchange](https://architect.exchange)

## Authentication

AX Exchange uses bearer token authentication via HTTP headers:

1. API key and secret obtain a session token via `/authenticate`.
2. The session token is used as a bearer token for subsequent REST and WebSocket requests.

## API endpoints

| Environment | HTTP API (market data)                           | HTTP API (orders)                                   | Market Data WS                                   | Orders WS                                            |
| ----------- | ------------------------------------------------ | --------------------------------------------------- | ------------------------------------------------ | ---------------------------------------------------- |
| Sandbox     | `https://gateway.sandbox.architect.exchange/api` | `https://gateway.sandbox.architect.exchange/orders` | `wss://gateway.sandbox.architect.exchange/md/ws` | `wss://gateway.sandbox.architect.exchange/orders/ws` |
| Production  | `https://gateway.architect.exchange/api`         | `https://gateway.architect.exchange/orders`         | `wss://gateway.architect.exchange/md/ws`         | `wss://gateway.architect.exchange/orders/ws`         |

## Usage

Run example binaries to test the adapter:

```bash
# HTTP client example
cargo run -p nautilus-architect-ax --bin ax-http-public

# WebSocket data client example
cargo run -p nautilus-architect-ax --bin ax-ws-data

# WebSocket orders client example
cargo run -p nautilus-architect-ax --bin ax-ws-orders
```

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
