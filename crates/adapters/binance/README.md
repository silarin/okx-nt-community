# OKX NT Community: `nautilus-binance`

This source crate is part of [OKX NT Community](../../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

[NautilusTrader](https://nautilustrader.io) adapter for the
[Binance](https://www.binance.com) cryptocurrency exchange.

The `nautilus-binance` crate provides client bindings (HTTP & WebSocket) and data models
for the official **Binance API**. Live data and execution
clients are available for:

- Spot markets, including Binance US (api.binance.com)
- USD-M Futures (fapi.binance.com)
- COIN-M Futures (dapi.binance.com)

The crate also includes shared enums, endpoint constants, URL routing, and credential
plumbing for adjacent Binance surfaces such as Margin and European Options. Those
surfaces do not have live data or execution clients in this crate.

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Authentication

This crate requires **Ed25519 API keys** for all authenticated endpoints (REST and WebSocket API).
Ed25519 is recommended by Binance for its superior performance and security. HMAC and RSA keys
are not supported.

Generate an Ed25519 keypair and register it with Binance:

```bash
# Generate private key (PKCS#8 PEM format)
openssl genpkey -algorithm ed25519 -out binance_ed25519_private.pem

# Extract public key for Binance registration
openssl pkey -in binance_ed25519_private.pem -pubout -out binance_ed25519_public.pem
```

Set credentials via environment variables:

```bash
export BINANCE_API_KEY="your-api-key-from-binance"
export BINANCE_API_SECRET="$(cat binance_ed25519_private.pem)"
```

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `arrow`: Enables Apache Arrow data support.
- `examples`: Enables the crate's example binaries.
- `extension-module`: Builds as a Python extension module.
- `high-precision` (default): Enables
  [high-precision mode](https://nautilustrader.io/docs/nightly/getting_started/installation/#precision-mode)
  to use 128-bit value types.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).

## Documentation

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-binance) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
