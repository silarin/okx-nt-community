# OKX NT Community: `nautilus-okx`

This source crate is part of [OKX NT Community](../../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

[NautilusTrader](https://nautilustrader.io) adapter for the [OKX](https://www.okx.com) cryptocurrency exchange.

The `nautilus-okx` crate provides client bindings (HTTP & WebSocket) and data
models for the official **OKX v5 API**.

The official OKX API reference can be found at <https://www.okx.com/docs-v5/en/>.

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
- `simulation`: Enables deterministic simulation testing with
  [MadSim](https://crates.io/crates/madsim). With `cfg(madsim)`, supported HTTP and Tungstenite
  WebSocket connections use controlled peers. See the [DST transport contract](../../../docs/concepts/dst.md#simulated-http-and-websocket-transport)
  for supported transport behavior and exclusions.

## Documentation

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-okx) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
