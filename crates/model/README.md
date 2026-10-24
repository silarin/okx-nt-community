# OKX NT Community: `nautilus-model`

This source crate is part of [OKX NT Community](../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

Trading domain model for [NautilusTrader](https://nautilustrader.io).

The `nautilus-model` crate provides a type-safe domain model that forms the backbone of the framework
and can serve as the foundation for building algorithmic trading systems.

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `arrow`: Enables Apache Arrow schema and `RecordBatch` registries for custom data.
- `defi`: Enables the DeFi (Decentralized Finance) domain model.
- `extension-module`: Builds as a Python extension module.
- `ffi`: Enables the C foreign function interface (FFI) from [cbindgen](https://crates.io/crates/cbindgen).
- `high-precision`: Enables
  [high-precision mode](https://nautilustrader.io/docs/nightly/getting_started/installation/#precision-mode)
  to use 128-bit value types.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).
- `python-arrow`: Enables Python bindings together with `PyArrow` `RecordBatch` bridging.
- `test-support`: Enables test fixtures, builders, specs, and defaults.

## Documentation

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-model) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
