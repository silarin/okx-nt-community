# OKX NT Community: `nautilus-common`

This source crate is part of [OKX NT Community](../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

Common componentry for [NautilusTrader](https://nautilustrader.io).

The `nautilus-common` crate provides shared components and utilities that form the system foundation for
NautilusTrader applications. This includes the actor system, message bus, caching layer, and other
essential services.

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `build-info-event-store`: Includes the event-store backend version in build information logs.
- `defi`: Enables DeFi (Decentralized Finance) support.
- `extension-module`: Builds as a Python extension module.
- `high-precision`: Enables
  [high-precision mode](https://nautilustrader.io/docs/nightly/getting_started/installation/#precision-mode)
  to use 128-bit value types.
- `indicators`: Includes the `nautilus-indicators` crate and indicator utilities.
- `live`: Enables the Tokio async runtime for live trading.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).
- `sbe`: Enables Simple Binary Encoding (SBE) serialization support.
- `simulation`: Enables deterministic simulation testing with
  [MadSim](https://crates.io/crates/madsim).
- `tracing-bridge`: Enables the `tracing` subscriber bridge for log integration.

## Documentation

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-common) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
