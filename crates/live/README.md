# OKX NT Community: `nautilus-live`

This source crate is part of [OKX NT Community](../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

Live system node for [NautilusTrader](https://nautilustrader.io).

The `nautilus-live` crate provides high-level abstractions and infrastructure for running live trading
systems, including data streaming, execution management, and system lifecycle handling.
It builds on top of the system kernel to provide simplified interfaces for live deployment:

- `LiveNode` High-level abstraction for live system nodes.
- `LiveNodeConfig` Configuration for live node deployment.
- `AsyncRunner` for managing system real-time data flow.

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `defi`: Enables DeFi (Decentralized Finance) support.
- `examples`: Enables example strategies and testkit support for live nodes.
- `extension-module`: Builds as a Python extension module.
- `fuzz`: Provides shared libFuzzer integration for adapter fuzz binaries.
- `node` (default): Enables the full live node, builder, config, and execution manager.
- `plugin` (default): Keeps compatibility stubs for plug-in config validation.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs) and auto-enables `node` and
  `streaming`.
- `simulation`: Enables deterministic simulation testing with
  [MadSim](https://crates.io/crates/madsim).
- `streaming`: Enables the `nautilus-persistence` dependency for streaming configuration and
  requires `node`.
- `test-support`: Enables shared execution harnesses for adapter integration tests.

## Documentation

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-live) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
