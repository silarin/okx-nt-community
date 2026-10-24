# OKX NT Community: `nautilus-system`

This source crate is part of [OKX NT Community](../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

System-level components and orchestration for [NautilusTrader](https://nautilustrader.io).

The `nautilus-system` crate provides the core system architecture for orchestrating trading systems,
including the kernel that manages all engines, configuration management,
and system-level factories for creating components:

- `NautilusKernel` - Core system orchestrator managing engines and components.
- `NautilusKernelConfig` - Configuration for kernel initialization.
- System builders and factories for component creation, including caller-supplied clock construction for live/sandbox systems.

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `defi`: Enables DeFi (Decentralized Finance) support.
- `extension-module`: Builds as a Python extension module.
- `live`: Enables live trading mode dependencies.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs) and auto-enables `streaming`.
- `streaming`: Enables the `nautilus-persistence` dependency for streaming configuration.
- `tracing-bridge`: Enables the `tracing` subscriber bridge for log integration.

## Documentation

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-system) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
