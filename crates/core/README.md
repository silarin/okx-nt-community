# OKX NT Community: `nautilus-core`

This source crate is part of [OKX NT Community](../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

Core foundational types and utilities for [NautilusTrader](https://nautilustrader.io).

The `nautilus-core` crate is designed to be lightweight, efficient, and to provide zero-cost abstractions
wherever possible. It supplies the essential building blocks used across the NautilusTrader
ecosystem, including:

- Time handling and atomic clock functionality.
- UUID generation and management.
- Mathematical functions and interpolation utilities.
- Correctness validation functions.
- Serialization traits and codecs.
- Cross-platform environment utilities.
- Abstractions over common collections.

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `extension-module`: Builds as a Python extension module.
- `ffi`: Enables the C foreign function interface (FFI) from [cbindgen](https://crates.io/crates/cbindgen).
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).
- `simulation`: Enables deterministic simulation testing with
  [MadSim](https://crates.io/crates/madsim).

## Documentation

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-core) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
