# OKX NT Community: `nautilus-plugin`

This source crate is part of [OKX NT Community](../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

Plug-in artifact identity and boundary primitives for
[NautilusTrader](https://nautilustrader.io).

The `nautilus-plugin` crate provides the public contract that lets an independently compiled Rust
cdylib carry a versioned identity. It defines versioned build metadata, allocator-safe boundary
values, opaque boundary tokens, and the `nautilus_plugin!` macro for exporting the standard entry
symbol and manifest.

This crate gives plug-in artifacts a consistent identity and a compact contract.

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `component-binding`: Enables the experimental executable component contract.
- `host`: Optional plug-in manifest compatibility flag.

## Documentation

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-plugin) for the API reference.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
