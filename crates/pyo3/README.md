# OKX NT Community: `nautilus-pyo3`

This source crate is part of [OKX NT Community](../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

Python bindings for [NautilusTrader](https://nautilustrader.io).

The `nautilus-pyo3` crate provides all [PyO3](https://pyo3.rs) Python bindings for the
main `nautilus_trader` Python package, built via [maturin](https://github.com/PyO3/maturin).

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `arrow`: Enables Apache Arrow support in dependent crates.
- `betfair`: Enables the Betfair adapter and its Python bindings.
- `coinapi`: Enables the CoinAPI adapter and its Python bindings.
- `defi`: Enables DeFi (Decentralized Finance) support, including blockchain adapters.
- `extension-module`: Builds as a Python extension module and is automatically enabled by `maturin`.
- `high-precision`: Uses 128-bit value types throughout the workspace.
- `hypersync`: Enables [`hypersync-client`](https://crates.io/crates/hypersync-client) support for the blockchain adapter.
- `mimalloc`: Sets [mimalloc](https://crates.io/crates/mimalloc) as Rust's global allocator.
- `postgres`: Enables PostgreSQL (sqlx) back-ends in dependent crates.
- `redis`: Enables Redis based infrastructure in dependent crates.
- `tracing-bridge`: Enables the `tracing` subscriber bridge for log integration.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
