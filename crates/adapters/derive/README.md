# OKX NT Community: `nautilus-derive`

This source crate is part of [OKX NT Community](../../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

[NautilusTrader](https://nautilustrader.io) adapter for the
[Derive](https://www.derive.xyz) decentralized derivatives exchange.

The `nautilus-derive` crate implements the Derive adapter for NautilusTrader, including typed HTTP
and WebSocket clients, REST and stream models, venue parsing, data and execution client wiring, and
EIP-712 signing for the official **Derive API**.

Derive offers European-style options, perpetual swaps, and spot markets. The v3 API authenticates
the owner's EOA or multisig through a session-key signer. Orders match off-chain, and operations
are batched for settlement on Ethereum while users retain custody.

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `examples`: Enables the crate's example binaries.
- `extension-module`: Builds as a Python extension module.
- `fuzz`: Enables libFuzzer integration for fuzz targets.
- `high-precision` (default): Enables
  [high-precision mode](https://nautilustrader.io/docs/nightly/getting_started/installation/#precision-mode)
  to use 128-bit value types.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).
- `simulation`: Enables deterministic simulation with `cfg(madsim)`.

## Fuzzing

Coverage-guided fuzz targets for Derive wire models, parsers, signing payloads, and nonce sequencing
live in [`fuzz/`](fuzz/README.md). They require the workspace-pinned `cargo-fuzz` binary and a Rust
nightly toolchain.

## Documentation

See the [Derive integration guide](https://nautilustrader.io/docs/nightly/integrations/derive/)
and [crate docs](https://docs.rs/nautilus-derive) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the
[GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
