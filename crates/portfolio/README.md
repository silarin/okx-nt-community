# OKX NT Community: `nautilus-portfolio`

This source crate is part of [OKX NT Community](../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

Portfolio management and risk analysis for [NautilusTrader](https://nautilustrader.io).

The `nautilus-portfolio` crate provides portfolio management capabilities including
real-time position tracking, performance calculations, and risk management. This includes
sophisticated portfolio analytics and multi-currency support:

- **Portfolio tracking**: Real-time portfolio state management with position and balance monitoring.
- **Account management**: Support for cash and margin accounts across multiple venues.
- **Performance calculations**: Real-time unrealized PnL, realized PnL, and mark-to-market valuations.
- **Risk management**: Initial margin calculations, maintenance margin tracking, and exposure monitoring.
- **Multi-currency support**: Currency conversion and cross-currency risk exposure analysis.
- **Configuration options**: Flexible settings for price types, currency conversion, and portfolio behavior.

The crate handles complex portfolio scenarios including multi-venue trading, currency conversions,
and sophisticated margin calculations for both live trading and backtesting environments.

## Upstream NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `extension-module`: Builds as a Python extension module.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).

## Documentation

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-portfolio) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
