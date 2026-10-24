# OKX NT Community: `nautilus-indicators`

This source crate is part of [OKX NT Community](../../README.md), maintained independently by silarin.

Neither Nautech Systems Pty Ltd nor the official NautilusTrader project is affiliated with this fork,
endorses it, or provides support for it. Use [this fork's issues](https://github.com/silarin/okx-nt-community/issues)
for questions about the changes maintained here.

Technical analysis indicators for [NautilusTrader](https://nautilustrader.io).

The `nautilus-indicators` crate provides a collection of technical analysis indicators
for quantitative trading and market research. This includes a wide variety of indicators
organized by category, with a unified trait-based architecture for consistent usage:

- **Moving averages**: SMA, EMA, DEMA, HMA, WMA, VWAP, adaptive averages, linear regression,
  and z-score.
- **Momentum indicators**: RSI, MACD, Aroon, Bollinger Bands, CCI, Stochastics, and rate of change.
- **Volatility indicators**: ATR, Donchian Channels, Keltner Channels, and volatility ratios.
- **Ratio analysis**: Efficiency ratios and spread analysis for relative performance.
- **Order book indicators**: Book imbalance ratio for analyzing market microstructure.
- **Common indicator trait**: Unified interface supporting bars, quotes, trades, and order book data.

All indicators are designed for high-performance real-time processing with bounded memory
usage and efficient circular buffer implementations. The crate supports both Rust-native
usage and Python integration for strategy development and backtesting.

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

For the upstream API reference, see [the upstream docs](https://docs.rs/nautilus-indicators) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

Upstream NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
