# Getting started

## 1. Install

Build OKX NT Community from this repository. First follow the platform and toolchain prerequisites
in [Environment Setup](../developer_guide/environment_setup.md), including Python 3.13-3.14,
Rust, and `cargo-binstall`. Then:

```bash
git clone --branch develop https://github.com/silarin/okx-nt-community.git
cd okx-nt-community
make install-tools
make build-debug
uv run --project python --no-sync python -c "import nautilus_trader; print(nautilus_trader.__version__)"
git rev-parse HEAD
```

The build uses `python/.venv`. Record the Git commit as well as the package version to identify
the fork's source. Upstream PyPI wheels and Nautech package-index wheels do not include fork-only
changes. See [Installation](installation) for the complete source-build instructions.

## 2. Run the quickstart

The [Quickstart](quickstart) runs your first backtest in five minutes using synthetic
data. No downloads, no catalog setup.

The getting-started tutorials all use a simple EMA crossover strategy. This is
deliberate. The trading logic is not the focus. These tutorials teach how the
engine operates: data loading, venue simulation, order lifecycle, and reporting.
The [tutorials](../tutorials/) introduce different strategies (mean reversion,
order book imbalance, grid market making) once the engine mechanics are clear.

## 3. Choose your path

- **Backtesting** - learn the two API levels below, then work through the
  [tutorials](../tutorials/) for strategy pattern walkthroughs.
- **Live trading** - see the
  [Configure a live trading node](../how_to/configure_live_trading.md) how-to
  and [Integrations](../integrations/) for supported venues.
- **Data workflows** - see the [how-to guides](../how_to/) for loading
  external data and setting up the Parquet data catalog.
- **Building adapters** - see the [Developer guide](../developer_guide/).

## Backtesting API levels

NautilusTrader provides two API levels for backtesting:

| API level                             | Entry point      | Best for                                                              |
| :------------------------------------ | :--------------- | :-------------------------------------------------------------------- |
| [Low-level API](backtest_low_level)   | `BacktestEngine` | Direct component access, library development                          |
| [High-level API](backtest_high_level) | `BacktestNode`   | Production workflows, easier transition to live trading (recommended) |

The high-level API requires a Parquet-based data catalog. The low-level API works with
in-memory data but has no live-trading path.

:::warning[One node per process]
Running multiple `BacktestNode` or `LiveNode` instances concurrently in the same
process is not supported due to global singleton state. Sequential execution with
proper disposal between runs is supported. A replacement `LiveNode` on the same
thread also requires dropping the previous node, or releasing all references to
it in Python, before construction.

See [Processes and threads](../concepts/architecture.md#processes-and-threads) for
details.
:::

See the [Backtesting](../concepts/backtesting/) concept guide for help choosing an
API level.

## Examples in the repository

The online documentation shows a subset of examples. For the full set, see the
repository on GitHub:

| Directory                                                                      | Contains                                              |
| :----------------------------------------------------------------------------- | :---------------------------------------------------- |
| [examples/](https://github.com/silarin/okx-nt-community/tree/develop/examples) | Runnable Python examples organized by environment     |
| [docs/tutorials/](../tutorials/)                                               | Tutorials demonstrating common workflows              |
| [docs/concepts/](../concepts/)                                                 | Concept guides with illustrative code snippets        |
| [python/tests/unit/](../../python/tests/unit/)                                 | Unit tests covering core functionality and edge cases |

## Running in Docker

The following upstream Jupyter image is an option for exploring upstream NautilusTrader. It does
not contain this fork's changes. Use the source build above to test fork behavior. Deleting the
container deletes any data.

```bash
# Pull the latest image
docker pull ghcr.io/nautechsystems/jupyterlab:nightly --platform linux/amd64

# Run the container
docker run -p 8888:8888 ghcr.io/nautechsystems/jupyterlab:nightly
```

Then open <http://localhost:8888> in your browser.

:::warning
Examples use `LoggerConfig(stdout_level=LogLevel.ERROR)` because Nautilus logging exceeds
Jupyter's stdout rate limit, causing notebooks to hang at lower log levels.
:::
