# Contributing to OKX NT Community

This guide covers contributions to [OKX NT Community](https://github.com/silarin/okx-nt-community),
the independent source fork maintained by [silarin](https://github.com/silarin). Use this fork's
[issue tracker](https://github.com/silarin/okx-nt-community/issues) for questions and proposals.
Nautech Systems and upstream NautilusTrader do not review or support contributions to this fork.

> [!IMPORTANT]
> Never report a security vulnerability as a public issue. Use the private reporting channel in
> [SECURITY.md](SECURITY.md).

## Review standard

**This software can execute live trades involving real capital. Every change must meet a high
standard for correctness, reliability, testing, clarity, and maintainability.**

Preserve exact price, quantity, money, and fee arithmetic, source ownership, and fail-closed
recovery behavior. Keep changes focused, match existing patterns, and cover changed behavior
with meaningful regression tests. Do not weaken tests or safety checks to obtain a passing result.

## Contribution assessment

Explain the concrete problem and how the change fits the [fork's scope](ROADMAP.md#fork-scope).
Contributors remain responsible for understanding the implementation, verifying claims, and
addressing review feedback. Speculative changes, unnecessary complexity, and incomplete validation
shift work to reviewers and are not ready for submission.

Read [FORK.md](FORK.md) for the maintained differences and upstream synchronization process,
[AGENTS.md](AGENTS.md) for repository rules, and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for community conduct.
If you use AI assistance, follow [AI_POLICY.md](AI_POLICY.md).

## Start with an issue

Discuss substantial features, integrations, design changes, or policy changes with this fork's
maintainer before implementation. An explicit maintainer request also establishes scope. Small,
self-contained fixes can proceed directly. Search existing issues and PRs to avoid duplicating
work already in progress.

Use this repository for fork proposals. A proposal here does not request upstream acceptance,
funding, review, or maintenance. New adapters need an explicit maintenance and validation plan;
see the distinction between fork support and upstream tiers in [ADAPTERS.md](ADAPTERS.md).

## Licensing and upstream submissions

Preserve applicable license, copyright, and attribution notices. Contribute only material you
have the right to submit. This guide does not change the source license or any existing obligations.

[CLA.md](CLA.md) is retained as the upstream agreement naming Nautech Systems. This fork does not
use signing that agreement or the upstream CLA Assistant as its contribution gate. Do not edit
its parties or present it as a new agreement with this fork's maintainer.

Submitting work to [upstream NautilusTrader](https://github.com/nautechsystems/nautilus_trader) is a
separate action. Follow upstream's current contribution, licensing, and AI requirements when
making that submission. Agents require explicit authorization for each public interaction.

## Find the right package

The Rust workspace is under `crates/`, and the PyO3 Python package is under `python/`.
[MIGRATION_V2.md](MIGRATION_V2.md) describes the inherited v1-to-v2 API migration.
Keep crate and import names compatible; distinguish fork behavior in documentation and tests.

## Set up your environment

Branch from this fork's current `develop`. Follow the
[Environment Setup](docs/developer_guide/environment_setup.md) guide for platform dependencies,
Rust, Python, and the pinned development tools. Use `python/.venv`, never a root `.venv`.

```bash
cargo install cargo-binstall --locked  # one-off prerequisite
make install-tools
make sync
prek install
make build-debug
```

`prek install` installs the repository's pre-commit and commit-message hooks. Tool versions are
pinned in `.nautilus-engineering/tools.toml`, `tools.toml`, and `Cargo.toml`.

## Make your change

Read the affected implementation and existing tests first. Follow the local
[Developer Guide](docs/developer_guide/index.md) and [Coding Standards](docs/developer_guide/coding_standards.md).
Documentation follows [Docs Style](docs/developer_guide/docs.md).

Run the smallest relevant test while developing. `make cargo-test` runs the Rust suite;
`make pytest` builds the extension and runs the Python suite. Use `#[rstest]` for Rust tests
(`#[tokio::test]` is also supported for async tests), as described in the
[Testing guide](docs/developer_guide/testing.md).

## Before you open a PR

### Prepare a review-ready change

Complete the implementation and relevant validation before opening a PR. Drafts are for work
explicitly requested as incomplete by the maintainer. Understand every submitted change and
verify the information in the description.

### Run local checks and follow repository rules

- Run `make format`, then `make pre-commit`, and confirm they pass.
- Run the tests relevant to the change. Documentation-only changes need Markdown and link checks;
  they do not require a new runtime test suite solely for prose edits.
- Regenerate Python stubs and wrapper docstrings with `make py-stubs` when their Rust sources change.
- After later edits or a rebase, rerun the affected checks. Report checks that cannot run and
  discuss the limitation before claiming readiness.
- Keep copyright and license notices. Leave `RELEASES.md` to maintainer release updates and
  upstream synchronization; describe fork-only changes in the PR and [FORK.md](FORK.md).
- Use imperative commit subjects and PR titles without Conventional Commits syntax or issue/PR
  numbers. Reference issues in the body when useful.

`make pre-flight` provides broader local validation. CI confirms locally validated work and
must not become a development loop. Batch related fixes before pushing updates.

### Open the pull request

Target `develop` in `silarin/okx-nt-community`. Follow the
[PR template](.github/pull_request_template.md): scope, changes or conflict resolutions, breaking
changes, and validation. Record the tested revision and distinguish local tests from live-exchange
qualification. Do not add contributor attestations on someone else's behalf.

The maintainer controls merging. Review and response times depend on availability; this fork
makes no fixed response-time commitment. An upstream submission requires a separate decision.
