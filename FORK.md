# Fork Maintenance

OKX NT Community is an independent source fork of NautilusTrader, maintained by
[silarin](https://github.com/silarin). Nautech Systems and the upstream project do not maintain,
endorse, or support this fork. Use [this repository's issues](https://github.com/silarin/okx-nt-community/issues)
for fork questions and [SECURITY.md](SECURITY.md) for private vulnerability reports.

## Upstream baseline

The recorded upstream integration is
[`f5754f916e`](https://github.com/nautechsystems/nautilus_trader/commit/f5754f916e51955cd468ad26796a3fdd8bd8a75c),
merged into this fork through [PR #1](https://github.com/silarin/okx-nt-community/pull/1)
on 25 October 2026. This identifies the imported source snapshot, not a promise that the fork
always tracks the newest upstream commit. Update this record with each upstream integration.

## Maintained differences

- Recover authoritative native OKX fills and retain source evidence during reconciliation.
- Preserve exact fee accounting, fill ownership, and record-time filtering.
- Reject incomplete or contradictory required per-order fill histories. Bulk startup history
  limits must not truncate required per-order recovery.
- Wait for strategy order cleanup and unresolved execution evidence before sizing managed exits.
- Retain pending cancel/update commands when the configured recovery policy requires venue evidence.
- Admit terminal observations only after native fill accounting; reject unresolved event-store restores.
- Identify this repository and its documentation as an independent fork.

The [OKX integration guide](docs/integrations/okx.md) describes the adapter's behavior and configuration.
Changes to these guarantees need focused regression coverage. Local or synthetic tests do not
establish live-exchange readiness; record the exact commit, environment, and validation limits.

## Source builds and attribution

Build this fork from its source using the [installation guide](docs/getting_started/installation.md#from-source).
Upstream PyPI wheels, package-index wheels, containers, and published crates do not include fork-only
changes. Existing crate, module, and import names are retained for source compatibility.

Preserve upstream history, copyright notices, licenses, and historical references. The retained
[CLA.md](CLA.md) identifies an upstream agreement, and [TRADEMARK.md](TRADEMARK.md) identifies the
upstream trademark policy. Neither document should be rewritten to name the fork as the original
rights holder. The fork's contribution process is in [CONTRIBUTING.md](CONTRIBUTING.md).

## Synchronizing upstream

1. Start a branch from this fork's current `develop` and record the upstream commit being imported.
1. Review the incoming changes against the maintained differences above. Preserve upstream ancestry
   and resolve conflicts without silently dropping fork behavior or safety checks.
1. Run the affected regression tests, `make format`, and `make pre-commit`. Regenerate artifacts
   through their generators when their sources change.
1. Review inherited documentation for upstream contacts, package installation commands, release
   claims, and contribution rules that must remain distinct from this fork's policies.
1. Update the baseline above and prepare a PR targeting `silarin/okx-nt-community:develop`, with
   conflict resolutions, breaking changes, and validation limits. Merging remains a maintainer action.

Keep local evidence and active workspaces when cleaning up a merged branch. Do not treat changes
in this fork as authorization to open an upstream issue or PR.

## Documentation boundaries

The root policies describe this fork. The inherited technical guides describe the shared engine
and retain its identifiers. Sections explicitly marked as upstream reference describe upstream
roadmaps, adapter classifications, release infrastructure, or artifact verification identities.
Those sections do not establish equivalent services or guarantees for this fork.
