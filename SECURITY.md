# Security Policy

This policy covers **OKX NT Community**, the independent source fork maintained by
[silarin](https://github.com/silarin). Nautech Systems and upstream NautilusTrader do not provide
security support for this repository.

## Scope

Report vulnerabilities in this fork's source and maintained changes. Include the exact commit,
build configuration, and affected dependency versions. Third-party services, exchanges, data
providers, and Nautech Systems websites are outside this fork's scope.

## Reporting a vulnerability

Use [this fork's private vulnerability reporting form](https://github.com/silarin/okx-nt-community/security/advisories/new).
Private vulnerability reporting is enabled for this repository. Do not put vulnerability details,
credentials, or private trading data in public issues, PRs, or conduct-contact requests.

Include the impact, reproduction steps, affected commits, and suggested remediation if available.
For a vulnerability reproduced in unmodified upstream software, coordinate with the fork maintainer
and follow [upstream's security policy](https://github.com/nautechsystems/nautilus_trader/blob/develop/SECURITY.md)
for a separate upstream disclosure.

## Response and disclosure

The fork maintainer handles triage and coordinates remediation and disclosure with the reporter.
Response and fix times depend on availability and the issue; this fork does not promise fixed
response or remediation deadlines. There is no formal bug bounty program.

- Keep vulnerability details private while coordinating disclosure.
- Only reproduce an issue to the extent needed to demonstrate it.
- Do not access unauthorized data or disrupt systems.
- Follow applicable laws and avoid including secrets or personal data in reports.

## Supported source

Maintenance focuses on this fork's `develop` branch. Historical commits and upstream package-index
releases are not separate supported releases of this fork. A version string alone does not identify
fork changes; record the Git commit used to build the software.

## Fork validation controls

The repository retains locked dependencies, dependency cooldown and audit policy files, pre-commit
checks, and regression tests. Contributions must preserve these controls and report the checks
actually run for their revision. See [CONTRIBUTING.md](CONTRIBUTING.md) and [Fork Maintenance](FORK.md).

The inherited workflow files and their documentation do not establish that this fork has
upstream's deployment environments, signing identities, publication credentials, branch protections,
or release guarantees. Verify a fork artifact against evidence published for that exact artifact.

## Security infrastructure

> [!NOTE]
> This section describes inherited upstream infrastructure. Statements about the Core team,
> repository settings, hosted services, and publication describe upstream NautilusTrader, not
> independently verified deployment of those services for this fork.

The sections below detail the upstream controls at each layer of that lifecycle.

### Public posture

- **OpenSSF Scorecard**: The repository publishes Scorecard results for the public badge and API,
  and uploads SARIF to GitHub code scanning. Scorecard is an automated repository-health signal
  that complements manual review and security audits.

### Source and review controls

- **CODEOWNERS**: Critical infrastructure files, dependency manifests, and lock files require Core
  team review before merge.
- **Branch and tag rulesets**: Protected branches require signed commits and passing CI checks.
  Release tags matching `v*` are immutable after creation.
- **Source restrictions**: Rust packages are sourced exclusively from crates.io. Git dependencies
  and unknown registries are prohibited.

### Dependency intake controls

- **Version pinning and lock files**: Rust dependencies are pinned in `Cargo.lock` with
  cryptographic checksums. Python dependencies are pinned in `python/uv.lock` with integrity
  hashes. Wildcard version requirements are prohibited.
- **Dependency cooldowns**: Python dependency resolution excludes packages published within the
  last 7 days through `exclude-newer` in `python/pyproject.toml`. Rust crate updates remain subject
  to a 3-day cooldown and cargo-vet review, and every Make target that compiles Rust checks the
  resolved lockfiles first. Resolved crates.io publication dates are committed in
  `.supply-chain/crate-dates.json` so the check can run without registry access; dates a branch adds
  beyond `origin/develop` are re-verified against crates.io, locally and in the CI pre-commit job.
  A security fix or critical bug fix may bypass either cooldown after explicit review. These windows give the
  community time to detect and quarantine compromised releases. Development tools are pinned to
  explicit versions across `.nautilus-engineering/tools.toml`, `tools.toml`, `Cargo.toml`, and
  related manifests, and version bumps are reviewed during security audits.
- **Container image cooldowns**: Require images to be at least 7 days old unless an exception is
  explicitly reviewed. Measure age from `created` in the image configuration for the exact
  pinned digest, not the bundled software release date or tag update time. For multi-architecture indexes,
  use the newest `created` timestamp among the target-platform images. Rebuilding an image restarts
  its cooldown.
- **Wheel-only Python installs**: The `no-build-package` list in `[tool.uv]` enumerates every
  third-party package locked in `python/uv.lock` and forbids `uv` from building any of them from source.
  In normal operation uv prefers wheels, so the setting is a no-op; it kicks in only if a listed
  upstream stops publishing wheels for the target platform, in which case `uv lock` fails instead
  of silently building from an sdist. The local workspace package is intentionally absent because it
  must be built by the workspace's own build backend. The `check-no-build-packages` pre-commit hook
  verifies the list stays in lock-step with `python/uv.lock` on every commit that touches the lock
  or the manifest.
- **Toolchain pinning**: `python/pyproject.toml` limits local uv use to the supported minor series.
  `.nautilus-engineering/tools.toml` pins the exact uv version used by CI, Docker, pre-commit, and
  project install commands. It also pins shared release and audit tools; `tools.toml` retains
  NautilusTrader-specific pins.
- **License compliance**: Automated checks verify Rust dependencies against an allow list of
  licenses compatible with NautilusTrader's `LGPL-3.0-only` license.

### Pre-merge and scheduled scanning

- **Pre-commit security**: Gitleaks credential screening, private key detection, Zizmor GitHub
  Actions auditing, and Unicode control character detection run before changes land.
- **Dependency auditing**: Automated security scanning runs via cargo-audit, cargo-deny, cargo-vet,
  and OSV Scanner (Rust), pip-audit (Python), and Zizmor (GitHub Actions).
- **Supply chain provenance**: cargo-vet verifies Rust dependency provenance by importing trusted
  audit data from organizations including Bytecode Alliance, Google, Mozilla, and Embark Studios.
- **Fuzzing**: cargo-fuzz targets cover selected adapter and signing surfaces, including Derive
  wire-model/signing internals and Lighter cryptographic/signing internals.
- **Code scanning**: CodeQL static analysis covers Python and Rust code on PRs to `master`,
  pushes to `nightly`, and manual dispatch. The scheduled security audit also uploads Zizmor SARIF
  results for GitHub Actions workflow findings when token permissions allow it.

### Build and publish controls

- **Build integrity**: SLSA build provenance attestations for Python release artifacts, GitHub
  release checksum manifests, immutable GitHub release attestations, immutable GitHub Actions
  pinned to commit SHAs, container digest pinning, Docker image signing via Sigstore cosign, SPDX
  SBOM generation and Sigstore attestation for container images, and hardened CI runners with
  network egress blocked to an explicit allow-list.
- **Release sequencing**: Stable releases create a draft GitHub release and attach wheel and sdist
  assets before publishing to package indexes (`packages.nautechsystems.io`, PyPI, crates.io).
  CI verifies the registries, attaches final checksum and provenance assets, then publishes the
  GitHub release and verifies its release attestation. This keeps the GitHub release and checksum
  manifest as the anchor for downstream registry verification while staying compatible with GitHub
  release immutability.
- **Deployment environments**: Release and package publishing jobs use scoped GitHub deployment
  environments (`release`, `r2-develop`, and `r2-nightly`) so publishing credentials and OIDC
  trusted-publisher identities stay isolated from test, lint, and build-only jobs.
- **Publish authentication**: PyPI and crates.io uploads use Trusted Publishing (OIDC) bound to the
  `release` GitHub Environment, eliminating long-lived API tokens. Each publish mints a short-lived
  token scoped to the specific repo, workflow, and environment.
- **GHCR authentication**: Container image pushes to GitHub Container Registry use the short-lived
  `GITHUB_TOKEN` scoped to the workflow run, not a long-lived personal access token.
- **Post-publish verification**: CI verifies PyPI wheels and sdists against the GitHub release
  manifest and expected PyPI publisher identity, verifies crates.io entries were trusted-published
  by this repository, records whether each crate matches the release commit or was already
  published, verifies the final GitHub release attestation, and verifies container image signatures
  and SBOM attestations against the expected GitHub Actions workflow identities after publishing.

### Runtime cryptography

- **Cryptography**: TLS and most runtime cryptography use
  [aws-lc-rs](https://github.com/aws/aws-lc-rs), the Rust binding for AWS-LC. Ed25519 signing uses
  [ed25519-dalek](https://github.com/dalek-cryptography/curve25519-dalek). AWS-LC runs in
  non-FIPS mode because the FIPS 140-3 module (`aws-lc-fips-sys`) requires the Go toolchain as a
  build dependency. The AWS-LC primitives used here (AES-GCM, SHA-2, ECDSA, ChaCha20-Poly1305)
  are identical in both modes; the FIPS module adds runtime self-tests and module boundary
  enforcement required for federal certification.

For the upstream supply chain security policy, see <https://nautilustrader.io/security/supply-chain/>.

For the end-to-end release supply chain model, see
[Security Architecture](docs/developer_guide/security.md).

For detailed CI/CD security practices, see [.github/OVERVIEW.md](.github/OVERVIEW.md#security).

## Known vulnerability management

This section records upstream vulnerability-management policy, including upstream timelines.
The fork uses the reporting and response process at the top of this document.

Where a known advisory exists in a transitive dependency without an available
fix, we document the risk assessment, context, and mitigation in our audit
configuration. Accepted risks are categorized by severity, scope (direct vs.
transitive, runtime vs. dev-time), and monitored for upstream resolution.

When a new vulnerability is identified in a dependency:

- The nightly security audit flags the advisory automatically.
- The Core team assesses severity and exposure within the NautilusTrader context.
- Critical vulnerabilities in direct dependencies are patched or mitigated within 30 days.
- Users are notified through release notes and, where appropriate, security advisories.

Users who build NautilusTrader from source or extend it with additional
dependencies are responsible for auditing their own dependency trees. The
controls described in this policy apply to official NautilusTrader releases and
the canonical repository.

## Advisories addressed

These are inherited upstream advisory records, not a complete security assessment of the fork.

Third-party security advisories we have addressed via dependency upgrades. Security scanning is
not delayed by the dependency cooldowns described above. When an advisory requires a newer
package, a reviewed fix can bypass the applicable cooldown.

- **1.227.0**:
  - [GHSA-mf9v-mfxr-j63j](https://github.com/urllib3/urllib3/security/advisories/GHSA-mf9v-mfxr-j63j):
    `urllib3` decompression-bomb safeguard bypass via `HTTPResponse.drain_conn()` after partial
    decompression, and via the second `read(amt=N)`/`stream(amt=N)` call when Brotli-decompressed.
    Upgraded `urllib3` to v2.7.0.
  - [GHSA-qccp-gfcp-xxvc](https://github.com/urllib3/urllib3/security/advisories/GHSA-qccp-gfcp-xxvc):
    `urllib3` pools created via `ProxyManager.connection_from_url` not stripping headers listed in
    `Retry.remove_headers_on_redirect` when redirecting cross-host. Upgraded `urllib3` to v2.7.0.

## Verifying releases

The commands below verify upstream artifacts and upstream publisher identities. They do not
verify a build of this fork. Do not substitute the fork name into an upstream trust identity.

Python release artifacts and Docker images are signed or attested via Sigstore-backed workflows.
Cargo crates are published through crates.io Trusted Publishing. The release verifier records
whether each crate version was published by the current release commit or already existed from an
earlier trusted-published commit in this repository. Emergency token-publish recovery requires an
explicit `CRATES_IO_MANUAL_PUBLISH_EXCEPTIONS` `crate@version` entry, and the recovered crate
version is recorded in `crates-manifest.json` with `release_status: "manual_token_publish"`. You
can independently verify artifacts before installing them.

### Python wheels and sdist

GitHub releases include a generated checksum table, an aggregate `SHA256SUMS`
file, per-asset `.sha256` files, and a machine-readable `dist-manifest.json`
for Python wheels and the sdist. Check the downloaded artifact against one of
those checksum sources before installation.

GitHub releases also include `.sigstore` Sigstore bundles and `.intoto.jsonl`
DSSE envelope siblings for each Python artifact. The GitHub CLI fetches
attestations from the GitHub API by default; use `--bundle <artifact>.sigstore`
to verify against a downloaded Sigstore bundle instead.

After downloading from PyPI or the GitHub release, verify each artifact with the
GitHub CLI. The `--cert-identity-regex` and `--cert-oidc-issuer` flags bind
verification to the `build.yml` release workflow, not just the repository:

```bash
ISSUER=https://token.actions.githubusercontent.com
IDENTITY='^https://github\.com/nautechsystems/nautilus_trader/\.github/workflows/build\.yml@refs/heads/(master|nightly)$'

# `gh attestation verify` takes one subject per call, so loop over wheels
for whl in nautilus_trader-*.whl; do
  gh attestation verify "$whl" \
    --repo nautechsystems/nautilus_trader \
    --cert-identity-regex "$IDENTITY" \
    --cert-oidc-issuer "$ISSUER"
done

gh attestation verify nautilus_trader-*.tar.gz \
  --repo nautechsystems/nautilus_trader \
  --cert-identity-regex "$IDENTITY" \
  --cert-oidc-issuer "$ISSUER"
```

### Docker images

Resolve the mutable tag to an immutable digest first so every check, the
subsequent `docker pull`, and the `docker run` operate on the same image:

```bash
# Use crane (or `docker buildx imagetools inspect <ref> --format '{{.Manifest.Digest}}'`)
DIGEST=$(crane digest ghcr.io/nautechsystems/nautilus_trader:latest)
IMAGE=ghcr.io/nautechsystems/nautilus_trader@${DIGEST}
ISSUER=https://token.actions.githubusercontent.com
IDENTITY='^https://github\.com/nautechsystems/nautilus_trader/\.github/workflows/docker\.yml@refs/heads/(master|nightly)$'
```

Verify the cosign signature, which proves the image was produced by the
NautilusTrader CI workflow:

```bash
cosign verify "$IMAGE" \
  --certificate-identity-regexp "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER"
```

Verify the SPDX SBOM attestation is bound to the same image digest:

```bash
cosign verify-attestation --type https://spdx.dev/Document/v2.3 "$IMAGE" \
  --certificate-identity-regexp "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER"
```

The GitHub CLI can also verify the SBOM attestation, but does not check the
cosign image signature, so use it in addition to `cosign verify` above:

```bash
gh attestation verify "oci://${IMAGE}" \
  --repo nautechsystems/nautilus_trader \
  --predicate-type https://spdx.dev/Document/v2.3 \
  --cert-identity-regex "$IDENTITY" \
  --cert-oidc-issuer "$ISSUER"
```
