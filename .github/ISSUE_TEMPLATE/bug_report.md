---
name: Bug Report
about: Bug - behavior that contradicts the platform's documented or intended design
labels:
  - bug
---

# Bug Report

Use this template only for issues that fit the **Bug** definition.

| Term                          | Definition                                                                                                                                           |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Bug**                       | Behavior that contradicts the platform's documented or intended design as per code, docs, or specs. (i.e., the implementation is incorrect.)         |
| **Expectation&nbsp;mismatch** | Behavior that follows the platform's documented or intended design but differs from what you expected. (i.e., the design/spec might be the problem.) |
| **Enhancement request**       | A request for new functionality or behavior that is not implied by existing design. (i.e., *"It would be great if the platform could…"*)             |

**Note:**

- Submitting this issue automatically applies the `bug` label.
- `bug`-labeled issues are triaged with higher priority because they require corrective implementation work.
- **Expectation mismatches** and design-level concerns should be opened as questions in [this fork's issue tracker](https://github.com/silarin/okx-nt-community/issues), or RFCs instead, where they can be validated and discussed to consensus before any work is scheduled.
- The absence of a feature is typically not an expectation mismatch, and should be filed as an enhancement request.

## Confirmation

**Before opening a bug report, please confirm:**

- [ ] I've re-read the relevant sections of the documentation.
- [ ] I've searched this fork's existing issues to avoid duplicates.
- [ ] I've reviewed or skimmed the source code (or examples) to confirm the behavior is not by design.
- [ ] I've reproduced this issue from this fork's source and recorded the exact commit below.

Build from the source in [this repository](https://github.com/silarin/okx-nt-community).
Upstream package-index wheels do not include the changes maintained here.

## Expected behavior

Add here...

## Actual behavior

Add here...

## Steps to reproduce

1.
2.
3.

Consider starting from our
[Minimal Reproducible Example](https://github.com/silarin/okx-nt-community/tree/develop/examples/other/minimal_reproducible_example)
template, which generates its own data so you do not need to attach market data files.

## Code snippets or logs

<!-- If applicable, provide relevant code snippets, error logs, or stack traces. Use code blocks for clarity. -->

## Specifications

Run this and paste the output, then fill in the remaining fields:

```bash
python - <<'EOF'
import platform
import sys

import nautilus_trader

print(f"- OS platform: {platform.platform()}")
print(f"- Python version: {sys.version.split()[0]}")
print(f"- `nautilus_trader` version: {nautilus_trader.__version__}")
EOF
```

- OS platform:
- Python version:
- `nautilus_trader` version:
- Source commit:
- Adapter/venue (if applicable):
