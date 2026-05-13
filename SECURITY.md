# Security Policy

## Reporting a vulnerability

If you believe you have found a security vulnerability, please report it privately.

- Preferred: open a private GitHub Security Advisory for this repository.
- Alternative: open an issue with minimal details and ask maintainers to move the discussion to a private channel.

Please include:
- A clear description of the issue and its impact.
- Steps to reproduce (or a proof-of-concept).
- Any suggested mitigation.

## Supported versions

Only the `main` branch is currently supported.

## Dependency vulnerabilities (RustSec)

This repository uses `cargo audit` (RustSec) to detect vulnerable dependencies.

Local run:

```bash
cargo audit --deny warnings
```

Handling advisories:

- Prefer upgrading: `cargo update -p <crate>` (and/or bumping crate versions) and re-run tests.
- If an advisory is not applicable or cannot be fixed immediately, add an allowlist entry in `.cargo/audit.toml` with the dependency path, rationale, mitigation, and revisit trigger, then open a tracking issue to remove the ignore.
- Do not add broad ignores. Each ignored RustSec ID must be specific and kept visible in review.
- If an ignored advisory is justified because an optional upstream dependency is not active, keep an executable proof in CI. The current security workflow fails if `cargo tree -i bincode -e features` ever shows `bincode` as active.
