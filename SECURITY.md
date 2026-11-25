# Security Policy

PgVolt is an independent fork of ElectricSQL, maintained by Beeldi. It is not
upstream ElectricSQL, and it is not covered by upstream's security policy.

Report here every vulnerability you find in this code, whether it comes from a change
this fork made or from **stock upstream** code it carries. A flaw that also affects stock
upstream is reported to upstream as soon as we receive it, and you may report it there
directly as well. We ship our own fix on the timeline below without waiting for an
upstream release, and we coordinate public advisory publication with upstream (respecting
upstream's stated disclosure window, up to 90 days) so upstream users have time to patch.

## Reporting a Vulnerability

Report through
[this repository's private vulnerability reporting](https://github.com/beeldi/pgvolt/security/advisories/new).

**Do not open a public issue for a security vulnerability.**

### What to include

- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof of concept
- The affected branch(es) or commit(s), and the image tag if known
- Any suggested fix, if you have one

### Response timeline

- **Acknowledgement**: within one business day
- **Initial assessment**: within two business days, we confirm whether the
  report is accepted and share an initial severity assessment
- **Resolution**: critical issues are fixed as fast as a reviewed change allows.
  Resolution means a fix on every supported branch, not on `main` alone.

## Supported Versions

`main` is supported, together with any `release/**` branch that is still the
source of a current release. Older commits are not patched: move to a supported
branch to pick up a fix.

## Scope

**In scope**:

- The sync service (`packages/sync-service`), including its Dockerfile
- The client packages in this tree (`packages/typescript-client`,
  `packages/elixir-client`)
- The changes this fork carries on top of upstream, **including code behind
  feature flags**
- Third-party dependencies of the above, as pinned in this tree. Reporting
  upstream to the dependency is welcome but does not replace a report here.

**Out of scope**:

- Upstream Electric Cloud and upstream's published images and packages
- Upstream packages this fork has removed

## Upstream advisories

We track security advisories published by upstream ElectricSQL and by the
dependencies of the packages in scope. An advisory affecting a supported branch
is triaged on the same timeline as a direct report.

## Disclosure

We follow coordinated disclosure. Please do not publish details before a fix is
released. Once a fix is released (and any upstream disclosure window has closed, for flaws
that also affect stock upstream), we publish a security advisory in this repository and
credit the reporter unless they ask not to be named.
