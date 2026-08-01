# Release Policy

## Ownership

`ramideltoro/nutsnews-worker-runtime` owns the `@ramideltoro/nutsnews-worker-runtime` package, package CI, release workflow, package access notes, and repo-local operational notes.

The package does not own backend host runtime, production deployment secrets, Grafana Cloud resources, explanatory architecture docs, deployable worker service business logic, or the active legacy worker runtime.

## Release Procedure

1. Merge package changes to `main` through pull request review and passing CI.
2. Confirm `package.json`, `CHANGELOG.md`, and exported package metadata use the same version.
3. Create a GitHub release whose tag is `v<package.json version>`.
4. Let the publish workflow run from the release event.
5. Confirm the workflow produced an artifact attestation and published the package to GitHub Packages.
6. Run the `Package Install Smoke` workflow with the exact published version and confirm it installs and imports the package using repository `GITHUB_TOKEN` package-read permissions.
7. For a major transition or an explicitly declared breaking release, include `docs/migrations/MIGRATION-v<version>.md` and confirm each upgrading service passes the documented adapter and telemetry conformance cases. Minor and patch releases do not require empty migration-note files.

The package validator infers a major transition from the two newest stable-version headings in `CHANGELOG.md`. Set `NUTSNEWS_BREAKING_RELEASE=true` when deliberately validating an exceptional breaking release that is not a major transition.

For the `1.0.0` telemetry migration, first publish and attest `@ramideltoro/nutsnews-worker-contracts@1.0.0`, regenerate this repository's lockfile from that immutable package, and rerun CI. Then publish and attest the runtime before changing any deployable service pin. Upgrade shadow services one at a time, verify histogram/probe series in Grafana, and keep the legacy worker as production ingestion owner throughout the migration.

Manual `workflow_dispatch` publish runs are allowed only for rerunning the same source version after an infrastructure failure before the version has been published.

## Release Workflow Controls

The publish workflow:

- grants `packages: write`, `id-token: write`, and `attestations: write` only to the publish job;
- installs dependencies with `npm ci --ignore-scripts` so the package-write token is not exposed to dependency lifecycle scripts;
- reruns the full package CI before publishing;
- rejects release tags that do not match `package.json`;
- rejects already-published versions;
- packs the tarball before publishing;
- attests the exact tarball;
- publishes the exact packed tarball to GitHub Packages.

## Rollback

Rollback happens in consumers by restoring the prior exact package version, regenerating the service lockfile, running service CI, and redeploying through that service's protected rollout path.

Published versions are immutable and must not be republished in place. Package deletion is reserved for exceptional security response and must be recorded in release notes and the relevant tracking issue.
