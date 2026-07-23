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

Manual `workflow_dispatch` publish runs are allowed only for rerunning the same source version after an infrastructure failure before the version has been published.

## Release Workflow Controls

The publish workflow:

- grants `packages: write`, `id-token: write`, and `attestations: write` only to the publish job;
- installs dependencies with `npm ci`;
- reruns the full package CI before publishing;
- rejects release tags that do not match `package.json`;
- rejects already-published versions;
- packs the tarball before publishing;
- attests the exact tarball;
- publishes the exact packed tarball to GitHub Packages.

## Rollback

Rollback happens in consumers by restoring the prior exact package version, regenerating the service lockfile, running service CI, and redeploying through that service's protected rollout path.

Published versions are immutable and must not be republished in place. Package deletion is reserved for exceptional security response and must be recorded in release notes and the relevant tracking issue.
