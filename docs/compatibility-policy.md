# Compatibility Policy

## Versioning

`@ramideltoro/nutsnews-worker-runtime` follows semantic versioning.

- Major versions may remove public APIs, change runtime result shapes, or require a newer major contracts package.
- Minor versions may add optional fields, new helper APIs, or support for additional backward-compatible contracts surfaces.
- Patch versions fix implementation, generated types, tests, or documentation without changing accepted public API shapes.

Consumers pin exact package versions. Version ranges are not part of the supported production upgrade path.

## Contracts Dependency

Runtime `1.0.0` resolves the exact `@ramideltoro/nutsnews-worker-contracts@1.0.0` package. That major contracts release corrects canonical article candidate ownership to the actual fetcher-to-canonicalizer boundary and makes consumer-aware payload routing part of the tested runtime/contract pairing. Package validation confirms the installed package name and exact version before release.

Contracts `1.0.0` must be published, attested, and install-smoke verified before the runtime release is merged. The runtime lockfile must then be regenerated from the immutable published package so its registry URL and integrity are recorded before the runtime release tag is created.

The runtime publish workflow sets `NUTSNEWS_REQUIRE_PUBLISHED_CONTRACT_LOCK=true`; package validation then rejects a lock entry without a `resolved` URL on the exact `https://npm.pkg.github.com/download/@ramideltoro/nutsnews-worker-contracts/<version>/<artifact>` path and `sha512` integrity. Ordinary source CI can use the locally packed contracts artifact while the ordered contracts release is pending, but that does not make the runtime releasable.

The package validator allows only the contracts package and the approved RabbitMQ client dependency, then rejects source imports outside relative runtime modules, the contracts package, and the broker client.

Runtime `1.0.0` is a deliberate major telemetry migration. It removes `recordDependencyLatency`, replaces `_duration_ms` summaries with `_duration_seconds` histograms, and replaces the aggregate health series with probe/check-specific families. Consumers remain on their exact pre-1.0 pins until the package is published and their service-local metric and readiness conformance tests pass.

Runtime `1.0.0` also requires idempotency adapters to return an opaque token from each successful claim and implement completion, failure, and `releaseClaim` as token-aware compare-and-set operations. `releaseClaim` may transition only that token's in-progress claim to failed and must never downgrade a completed record or another delivery's claim. Service-local adapter tests must cover reject-before-commit, commit-then-reject, and concurrent ownership before upgrading.

## Deprecation Window

Backward-compatible minor and patch releases stay supported for at least 90 days after a replacement release. Breaking changes require a major version, migration notes, and service-level conformance tests before rollout.

## Rollback

Rollback happens in consumers by restoring the previously pinned runtime package version, regenerating the service lockfile, running service CI, and redeploying through that service's protected rollout path. Published runtime package versions are immutable and must not be republished in place.
