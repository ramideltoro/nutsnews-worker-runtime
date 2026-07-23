# Compatibility Policy

## Versioning

`@ramideltoro/nutsnews-worker-runtime` follows semantic versioning.

- Major versions may remove public APIs, change runtime result shapes, or require a newer major contracts package.
- Minor versions may add optional fields, new helper APIs, or support for additional backward-compatible contracts surfaces.
- Patch versions fix implementation, generated types, tests, or documentation without changing accepted public API shapes.

Consumers pin exact package versions. Version ranges are not part of the supported production upgrade path.

## Contracts Dependency

Runtime `0.1.0` depends on `@ramideltoro/nutsnews-worker-contracts@0.3.1` exactly. The package validator rejects additional runtime dependencies and rejects source imports outside relative runtime modules or the contracts package.

## Deprecation Window

Backward-compatible minor and patch releases stay supported for at least 90 days after a replacement release. Breaking changes require a major version, migration notes, and service-level conformance tests before rollout.

## Rollback

Rollback happens in consumers by restoring the previously pinned runtime package version, regenerating the service lockfile, running service CI, and redeploying through that service's protected rollout path. Published runtime package versions are immutable and must not be republished in place.
