# Changelog

All notable changes to `@ramideltoro/nutsnews-worker-runtime` are documented here.

This package follows semantic versioning. Published versions are immutable; consumers roll back by pinning a previously published version.

## 0.3.0 - 2026-07-23

- Add failure classification, redaction, retry envelope generation, terminal failure payload creation, replay primitives, and at-least-once inbox/outbox middleware interfaces.
- Update RabbitMQ retry settlement to publish a new message ID, preserve causation/correlation/idempotency fields, increment attempt metadata, include retry jitter metadata, and wait for publisher confirm before original ack.
- Add tests for duplicate, conflict, crash-after-side-effect recovery, poison, replay gates, redaction, retry exhaustion, terminal failure payloads, and retry transfer ordering.

## 0.2.0 - 2026-07-23

- Add resilient RabbitMQ transport primitives using confirm channels, mandatory persistent publishing, confirm timeouts, and unroutable-message errors.
- Add topology assertion for contracted exchanges, quorum queues, retry queues, DLQ queues, bindings, TTL retry transfer, bounded prefetch, manual ack/nack settlement, and safe shutdown.
- Add injectable payload loader, network, clock, sleep, and random seams for deterministic payload, reconnect, channel recovery, publish failure, and consumer settlement tests.

## 0.1.0 - 2026-07-23

- Bootstrap the strict TypeScript runtime package, public metadata API, linting, unit tests, build, package validation, and GitHub Packages release workflow.
- Add narrow broker lifecycle, message handling, idempotency, telemetry, health, and shutdown modules that depend only on the contracts package.
- Document public APIs, package integrity verification, compatibility policy, and release/rollback controls.
