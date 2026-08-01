# Changelog

All notable changes to `@ramideltoro/nutsnews-worker-runtime` are documented here.

This package follows semantic versioning. Published versions are immutable; consumers roll back by pinning a previously published version.

## 1.0.0 - 2026-08-01

- Validate incoming payload schemas against the routed worker consumer, allowing valid cross-stage handoffs such as enrichment to approval while rejecting schemas owned by another consumer.
- Align the runtime's exact contract dependency with `@ramideltoro/nutsnews-worker-contracts@1.0.0`, whose corrected ownership metadata accepts the real fetcher-to-canonicalizer candidate handoff.
- Convert idempotency claim, completion, and failure-record errors into one bounded retry/DLQ outcome per validated delivery. Ambiguous claim failures are never released without a returned opaque ownership token; after a known claim, token-aware completion/failure operations and the new `releaseClaim` compare-and-set hook cannot downgrade completed work or another delivery's claim. A completion confirmed during conditional release is acknowledged instead of retried or sent to DLQ.
- Replace millisecond sum/count summaries with fixed-bucket Prometheus histograms in seconds for processing, dependency, and health-check duration.
- Make `runtime.dependency.observed` the sole dependency-duration observation path so missing and duplicate zero-duration samples cannot distort latency.
- Export isolated liveness, startup, readiness, and bounded dependency-check states, plus build, deployment-adapter, expected-active, and last-success signals.
- Preserve invalid validation events as the bounded `invalid` terminal outcome instead of folding them into generic failure metrics.
- Contain telemetry sink failures at the shared message-processor boundary so an attempted log or metric cannot change completed idempotency state or create retry/DLQ decisions.
- Carry the validated payload's optional `pipelineRunId` as policy-bounded structured log metadata for cross-stage Loki drilldowns, canonicalizing UUID/trace identifiers and omitting malformed or oversized values while explicitly excluding identifiers from metric and Loki stream labels.
- Require explicit dependency and health-check allowlists; unapproved values collapse to `other`, while correlation and content identifiers remain log metadata only.
- Bound event-derived stage and per-family message, dependency, and consumer outcomes to fixed allowlists with a shared `other` fallback.
- Require release locks to use the exact GitHub Packages contracts origin and version path, require migration notes only for major transitions or explicitly breaking releases, and disable dependency lifecycle scripts in package workflows.
- Refresh the development lockfile to patched `brace-expansion` releases.
- Remove the public `recordDependencyLatency` method. Consumers must migrate through service-local conformance tests before adopting this major version.

## 0.5.0 - 2026-07-28

- Add broker consumer state monitors and lifecycle status so service readiness can fail when its main queue has zero active consumers.
- Emit structured cancellation, channel-drop, recovery, and close transitions with bounded consumer-count and consumer-event Prometheus metrics.
- Recover shared RabbitMQ transport consumers after broker cancellation or a dropped connection/channel.

## 0.4.0 - 2026-07-23

- Add separate liveness, startup, and readiness health probe helpers with safe redacted dependency details.
- Add redacted JSON telemetry logs and bounded Prometheus metrics for throughput, retries, DLQ, processing/dependency latency, in-flight work, health, and shutdown draining.
- Add W3C trace-context propagation helpers and RabbitMQ `tracestate` header propagation without requiring span export.
- Add in-flight drain support so RabbitMQ shutdown cancels consumers, waits for active deliveries to settle, and only then closes channels.

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
