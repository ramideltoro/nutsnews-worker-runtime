# Runtime API

`@ramideltoro/nutsnews-worker-runtime` provides small, stage-neutral primitives for worker-uplift services. The package depends only on `@ramideltoro/nutsnews-worker-contracts` and delegates service-specific broker clients, storage, business logic, and credentials to each deployable service.

## Modules

| Module | Purpose |
| --- | --- |
| `broker` | Manage a service-provided broker transport lifecycle, topology assertion, publishing, consuming, consumer-state monitoring, and shutdown state transitions. |
| `rabbitmq` | Provide RabbitMQ confirm-channel transport, topology assertion, mandatory persistent publishing, manual ack/nack settlement, retry/DLQ transfer, bounded prefetch, reconnect backoff, payload-loader injection, and network seams. |
| `message` | Validate contract envelopes and payloads, apply idempotency hooks, wrap service handlers, and return ack/retry/DLQ actions. |
| `processing` | Provide at-least-once middleware contracts for service-owned PostgreSQL inbox/outbox stores, duplicate handling, conflict detection, crash-after-side-effect recovery, failure classification, retry, and terminal failure recording. |
| `failure` / `retry` | Classify failures, redact diagnostics, create retry envelopes with new message IDs, create terminal failure payloads, and create replay envelopes with audit records. |
| `idempotency` | Define durable idempotency hook contracts plus pass-through and in-memory implementations for local tests. |
| `telemetry` | Emit structured runtime events, JSON log lines, and Prometheus exposition text through service-provided sinks without owning metrics backends or credentials. |
| `health` | Evaluate separate liveness, startup, and readiness probes with critical-check semantics and safe dependency reasons. |
| `shutdown` | Coordinate idempotent shutdown callbacks, in-flight drain tracking, and telemetry flush hooks from service-provided signal sources. |
| `trace` | Propagate W3C `traceparent`/`tracestate` headers and correlation fields without requiring span export. |

## Import

```ts
import {
  createBrokerLifecycle,
  createBrokerConsumerMonitor,
  createBrokerConsumerReadinessCheck,
  createAtLeastOnceMiddleware,
  createJsonRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink,
  createRabbitMqTransport,
  createRuntimeHealthProbeSet,
  createRuntimeMessageProcessor,
  getRuntimePackageMetadata
} from "@ramideltoro/nutsnews-worker-runtime";
```

## Boundary

The runtime package does not import legacy worker source, backend host code, or deployable worker services. Public APIs accept interfaces for service-owned PostgreSQL inbox/outbox stores, idempotency stores, telemetry sinks, health checks, signal sources, RabbitMQ payload loaders, and RabbitMQ network seams so each service owns credentials, durable payload storage, and business adapters.

The package does not include RabbitMQ credentials, backend credentials, Grafana credentials, model prompts, feed-fetching logic, canonicalization rules, approval rules, translation logic, persistence implementation, or publication decisions.

The message processor validates a payload schema's contracted `consumer` against the consumer assigned to the envelope route. Runtime 1.0 intentionally treats schema `producer` metadata as a publisher-conformance and audit contract rather than comparing it with `envelope.producer.name`: one consumer route can accept schema families from multiple upstream producers, and deployed service names are not yet a normalized contract-stage identity. Publishers must test that each emitted schema and producer pair matches the contracts package. Runtime producer enforcement remains deferred until those identities are normalized across every service.

Message-processor telemetry is best-effort. The configured sink is still invoked for every lifecycle event so any available safe logging or metrics are retained, but a synchronous throw or rejected write is contained at the processor boundary. Telemetry failure cannot replace an `ack`, retry, or DLQ decision, change completed idempotency state to failed, or create an additional retry/DLQ side effect.

## Health And Telemetry

`createRuntimeHealthProbeSet` returns distinct liveness, startup, and readiness probe evaluators. Readiness checks should include only dependencies required to accept work, such as the broker channel or durable idempotency store, and health details are redacted before they are returned.

Consumer services use `createBrokerConsumerMonitor` in their broker transport and add `createBrokerConsumerReadinessCheck` for their consumed stage. The check is critical and reports the bounded main queue, active consumer count, lifecycle state, reason, and transition timestamp. Scheduler-only producers do not add this check.

`createJsonRuntimeTelemetrySink` writes one redacted JSON object per runtime event with `service`, `version`, `environment`, optional host/revision/deployment/adapter identity, queue, attempt, duration, outcome, and message/pipeline-run/idempotency/correlation/trace IDs as log fields. The message processor copies `pipelineRunId` only after the payload contract validates its canonical UUID form. The JSON boundary applies the same canonical UUID and W3C trace normalization to direct sink calls, omits malformed or oversized identifier metadata, and never truncates an invalid value into a misleading identifier. These IDs are structured log metadata only and are never promoted to metric labels or Loki stream labels.

`createPrometheusRuntimeTelemetrySink` exposes bounded Prometheus text for throughput, retry, DLQ, in-flight work, isolated liveness/startup/readiness state, active consumers, consumer lifecycle events, shutdown drain state, build/deployment identity, expected activity, and service-owned last success.

Terminal lifecycle metrics keep `success`, `duplicate`, `invalid`, `failure`, and `dlq` distinct; `retry` remains an intermediate outcome. Contract or payload validation failures emit `runtime.message.invalid` with `outcome="invalid"` and increment both terminal-message and DLQ counters under that bounded outcome.

Validated deliveries always produce exactly one terminal message event. Idempotency-store claim, completion, and failure-record errors become bounded `idempotency-claim-error`, `idempotency-completion-error`, or `idempotency-failure-record-error` retry/DLQ reasons. A successful claim returns an opaque ownership token that completion, failure, and conditional release operations must compare. A rejected claim response is never released because no ownership token was returned; redelivery remains blocked by the store's lease/in-progress policy. After a known successful claim, completion errors call `RuntimeIdempotencyStore.releaseClaim`, which may release only the token's in-progress claim and must preserve completed records and claims owned by another delivery. If the store reports that completion was preserved, the processor acknowledges the work and emits `accepted`, including on the final attempt. This compare-and-set boundary handles completion reject-before-commit and commit-then-reject responses without downgrading completed work. Store failures never escape with only a `runtime.message.started` event.

Production idempotency adapters must expire or safely renew in-progress claims with a lease no longer than five minutes, then atomically reclaim expired rows. The in-memory store is test-only and does not model lease expiry. See [the 1.0 migration guide](migrations/MIGRATION-v1.0.0.md) for required adapter conformance cases.

Processing and dependency latency use `_seconds` histograms with fixed buckets `0.005`, `0.01`, `0.025`, `0.05`, `0.1`, `0.25`, `0.5`, `1`, `2.5`, `5`, `10`, `30`, `60`, `120`, and `300`, plus `+Inf`. Version 1.0 intentionally removes the old `_duration_ms_{sum,count}` summaries. Dependency duration is observed only from a `runtime.dependency.observed` event carrying a real `durationMs`; absent and non-finite values produce no sample.

Services provide stable dependency and health-check allowlists when constructing the sink. Each list is capped at 32 normalized values; missing values map to `unknown` and unlisted values map to `other`. Metric-family schemas use only the dimensions meaningful to that family. Build revision is present only on the single `build_info` series, while message, pipeline-run, article, feed, request, correlation, idempotency, and trace identifiers remain structured log metadata.

Queue labels are restricted to the queues defined by the worker contract (plus the configured default queue and `none`). Arbitrary queue strings collapse to `other`, preventing malformed telemetry from creating unbounded series.

`setExpectedActive` and `setLastSuccessTimestamp` replace their single service-level gauge sample. Prometheus `up` and scrape metadata remain authoritative for scrape freshness; the runtime does not emit a self-refreshing freshness timestamp.
