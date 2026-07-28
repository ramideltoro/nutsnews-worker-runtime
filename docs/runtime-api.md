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

## Health And Telemetry

`createRuntimeHealthProbeSet` returns distinct liveness, startup, and readiness probe evaluators. Readiness checks should include only dependencies required to accept work, such as the broker channel or durable idempotency store, and health details are redacted before they are returned.

Consumer services use `createBrokerConsumerMonitor` in their broker transport and add `createBrokerConsumerReadinessCheck` for their consumed stage. The check is critical and reports the bounded main queue, active consumer count, lifecycle state, reason, and transition timestamp. Scheduler-only producers do not add this check.

`createJsonRuntimeTelemetrySink` writes one redacted JSON object per runtime event with `service`, `version`, `environment`, optional `host`, queue, attempt, duration, outcome, and message/idempotency/correlation/trace IDs as log fields. These IDs are never promoted to metric labels.

`createPrometheusRuntimeTelemetrySink` exposes bounded Prometheus text for throughput, retry, DLQ, processing latency, dependency latency, in-flight work, health, active consumers, consumer lifecycle events, and shutdown drain state. Metric labels are limited to `environment`, `host`, `service`, `version`, `queue`, and `outcome`.
