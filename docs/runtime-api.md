# Runtime API

`@ramideltoro/nutsnews-worker-runtime` provides small, stage-neutral primitives for worker-uplift services. The package depends only on `@ramideltoro/nutsnews-worker-contracts` and delegates service-specific broker clients, storage, business logic, and credentials to each deployable service.

## Modules

| Module | Purpose |
| --- | --- |
| `broker` | Manage a service-provided broker transport lifecycle, topology assertion, publishing, consuming, and shutdown state transitions. |
| `rabbitmq` | Provide RabbitMQ confirm-channel transport, topology assertion, mandatory persistent publishing, manual ack/nack settlement, retry/DLQ transfer, bounded prefetch, reconnect backoff, payload-loader injection, and network seams. |
| `message` | Validate contract envelopes and payloads, apply idempotency hooks, wrap service handlers, and return ack/retry/DLQ actions. |
| `processing` | Provide at-least-once middleware contracts for service-owned PostgreSQL inbox/outbox stores, duplicate handling, conflict detection, crash-after-side-effect recovery, failure classification, retry, and terminal failure recording. |
| `failure` / `retry` | Classify failures, redact diagnostics, create retry envelopes with new message IDs, create terminal failure payloads, and create replay envelopes with audit records. |
| `idempotency` | Define durable idempotency hook contracts plus pass-through and in-memory implementations for local tests. |
| `telemetry` | Emit structured runtime events through service-provided telemetry sinks without owning metrics backends or credentials. |
| `health` | Evaluate readiness/liveness checks with critical-check semantics. |
| `shutdown` | Coordinate idempotent shutdown callbacks from a service-provided signal source. |

## Import

```ts
import {
  createBrokerLifecycle,
  createAtLeastOnceMiddleware,
  createRabbitMqTransport,
  createRuntimeMessageProcessor,
  getRuntimePackageMetadata
} from "@ramideltoro/nutsnews-worker-runtime";
```

## Boundary

The runtime package does not import legacy worker source, backend host code, or deployable worker services. Public APIs accept interfaces for service-owned PostgreSQL inbox/outbox stores, idempotency stores, telemetry sinks, health checks, signal sources, RabbitMQ payload loaders, and RabbitMQ network seams so each service owns credentials, durable payload storage, and business adapters.

The package does not include RabbitMQ credentials, backend credentials, Grafana credentials, model prompts, feed-fetching logic, canonicalization rules, approval rules, translation logic, persistence implementation, or publication decisions.
