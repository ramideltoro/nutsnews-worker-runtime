# Runtime API

`@ramideltoro/nutsnews-worker-runtime` provides small, stage-neutral primitives for worker-uplift services. The package depends only on `@ramideltoro/nutsnews-worker-contracts` and delegates service-specific broker clients, storage, business logic, and credentials to each deployable service.

## Modules

| Module | Purpose |
| --- | --- |
| `broker` | Manage a service-provided broker transport lifecycle, topology assertion, publishing, consuming, and shutdown state transitions. |
| `message` | Validate contract envelopes and payloads, apply idempotency hooks, wrap service handlers, and return ack/retry/DLQ actions. |
| `idempotency` | Define durable idempotency hook contracts plus pass-through and in-memory implementations for local tests. |
| `telemetry` | Emit structured runtime events through service-provided telemetry sinks without owning metrics backends or credentials. |
| `health` | Evaluate readiness/liveness checks with critical-check semantics. |
| `shutdown` | Coordinate idempotent shutdown callbacks from a service-provided signal source. |

## Import

```ts
import {
  createBrokerLifecycle,
  createRuntimeMessageProcessor,
  getRuntimePackageMetadata
} from "@ramideltoro/nutsnews-worker-runtime";
```

## Boundary

The runtime package does not import legacy worker source, backend host code, or deployable worker services. Public APIs accept interfaces for transports, idempotency stores, telemetry sinks, health checks, and signal sources so each service owns its own adapters.

The package does not include RabbitMQ credentials, backend credentials, Grafana credentials, model prompts, feed-fetching logic, canonicalization rules, approval rules, translation logic, persistence implementation, or publication decisions.
