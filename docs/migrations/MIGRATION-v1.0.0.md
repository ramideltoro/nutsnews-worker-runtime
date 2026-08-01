# Migration to runtime 1.0.0

Runtime `1.0.0` is a breaking telemetry and idempotency release. Publish and
install-smoke `@ramideltoro/nutsnews-worker-contracts@1.0.0` first, then
regenerate this repository's lockfile from that immutable package before the
runtime release is merged or tagged.

## Telemetry migration

- Replace `_duration_ms` summary queries with the new fixed-bucket
  `_duration_seconds` histograms.
- Remove calls to `recordDependencyLatency`; emit one
  `runtime.dependency.observed` event with the real duration instead.
- Replace the aggregate health family with separate liveness, startup,
  readiness, and bounded dependency-check metrics.
- Keep message, pipeline-run, correlation, trace, and content IDs in structured
  logs only. Do not add them to Prometheus labels or Loki stream labels.
- Pass `pipelineRunId` only from a successfully validated payload. Runtime JSON
  telemetry canonicalizes UUID/trace identifiers and omits malformed or
  oversized values rather than indexing or serializing unsafe correlation data.

## Idempotency adapter migration

Every `RuntimeIdempotencyStore` implementation must:

1. Return a fresh opaque `claimToken` only when `claim` atomically acquires the
   idempotency key.
2. Require that token for `markCompleted`, `markFailed`, and `releaseClaim`.
3. Implement those operations as compare-and-set transitions. A stale token
   must never change another delivery's claim, and no failure transition may
   downgrade a completed record.
4. Return `preserved-completed` from `releaseClaim` when completion committed
   but its response was lost. The processor then acknowledges the delivery and
   emits one accepted terminal event, including on the final attempt.
5. Never release a rejected `claim` call: no ownership token was returned.

Production claim rows must have a bounded lease so a claim whose response was
lost cannot remain in-progress forever. Use a lease no longer than five minutes
(the second contracted retry tier), atomically reclaim expired rows, and bound
the service operation below that lease or renew it safely in the adapter. The
in-memory store is test-only and intentionally does not model lease expiry.

Adapter conformance tests must cover:

- claim commit followed by response rejection;
- completion rejection before commit;
- completion commit followed by response rejection, including the final attempt;
- a stale token attempting to release another delivery's claim;
- completed records resisting every failure/release path;
- expiry and reclaim at the configured production lease bound.

## Producer conformance

Runtime 1.0 enforces the contracted consumer for each decoded payload. It does
not compare `envelope.producer.name` with the schema's producer metadata because
deployed service names are not yet a normalized contract-stage identity.
Publisher tests must assert that every emitted schema and producer pair matches
`@ramideltoro/nutsnews-worker-contracts@1.0.0`. Add runtime enforcement only
after all publisher identities have been normalized and audited.

## Rollback

Restore the previous exact runtime and contracts pins together, regenerate the
consumer lockfile, run service CI, and redeploy through the protected shadow
rollout. Do not republish either package version in place.
