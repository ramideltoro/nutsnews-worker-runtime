import {
  CURRENT_ENVELOPE_SCHEMA_VERSION,
  STAGE_PAYLOAD_FIXTURES,
  WORKER_DELIVERY_BEHAVIOR,
  WORKER_ENVELOPE_SCHEMA_ID,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import { describe, expect, it } from "vitest";

import {
  createBufferedRuntimeTelemetrySink,
  createInMemoryIdempotencyStore,
  createRuntimeMessageProcessor,
  type RuntimeIdempotencyFailure,
  type RuntimeIdempotencyStore,
  type RuntimeMessageDelivery,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "../src/index.js";

const fetchFixture = STAGE_PAYLOAD_FIXTURES.find((fixture) => fixture.name === "feed-fetch-request-scheduled");
const canonicalCandidateFixture = STAGE_PAYLOAD_FIXTURES.find((fixture) => fixture.name === "canonical-candidate-new-from-fetcher");

if (fetchFixture === undefined) {
  throw new Error("Expected feed fetch request fixture to exist.");
}

if (canonicalCandidateFixture === undefined) {
  throw new Error("Expected fetcher-shaped canonical candidate fixture to exist.");
}

const fetchPipelineRunId = fetchFixture.payload["pipelineRunId"];

if (typeof fetchPipelineRunId !== "string") {
  throw new Error("Expected feed fetch request fixture to carry a pipeline run ID.");
}

describe("runtime message processor", () => {
  it("validates contracts, calls the handler, and acks successful work", async () => {
    const telemetry = createBufferedRuntimeTelemetrySink();
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      telemetry,
      handler: (context) => {
        expect(context.payload["schemaId"]).toBe("nutsnews.worker.payload.feed-fetch-request.v1");

        return {
          status: "ok"
        };
      }
    });

    const result = await processor(validDelivery());

    expect(result.action).toBe("ack");
    expect(result.reason).toBe("handled");
    expect(telemetry.events.map((event) => event.name)).toContain("runtime.message.accepted");
    expect(telemetry.events.find((event) => event.name === "runtime.message.accepted")).toMatchObject({
      pipelineRunId: fetchPipelineRunId
    });
  });

  it("acks duplicate completed idempotency keys without calling the handler again", async () => {
    const idempotencyStore = createInMemoryIdempotencyStore();
    let handlerCalls = 0;
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore,
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });

    await processor(validDelivery());
    const duplicate = await processor(validDelivery());

    expect(duplicate.action).toBe("ack");
    expect(duplicate.reason).toBe("duplicate");
    expect(handlerCalls).toBe(1);
  });

  it("keeps completed idempotency state when accepted telemetry rejects", async () => {
    const observed: RuntimeTelemetryEvent[] = [];
    const stateStore = createInMemoryIdempotencyStore();
    const failures: RuntimeIdempotencyFailure[] = [];
    const idempotencyStore: RuntimeIdempotencyStore = {
      ...stateStore,
      markFailed: (idempotencyKey, failure) => {
        failures.push(failure);
        return stateStore.markFailed(idempotencyKey, failure);
      }
    };
    let handlerCalls = 0;
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore,
      telemetry: rejectingTelemetrySink("runtime.message.accepted", observed),
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });

    const completed = await processor(validDelivery());
    const duplicate = await processor(validDelivery());

    expect(completed).toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(duplicate).toMatchObject({
      action: "ack",
      reason: "duplicate"
    });
    expect(handlerCalls).toBe(1);
    expect(failures).toEqual([]);
    expect(terminalEventNames(observed)).toEqual([
      "runtime.message.accepted",
      "runtime.message.duplicate"
    ]);
    expect(observed.find((event) => event.name === "runtime.message.accepted")).toMatchObject({
      pipelineRunId: fetchPipelineRunId
    });
    expect(observed.find((event) => event.name === "runtime.message.duplicate")).toMatchObject({
      pipelineRunId: fetchPipelineRunId
    });
  });

  it("emits one terminal outcome without releasing ambiguous claim ownership", async () => {
    const telemetry = createBufferedRuntimeTelemetrySink();
    const stateStore = createInMemoryIdempotencyStore();
    let handlerCalls = 0;
    const idempotencyStore: RuntimeIdempotencyStore = {
      ...stateStore,
      claim: async (idempotencyKey, context) => {
        await stateStore.claim(idempotencyKey, context);
        throw new Error("claim response lost");
      }
    };
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore,
      telemetry,
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });

    const delivery = validDelivery();
    const result = await processor(delivery);
    const redelivery = await createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore: stateStore,
      telemetry,
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    })(delivery);

    expect(result).toMatchObject({
      action: "retry",
      reason: "idempotency-claim-error",
      destination: {
        name: "nutsnews.worker.fetch.v1.retry-30s"
      }
    });
    expect(redelivery).toMatchObject({
      action: "retry",
      reason: "idempotency-in-progress"
    });
    expect(handlerCalls).toBe(0);
    expect(terminalEventNames(telemetry.events)).toEqual([
      "runtime.message.retry",
      "runtime.message.retry"
    ]);
  });

  it("does not downgrade completed state when an idempotency claim response rejects", async () => {
    const telemetry = createBufferedRuntimeTelemetrySink();
    const stateStore = createInMemoryIdempotencyStore();
    const delivery = validDelivery();
    const envelope = delivery.envelope as WorkerMessageEnvelope;
    const seededClaim = await stateStore.claim(envelope.idempotencyKey, {
      envelope,
      stage: "fetch",
      receivedAt: "2026-07-23T05:00:01.000Z"
    });

    if (seededClaim.status !== "claimed") {
      throw new Error("Expected to seed an owned idempotency claim.");
    }

    await stateStore.markCompleted(envelope.idempotencyKey, {
      completedAt: "2026-07-23T05:00:02.000Z",
      messageId: envelope.messageId,
      claimToken: seededClaim.claimToken,
      stage: "fetch"
    });

    const rejectingStore: RuntimeIdempotencyStore = {
      ...stateStore,
      claim: async (idempotencyKey, context) => {
        await stateStore.claim(idempotencyKey, context);
        throw new Error("claim response lost");
      }
    };
    let handlerCalls = 0;
    const firstProcessor = createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore: rejectingStore,
      telemetry,
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });

    const first = await firstProcessor(delivery);
    const second = await createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore: stateStore,
      telemetry,
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    })(delivery);

    expect(first).toMatchObject({
      action: "retry",
      reason: "idempotency-claim-error"
    });
    expect(second).toMatchObject({
      action: "ack",
      reason: "duplicate"
    });
    expect(handlerCalls).toBe(0);
    expect(terminalEventNames(telemetry.events)).toEqual([
      "runtime.message.retry",
      "runtime.message.duplicate"
    ]);
  });

  it("retries once and releases the claim when completion recording rejects", async () => {
    const telemetry = createBufferedRuntimeTelemetrySink();
    const stateStore = createInMemoryIdempotencyStore();
    const failures: RuntimeIdempotencyFailure[] = [];
    const idempotencyStore: RuntimeIdempotencyStore = {
      ...stateStore,
      markCompleted: () => Promise.reject(new Error("completion store unavailable")),
      releaseClaim: (idempotencyKey, failure) => {
        failures.push(failure);
        return stateStore.releaseClaim(idempotencyKey, failure);
      }
    };
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore,
      telemetry,
      handler: () => ({
        status: "ok"
      })
    });

    const result = await processor(validDelivery());

    expect(result).toMatchObject({
      action: "retry",
      reason: "idempotency-completion-error",
      destination: {
        name: "nutsnews.worker.fetch.v1.retry-30s"
      }
    });
    expect(failures).toEqual([
      expect.objectContaining({
        reason: "idempotency-completion-error",
        retryable: true
      })
    ]);
    expect(terminalEventNames(telemetry.events)).toEqual([
      "runtime.message.retry"
    ]);
  });

  it("acks final-attempt work when completion commits before its response rejects", async () => {
    const telemetry = createBufferedRuntimeTelemetrySink();
    const stateStore = createInMemoryIdempotencyStore();
    const idempotencyStore: RuntimeIdempotencyStore = {
      ...stateStore,
      markCompleted: async (idempotencyKey, completion) => {
        await stateStore.markCompleted(idempotencyKey, completion);
        throw new Error("completion response lost");
      }
    };
    let handlerCalls = 0;
    const firstProcessor = createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore,
      telemetry,
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });

    const delivery = validDelivery({
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-07-23T05:00:00.000Z",
        lastAttemptAt: "2026-07-23T05:30:00.000Z"
      }
    });
    const first = await firstProcessor(delivery);
    const second = await createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore: stateStore,
      telemetry,
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    })(delivery);

    expect(first).toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(second).toMatchObject({
      action: "ack",
      reason: "duplicate"
    });
    expect(handlerCalls).toBe(1);
    expect(terminalEventNames(telemetry.events)).toEqual([
      "runtime.message.accepted",
      "runtime.message.duplicate"
    ]);
  });

  it("retries with a bounded state-store reason when failure recording rejects", async () => {
    const telemetry = createBufferedRuntimeTelemetrySink();
    const stateStore = createInMemoryIdempotencyStore();
    const idempotencyStore: RuntimeIdempotencyStore = {
      ...stateStore,
      markFailed: () => Promise.reject(new Error("failure store unavailable"))
    };
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore,
      telemetry,
      handler: () => ({
        status: "retry",
        reason: "unbounded-upstream-detail-must-not-reach-telemetry"
      })
    });

    const result = await processor(validDelivery());

    expect(result).toMatchObject({
      action: "retry",
      reason: "idempotency-failure-record-error",
      destination: {
        name: "nutsnews.worker.fetch.v1.retry-30s"
      }
    });
    expect(terminalEventNames(telemetry.events)).toEqual([
      "runtime.message.retry"
    ]);
    expect(telemetry.events.find((event) => event.name === "runtime.message.retry")?.attributes).toMatchObject({
      reason: "idempotency-failure-record-error"
    });
  });

  it("routes handler retry outcomes to the contracted retry tier", async () => {
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      handler: () => ({
        status: "retry",
        reason: "transient-fixture-error",
        retryAfterMs: 2_000
      })
    });

    const result = await processor(validDelivery());

    expect(result.action).toBe("retry");
    expect(result.reason).toBe("transient-fixture-error");

    if (result.action === "retry") {
      expect(result.destination.name).toBe("nutsnews.worker.fetch.v1.retry-30s");
      expect(result.retryAfterMs).toBe(2_000);
    }
  });

  it("preserves the requested retry when retry telemetry rejects", async () => {
    const observed: RuntimeTelemetryEvent[] = [];
    const stateStore = createInMemoryIdempotencyStore();
    const failures: RuntimeIdempotencyFailure[] = [];
    const idempotencyStore: RuntimeIdempotencyStore = {
      ...stateStore,
      markFailed: (idempotencyKey, failure) => {
        failures.push(failure);
        return stateStore.markFailed(idempotencyKey, failure);
      }
    };
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore,
      telemetry: rejectingTelemetrySink("runtime.message.retry", observed),
      handler: () => ({
        status: "retry",
        reason: "transient-fixture-error",
        retryAfterMs: 2_000
      })
    });

    const result = await processor(validDelivery());

    expect(result).toMatchObject({
      action: "retry",
      reason: "transient-fixture-error",
      retryAfterMs: 2_000,
      destination: {
        name: "nutsnews.worker.fetch.v1.retry-30s"
      }
    });
    expect(terminalEventNames(observed)).toEqual([
      "runtime.message.retry"
    ]);
    expect(observed.find((event) => event.name === "runtime.message.retry")).toMatchObject({
      pipelineRunId: fetchPipelineRunId
    });
    expect(failures).toEqual([
      expect.objectContaining({
        reason: "transient-fixture-error",
        retryable: true
      })
    ]);
  });

  it("sends retry-exhausted messages to the terminal DLQ", async () => {
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      handler: () => ({
        status: "retry",
        reason: "retry-exhausted"
      })
    });

    const result = await processor(validDelivery({
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-07-23T05:00:00.000Z",
        lastAttemptAt: "2026-07-23T05:30:00.000Z"
      }
    }));

    expect(result.action).toBe("dlq");

    if (result.action === "dlq") {
      expect(result.destination?.name).toBe("nutsnews.worker.fetch.v1.dlq");
    }
  });

  it("preserves the terminal DLQ decision when DLQ telemetry rejects", async () => {
    const observed: RuntimeTelemetryEvent[] = [];
    const stateStore = createInMemoryIdempotencyStore();
    const failures: RuntimeIdempotencyFailure[] = [];
    const idempotencyStore: RuntimeIdempotencyStore = {
      ...stateStore,
      markFailed: (idempotencyKey, failure) => {
        failures.push(failure);
        return stateStore.markFailed(idempotencyKey, failure);
      }
    };
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore,
      telemetry: rejectingTelemetrySink("runtime.message.dlq", observed),
      handler: () => ({
        status: "terminal-failure",
        reason: "permanent-fixture-error"
      })
    });

    const result = await processor(validDelivery());

    expect(result).toMatchObject({
      action: "dlq",
      reason: "permanent-fixture-error",
      destination: {
        name: "nutsnews.worker.fetch.v1.dlq"
      }
    });
    expect(terminalEventNames(observed)).toEqual([
      "runtime.message.dlq"
    ]);
    expect(observed.find((event) => event.name === "runtime.message.dlq")).toMatchObject({
      pipelineRunId: fetchPipelineRunId
    });
    expect(failures).toEqual([
      expect.objectContaining({
        reason: "permanent-fixture-error",
        retryable: false
      })
    ]);
  });

  it("rejects malformed envelopes before the handler runs", async () => {
    let handlerCalls = 0;
    const telemetry = createBufferedRuntimeTelemetrySink();
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      telemetry,
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });

    const result = await processor({
      envelope: {
        route: "fetch"
      },
      payload: fetchFixture.payload
    });

    expect(result.action).toBe("dlq");
    expect(result.reason).toBe("invalid-envelope");
    expect(handlerCalls).toBe(0);
    expect(telemetry.events).toContainEqual(expect.objectContaining({
      name: "runtime.message.invalid",
      level: "warn",
      stage: "fetch",
      outcome: "invalid"
    }));
  });

  it("never copies malformed or oversized pipeline run IDs from rejected payloads", async () => {
    let handlerCalls = 0;
    const telemetry = createBufferedRuntimeTelemetrySink();
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      telemetry,
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });

    for (const pipelineRunId of [
      "not-a-pipeline-uuid",
      "x".repeat(2_048)
    ]) {
      const result = await processor(validDelivery(undefined, {
        ...fetchFixture.payload,
        pipelineRunId
      }));

      expect(result).toMatchObject({
        action: "dlq",
        reason: "invalid-payload"
      });
    }

    expect(handlerCalls).toBe(0);
    const invalidEvents = telemetry.events.filter((event) => event.name === "runtime.message.invalid");
    expect(invalidEvents).toHaveLength(2);
    expect(invalidEvents.every((event) => event.pipelineRunId === undefined)).toBe(true);
  });

  it("preserves the invalid DLQ decision when validation telemetry rejects", async () => {
    const observed: RuntimeTelemetryEvent[] = [];
    const stateStore = createInMemoryIdempotencyStore();
    let claimCalls = 0;
    const idempotencyStore: RuntimeIdempotencyStore = {
      ...stateStore,
      claim: (idempotencyKey, context) => {
        claimCalls += 1;
        return stateStore.claim(idempotencyKey, context);
      }
    };
    let handlerCalls = 0;
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      idempotencyStore,
      telemetry: rejectingTelemetrySink("runtime.message.invalid", observed),
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });

    const result = await processor({
      envelope: {
        route: "fetch"
      },
      payload: fetchFixture.payload
    });

    expect(result).toMatchObject({
      action: "dlq",
      reason: "invalid-envelope"
    });
    expect(handlerCalls).toBe(0);
    expect(claimCalls).toBe(0);
    expect(terminalEventNames(observed)).toEqual([
      "runtime.message.invalid"
    ]);
    expect(observed.find((event) => event.name === "runtime.message.invalid")?.pipelineRunId).toBeUndefined();
  });

  it("accepts a payload produced by another stage when this worker owns its consumer", async () => {
    const enrichmentFixture = STAGE_PAYLOAD_FIXTURES.find((fixture) => fixture.name === "enrichment-result-success");

    if (enrichmentFixture === undefined) {
      throw new Error("Expected enrichment result fixture to exist.");
    }

    let handlerCalls = 0;
    const processor = createRuntimeMessageProcessor({
      stage: "approval",
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });

    const result = await processor(validDelivery({
      route: "approval",
      producer: {
        name: "enrichment",
        version: "0.1.0"
      }
    }, enrichmentFixture.payload));

    expect(result.action).toBe("ack");
    expect(result.reason).toBe("handled");
    expect(handlerCalls).toBe(1);
  });

  it("accepts the real fetcher-shaped candidate only at the canonicalizer boundary", async () => {
    let handlerCalls = 0;
    const processor = createRuntimeMessageProcessor({
      stage: "canonicalization",
      handler: (context) => {
        handlerCalls += 1;
        expect(context.payload).toMatchObject({
          candidateId: "cand-001",
          feedId: "feed-world",
          dedupeStatus: "new"
        });

        return {
          status: "ok"
        };
      }
    });

    const result = await processor(validDelivery({
      route: "canonicalization",
      idempotencyKey: String(canonicalCandidateFixture.payload["idempotencyKey"]),
      producer: {
        name: "fetcher",
        version: "0.1.0"
      },
      aggregate: {
        type: "candidate",
        id: "cand-001",
        version: 1
      }
    }, canonicalCandidateFixture.payload));

    expect(result).toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(handlerCalls).toBe(1);

    const oldConsumerProcessor = createRuntimeMessageProcessor({
      stage: "enrichment",
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });
    const rejected = await oldConsumerProcessor(validDelivery({
      route: "enrichment"
    }, canonicalCandidateFixture.payload));

    expect(rejected).toMatchObject({
      action: "dlq",
      reason: "payload-consumer-mismatch"
    });
    expect(handlerCalls).toBe(1);
  });

  it("rejects payloads owned by a different consumer", async () => {
    const translationFixture = STAGE_PAYLOAD_FIXTURES.find((fixture) => fixture.name === "translation-task-new-article");

    if (translationFixture === undefined) {
      throw new Error("Expected translation task fixture to exist.");
    }

    const telemetry = createBufferedRuntimeTelemetrySink();
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      telemetry,
      handler: () => ({
        status: "ok"
      })
    });

    const result = await processor(validDelivery(undefined, translationFixture.payload));

    expect(result.action).toBe("dlq");
    expect(result.reason).toBe("payload-consumer-mismatch");
    expect(telemetry.events.find((event) => event.name === "runtime.message.invalid")).toMatchObject({
      pipelineRunId: translationFixture.payload["pipelineRunId"]
    });
  });
});

function rejectingTelemetrySink(
  rejectedEvent: RuntimeTelemetryEvent["name"],
  observed: RuntimeTelemetryEvent[]
): RuntimeTelemetrySink {
  return {
    emit: (event) => {
      observed.push(event);

      return event.name === rejectedEvent
        ? Promise.reject(new Error(`telemetry unavailable for ${event.name}`))
        : undefined;
    }
  };
}

function terminalEventNames(events: readonly RuntimeTelemetryEvent[]): RuntimeTelemetryEvent["name"][] {
  return events
    .map((event) => event.name)
    .filter((name) => (
      name === "runtime.message.accepted"
      || name === "runtime.message.duplicate"
      || name === "runtime.message.invalid"
      || name === "runtime.message.retry"
      || name === "runtime.message.dlq"
    ));
}

function validDelivery(
  envelopeOverrides: Partial<WorkerMessageEnvelope> = {},
  payload: Readonly<Record<string, unknown>> = fetchFixture.payload
): RuntimeMessageDelivery {
  return {
    envelope: {
      ...baseEnvelope(),
      ...envelopeOverrides
    },
    payload
  };
}

function baseEnvelope(): WorkerMessageEnvelope {
  return {
    schemaId: WORKER_ENVELOPE_SCHEMA_ID,
    schemaVersion: CURRENT_ENVELOPE_SCHEMA_VERSION,
    route: "fetch",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3701",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3700",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b36ff",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "feed:world:story-001",
    aggregate: {
      type: "feed",
      id: "feed-world",
      version: 1
    },
    occurredAt: "2026-07-23T05:00:00.000Z",
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: "2026-07-23T05:00:00.000Z"
    },
    producer: {
      name: "scheduler",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "worker-payload://fixture/feed-fetch-request",
      mediaType: "application/json",
      sizeBytes: 512
    }
  };
}
