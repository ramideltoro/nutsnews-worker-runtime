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
  type RuntimeMessageDelivery
} from "../src/index.js";

const fetchFixture = STAGE_PAYLOAD_FIXTURES.find((fixture) => fixture.name === "feed-fetch-request-scheduled");

if (fetchFixture === undefined) {
  throw new Error("Expected feed fetch request fixture to exist.");
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

  it("rejects malformed envelopes before the handler runs", async () => {
    let handlerCalls = 0;
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
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
  });

  it("rejects payloads from a different stage", async () => {
    const translationFixture = STAGE_PAYLOAD_FIXTURES.find((fixture) => fixture.name === "translation-task-new-article");

    if (translationFixture === undefined) {
      throw new Error("Expected translation task fixture to exist.");
    }

    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      handler: () => ({
        status: "ok"
      })
    });

    const result = await processor(validDelivery(undefined, translationFixture.payload));

    expect(result.action).toBe("dlq");
    expect(result.reason).toBe("payload-stage-mismatch");
  });
});

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
