import {
  CURRENT_ENVELOPE_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  WORKER_ENVELOPE_SCHEMA_ID,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import { describe, expect, it } from "vitest";

import {
  RuntimeStageError,
  createAtLeastOnceMiddleware,
  type RuntimeInboxClaimResult,
  type RuntimeInboxOutboxStore,
  type RuntimeMessageContext
} from "../src/index.js";

describe("at-least-once middleware", () => {
  it("returns recorded duplicate success without repeating side effects", async () => {
    const store = new FakeInboxOutboxStore({
      status: "duplicate-completed",
      recordedAt: "2026-07-23T06:00:00.000Z"
    });
    let handlerCalls = 0;
    const middleware = createAtLeastOnceMiddleware({
      stage: "fetch",
      store,
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });

    await expect(middleware(context())).resolves.toEqual({
      status: "ok"
    });
    expect(handlerCalls).toBe(0);
  });

  it("distinguishes conflicting idempotency reuse as terminal failure", async () => {
    const store = new FakeInboxOutboxStore({
      status: "conflicting-idempotency-reuse",
      reason: "aggregate mismatch"
    });
    const middleware = createAtLeastOnceMiddleware({
      stage: "fetch",
      store,
      messageIdFactory: () => "018f1598-2dd5-7c4f-9f92-8f7a7f8b3b02",
      handler: () => ({
        status: "ok"
      })
    });

    const result = await middleware(context());

    expect(result).toEqual({
      status: "terminal-failure",
      reason: "conflicting-idempotency-reuse"
    });
    expect(store.terminalFailures).toHaveLength(1);
  });

  it("records retry for transient failures before returning retry", async () => {
    const store = new FakeInboxOutboxStore({
      status: "new"
    });
    const middleware = createAtLeastOnceMiddleware({
      stage: "fetch",
      store,
      handler: () => {
        throw new RuntimeStageError("HTTP timeout", {
          failureClass: "transient"
        });
      }
    });

    await expect(middleware(context())).resolves.toEqual({
      status: "retry",
      reason: "http-timeout"
    });
    expect(store.retries).toEqual([
      "transient:http-timeout"
    ]);
  });

  it("routes retry exhaustion to terminal failure", async () => {
    const store = new FakeInboxOutboxStore({
      status: "new"
    });
    const middleware = createAtLeastOnceMiddleware({
      stage: "fetch",
      store,
      handler: () => {
        throw new RuntimeStageError("Still unavailable", {
          failureClass: "transient"
        });
      }
    });

    const result = await middleware(context(envelope({
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-07-23T06:00:00.000Z"
      }
    })));

    expect(result).toEqual({
      status: "terminal-failure",
      reason: "still-unavailable"
    });
    expect(store.terminalFailures[0]?.payload["retryExhausted"]).toBe(true);
  });

  it("treats crash-after-side-effect recovery as recorded success", async () => {
    const store = new FakeInboxOutboxStore({
      status: "crash-after-side-effect",
      recordedAt: "2026-07-23T06:00:00.000Z"
    });
    let handlerCalls = 0;
    const middleware = createAtLeastOnceMiddleware({
      stage: "fetch",
      store,
      handler: () => {
        handlerCalls += 1;

        return {
          status: "ok"
        };
      }
    });

    await expect(middleware(context())).resolves.toEqual({
      status: "ok"
    });
    expect(handlerCalls).toBe(0);
  });
});

class FakeInboxOutboxStore implements RuntimeInboxOutboxStore {
  retries: string[] = [];
  terminalFailures: {
    readonly failureClass: string;
    readonly payload: Readonly<Record<string, unknown>>;
  }[] = [];

  constructor(private readonly claimResult: RuntimeInboxClaimResult) {}

  claim(contextValue: RuntimeMessageContext): Promise<RuntimeInboxClaimResult> {
    void contextValue;

    return Promise.resolve(this.claimResult);
  }

  recordSuccess(contextValue: RuntimeMessageContext): Promise<void> {
    void contextValue;

    return Promise.resolve();
  }

  recordRetry(contextValue: RuntimeMessageContext, failure: { readonly failureClass: string; readonly reason: string }): Promise<void> {
    void contextValue;
    this.retries.push(`${failure.failureClass}:${failure.reason}`);

    return Promise.resolve();
  }

  recordTerminalFailure(
    contextValue: RuntimeMessageContext,
    failure: { readonly failureClass: string },
    payload: Readonly<Record<string, unknown>>
  ): Promise<void> {
    void contextValue;
    this.terminalFailures.push({
      failureClass: failure.failureClass,
      payload
    });

    return Promise.resolve();
  }
}

function context(envelopeValue: WorkerMessageEnvelope = envelope()): RuntimeMessageContext {
  return {
    envelope: envelopeValue,
    payload: {
      schemaId: "nutsnews.worker.payload.feed-fetch-request.v1"
    },
    stage: "fetch",
    receivedAt: "2026-07-23T06:00:00.000Z"
  };
}

function envelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  return {
    schemaId: WORKER_ENVELOPE_SCHEMA_ID,
    schemaVersion: CURRENT_ENVELOPE_SCHEMA_VERSION,
    route: "fetch",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3b01",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3b00",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3aff",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "feed:world:story-005",
    aggregate: {
      type: "feed",
      id: "feed-world",
      version: 1
    },
    occurredAt: "2026-07-23T06:00:00.000Z",
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: "2026-07-23T06:00:00.000Z"
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
    },
    ...overrides
  };
}
