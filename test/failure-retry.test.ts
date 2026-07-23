import {
  CURRENT_ENVELOPE_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  WORKER_ENVELOPE_SCHEMA_ID,
  validateStagePayload,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import { describe, expect, it } from "vitest";

import {
  RuntimeStageError,
  classifyRuntimeFailure,
  createReplayEnvelope,
  createRetryEnvelope,
  createTerminalFailurePayload,
  getRuntimeRetryDecision,
  redactRuntimeDiagnostics
} from "../src/index.js";

describe("runtime failure classification and redaction", () => {
  it("classifies explicit stage errors and redacts sensitive diagnostics", () => {
    const failure = classifyRuntimeFailure(new RuntimeStageError("Provider timeout", {
      failureClass: "transient",
      diagnostics: {
        authorization: "Bearer secret",
        responseBody: "model output",
        safeCode: "timeout"
      }
    }));

    expect(failure).toMatchObject({
      failureClass: "transient",
      retryable: true,
      reason: "provider-timeout"
    });
    expect(failure.diagnostics).toEqual({
      authorization: "[redacted]",
      responseBody: "[redacted]",
      safeCode: "timeout"
    });
  });

  it("classifies rate limits and preserves retry-after metadata", () => {
    const failure = classifyRuntimeFailure({
      status: 429,
      retryAfterMs: 2_500
    });

    expect(failure.failureClass).toBe("rate-limited");
    expect(failure.retryable).toBe(true);
    expect(failure.retryAfterMs).toBe(2_500);
  });

  it("redacts nested request and response data", () => {
    expect(redactRuntimeDiagnostics({
      request: {
        apiKey: "secret",
        url: "https://example.test"
      },
      body: {
        text: "article body"
      }
    })).toEqual({
      request: {
        apiKey: "[redacted]",
        url: "https://example.test"
      },
      body: "[redacted]"
    });
  });
});

describe("runtime retry and replay primitives", () => {
  it("creates retry envelopes with new message IDs and preserved routing identity", () => {
    const retryEnvelope = createRetryEnvelope(envelope(), {
      now: "2026-07-23T05:50:00.000Z",
      messageIdFactory: () => "018f1598-2dd5-7c4f-9f92-8f7a7f8b3a02"
    });

    expect(retryEnvelope).toMatchObject({
      messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3a02",
      causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3a00",
      correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b39ff",
      idempotencyKey: "feed:world:story-004",
      occurredAt: "2026-07-23T05:50:00.000Z",
      attempt: {
        count: 2,
        lastAttemptAt: "2026-07-23T05:50:00.000Z"
      }
    });
  });

  it("detects retry exhaustion at the declared terminal DLQ", () => {
    const decision = getRuntimeRetryDecision(envelope({
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-07-23T05:40:00.000Z",
        lastAttemptAt: "2026-07-23T05:55:00.000Z"
      }
    }));

    expect(decision.exhausted).toBe(true);
    expect(decision.destination.name).toBe("nutsnews.worker.fetch.v1.dlq");
  });

  it("creates contract-valid terminal failure payloads with redacted diagnostics", () => {
    const failure = classifyRuntimeFailure(new RuntimeStageError("Sensitive failure", {
      failureClass: "permanent",
      diagnostics: {
        token: "secret",
        code: "bad_input"
      }
    }));
    const payload = createTerminalFailurePayload(envelope(), failure, {
      now: "2026-07-23T05:55:00.000Z",
      retryExhausted: true,
      stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3a03"
    });

    expect(validateStagePayload(payload).ok).toBe(true);
    expect(String(payload["sanitizedError"])).toContain("[redacted]");
  });

  it("creates replay envelopes with audit history and enforced attempt gates", () => {
    const replay = createReplayEnvelope(envelope(), {
      now: "2026-07-23T05:56:00.000Z",
      requestedBy: "operator",
      reason: "manual-replay",
      messageIdFactory: () => "018f1598-2dd5-7c4f-9f92-8f7a7f8b3a04"
    });

    expect(replay.audit).toMatchObject({
      replayOfMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3a01",
      replayMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3a04",
      originalAttemptCount: 1,
      replayAttemptCount: 2
    });
    expect(() => createReplayEnvelope(envelope({
      attempt: {
        count: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: "2026-07-23T05:40:00.000Z"
      }
    }), {
      now: "2026-07-23T05:56:00.000Z",
      requestedBy: "operator",
      reason: "manual-replay"
    })).toThrow("attempt gate");
  });
});

function envelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  return {
    schemaId: WORKER_ENVELOPE_SCHEMA_ID,
    schemaVersion: CURRENT_ENVELOPE_SCHEMA_VERSION,
    route: "fetch",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3a01",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3a00",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b39ff",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "feed:world:story-004",
    aggregate: {
      type: "feed",
      id: "feed-world",
      version: 1
    },
    occurredAt: "2026-07-23T05:40:00.000Z",
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: "2026-07-23T05:40:00.000Z"
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
