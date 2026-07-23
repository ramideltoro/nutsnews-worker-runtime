import { randomUUID } from "node:crypto";

import {
  CURRENT_ENVELOPE_SCHEMA_VERSION,
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  getRetryDestination,
  getWorkerRoute,
  type WorkerDeadLetterQueue,
  type WorkerMessageEnvelope,
  type WorkerRetryQueue
} from "@ramideltoro/nutsnews-worker-contracts";

import type {
  RuntimeFailureClassification
} from "./failure.js";

export type RuntimeMessageIdFactory = () => string;

export const randomUuidMessageIdFactory: RuntimeMessageIdFactory = () => randomUUID();

export interface RuntimeRetryEnvelopeOptions {
  readonly now: string;
  readonly messageIdFactory?: RuntimeMessageIdFactory;
}

export interface RuntimeRetryDecision {
  readonly exhausted: boolean;
  readonly destination: WorkerRetryQueue | WorkerDeadLetterQueue;
}

export interface RuntimeReplayAudit {
  readonly replayOfMessageId: string;
  readonly replayMessageId: string;
  readonly requestedBy: string;
  readonly reason: string;
  readonly createdAt: string;
  readonly originalAttemptCount: number;
  readonly replayAttemptCount: number;
}

export interface RuntimeReplayResult {
  readonly envelope: WorkerMessageEnvelope;
  readonly audit: RuntimeReplayAudit;
}

export function createRetryEnvelope(
  envelope: WorkerMessageEnvelope,
  options: RuntimeRetryEnvelopeOptions
): WorkerMessageEnvelope {
  const messageIdFactory = options.messageIdFactory ?? randomUuidMessageIdFactory;

  return {
    ...envelope,
    messageId: messageIdFactory(),
    occurredAt: options.now,
    attempt: {
      ...envelope.attempt,
      count: Math.min(envelope.attempt.count + 1, envelope.attempt.max),
      lastAttemptAt: options.now
    }
  };
}

export function getRuntimeRetryDecision(envelope: WorkerMessageEnvelope): RuntimeRetryDecision {
  const destination = getRetryDestination(envelope.route, envelope.attempt.count);

  return {
    exhausted: !("ttlMs" in destination),
    destination
  };
}

export function computeRetryJitterMs(
  delayMs: number,
  jitterRatio: number,
  random: () => number = Math.random
): number {
  const boundedRatio = Math.max(0, Math.min(1, jitterRatio));
  const jitter = delayMs * boundedRatio * ((random() * 2) - 1);

  return Math.round(jitter);
}

export function createTerminalFailurePayload(
  envelope: WorkerMessageEnvelope,
  failure: RuntimeFailureClassification,
  options: {
    readonly now: string;
    readonly stageExecutionId?: string;
    readonly retryExhausted: boolean;
    readonly messageIdFactory?: RuntimeMessageIdFactory;
  }
): Readonly<Record<string, unknown>> {
  const messageIdFactory = options.messageIdFactory ?? randomUuidMessageIdFactory;

  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.terminalFailure,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: envelope.correlationId,
    stageExecutionId: options.stageExecutionId ?? messageIdFactory(),
    sourceMessageId: envelope.messageId,
    idempotencyKey: envelope.idempotencyKey,
    traceparent: envelope.traceparent,
    producedAt: options.now,
    failedSchemaId: envelope.schemaId,
    failedMessageId: envelope.messageId,
    failedRoute: getWorkerRoute(envelope.route).routingKey,
    failedAttemptCount: envelope.attempt.count,
    failureClass: failure.failureClass,
    errorCode: failure.reason,
    sanitizedError: JSON.stringify(failure.diagnostics),
    retryExhausted: options.retryExhausted,
    originalPayloadRef: envelope.payloadRef
  };
}

export function createReplayEnvelope(
  envelope: WorkerMessageEnvelope,
  options: {
    readonly now: string;
    readonly requestedBy: string;
    readonly reason: string;
    readonly messageIdFactory?: RuntimeMessageIdFactory;
  }
): RuntimeReplayResult {
  const schemaVersion: number = envelope.schemaVersion;

  if (schemaVersion !== CURRENT_ENVELOPE_SCHEMA_VERSION) {
    throw new Error("Replay requires the current envelope schema version.");
  }

  if (envelope.attempt.count >= envelope.attempt.max) {
    throw new Error("Replay cannot bypass the contracted attempt gate.");
  }

  const replayEnvelope = createRetryEnvelope(envelope, options);

  return {
    envelope: replayEnvelope,
    audit: {
      replayOfMessageId: envelope.messageId,
      replayMessageId: replayEnvelope.messageId,
      requestedBy: options.requestedBy,
      reason: options.reason,
      createdAt: options.now,
      originalAttemptCount: envelope.attempt.count,
      replayAttemptCount: replayEnvelope.attempt.count
    }
  };
}
