import {
  getRetryDestination,
  validateStagePayload,
  validateWorkerEnvelope,
  getWorkerRoute,
  type StagePayloadValidationIssue,
  type WorkerDeadLetterQueue,
  type WorkerMessageEnvelope,
  type WorkerRetryQueue,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";

import {
  createPassThroughIdempotencyStore,
  type RuntimeIdempotencyClaimReleaseResult,
  type RuntimeIdempotencyStore
} from "./idempotency.js";
import {
  SYSTEM_RUNTIME_CLOCK,
  emitRuntimeTelemetry,
  runtimeNow,
  type RuntimeClock,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "./telemetry.js";
import {
  normalizeRuntimeUuidIdentifier
} from "./telemetry-identifiers.js";

export interface RuntimeMessageDelivery {
  readonly envelope: unknown;
  readonly payload: unknown;
  readonly receivedAt?: string;
}

export interface RuntimeValidationIssue {
  readonly path: string;
  readonly code: string;
  readonly message: string;
}

export interface RuntimeMessageContext {
  readonly envelope: WorkerMessageEnvelope;
  readonly payload: Readonly<Record<string, unknown>>;
  readonly stage: WorkerStage;
  readonly receivedAt: string;
}

export type RuntimeHandlerResult =
  | {
      readonly status: "ok";
    }
  | {
      readonly status: "retry";
      readonly reason: string;
      readonly retryAfterMs?: number;
    }
  | {
      readonly status: "terminal-failure";
      readonly reason: string;
    };

export type RuntimeMessageHandler = (
  context: RuntimeMessageContext
) => RuntimeHandlerResult | Promise<RuntimeHandlerResult>;

export type RuntimeMessageProcessingResult =
  | {
      readonly action: "ack";
      readonly reason: "handled" | "duplicate";
      readonly envelope: WorkerMessageEnvelope;
    }
  | {
      readonly action: "retry";
      readonly reason: string;
      readonly envelope: WorkerMessageEnvelope;
      readonly destination: WorkerRetryQueue;
      readonly retryAfterMs?: number;
    }
  | {
      readonly action: "dlq";
      readonly reason: string;
      readonly envelope?: WorkerMessageEnvelope;
      readonly destination?: WorkerDeadLetterQueue;
      readonly issues?: readonly RuntimeValidationIssue[];
    };

export interface RuntimeMessageProcessorOptions {
  readonly stage: WorkerStage;
  readonly handler: RuntimeMessageHandler;
  readonly idempotencyStore?: RuntimeIdempotencyStore;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly clock?: RuntimeClock;
}

export type RuntimeMessageProcessor = (
  delivery: RuntimeMessageDelivery
) => Promise<RuntimeMessageProcessingResult>;

export function createRuntimeMessageProcessor(options: RuntimeMessageProcessorOptions): RuntimeMessageProcessor {
  const idempotencyStore = options.idempotencyStore ?? createPassThroughIdempotencyStore();
  const clock = options.clock ?? SYSTEM_RUNTIME_CLOCK;

  return async (delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> => {
    const receivedAt = delivery.receivedAt ?? runtimeNow(clock);
    const startedAtMs = clock.now().getTime();
    const queue = getWorkerRoute(options.stage).mainQueue.name;
    await emitProcessorTelemetry(options.telemetry, {
      name: "runtime.message.started",
      level: "info",
      at: runtimeNow(clock),
      stage: options.stage,
      queue,
      outcome: "started"
    });

    const envelopeResult = validateWorkerEnvelope(delivery.envelope);

    if (!envelopeResult.ok) {
      const issues = envelopeResult.issues.map(toRuntimeValidationIssue);
      await emitInvalid(options.telemetry, options.stage, undefined, issues, clock, queue, elapsedMs(clock, startedAtMs));

      return {
        action: "dlq",
        reason: "invalid-envelope",
        issues
      };
    }

    const envelope = envelopeResult.value;

    if (envelope.route !== options.stage) {
      const issues = [
        {
          path: "$.route",
          code: "stage-mismatch",
          message: `Envelope route ${envelope.route} does not match processor stage ${options.stage}.`
        }
      ];
      await emitInvalid(options.telemetry, options.stage, envelope, issues, clock, queue, elapsedMs(clock, startedAtMs));

      return terminalResult(envelope, "stage-mismatch", issues);
    }

    const payloadResult = validateStagePayload(delivery.payload);

    if (!payloadResult.ok) {
      const issues = payloadResult.issues.map(toRuntimeValidationIssue);
      await emitInvalid(options.telemetry, options.stage, envelope, issues, clock, queue, elapsedMs(clock, startedAtMs));

      return terminalResult(envelope, "invalid-payload", issues);
    }

    const pipelineRunId = pipelineRunIdFromValidatedPayload(payloadResult.value);
    const expectedConsumer = getWorkerRoute(options.stage).consumer;

    if (payloadResult.definition.consumer !== expectedConsumer) {
      const issues = [
        {
          path: "$.schemaId",
          code: "payload-consumer-mismatch",
          message: `Payload schema is consumed by ${payloadResult.definition.consumer}, not ${expectedConsumer}.`
        }
      ];
      await emitInvalid(
        options.telemetry,
        options.stage,
        envelope,
        issues,
        clock,
        queue,
        elapsedMs(clock, startedAtMs),
        pipelineRunId
      );

      return terminalResult(envelope, "payload-consumer-mismatch", issues);
    }

    const finishRetryOrDlq = async (
      reason: string,
      retryAfterMs?: number
    ): Promise<RuntimeMessageProcessingResult> => {
      const result = retryOrDlq(envelope, reason, retryAfterMs);
      await emitRetryOrDlq(
        options.telemetry,
        options.stage,
        result,
        pipelineRunId,
        clock,
        queue,
        elapsedMs(clock, startedAtMs)
      );

      return result;
    };
    const finishAccepted = async (): Promise<RuntimeMessageProcessingResult> => {
      await emitProcessorTelemetry(options.telemetry, {
        name: "runtime.message.accepted",
        level: "info",
        at: runtimeNow(clock),
        stage: options.stage,
        ...envelopeTelemetryFields(envelope, queue, elapsedMs(clock, startedAtMs), pipelineRunId),
        outcome: "success"
      });

      return {
        action: "ack",
        reason: "handled",
        envelope
      };
    };
    let claim: Awaited<ReturnType<RuntimeIdempotencyStore["claim"]>>;

    try {
      claim = await idempotencyStore.claim(envelope.idempotencyKey, {
        envelope,
        stage: options.stage,
        receivedAt
      });
    } catch {
      return finishRetryOrDlq(IDEMPOTENCY_FAILURE_REASONS.claim);
    }

    if (claim.status === "already-completed") {
      await emitProcessorTelemetry(options.telemetry, {
        name: "runtime.message.duplicate",
        level: "info",
        at: runtimeNow(clock),
        stage: options.stage,
        ...envelopeTelemetryFields(envelope, queue, elapsedMs(clock, startedAtMs), pipelineRunId),
        outcome: "duplicate",
        attributes: {
          firstSeenAt: claim.firstSeenAt,
          completedAt: claim.completedAt
        }
      });

      return {
        action: "ack",
        reason: "duplicate",
        envelope
      };
    }

    if (claim.status === "in-progress") {
      return finishRetryOrDlq("idempotency-in-progress", 1_000);
    }
    const claimToken = claim.claimToken;

    const context: RuntimeMessageContext = {
      envelope,
      payload: payloadResult.value,
      stage: options.stage,
      receivedAt
    };

    let handlerResult: RuntimeHandlerResult;

    try {
      handlerResult = await options.handler(context);
    } catch (error: unknown) {
      const failureRecorded = await recordIdempotencyFailure(idempotencyStore, envelope, options.stage, clock, claimToken, {
        reason: classifyHandlerError(error),
        retryable: true
      });

      return finishRetryOrDlq(failureRecorded
        ? "handler-error"
        : IDEMPOTENCY_FAILURE_REASONS.failureRecord);
    }

    if (handlerResult.status === "ok") {
      try {
        await idempotencyStore.markCompleted(envelope.idempotencyKey, {
          completedAt: runtimeNow(clock),
          messageId: envelope.messageId,
          claimToken,
          stage: options.stage
        });
      } catch {
        const releaseResult = await releaseIdempotencyClaim(idempotencyStore, envelope, options.stage, clock, claimToken, {
          reason: IDEMPOTENCY_FAILURE_REASONS.completion,
          retryable: true
        });

        if (releaseResult !== "release-error" && releaseResult.status === "preserved-completed") {
          return finishAccepted();
        }

        return finishRetryOrDlq(IDEMPOTENCY_FAILURE_REASONS.completion);
      }

      return finishAccepted();
    }

    if (handlerResult.status === "retry") {
      const failureRecorded = await recordIdempotencyFailure(idempotencyStore, envelope, options.stage, clock, claimToken, {
        reason: handlerResult.reason,
        retryable: true
      });

      return finishRetryOrDlq(
        failureRecorded ? handlerResult.reason : IDEMPOTENCY_FAILURE_REASONS.failureRecord,
        handlerResult.retryAfterMs
      );
    }

    const failureRecorded = await recordIdempotencyFailure(idempotencyStore, envelope, options.stage, clock, claimToken, {
      reason: handlerResult.reason,
      retryable: false
    });

    if (!failureRecorded) {
      return finishRetryOrDlq(IDEMPOTENCY_FAILURE_REASONS.failureRecord);
    }

    const result = terminalResult(envelope, handlerResult.reason);
    await emitRetryOrDlq(
      options.telemetry,
      options.stage,
      result,
      pipelineRunId,
      clock,
      queue,
      elapsedMs(clock, startedAtMs)
    );

    return result;
  };
}

const IDEMPOTENCY_FAILURE_REASONS = {
  claim: "idempotency-claim-error",
  completion: "idempotency-completion-error",
  failureRecord: "idempotency-failure-record-error"
} as const;

async function releaseIdempotencyClaim(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  stage: WorkerStage,
  clock: RuntimeClock,
  claimToken: string,
  failure: {
    readonly reason: string;
    readonly retryable: boolean;
  }
): Promise<RuntimeIdempotencyClaimReleaseResult | "release-error"> {
  try {
    return await store.releaseClaim(envelope.idempotencyKey, {
      failedAt: runtimeNow(clock),
      messageId: envelope.messageId,
      claimToken,
      stage,
      reason: failure.reason,
      retryable: failure.retryable
    });
  } catch {
    // A conditional release is best-effort; the retry/DLQ result remains authoritative.
    return "release-error";
  }
}

async function recordIdempotencyFailure(
  store: RuntimeIdempotencyStore,
  envelope: WorkerMessageEnvelope,
  stage: WorkerStage,
  clock: RuntimeClock,
  claimToken: string,
  failure: {
    readonly reason: string;
    readonly retryable: boolean;
  }
): Promise<boolean> {
  try {
    await store.markFailed(envelope.idempotencyKey, {
      failedAt: runtimeNow(clock),
      messageId: envelope.messageId,
      claimToken,
      stage,
      reason: failure.reason,
      retryable: failure.retryable
    });

    return true;
  } catch {
    return false;
  }
}

async function emitProcessorTelemetry(
  sink: RuntimeTelemetrySink | undefined,
  event: RuntimeTelemetryEvent
): Promise<void> {
  try {
    await emitRuntimeTelemetry(sink, event);
  } catch {
    // Telemetry is best-effort and must never alter message or idempotency state.
  }
}

function retryOrDlq(
  envelope: WorkerMessageEnvelope,
  reason: string,
  retryAfterMs?: number
): RuntimeMessageProcessingResult {
  const destination = getRetryDestination(envelope.route, envelope.attempt.count);

  if ("ttlMs" in destination) {
    if (retryAfterMs === undefined) {
      return {
        action: "retry",
        reason,
        envelope,
        destination
      };
    }

    return {
      action: "retry",
      reason,
      envelope,
      destination,
      retryAfterMs
    };
  }

  return {
    action: "dlq",
    reason,
    envelope,
    destination
  };
}

function terminalResult(
  envelope: WorkerMessageEnvelope,
  reason: string,
  issues?: readonly RuntimeValidationIssue[]
): RuntimeMessageProcessingResult {
  const destination = getRetryDestination(envelope.route, envelope.attempt.max);

  if (!("routingKey" in destination) || "ttlMs" in destination) {
    if (issues === undefined) {
      return {
        action: "dlq",
        reason,
        envelope
      };
    }

    return {
      action: "dlq",
      reason,
      envelope,
      issues
    };
  }

  if (issues === undefined) {
    return {
      action: "dlq",
      reason,
      envelope,
      destination
    };
  }

  return {
    action: "dlq",
    reason,
    envelope,
    destination,
    issues
  };
}

function toRuntimeValidationIssue(issue: StagePayloadValidationIssue | RuntimeValidationIssue): RuntimeValidationIssue {
  return {
    path: issue.path,
    code: issue.code,
    message: issue.message
  };
}

function classifyHandlerError(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown-handler-error";
}

async function emitInvalid(
  telemetry: RuntimeTelemetrySink | undefined,
  stage: WorkerStage,
  envelope: WorkerMessageEnvelope | undefined,
  issues: readonly RuntimeValidationIssue[],
  clock: RuntimeClock,
  queue: string,
  durationMs: number,
  pipelineRunId?: string
): Promise<void> {
  const firstIssue = issues[0];
  const attributes = firstIssue === undefined
    ? undefined
    : {
        issueCode: firstIssue.code,
        issuePath: firstIssue.path
      };

  await emitProcessorTelemetry(telemetry, {
    name: "runtime.message.invalid",
    level: "warn",
    at: runtimeNow(clock),
    stage,
    queue,
    durationMs,
    outcome: "invalid",
    ...(envelope === undefined
      ? {}
      : envelopeTelemetryFields(envelope, queue, durationMs, pipelineRunId)),
    ...(attributes === undefined
      ? {}
      : {
          attributes
        })
  });
}

async function emitRetryOrDlq(
  telemetry: RuntimeTelemetrySink | undefined,
  stage: WorkerStage,
  result: RuntimeMessageProcessingResult,
  pipelineRunId: string | undefined,
  clock: RuntimeClock,
  queue: string,
  durationMs: number
): Promise<void> {
  if (result.action === "retry") {
    await emitProcessorTelemetry(telemetry, {
      name: "runtime.message.retry",
      level: "warn",
      at: runtimeNow(clock),
      stage,
      ...envelopeTelemetryFields(result.envelope, queue, durationMs, pipelineRunId),
      outcome: "retry",
      attributes: {
        reason: result.reason,
        destination: result.destination.name
      }
    });

    return;
  }

  if (result.action === "dlq") {
    await emitProcessorTelemetry(telemetry, {
      name: "runtime.message.dlq",
      level: "error",
      at: runtimeNow(clock),
      stage,
      ...(result.envelope === undefined
        ? {}
        : envelopeTelemetryFields(result.envelope, queue, durationMs, pipelineRunId)),
      outcome: "dlq",
      attributes: {
        reason: result.reason,
        destination: result.destination?.name ?? "unroutable"
      }
    });
  }
}

function elapsedMs(clock: RuntimeClock, startedAtMs: number): number {
  return Math.max(0, clock.now().getTime() - startedAtMs);
}

function envelopeTelemetryFields(
  envelope: WorkerMessageEnvelope,
  queue: string,
  durationMs: number,
  pipelineRunId?: string
): Readonly<Record<string, string | number>> {
  const base = {
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    causationId: envelope.causationId,
    traceparent: envelope.traceparent,
    idempotencyKey: envelope.idempotencyKey,
    queue,
    attempt: envelope.attempt.count,
    durationMs,
    ...(pipelineRunId === undefined ? {} : {
      pipelineRunId
    })
  } as const;

  if (envelope.tracestate === undefined) {
    return base;
  }

  return {
    ...base,
    tracestate: envelope.tracestate
  };
}

function pipelineRunIdFromValidatedPayload(payload: Readonly<Record<string, unknown>>): string | undefined {
  return normalizeRuntimeUuidIdentifier(payload["pipelineRunId"]);
}
