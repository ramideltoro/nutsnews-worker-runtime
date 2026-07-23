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
  type RuntimeIdempotencyStore
} from "./idempotency.js";
import {
  SYSTEM_RUNTIME_CLOCK,
  emitRuntimeTelemetry,
  runtimeNow,
  type RuntimeClock,
  type RuntimeTelemetrySink
} from "./telemetry.js";

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
    await emitRuntimeTelemetry(options.telemetry, {
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

    if (payloadResult.definition.stage !== options.stage) {
      const issues = [
        {
          path: "$.schemaId",
          code: "payload-stage-mismatch",
          message: `Payload schema belongs to ${payloadResult.definition.stage}, not ${options.stage}.`
        }
      ];
      await emitInvalid(options.telemetry, options.stage, envelope, issues, clock, queue, elapsedMs(clock, startedAtMs));

      return terminalResult(envelope, "payload-stage-mismatch", issues);
    }

    const claim = await idempotencyStore.claim(envelope.idempotencyKey, {
      envelope,
      stage: options.stage,
      receivedAt
    });

    if (claim.status === "already-completed") {
      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.message.duplicate",
        level: "info",
        at: runtimeNow(clock),
        stage: options.stage,
        ...envelopeTelemetryFields(envelope, queue, elapsedMs(clock, startedAtMs)),
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
      const result = retryOrDlq(envelope, "idempotency-in-progress", 1_000);
      await emitRetryOrDlq(options.telemetry, options.stage, result, clock, queue, elapsedMs(clock, startedAtMs));

      return result;
    }

    const context: RuntimeMessageContext = {
      envelope,
      payload: payloadResult.value,
      stage: options.stage,
      receivedAt
    };

    try {
      const handlerResult = await options.handler(context);

      if (handlerResult.status === "ok") {
        await idempotencyStore.markCompleted(envelope.idempotencyKey, {
          completedAt: runtimeNow(clock),
          messageId: envelope.messageId,
          stage: options.stage
        });
        await emitRuntimeTelemetry(options.telemetry, {
          name: "runtime.message.accepted",
          level: "info",
          at: runtimeNow(clock),
          stage: options.stage,
          ...envelopeTelemetryFields(envelope, queue, elapsedMs(clock, startedAtMs)),
          outcome: "success"
        });

        return {
          action: "ack",
          reason: "handled",
          envelope
        };
      }

      if (handlerResult.status === "retry") {
        await idempotencyStore.markFailed(envelope.idempotencyKey, {
          failedAt: runtimeNow(clock),
          messageId: envelope.messageId,
          stage: options.stage,
          reason: handlerResult.reason,
          retryable: true
        });

        const result = retryOrDlq(envelope, handlerResult.reason, handlerResult.retryAfterMs);
        await emitRetryOrDlq(options.telemetry, options.stage, result, clock, queue, elapsedMs(clock, startedAtMs));

        return result;
      }

      await idempotencyStore.markFailed(envelope.idempotencyKey, {
        failedAt: runtimeNow(clock),
        messageId: envelope.messageId,
        stage: options.stage,
        reason: handlerResult.reason,
        retryable: false
      });

      const result = terminalResult(envelope, handlerResult.reason);
      await emitRetryOrDlq(options.telemetry, options.stage, result, clock, queue, elapsedMs(clock, startedAtMs));

      return result;
    } catch (error: unknown) {
      await idempotencyStore.markFailed(envelope.idempotencyKey, {
        failedAt: runtimeNow(clock),
        messageId: envelope.messageId,
        stage: options.stage,
        reason: classifyHandlerError(error),
        retryable: true
      });

      const result = retryOrDlq(envelope, "handler-error");
      await emitRetryOrDlq(options.telemetry, options.stage, result, clock, queue, elapsedMs(clock, startedAtMs));

      return result;
    }
  };
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
  durationMs: number
): Promise<void> {
  const firstIssue = issues[0];
  const attributes = firstIssue === undefined
    ? undefined
    : {
        issueCode: firstIssue.code,
        issuePath: firstIssue.path
      };

  await emitRuntimeTelemetry(telemetry, {
    name: "runtime.message.invalid",
    level: "warn",
    at: runtimeNow(clock),
    stage,
    queue,
    durationMs,
    outcome: "failure",
    ...(envelope === undefined
      ? {}
      : envelopeTelemetryFields(envelope, queue, durationMs)),
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
  clock: RuntimeClock,
  queue: string,
  durationMs: number
): Promise<void> {
  if (result.action === "retry") {
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.retry",
      level: "warn",
      at: runtimeNow(clock),
      stage,
      ...envelopeTelemetryFields(result.envelope, queue, durationMs),
      outcome: "retry",
      attributes: {
        reason: result.reason,
        destination: result.destination.name
      }
    });

    return;
  }

  if (result.action === "dlq") {
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.message.dlq",
      level: "error",
      at: runtimeNow(clock),
      stage,
      ...(result.envelope === undefined
        ? {}
        : envelopeTelemetryFields(result.envelope, queue, durationMs)),
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
  durationMs: number
): Readonly<Record<string, string | number>> {
  const base = {
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    causationId: envelope.causationId,
    traceparent: envelope.traceparent,
    idempotencyKey: envelope.idempotencyKey,
    queue,
    attempt: envelope.attempt.count,
    durationMs
  } as const;

  if (envelope.tracestate === undefined) {
    return base;
  }

  return {
    ...base,
    tracestate: envelope.tracestate
  };
}
