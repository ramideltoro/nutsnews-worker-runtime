import type {
  WorkerMessageEnvelope,
  WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";

import {
  classifyRuntimeFailure,
  type RuntimeFailureClassification
} from "./failure.js";
import type {
  RuntimeMessageContext,
  RuntimeMessageHandler,
  RuntimeHandlerResult
} from "./message.js";
import {
  createTerminalFailurePayload,
  getRuntimeRetryDecision,
  type RuntimeMessageIdFactory
} from "./retry.js";
import {
  SYSTEM_RUNTIME_CLOCK,
  runtimeNow,
  type RuntimeClock
} from "./telemetry.js";

export type RuntimeInboxClaimResult =
  | {
      readonly status: "new";
    }
  | {
      readonly status: "duplicate-completed";
      readonly recordedAt: string;
    }
  | {
      readonly status: "in-progress";
      readonly retryAfterMs?: number;
    }
  | {
      readonly status: "conflicting-idempotency-reuse";
      readonly reason: string;
    }
  | {
      readonly status: "crash-after-side-effect";
      readonly recordedAt: string;
    };

export interface RuntimeInboxOutboxStore {
  claim(context: RuntimeMessageContext): Promise<RuntimeInboxClaimResult>;
  recordSuccess(context: RuntimeMessageContext): Promise<void>;
  recordRetry(context: RuntimeMessageContext, failure: RuntimeFailureClassification): Promise<void>;
  recordTerminalFailure(
    context: RuntimeMessageContext,
    failure: RuntimeFailureClassification,
    payload: Readonly<Record<string, unknown>>
  ): Promise<void>;
}

export interface RuntimeAtLeastOnceMiddlewareOptions {
  readonly stage: WorkerStage;
  readonly store: RuntimeInboxOutboxStore;
  readonly handler: RuntimeMessageHandler;
  readonly clock?: RuntimeClock;
  readonly messageIdFactory?: RuntimeMessageIdFactory;
}

export function createAtLeastOnceMiddleware(options: RuntimeAtLeastOnceMiddlewareOptions): RuntimeMessageHandler {
  const clock = options.clock ?? SYSTEM_RUNTIME_CLOCK;

  return async (context: RuntimeMessageContext): Promise<RuntimeHandlerResult> => {
    assertContextStage(options.stage, context.envelope);
    const claim = await options.store.claim(context);

    if (claim.status === "duplicate-completed" || claim.status === "crash-after-side-effect") {
      return {
        status: "ok"
      };
    }

    if (claim.status === "in-progress") {
      if (claim.retryAfterMs === undefined) {
        return {
          status: "retry",
          reason: "idempotency-in-progress"
        };
      }

      return {
        status: "retry",
        reason: "idempotency-in-progress",
        retryAfterMs: claim.retryAfterMs
      };
    }

    if (claim.status === "conflicting-idempotency-reuse") {
      const failure = classifyRuntimeFailure(new Error(claim.reason));
      await options.store.recordTerminalFailure(context, failure, terminalPayload(context.envelope, failure, clock, options.messageIdFactory, false));

      return {
        status: "terminal-failure",
        reason: "conflicting-idempotency-reuse"
      };
    }

    try {
      const result = await options.handler(context);

      if (result.status === "ok") {
        await options.store.recordSuccess(context);
        return result;
      }

      if (result.status === "retry") {
        await options.store.recordRetry(context, classifyRuntimeFailure(new Error(result.reason)));
        return result;
      }

      const failure = classifyRuntimeFailure(new Error(result.reason));
      await options.store.recordTerminalFailure(context, failure, terminalPayload(context.envelope, failure, clock, options.messageIdFactory, false));
      return result;
    } catch (error: unknown) {
      const failure = classifyRuntimeFailure(error);
      const retryDecision = getRuntimeRetryDecision(context.envelope);

      if (failure.retryable && !retryDecision.exhausted) {
        await options.store.recordRetry(context, failure);

        if (failure.retryAfterMs === undefined) {
          return {
            status: "retry",
            reason: failure.reason
          };
        }

        return {
          status: "retry",
          reason: failure.reason,
          retryAfterMs: failure.retryAfterMs
        };
      }

      await options.store.recordTerminalFailure(context, failure, terminalPayload(context.envelope, failure, clock, options.messageIdFactory, retryDecision.exhausted));

      return {
        status: "terminal-failure",
        reason: failure.reason
      };
    }
  };
}

function assertContextStage(stage: WorkerStage, envelope: WorkerMessageEnvelope): void {
  if (stage !== envelope.route) {
    throw new Error(`At-least-once middleware stage ${stage} received ${envelope.route}.`);
  }
}

function terminalPayload(
  envelope: WorkerMessageEnvelope,
  failure: RuntimeFailureClassification,
  clock: RuntimeClock,
  messageIdFactory: RuntimeMessageIdFactory | undefined,
  retryExhausted: boolean
): Readonly<Record<string, unknown>> {
  if (messageIdFactory === undefined) {
    return createTerminalFailurePayload(envelope, failure, {
      now: runtimeNow(clock),
      retryExhausted
    });
  }

  return createTerminalFailurePayload(envelope, failure, {
    now: runtimeNow(clock),
    retryExhausted,
    messageIdFactory
  });
}
