import type {
  WorkerMessageEnvelope,
  WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";

import {
  SYSTEM_RUNTIME_CLOCK,
  runtimeNow,
  type RuntimeClock
} from "./telemetry.js";

export interface RuntimeIdempotencyClaimContext {
  readonly envelope: WorkerMessageEnvelope;
  readonly stage: WorkerStage;
  readonly receivedAt: string;
}

export interface RuntimeIdempotencyCompletion {
  readonly completedAt: string;
  readonly messageId: string;
  readonly stage: WorkerStage;
}

export interface RuntimeIdempotencyFailure {
  readonly failedAt: string;
  readonly messageId: string;
  readonly stage: WorkerStage;
  readonly reason: string;
  readonly retryable: boolean;
}

export type RuntimeIdempotencyClaimResult =
  | {
      readonly status: "claimed";
      readonly firstSeenAt: string;
      readonly replay: boolean;
    }
  | {
      readonly status: "already-completed";
      readonly firstSeenAt: string;
      readonly completedAt: string;
      readonly completion?: RuntimeIdempotencyCompletion;
    }
  | {
      readonly status: "in-progress";
      readonly firstSeenAt: string;
    };

export interface RuntimeIdempotencyStore {
  claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult>;
  markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void>;
  markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void>;
}

interface InMemoryIdempotencyEntry {
  readonly status: "in-progress" | "completed" | "failed";
  readonly firstSeenAt: string;
  readonly completion?: RuntimeIdempotencyCompletion;
  readonly failure?: RuntimeIdempotencyFailure;
}

export function createPassThroughIdempotencyStore(): RuntimeIdempotencyStore {
  return {
    claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
      void idempotencyKey;

      return Promise.resolve({
        status: "claimed",
        firstSeenAt: context.receivedAt,
        replay: false
      });
    },
    markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
      void idempotencyKey;
      void completion;

      return Promise.resolve();
    },
    markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
      void idempotencyKey;
      void failure;

      return Promise.resolve();
    }
  };
}

export function createInMemoryIdempotencyStore(
  clock: RuntimeClock = SYSTEM_RUNTIME_CLOCK
): RuntimeIdempotencyStore {
  const records = new Map<string, InMemoryIdempotencyEntry>();

  return {
    claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
      const existing = records.get(idempotencyKey);

      if (existing === undefined) {
        records.set(idempotencyKey, {
          status: "in-progress",
          firstSeenAt: context.receivedAt
        });

        return Promise.resolve({
          status: "claimed",
          firstSeenAt: context.receivedAt,
          replay: false
        });
      }

      if (existing.status === "completed") {
        const completedAt = existing.completion?.completedAt ?? existing.firstSeenAt;

        if (existing.completion === undefined) {
          return Promise.resolve({
            status: "already-completed",
            firstSeenAt: existing.firstSeenAt,
            completedAt
          });
        }

        return Promise.resolve({
          status: "already-completed",
          firstSeenAt: existing.firstSeenAt,
          completedAt,
          completion: existing.completion
        });
      }

      if (existing.status === "in-progress") {
        return Promise.resolve({
          status: "in-progress",
          firstSeenAt: existing.firstSeenAt
        });
      }

      records.set(idempotencyKey, {
        status: "in-progress",
        firstSeenAt: existing.firstSeenAt
      });

      return Promise.resolve({
        status: "claimed",
        firstSeenAt: existing.firstSeenAt,
        replay: true
      });
    },
    markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
      const existing = records.get(idempotencyKey);

      records.set(idempotencyKey, {
        status: "completed",
        firstSeenAt: existing?.firstSeenAt ?? completion.completedAt,
        completion
      });

      return Promise.resolve();
    },
    markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
      const existing = records.get(idempotencyKey);

      records.set(idempotencyKey, {
        status: "failed",
        firstSeenAt: existing?.firstSeenAt ?? runtimeNow(clock),
        failure
      });

      return Promise.resolve();
    }
  };
}
