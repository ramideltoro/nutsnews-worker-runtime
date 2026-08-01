import type {
  WorkerMessageEnvelope,
  WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";

import {
  SYSTEM_RUNTIME_CLOCK,
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
  readonly claimToken: string;
  readonly stage: WorkerStage;
}

export interface RuntimeIdempotencyFailure {
  readonly failedAt: string;
  readonly messageId: string;
  readonly claimToken: string;
  readonly stage: WorkerStage;
  readonly reason: string;
  readonly retryable: boolean;
}

export type RuntimeIdempotencyClaimReleaseResult =
  | {
      readonly status: "released";
    }
  | {
      readonly status: "preserved-completed";
    }
  | {
      readonly status: "not-owned";
    };

export type RuntimeIdempotencyClaimResult =
  | {
      readonly status: "claimed";
      readonly firstSeenAt: string;
      readonly replay: boolean;
      readonly claimToken: string;
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
  /**
   * Release only an in-progress claim owned by failure.claimToken. Implementations
   * must preserve completed records and claims owned by another delivery.
   */
  releaseClaim(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<RuntimeIdempotencyClaimReleaseResult>;
}

interface InMemoryIdempotencyEntry {
  readonly status: "in-progress" | "completed" | "failed";
  readonly firstSeenAt: string;
  readonly claimToken?: string;
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
        replay: false,
        claimToken: `${context.envelope.messageId}:pass-through`
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
    },
    releaseClaim(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<RuntimeIdempotencyClaimReleaseResult> {
      void idempotencyKey;
      void failure;

      return Promise.resolve({
        status: "released"
      });
    }
  };
}

export function createInMemoryIdempotencyStore(
  clock: RuntimeClock = SYSTEM_RUNTIME_CLOCK
): RuntimeIdempotencyStore {
  const records = new Map<string, InMemoryIdempotencyEntry>();
  let claimSequence = 0;

  return {
    claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
      const existing = records.get(idempotencyKey);

      if (existing === undefined) {
        const claimToken = nextClaimToken(context.envelope.messageId);
        records.set(idempotencyKey, {
          status: "in-progress",
          firstSeenAt: context.receivedAt,
          claimToken
        });

        return Promise.resolve({
          status: "claimed",
          firstSeenAt: context.receivedAt,
          replay: false,
          claimToken
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

      const claimToken = nextClaimToken(context.envelope.messageId);
      records.set(idempotencyKey, {
        status: "in-progress",
        firstSeenAt: existing.firstSeenAt,
        claimToken
      });

      return Promise.resolve({
        status: "claimed",
        firstSeenAt: existing.firstSeenAt,
        replay: true,
        claimToken
      });
    },
    markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
      const existing = records.get(idempotencyKey);

      if (existing?.status !== "in-progress" || existing.claimToken !== completion.claimToken) {
        return Promise.reject(new Error("Cannot complete an idempotency claim owned by another delivery."));
      }

      records.set(idempotencyKey, {
        status: "completed",
        firstSeenAt: existing.firstSeenAt,
        completion
      });

      return Promise.resolve();
    },
    markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
      const existing = records.get(idempotencyKey);

      if (existing?.status !== "in-progress" || existing.claimToken !== failure.claimToken) {
        return Promise.reject(new Error("Cannot fail an idempotency claim owned by another delivery."));
      }

      records.set(idempotencyKey, {
        status: "failed",
        firstSeenAt: existing.firstSeenAt,
        failure
      });

      return Promise.resolve();
    },
    releaseClaim(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<RuntimeIdempotencyClaimReleaseResult> {
      const existing = records.get(idempotencyKey);

      if (existing?.status === "completed") {
        return Promise.resolve({
          status: "preserved-completed"
        });
      }

      if (existing?.status !== "in-progress" || existing.claimToken !== failure.claimToken) {
        return Promise.resolve({
          status: "not-owned"
        });
      }

      records.set(idempotencyKey, {
        status: "failed",
        firstSeenAt: existing.firstSeenAt,
        failure
      });

      return Promise.resolve({
        status: "released"
      });
    }
  };

  function nextClaimToken(messageId: string): string {
    claimSequence += 1;

    return `${messageId}:${String(clock.now().getTime())}:${String(claimSequence)}`;
  }
}
