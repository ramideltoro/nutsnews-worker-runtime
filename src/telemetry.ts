import type { WorkerStage } from "@ramideltoro/nutsnews-worker-contracts";

export interface RuntimeClock {
  now(): Date;
}

export const SYSTEM_RUNTIME_CLOCK: RuntimeClock = {
  now: () => new Date()
};

export type RuntimeTelemetryLevel = "debug" | "info" | "warn" | "error";

export const RUNTIME_TELEMETRY_EVENT_NAMES = [
  "runtime.broker.state_changed",
  "runtime.broker.topology_asserted",
  "runtime.message.accepted",
  "runtime.message.duplicate",
  "runtime.message.retry",
  "runtime.message.dlq",
  "runtime.message.invalid",
  "runtime.health.evaluated",
  "runtime.shutdown.started",
  "runtime.shutdown.completed",
  "runtime.shutdown.failed"
] as const;

export type RuntimeTelemetryEventName = (typeof RUNTIME_TELEMETRY_EVENT_NAMES)[number];
export type RuntimeTelemetryAttributes = Readonly<Record<string, string | number | boolean>>;

export interface RuntimeTelemetryEvent {
  readonly name: RuntimeTelemetryEventName;
  readonly level: RuntimeTelemetryLevel;
  readonly at: string;
  readonly stage?: WorkerStage;
  readonly messageId?: string;
  readonly correlationId?: string;
  readonly attributes?: RuntimeTelemetryAttributes;
}

export interface RuntimeTelemetrySink {
  emit(event: RuntimeTelemetryEvent): void | Promise<void>;
}

export interface BufferedRuntimeTelemetrySink extends RuntimeTelemetrySink {
  readonly events: readonly RuntimeTelemetryEvent[];
  clear(): void;
}

export function runtimeNow(clock: RuntimeClock = SYSTEM_RUNTIME_CLOCK): string {
  return clock.now().toISOString();
}

export function createNoopRuntimeTelemetrySink(): RuntimeTelemetrySink {
  return {
    emit: () => undefined
  };
}

export function createBufferedRuntimeTelemetrySink(limit = 100): BufferedRuntimeTelemetrySink {
  const events: RuntimeTelemetryEvent[] = [];

  return {
    get events(): readonly RuntimeTelemetryEvent[] {
      return [
        ...events
      ];
    },
    emit(event: RuntimeTelemetryEvent): void {
      events.push(event);

      if (events.length > limit) {
        events.splice(0, events.length - limit);
      }
    },
    clear(): void {
      events.length = 0;
    }
  };
}

export async function emitRuntimeTelemetry(
  sink: RuntimeTelemetrySink | undefined,
  event: RuntimeTelemetryEvent
): Promise<void> {
  if (sink === undefined) {
    return;
  }

  await sink.emit(event);
}
