import {
  SYSTEM_RUNTIME_CLOCK,
  emitRuntimeTelemetry,
  runtimeNow,
  type RuntimeClock,
  type RuntimeTelemetrySink
} from "./telemetry.js";

export const DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_MS = 30_000;

export type RuntimeShutdownSignal = "SIGINT" | "SIGTERM";

export interface RuntimeShutdownContext {
  readonly signal: RuntimeShutdownSignal | "manual";
  readonly startedAt: string;
}

export type RuntimeShutdownCallback = (
  context: RuntimeShutdownContext
) => void | Promise<void>;

export interface RuntimeShutdownSignalSource {
  on(signal: RuntimeShutdownSignal, listener: () => void): void;
  off(signal: RuntimeShutdownSignal, listener: () => void): void;
}

export interface RuntimeShutdownController {
  readonly isStarted: boolean;
  readonly isShuttingDown: boolean;
  start(): void;
  stop(): void;
  trigger(signal?: RuntimeShutdownSignal | "manual"): Promise<void>;
}

export interface RuntimeShutdownControllerOptions {
  readonly callbacks: readonly RuntimeShutdownCallback[];
  readonly signalSource?: RuntimeShutdownSignalSource;
  readonly signals?: readonly RuntimeShutdownSignal[];
  readonly timeoutMs?: number;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly clock?: RuntimeClock;
}

export function createRuntimeShutdownController(
  options: RuntimeShutdownControllerOptions
): RuntimeShutdownController {
  const clock = options.clock ?? SYSTEM_RUNTIME_CLOCK;
  const signals = options.signals ?? [
    "SIGTERM",
    "SIGINT"
  ];
  const timeoutMs = options.timeoutMs ?? DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_MS;
  const listeners = new Map<RuntimeShutdownSignal, () => void>();
  let started = false;
  let shuttingDown = false;

  async function trigger(signal: RuntimeShutdownSignal | "manual" = "manual"): Promise<void> {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    const context: RuntimeShutdownContext = {
      signal,
      startedAt: runtimeNow(clock)
    };

    await emitRuntimeTelemetry(options.telemetry, {
      name: "runtime.shutdown.started",
      level: "info",
      at: context.startedAt,
      attributes: {
        signal
      }
    });

    try {
      await withTimeout(runCallbacks(options.callbacks, context), timeoutMs);
      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.shutdown.completed",
        level: "info",
        at: runtimeNow(clock),
        attributes: {
          signal
        }
      });
    } catch (error: unknown) {
      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.shutdown.failed",
        level: "error",
        at: runtimeNow(clock),
        attributes: {
          signal,
          error: classifyShutdownError(error)
        }
      });
      throw error;
    } finally {
      stop();
    }
  }

  function start(): void {
    if (started) {
      return;
    }

    for (const signal of signals) {
      const listener = (): void => {
        void trigger(signal);
      };

      listeners.set(signal, listener);
      options.signalSource?.on(signal, listener);
    }

    started = true;
  }

  function stop(): void {
    if (!started) {
      return;
    }

    for (const [signal, listener] of listeners.entries()) {
      options.signalSource?.off(signal, listener);
    }

    listeners.clear();
    started = false;
  }

  return {
    get isStarted(): boolean {
      return started;
    },
    get isShuttingDown(): boolean {
      return shuttingDown;
    },
    start,
    stop,
    trigger
  };
}

async function runCallbacks(
  callbacks: readonly RuntimeShutdownCallback[],
  context: RuntimeShutdownContext
): Promise<void> {
  for (const callback of callbacks) {
    await callback(context);
  }
}

async function withTimeout(operation: Promise<void>, timeoutMs: number): Promise<void> {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  try {
    await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => {
          reject(new Error(`Shutdown exceeded ${String(timeoutMs)}ms.`));
        }, timeoutMs);
      })
    ]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

function classifyShutdownError(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown-shutdown-error";
}
