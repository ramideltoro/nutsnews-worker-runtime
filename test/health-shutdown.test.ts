import { describe, expect, it } from "vitest";

import {
  createBufferedRuntimeTelemetrySink,
  createRuntimeShutdownController,
  evaluateRuntimeHealth,
  type RuntimeShutdownSignal,
  type RuntimeShutdownSignalSource
} from "../src/index.js";

describe("health evaluation", () => {
  it("reports ok when all checks pass", async () => {
    const report = await evaluateRuntimeHealth([
      {
        name: "broker",
        critical: true,
        check: () => "ok"
      },
      {
        name: "idempotency-store",
        check: () => ({
          status: "ok",
          details: {
            adapter: "test"
          }
        })
      }
    ]);

    expect(report.status).toBe("ok");
    expect(report.checks).toHaveLength(2);
  });

  it("reports unhealthy when a critical check throws", async () => {
    const telemetry = createBufferedRuntimeTelemetrySink();
    const report = await evaluateRuntimeHealth([
      {
        name: "broker",
        critical: true,
        check: () => {
          throw new TypeError("connection failed");
        }
      }
    ], {
      telemetry
    });

    expect(report.status).toBe("unhealthy");
    expect(report.checks[0]?.details?.["error"]).toBe("TypeError");
    expect(telemetry.events[0]?.name).toBe("runtime.health.evaluated");
  });
});

describe("shutdown controller", () => {
  it("registers signal listeners and runs callbacks once", async () => {
    const signalSource = new FakeSignalSource();
    const observedSignals: string[] = [];
    const controller = createRuntimeShutdownController({
      signalSource,
      callbacks: [
        (context) => {
          observedSignals.push(context.signal);
        }
      ]
    });

    controller.start();
    expect(signalSource.has("SIGTERM")).toBe(true);
    await controller.trigger("SIGTERM");

    expect(controller.isStarted).toBe(false);
    expect(controller.isShuttingDown).toBe(true);
    expect(observedSignals).toEqual([
      "SIGTERM"
    ]);
  });

  it("can run manual shutdown without a signal source", async () => {
    const telemetry = createBufferedRuntimeTelemetrySink();
    const observedSignals: string[] = [];
    const controller = createRuntimeShutdownController({
      telemetry,
      callbacks: [
        (context) => {
          observedSignals.push(context.signal);
        }
      ]
    });

    await controller.trigger();

    expect(observedSignals).toEqual([
      "manual"
    ]);
    expect(telemetry.events.map((event) => event.name)).toEqual([
      "runtime.shutdown.started",
      "runtime.shutdown.completed"
    ]);
  });
});

class FakeSignalSource implements RuntimeShutdownSignalSource {
  private readonly listeners = new Map<RuntimeShutdownSignal, () => void>();

  on(signal: RuntimeShutdownSignal, listener: () => void): void {
    this.listeners.set(signal, listener);
  }

  off(signal: RuntimeShutdownSignal, listener: () => void): void {
    const registered = this.listeners.get(signal);

    if (registered === listener) {
      this.listeners.delete(signal);
    }
  }

  has(signal: RuntimeShutdownSignal): boolean {
    return this.listeners.has(signal);
  }
}
