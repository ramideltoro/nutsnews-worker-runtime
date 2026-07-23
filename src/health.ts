import {
  SYSTEM_RUNTIME_CLOCK,
  emitRuntimeTelemetry,
  runtimeNow,
  type RuntimeClock,
  type RuntimeTelemetrySink
} from "./telemetry.js";

export type RuntimeHealthStatus = "ok" | "degraded" | "unhealthy";

export type RuntimeHealthCheckOutput =
  | RuntimeHealthStatus
  | {
      readonly status: RuntimeHealthStatus;
      readonly details?: Readonly<Record<string, string | number | boolean>>;
    };

export interface RuntimeHealthCheck {
  readonly name: string;
  readonly critical?: boolean;
  check(): RuntimeHealthCheckOutput | Promise<RuntimeHealthCheckOutput>;
}

export interface RuntimeHealthCheckResult {
  readonly name: string;
  readonly status: RuntimeHealthStatus;
  readonly critical: boolean;
  readonly checkedAt: string;
  readonly durationMs: number;
  readonly details?: Readonly<Record<string, string | number | boolean>>;
}

export interface RuntimeHealthReport {
  readonly status: RuntimeHealthStatus;
  readonly checkedAt: string;
  readonly checks: readonly RuntimeHealthCheckResult[];
}

export interface RuntimeHealthOptions {
  readonly telemetry?: RuntimeTelemetrySink;
  readonly clock?: RuntimeClock;
}

export async function evaluateRuntimeHealth(
  checks: readonly RuntimeHealthCheck[],
  options: RuntimeHealthOptions = {}
): Promise<RuntimeHealthReport> {
  const clock = options.clock ?? SYSTEM_RUNTIME_CLOCK;
  const results = await Promise.all(checks.map((check) => runHealthCheck(check, clock)));
  const report = {
    status: combineHealth(results),
    checkedAt: runtimeNow(clock),
    checks: results
  } as const satisfies RuntimeHealthReport;

  await emitRuntimeTelemetry(options.telemetry, {
    name: "runtime.health.evaluated",
    level: report.status === "ok" ? "info" : "warn",
    at: report.checkedAt,
    attributes: {
      status: report.status,
      checkCount: results.length
    }
  });

  return report;
}

async function runHealthCheck(
  check: RuntimeHealthCheck,
  clock: RuntimeClock
): Promise<RuntimeHealthCheckResult> {
  const start = clock.now().getTime();

  try {
    const output = await check.check();
    const normalized = typeof output === "string"
      ? {
          status: output
        }
      : output;
    const base = {
      name: check.name,
      status: normalized.status,
      critical: check.critical === true,
      checkedAt: runtimeNow(clock),
      durationMs: Math.max(0, clock.now().getTime() - start)
    };

    if (normalized.details === undefined) {
      return base;
    }

    return {
      ...base,
      details: normalized.details
    };
  } catch (error: unknown) {
    return {
      name: check.name,
      status: "unhealthy",
      critical: check.critical === true,
      checkedAt: runtimeNow(clock),
      durationMs: Math.max(0, clock.now().getTime() - start),
      details: {
        error: classifyHealthError(error)
      }
    };
  }
}

function combineHealth(results: readonly RuntimeHealthCheckResult[]): RuntimeHealthStatus {
  if (results.some((result) => result.critical && result.status === "unhealthy")) {
    return "unhealthy";
  }

  if (results.some((result) => result.status !== "ok")) {
    return "degraded";
  }

  return "ok";
}

function classifyHealthError(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown-health-error";
}
