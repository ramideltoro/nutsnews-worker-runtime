import {
  SYSTEM_RUNTIME_CLOCK,
  emitRuntimeTelemetry,
  redactRuntimeTelemetryValue,
  runtimeNow,
  type RuntimeClock,
  type RuntimeTelemetrySink
} from "./telemetry.js";

export type RuntimeHealthStatus = "ok" | "degraded" | "unhealthy";
export type RuntimeHealthProbe = "liveness" | "startup" | "readiness";

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
  readonly probe?: RuntimeHealthProbe;
  readonly checks: readonly RuntimeHealthCheckResult[];
}

export interface RuntimeHealthOptions {
  readonly probe?: RuntimeHealthProbe;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly clock?: RuntimeClock;
}

export interface RuntimeHealthProbeSetOptions extends RuntimeHealthOptions {
  readonly livenessChecks?: readonly RuntimeHealthCheck[];
  readonly startupChecks?: readonly RuntimeHealthCheck[];
  readonly readinessChecks?: readonly RuntimeHealthCheck[];
}

export interface RuntimeHealthProbeSet {
  liveness(): Promise<RuntimeHealthReport>;
  startup(): Promise<RuntimeHealthReport>;
  readiness(): Promise<RuntimeHealthReport>;
}

export interface RuntimeHealthEndpointResponse {
  readonly statusCode: 200 | 503;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: RuntimeHealthReport;
}

export function createRuntimeHealthProbeSet(
  options: RuntimeHealthProbeSetOptions
): RuntimeHealthProbeSet {
  return {
    liveness: () => evaluateRuntimeHealth(options.livenessChecks ?? [], {
      ...options,
      probe: "liveness"
    }),
    startup: () => evaluateRuntimeHealth(options.startupChecks ?? [], {
      ...options,
      probe: "startup"
    }),
    readiness: () => evaluateRuntimeHealth(options.readinessChecks ?? [], {
      ...options,
      probe: "readiness"
    })
  };
}

export function runtimeHealthEndpointResponse(
  report: RuntimeHealthReport
): RuntimeHealthEndpointResponse {
  return {
    statusCode: report.status === "unhealthy" ? 503 : 200,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    },
    body: report
  };
}

export async function evaluateRuntimeHealth(
  checks: readonly RuntimeHealthCheck[],
  options: RuntimeHealthOptions = {}
): Promise<RuntimeHealthReport> {
  const clock = options.clock ?? SYSTEM_RUNTIME_CLOCK;
  const results = await Promise.all(checks.map((check) => runHealthCheck(check, clock)));
  const baseReport = {
    status: combineHealth(results),
    checkedAt: runtimeNow(clock),
    checks: results
  } as const;
  const report = options.probe === undefined
    ? baseReport
    : {
        ...baseReport,
        probe: options.probe
      };

  await emitRuntimeTelemetry(options.telemetry, {
    name: "runtime.health.evaluated",
    level: report.status === "ok" ? "info" : "warn",
    at: report.checkedAt,
    outcome: report.status,
    attributes: {
      ...(options.probe === undefined
        ? {}
        : {
            probe: options.probe
          }),
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
      details: sanitizeHealthDetails(normalized.details)
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

function sanitizeHealthDetails(
  details: Readonly<Record<string, string | number | boolean>>
): Readonly<Record<string, string | number | boolean>> {
  const redacted = redactRuntimeTelemetryValue(details);
  const output: Record<string, string | number | boolean> = {};

  if (typeof redacted !== "object" || redacted === null || Array.isArray(redacted)) {
    return output;
  }

  for (const [key, value] of Object.entries(redacted)) {
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      output[key] = value;
    }
  }

  return output;
}
