export const RUNTIME_FAILURE_CLASSES = [
  "transient",
  "permanent",
  "validation",
  "unauthorized",
  "rate-limited",
  "stale-conflict",
  "unknown"
] as const;

export type RuntimeFailureClass = (typeof RUNTIME_FAILURE_CLASSES)[number];

export interface RuntimeFailureClassification {
  readonly failureClass: RuntimeFailureClass;
  readonly retryable: boolean;
  readonly reason: string;
  readonly retryAfterMs?: number;
  readonly diagnostics: Readonly<Record<string, unknown>>;
}

export class RuntimeStageError extends Error {
  readonly failureClass: RuntimeFailureClass;
  readonly retryAfterMs: number | undefined;
  readonly diagnostics: Readonly<Record<string, unknown>>;

  constructor(
    message: string,
    options: {
      readonly failureClass: RuntimeFailureClass;
      readonly retryAfterMs?: number;
      readonly diagnostics?: Readonly<Record<string, unknown>>;
    }
  ) {
    super(message);
    this.name = "RuntimeStageError";
    this.failureClass = options.failureClass;
    this.retryAfterMs = options.retryAfterMs;
    this.diagnostics = options.diagnostics ?? {};
  }
}

const SENSITIVE_KEY_RE = /authorization|cookie|token|secret|password|api[-_]?key|private[-_]?key|prompt|response|body|content/u;
const MAX_REDACTED_STRING_LENGTH = 512;

export function classifyRuntimeFailure(error: unknown): RuntimeFailureClassification {
  if (error instanceof RuntimeStageError) {
    const classification = {
      failureClass: error.failureClass,
      retryable: isRetryableFailureClass(error.failureClass),
      reason: normalizeReason(error.message),
      diagnostics: redactRuntimeDiagnostics(error.diagnostics)
    };

    if (error.retryAfterMs === undefined) {
      return classification;
    }

    return {
      ...classification,
      retryAfterMs: error.retryAfterMs
    };
  }

  const statusCode = readNumericProperty(error, "status") ?? readNumericProperty(error, "statusCode");

  if (statusCode !== undefined) {
    return classifyHttpLikeFailure(statusCode, error);
  }

  if (error instanceof SyntaxError) {
    return failure("validation", "syntax-error", error);
  }

  if (error instanceof Error) {
    const text = `${error.name} ${error.message}`.toLowerCase();

    if (text.includes("timeout") || text.includes("econnreset") || text.includes("network")) {
      return failure("transient", error.name, error);
    }

    if (text.includes("validation") || text.includes("schema")) {
      return failure("validation", error.name, error);
    }

    if (text.includes("unauthorized") || text.includes("forbidden")) {
      return failure("unauthorized", error.name, error);
    }

    if (text.includes("conflict") || text.includes("stale")) {
      return failure("stale-conflict", error.name, error);
    }

    return failure("unknown", error.name, error);
  }

  return failure("unknown", "unknown-error", error);
}

export function isRetryableFailureClass(failureClass: RuntimeFailureClass): boolean {
  return failureClass === "transient" || failureClass === "rate-limited" || failureClass === "stale-conflict" || failureClass === "unknown";
}

export function redactRuntimeDiagnostics(
  value: unknown,
  maxDepth = 4
): Readonly<Record<string, unknown>> {
  const redacted = redactValue(value, maxDepth);

  if (isRecord(redacted)) {
    return redacted;
  }

  return {
    value: redacted
  };
}

function classifyHttpLikeFailure(statusCode: number, error: unknown): RuntimeFailureClassification {
  if (statusCode === 401 || statusCode === 403) {
    return failure("unauthorized", `http-${String(statusCode)}`, error);
  }

  if (statusCode === 409 || statusCode === 412) {
    return failure("stale-conflict", `http-${String(statusCode)}`, error);
  }

  if (statusCode === 429) {
    const classification = failure("rate-limited", "http-429", error);
    const retryAfterMs = readNumericProperty(error, "retryAfterMs");

    if (retryAfterMs === undefined) {
      return classification;
    }

    return {
      ...classification,
      retryAfterMs
    };
  }

  if (statusCode >= 400 && statusCode < 500) {
    return failure("validation", `http-${String(statusCode)}`, error);
  }

  if (statusCode >= 500) {
    return failure("transient", `http-${String(statusCode)}`, error);
  }

  return failure("unknown", `http-${String(statusCode)}`, error);
}

function failure(
  failureClass: RuntimeFailureClass,
  reason: string,
  error: unknown
): RuntimeFailureClassification {
  return {
    failureClass,
    retryable: isRetryableFailureClass(failureClass),
    reason: normalizeReason(reason),
    diagnostics: redactRuntimeDiagnostics(error)
  };
}

function readNumericProperty(value: unknown, property: string): number | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const propertyValue = value[property];

  if (typeof propertyValue === "number" && Number.isFinite(propertyValue)) {
    return propertyValue;
  }

  return undefined;
}

function normalizeReason(reason: string): string {
  return reason
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .slice(0, 80) || "unknown";
}

function redactValue(value: unknown, depth: number): unknown {
  if (depth < 0) {
    return "[redacted-depth]";
  }

  if (typeof value === "string") {
    return value.length > MAX_REDACTED_STRING_LENGTH
      ? `${value.slice(0, MAX_REDACTED_STRING_LENGTH)}...`
      : value;
  }

  if (typeof value !== "object" || value === null) {
    return value;
  }

  if (Array.isArray(value)) {
    return value.slice(0, 20).map((entry) => redactValue(entry, depth - 1));
  }

  const output: Record<string, unknown> = {};

  for (const [key, nestedValue] of Object.entries(value)) {
    if (SENSITIVE_KEY_RE.test(key.toLowerCase())) {
      output[key] = "[redacted]";
      continue;
    }

    output[key] = redactValue(nestedValue, depth - 1);
  }

  return output;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
