import type {
  WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";

export interface RuntimeTraceContext {
  readonly traceparent: string;
  readonly tracestate?: string;
  readonly correlationId?: string;
  readonly causationId?: string;
  readonly messageId?: string;
  readonly idempotencyKey?: string;
}

export type RuntimeTraceHeaders = Readonly<Record<"traceparent", string> & Partial<Record<"tracestate", string>>>;

const TRACEPARENT_RE = /^00-[a-f0-9]{32}-[a-f0-9]{16}-[a-f0-9]{2}$/u;

export function runtimeTraceContextFromEnvelope(envelope: WorkerMessageEnvelope): RuntimeTraceContext {
  const base = {
    traceparent: envelope.traceparent,
    correlationId: envelope.correlationId,
    causationId: envelope.causationId,
    messageId: envelope.messageId,
    idempotencyKey: envelope.idempotencyKey
  } as const satisfies RuntimeTraceContext;

  if (envelope.tracestate === undefined) {
    return base;
  }

  return {
    ...base,
    tracestate: envelope.tracestate
  };
}

export function runtimeTraceHeadersFromEnvelope(envelope: WorkerMessageEnvelope): RuntimeTraceHeaders {
  return runtimeTraceHeaders(runtimeTraceContextFromEnvelope(envelope));
}

export function runtimeTraceHeaders(context: RuntimeTraceContext): RuntimeTraceHeaders {
  if (context.tracestate === undefined) {
    return {
      traceparent: context.traceparent
    };
  }

  return {
    traceparent: context.traceparent,
    tracestate: context.tracestate
  };
}

export function readRuntimeTraceContextFromHeaders(
  headers: Readonly<Record<string, unknown>> | undefined
): RuntimeTraceContext | undefined {
  if (headers === undefined) {
    return undefined;
  }

  const traceparent = readHeader(headers, "traceparent");

  if (traceparent === undefined || !TRACEPARENT_RE.test(traceparent)) {
    return undefined;
  }

  const tracestate = readHeader(headers, "tracestate");

  if (tracestate === undefined) {
    return {
      traceparent
    };
  }

  return {
    traceparent,
    tracestate
  };
}

function readHeader(headers: Readonly<Record<string, unknown>>, name: string): string | undefined {
  const exact = headers[name];

  if (typeof exact === "string" && exact.length > 0) {
    return exact;
  }

  const lowerName = name.toLowerCase();

  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() === lowerName && typeof value === "string" && value.length > 0) {
      return value;
    }
  }

  return undefined;
}
