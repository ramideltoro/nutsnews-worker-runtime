const UUID_IDENTIFIER_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const TRACEPARENT_RE = /^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/u;
const IDEMPOTENCY_KEY_RE = /^[a-z0-9][a-z0-9._:/-]{0,255}$/u;
const MAX_TRACESTATE_BYTES = 512;

export function normalizeRuntimeUuidIdentifier(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim().toLowerCase();

  return UUID_IDENTIFIER_RE.test(normalized) ? normalized : undefined;
}

export function normalizeRuntimeTraceparent(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim().toLowerCase();

  return TRACEPARENT_RE.test(normalized) ? normalized : undefined;
}

export function normalizeRuntimeTracestate(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();

  if (
    normalized.length === 0
    || Buffer.byteLength(normalized, "utf8") > MAX_TRACESTATE_BYTES
    || !isPrintableAscii(normalized)
  ) {
    return undefined;
  }

  return normalized;
}

export function normalizeRuntimeIdempotencyKey(value: unknown): string | undefined {
  return typeof value === "string" && IDEMPOTENCY_KEY_RE.test(value)
    ? value
    : undefined;
}

function isPrintableAscii(value: string): boolean {
  for (const character of value) {
    const codePoint = character.codePointAt(0);

    if (codePoint === undefined || codePoint < 0x20 || codePoint > 0x7e) {
      return false;
    }
  }

  return true;
}
