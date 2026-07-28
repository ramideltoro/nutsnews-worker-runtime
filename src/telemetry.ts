import {
  getWorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";

export interface RuntimeClock {
  now(): Date;
}

export const SYSTEM_RUNTIME_CLOCK: RuntimeClock = {
  now: () => new Date()
};

export type RuntimeTelemetryLevel = "debug" | "info" | "warn" | "error";

export type RuntimeTelemetryOutcome =
  | "started"
  | "success"
  | "duplicate"
  | "retry"
  | "dlq"
  | "failure"
  | "ok"
  | "degraded"
  | "unhealthy"
  | "inactive"
  | "active"
  | "cancelled"
  | "channel-dropped"
  | "recovering"
  | "closed"
  | "draining"
  | "drained";

export const RUNTIME_TELEMETRY_EVENT_NAMES = [
  "runtime.broker.state_changed",
  "runtime.broker.topology_asserted",
  "runtime.broker.consumer_state_changed",
  "runtime.message.started",
  "runtime.message.accepted",
  "runtime.message.duplicate",
  "runtime.message.retry",
  "runtime.message.dlq",
  "runtime.message.invalid",
  "runtime.dependency.observed",
  "runtime.health.evaluated",
  "runtime.shutdown.started",
  "runtime.shutdown.completed",
  "runtime.shutdown.failed"
] as const;

export const RUNTIME_ALLOWED_METRIC_LABELS = [
  "environment",
  "host",
  "service",
  "version",
  "queue",
  "outcome"
] as const;

export const RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS = [
  "article",
  "feed",
  "message",
  "idempotency",
  "trace",
  "span",
  "correlation",
  "causation",
  "payload",
  "url",
  "path",
  "user",
  "ip",
  "token",
  "secret",
  "prompt",
  "model_output"
] as const;

export type RuntimeTelemetryEventName = (typeof RUNTIME_TELEMETRY_EVENT_NAMES)[number];
export type RuntimeTelemetryAttributes = Readonly<Record<string, unknown>>;

export interface RuntimeServiceIdentity {
  readonly service: string;
  readonly version: string;
  readonly environment: string;
  readonly host?: string;
}

export interface RuntimeTelemetryEvent {
  readonly name: RuntimeTelemetryEventName;
  readonly level: RuntimeTelemetryLevel;
  readonly at: string;
  readonly stage?: WorkerStage;
  readonly messageId?: string;
  readonly idempotencyKey?: string;
  readonly correlationId?: string;
  readonly causationId?: string;
  readonly traceparent?: string;
  readonly tracestate?: string;
  readonly queue?: string;
  readonly attempt?: number;
  readonly durationMs?: number;
  readonly outcome?: RuntimeTelemetryOutcome;
  readonly attributes?: RuntimeTelemetryAttributes;
}

export interface RuntimeTelemetrySink {
  emit(event: RuntimeTelemetryEvent): void | Promise<void>;
}

export interface RuntimeTelemetryFlusher {
  flush(): void | Promise<void>;
}

export interface BufferedRuntimeTelemetrySink extends RuntimeTelemetrySink {
  readonly events: readonly RuntimeTelemetryEvent[];
  clear(): void;
}

export type RuntimeLogWriter = (line: string) => void | Promise<void>;

export interface JsonRuntimeTelemetrySink extends RuntimeTelemetrySink, RuntimeTelemetryFlusher {}

export interface JsonRuntimeTelemetrySinkOptions {
  readonly identity: RuntimeServiceIdentity;
  readonly writer: RuntimeLogWriter;
  readonly flush?: () => void | Promise<void>;
}

export interface RuntimeMetricLabels {
  readonly environment: string;
  readonly host: string;
  readonly service: string;
  readonly version: string;
  readonly queue: string;
  readonly outcome: string;
}

export interface PrometheusRuntimeTelemetrySink extends RuntimeTelemetrySink {
  readonly allowedLabels: typeof RUNTIME_ALLOWED_METRIC_LABELS;
  collect(): string;
  recordDependencyLatency(queue: string, durationMs: number, outcome?: RuntimeTelemetryOutcome): void;
  setInFlight(queue: string, value: number): void;
  setShutdownDraining(draining: boolean): void;
}

export interface PrometheusRuntimeTelemetrySinkOptions {
  readonly identity: RuntimeServiceIdentity;
  readonly defaultQueue?: string;
}

interface SummaryState {
  count: number;
  sum: number;
}

type MetricSampleMap = Map<string, number>;
type SummarySampleMap = Map<string, SummaryState>;

const REDACTED = "[redacted]";
const MAX_LOG_STRING_LENGTH = 1_024;
const MAX_LABEL_LENGTH = 96;
const SENSITIVE_TELEMETRY_KEY_RE = /authorization|cookie|token|secret|password|credential|api[-_]?key|private[-_]?key|prompt|response|body|content|articlebody|modeloutput|model_output/u;

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

export function createJsonRuntimeTelemetrySink(
  options: JsonRuntimeTelemetrySinkOptions
): JsonRuntimeTelemetrySink {
  return {
    async emit(event: RuntimeTelemetryEvent): Promise<void> {
      await options.writer(JSON.stringify(runtimeTelemetryEventToLogRecord(options.identity, event)));
    },
    async flush(): Promise<void> {
      await options.flush?.();
    }
  };
}

export function runtimeTelemetryEventToLogRecord(
  identity: RuntimeServiceIdentity,
  event: RuntimeTelemetryEvent
): Readonly<Record<string, unknown>> {
  return redactRuntimeTelemetryValue(compactRecord({
    at: event.at,
    level: event.level,
    event: event.name,
    service: identity.service,
    version: identity.version,
    environment: identity.environment,
    host: identity.host,
    stage: event.stage,
    queue: event.queue,
    attempt: event.attempt,
    durationMs: event.durationMs,
    outcome: event.outcome,
    messageId: event.messageId,
    idempotencyKey: event.idempotencyKey,
    correlationId: event.correlationId,
    causationId: event.causationId,
    traceparent: event.traceparent,
    tracestate: event.tracestate,
    attributes: event.attributes
  })) as Readonly<Record<string, unknown>>;
}

export function createPrometheusRuntimeTelemetrySink(
  options: PrometheusRuntimeTelemetrySinkOptions
): PrometheusRuntimeTelemetrySink {
  return new InMemoryPrometheusRuntimeTelemetrySink(options);
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

export function redactRuntimeTelemetryValue(value: unknown, maxDepth = 5): unknown {
  if (maxDepth < 0) {
    return "[redacted-depth]";
  }

  if (typeof value === "string") {
    return value.length > MAX_LOG_STRING_LENGTH
      ? `${value.slice(0, MAX_LOG_STRING_LENGTH)}...`
      : value;
  }

  if (typeof value !== "object" || value === null) {
    return value;
  }

  if (Array.isArray(value)) {
    return value.slice(0, 20).map((entry) => redactRuntimeTelemetryValue(entry, maxDepth - 1));
  }

  const output: Record<string, unknown> = {};

  for (const [key, nestedValue] of Object.entries(value)) {
    if (SENSITIVE_TELEMETRY_KEY_RE.test(key.toLowerCase())) {
      output[key] = REDACTED;
      continue;
    }

    output[key] = redactRuntimeTelemetryValue(nestedValue, maxDepth - 1);
  }

  return output;
}

class InMemoryPrometheusRuntimeTelemetrySink implements PrometheusRuntimeTelemetrySink {
  readonly allowedLabels = RUNTIME_ALLOWED_METRIC_LABELS;
  private readonly identity: RuntimeServiceIdentity;
  private readonly defaultQueue: string;
  private readonly counters = new Map<string, MetricSampleMap>();
  private readonly gauges = new Map<string, MetricSampleMap>();
  private readonly summaries = new Map<string, SummarySampleMap>();

  constructor(options: PrometheusRuntimeTelemetrySinkOptions) {
    this.identity = options.identity;
    this.defaultQueue = options.defaultQueue ?? "none";
  }

  emit(event: RuntimeTelemetryEvent): void {
    const queue = queueForEvent(event, this.defaultQueue);

    switch (event.name) {
      case "runtime.message.started":
        this.incrementGauge("nutsnews_worker_inflight", this.labels(queue, "in_flight"), 1);
        break;
      case "runtime.message.accepted":
        this.completeMessage(queue, event, event.outcome ?? "success");
        break;
      case "runtime.message.duplicate":
        this.completeMessage(queue, event, "duplicate");
        break;
      case "runtime.message.retry":
        this.completeMessage(queue, event, "retry");
        this.incrementCounter("nutsnews_worker_retries_total", this.labels(queue, "retry"), 1);
        break;
      case "runtime.message.dlq":
        this.completeMessage(queue, event, "dlq");
        this.incrementCounter("nutsnews_worker_dlq_total", this.labels(queue, "dlq"), 1);
        break;
      case "runtime.message.invalid":
        this.completeMessage(queue, event, "failure");
        this.incrementCounter("nutsnews_worker_dlq_total", this.labels(queue, "failure"), 1);
        break;
      case "runtime.dependency.observed":
        this.recordDependencyLatency(queue, event.durationMs ?? numericAttribute(event, "durationMs") ?? 0, event.outcome ?? outcomeAttribute(event) ?? "success");
        break;
      case "runtime.health.evaluated":
        this.setHealth(queue, event.outcome ?? outcomeAttribute(event) ?? "ok");
        break;
      case "runtime.broker.consumer_state_changed":
        this.setGauge(
          "nutsnews_worker_consumers",
          this.labels(queue, "active"),
          numericAttribute(event, "activeConsumers") ?? (event.outcome === "active" ? 1 : 0)
        );
        this.incrementCounter(
          "nutsnews_worker_consumer_events_total",
          this.labels(queue, event.outcome ?? outcomeAttribute(event) ?? "inactive"),
          1
        );
        break;
      case "runtime.shutdown.started":
        this.setShutdownDraining(true);
        break;
      case "runtime.shutdown.completed":
        this.setShutdownDraining(false);
        break;
      case "runtime.shutdown.failed":
        this.setGauge("nutsnews_worker_shutdown_draining", this.labels("none", "failure"), 0);
        break;
      case "runtime.broker.state_changed":
      case "runtime.broker.topology_asserted":
        break;
    }
  }

  collect(): string {
    const lines: string[] = [];
    this.collectMetric(lines, "nutsnews_worker_messages_total", "counter", "Worker message outcomes by bounded queue and outcome.");
    this.collectMetric(lines, "nutsnews_worker_retries_total", "counter", "Worker retry outcomes by bounded queue.");
    this.collectMetric(lines, "nutsnews_worker_dlq_total", "counter", "Worker DLQ outcomes by bounded queue.");
    this.collectMetric(lines, "nutsnews_worker_inflight", "gauge", "Worker in-flight deliveries by bounded queue.");
    this.collectMetric(lines, "nutsnews_worker_health", "gauge", "Worker health probe status by bounded queue and outcome.");
    this.collectMetric(lines, "nutsnews_worker_consumers", "gauge", "Active worker consumers for the service main queue.");
    this.collectMetric(lines, "nutsnews_worker_consumer_events_total", "counter", "Worker consumer lifecycle events by bounded queue and outcome.");
    this.collectMetric(lines, "nutsnews_worker_shutdown_draining", "gauge", "Worker shutdown drain state.");
    this.collectSummary(lines, "nutsnews_worker_processing_duration_ms", "Worker processing latency in milliseconds.");
    this.collectSummary(lines, "nutsnews_worker_dependency_duration_ms", "Worker dependency latency in milliseconds.");

    return `${lines.join("\n")}\n`;
  }

  recordDependencyLatency(queue: string, durationMs: number, outcome: RuntimeTelemetryOutcome = "success"): void {
    this.observeSummary("nutsnews_worker_dependency_duration_ms", this.labels(queue, outcome), durationMs);
  }

  setInFlight(queue: string, value: number): void {
    this.setGauge("nutsnews_worker_inflight", this.labels(queue, "in_flight"), Math.max(0, value));
  }

  setShutdownDraining(draining: boolean): void {
    this.setGauge("nutsnews_worker_shutdown_draining", this.labels("none", draining ? "draining" : "drained"), draining ? 1 : 0);
  }

  private completeMessage(
    queue: string,
    event: RuntimeTelemetryEvent,
    outcome: RuntimeTelemetryOutcome
  ): void {
    this.incrementGauge("nutsnews_worker_inflight", this.labels(queue, "in_flight"), -1);
    this.incrementCounter("nutsnews_worker_messages_total", this.labels(queue, outcome), 1);

    if (event.durationMs !== undefined) {
      this.observeSummary("nutsnews_worker_processing_duration_ms", this.labels(queue, outcome), event.durationMs);
    }
  }

  private setHealth(queue: string, outcome: RuntimeTelemetryOutcome): void {
    for (const status of [
      "ok",
      "degraded",
      "unhealthy"
    ] as const) {
      this.setGauge("nutsnews_worker_health", this.labels(queue, status), status === outcome ? 1 : 0);
    }
  }

  private labels(queue: string, outcome: string): RuntimeMetricLabels {
    return {
      environment: metricLabelValue(this.identity.environment),
      host: metricLabelValue(this.identity.host ?? "unknown"),
      service: metricLabelValue(this.identity.service),
      version: metricLabelValue(this.identity.version),
      queue: metricLabelValue(queue),
      outcome: metricLabelValue(outcome)
    };
  }

  private incrementCounter(metric: string, labels: RuntimeMetricLabels, value: number): void {
    const samples = samplesFor(this.counters, metric);
    const key = labelsKey(labels);
    samples.set(key, (samples.get(key) ?? 0) + value);
  }

  private setGauge(metric: string, labels: RuntimeMetricLabels, value: number): void {
    const samples = samplesFor(this.gauges, metric);
    samples.set(labelsKey(labels), value);
  }

  private incrementGauge(metric: string, labels: RuntimeMetricLabels, delta: number): void {
    const samples = samplesFor(this.gauges, metric);
    const key = labelsKey(labels);
    samples.set(key, Math.max(0, (samples.get(key) ?? 0) + delta));
  }

  private observeSummary(metric: string, labels: RuntimeMetricLabels, value: number): void {
    const summaries = samplesFor(this.summaries, metric);
    const key = labelsKey(labels);
    const existing = summaries.get(key) ?? {
      count: 0,
      sum: 0
    };
    summaries.set(key, {
      count: existing.count + 1,
      sum: existing.sum + Math.max(0, value)
    });
  }

  private collectMetric(
    lines: string[],
    metric: string,
    type: "counter" | "gauge",
    help: string
  ): void {
    const samples = type === "counter" ? this.counters.get(metric) : this.gauges.get(metric);

    if (samples === undefined) {
      return;
    }

    lines.push(`# HELP ${metric} ${help}`);
    lines.push(`# TYPE ${metric} ${type}`);

    for (const [key, value] of sortedEntries(samples)) {
      lines.push(`${metric}${key} ${formatMetricNumber(value)}`);
    }
  }

  private collectSummary(lines: string[], metric: string, help: string): void {
    const summaries = this.summaries.get(metric);

    if (summaries === undefined) {
      return;
    }

    lines.push(`# HELP ${metric} ${help}`);
    lines.push(`# TYPE ${metric} summary`);

    for (const [key, value] of sortedEntries(summaries)) {
      lines.push(`${metric}_count${key} ${formatMetricNumber(value.count)}`);
      lines.push(`${metric}_sum${key} ${formatMetricNumber(value.sum)}`);
    }
  }
}

function queueForEvent(event: RuntimeTelemetryEvent, fallback: string): string {
  if (event.queue !== undefined) {
    return event.queue;
  }

  if (event.stage !== undefined) {
    return getWorkerRoute(event.stage).mainQueue.name;
  }

  return fallback;
}

function numericAttribute(event: RuntimeTelemetryEvent, key: string): number | undefined {
  const value = event.attributes?.[key];

  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function outcomeAttribute(event: RuntimeTelemetryEvent): RuntimeTelemetryOutcome | undefined {
  const value = event.attributes?.["outcome"] ?? event.attributes?.["status"];

  if (typeof value !== "string") {
    return undefined;
  }

  return isRuntimeTelemetryOutcome(value) ? value : undefined;
}

function isRuntimeTelemetryOutcome(value: string): value is RuntimeTelemetryOutcome {
  return [
    "started",
    "success",
    "duplicate",
    "retry",
    "dlq",
    "failure",
    "ok",
    "degraded",
    "unhealthy",
    "inactive",
    "active",
    "cancelled",
    "channel-dropped",
    "recovering",
    "closed",
    "draining",
    "drained"
  ].includes(value);
}

function samplesFor<T>(container: Map<string, Map<string, T>>, metric: string): Map<string, T> {
  const existing = container.get(metric);

  if (existing !== undefined) {
    return existing;
  }

  const samples = new Map<string, T>();
  container.set(metric, samples);

  return samples;
}

function labelsKey(labels: RuntimeMetricLabels): string {
  const parts = RUNTIME_ALLOWED_METRIC_LABELS.map((name) => `${name}="${escapeLabelValue(labels[name])}"`);

  return `{${parts.join(",")}}`;
}

function metricLabelValue(value: string): string {
  const cleaned = value
    .trim()
    .replace(/[^A-Za-z0-9_.:-]+/gu, "_")
    .slice(0, MAX_LABEL_LENGTH);

  return cleaned.length > 0 ? cleaned : "unknown";
}

function escapeLabelValue(value: string): string {
  return value
    .replace(/\\/gu, "\\\\")
    .replace(/"/gu, "\\\"");
}

function formatMetricNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(6)));
}

function sortedEntries<T>(map: Map<string, T>): [string, T][] {
  return [
    ...map.entries()
  ].sort(([left], [right]) => left.localeCompare(right));
}

function compactRecord(input: Readonly<Record<string, unknown>>): Readonly<Record<string, unknown>> {
  const output: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(input)) {
    if (value !== undefined) {
      output[key] = value;
    }
  }

  return output;
}
