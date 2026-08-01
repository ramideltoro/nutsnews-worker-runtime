import {
  getWorkerRoute,
  isWorkerStage,
  WORKER_STAGES,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";

import {
  normalizeRuntimeIdempotencyKey,
  normalizeRuntimeTraceparent,
  normalizeRuntimeTracestate,
  normalizeRuntimeUuidIdentifier
} from "./telemetry-identifiers.js";

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
  | "invalid"
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
  "stage",
  "queue",
  "outcome",
  "dependency",
  "probe",
  "check",
  "revision",
  "deployment",
  "adapter"
] as const;

export const RUNTIME_DURATION_HISTOGRAM_BUCKETS_SECONDS = [
  0.005,
  0.01,
  0.025,
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2.5,
  5,
  10,
  30,
  60,
  120,
  300
] as const;

const RUNTIME_HEALTH_OUTCOMES = [
  "ok",
  "degraded",
  "unhealthy"
] as const;
const RUNTIME_MESSAGE_METRIC_OUTCOMES = new Set<string>([
  "success",
  "duplicate",
  "invalid",
  "retry",
  "dlq",
  "failure"
]);
const RUNTIME_DEPENDENCY_METRIC_OUTCOMES = new Set<string>([
  "success",
  "retry",
  "failure"
]);
const RUNTIME_CONSUMER_METRIC_OUTCOMES = new Set<string>([
  "inactive",
  "active",
  "cancelled",
  "channel-dropped",
  "recovering",
  "closed"
]);
const RUNTIME_METRIC_STAGES = new Set<string>(WORKER_STAGES);
const MAX_CARDINALITY_ALLOWLIST_SIZE = 32;

export const RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS = [
  "article",
  "feed",
  "message",
  "pipeline",
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
export type RuntimeDeployment = "local" | "test" | "shadow" | "production" | "unknown";
export type RuntimeAdapterMode = "in_memory" | "mixed" | "production" | "unknown";

export interface RuntimeServiceIdentity {
  readonly service: string;
  readonly version: string;
  readonly environment: string;
  readonly host?: string;
  readonly revision?: string;
  readonly deployment?: RuntimeDeployment;
  readonly adapter?: RuntimeAdapterMode;
}

export interface RuntimeTelemetryEvent {
  readonly name: RuntimeTelemetryEventName;
  readonly level: RuntimeTelemetryLevel;
  readonly at: string;
  readonly stage?: WorkerStage;
  readonly messageId?: string;
  readonly pipelineRunId?: string;
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

export type RuntimeMetricLabel = (typeof RUNTIME_ALLOWED_METRIC_LABELS)[number];
export type RuntimeMetricLabels = Readonly<Partial<Record<RuntimeMetricLabel, string>>>;

export interface RuntimeMetricCardinalityPolicy {
  readonly dependencies?: readonly string[];
  readonly healthChecks?: readonly string[];
}

export interface PrometheusRuntimeTelemetrySink extends RuntimeTelemetrySink {
  readonly allowedLabels: typeof RUNTIME_ALLOWED_METRIC_LABELS;
  collect(): string;
  setInFlight(queue: string, value: number): void;
  setShutdownDraining(draining: boolean): void;
  setExpectedActive(expected: boolean): void;
  setLastSuccessTimestamp(timestampSeconds: number): void;
}

export interface PrometheusRuntimeTelemetrySinkOptions {
  readonly identity: RuntimeServiceIdentity;
  readonly defaultQueue?: string;
  readonly cardinality?: RuntimeMetricCardinalityPolicy;
  readonly expectedActive?: boolean;
}

interface HistogramState {
  count: number;
  sum: number;
  buckets: number[];
}

type MetricSampleMap = Map<string, number>;
type HistogramSampleMap = Map<string, HistogramState>;

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
    revision: identity.revision,
    deployment: identity.deployment,
    adapter: identity.adapter,
    stage: event.stage,
    queue: event.queue,
    attempt: event.attempt,
    durationMs: event.durationMs,
    outcome: event.outcome,
    messageId: normalizeRuntimeUuidIdentifier(event.messageId),
    pipelineRunId: normalizeRuntimeUuidIdentifier(event.pipelineRunId),
    idempotencyKey: normalizeRuntimeIdempotencyKey(event.idempotencyKey),
    correlationId: normalizeRuntimeUuidIdentifier(event.correlationId),
    causationId: normalizeRuntimeUuidIdentifier(event.causationId),
    traceparent: normalizeRuntimeTraceparent(event.traceparent),
    tracestate: normalizeRuntimeTracestate(event.tracestate),
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
  private readonly allowedDependencies: ReadonlySet<string>;
  private readonly allowedHealthChecks: ReadonlySet<string>;
  private readonly allowedQueues: ReadonlySet<string>;
  private readonly counters = new Map<string, MetricSampleMap>();
  private readonly gauges = new Map<string, MetricSampleMap>();
  private readonly histograms = new Map<string, HistogramSampleMap>();

  constructor(options: PrometheusRuntimeTelemetrySinkOptions) {
    this.identity = options.identity;
    this.defaultQueue = options.defaultQueue ?? "none";
    this.allowedDependencies = createCardinalityAllowlist(
      options.cardinality?.dependencies ?? [],
      "dependencies"
    );
    this.allowedHealthChecks = createCardinalityAllowlist(
      options.cardinality?.healthChecks ?? [],
      "healthChecks"
    );
    this.allowedQueues = contractedMetricQueues(this.defaultQueue);

    this.setGauge("nutsnews_worker_build_info", {
      environment: metricLabelValue(this.identity.environment),
      service: metricLabelValue(this.identity.service),
      version: metricLabelValue(this.identity.version),
      revision: metricLabelValue(this.identity.revision ?? "unknown")
    }, 1);
    this.setGauge("nutsnews_worker_deployment_info", {
      environment: metricLabelValue(this.identity.environment),
      service: metricLabelValue(this.identity.service),
      deployment: metricLabelValue(this.identity.deployment ?? "unknown"),
      adapter: metricLabelValue(this.identity.adapter ?? "unknown")
    }, 1);
    if (options.expectedActive !== undefined) {
      this.setExpectedActive(options.expectedActive);
    }
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
        this.incrementCounter("nutsnews_worker_retries_total", this.messageLabels(event, queue, "retry"), 1);
        break;
      case "runtime.message.dlq":
        this.completeMessage(queue, event, "dlq");
        this.incrementCounter("nutsnews_worker_dlq_total", this.messageLabels(event, queue, "dlq"), 1);
        break;
      case "runtime.message.invalid":
        this.completeMessage(queue, event, "invalid");
        this.incrementCounter("nutsnews_worker_dlq_total", this.messageLabels(event, queue, "invalid"), 1);
        break;
      case "runtime.dependency.observed": {
        const durationMs = event.durationMs ?? numericAttribute(event, "durationMs");
        if (durationMs !== undefined && Number.isFinite(durationMs)) {
          this.observeHistogram(
            "nutsnews_worker_dependency_duration_seconds",
            {
              ...this.messageLabels(
                event,
                queue,
                event.outcome ?? outcomeAttribute(event) ?? "success",
                RUNTIME_DEPENDENCY_METRIC_OUTCOMES
              ),
              dependency: this.boundedDependency(stringAttribute(event, "dependency"))
            },
            durationMs / 1_000
          );
        }
        break;
      }
      case "runtime.health.evaluated":
        this.setHealth(event);
        break;
      case "runtime.broker.consumer_state_changed":
        this.setGauge(
          "nutsnews_worker_consumers",
          this.messageLabels(event, queue, "active", RUNTIME_CONSUMER_METRIC_OUTCOMES),
          numericAttribute(event, "activeConsumers") ?? (event.outcome === "active" ? 1 : 0)
        );
        this.incrementCounter(
          "nutsnews_worker_consumer_events_total",
          this.messageLabels(
            event,
            queue,
            event.outcome ?? outcomeAttribute(event) ?? "inactive",
            RUNTIME_CONSUMER_METRIC_OUTCOMES
          ),
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
        this.setShutdownDraining(true);
        break;
      case "runtime.broker.state_changed":
      case "runtime.broker.topology_asserted":
        break;
    }
  }

  collect(): string {
    const lines: string[] = [];
    this.collectMetric(lines, "nutsnews_worker_build_info", "gauge", "Worker build identity; revision changes create one bounded deploy series.");
    this.collectMetric(lines, "nutsnews_worker_deployment_info", "gauge", "Worker deployment and dependency-adapter mode.");
    this.collectMetric(lines, "nutsnews_worker_expected_active", "gauge", "Whether this deployment is expected to own active production work.");
    this.collectMetric(lines, "nutsnews_worker_last_success_timestamp_seconds", "gauge", "Unix timestamp of the latest service-owned successful work cycle.");
    this.collectMetric(lines, "nutsnews_worker_messages_total", "counter", "Worker message outcomes by bounded queue and outcome.");
    this.collectMetric(lines, "nutsnews_worker_retries_total", "counter", "Worker retry outcomes by bounded queue.");
    this.collectMetric(lines, "nutsnews_worker_dlq_total", "counter", "Worker DLQ outcomes by bounded queue.");
    this.collectMetric(lines, "nutsnews_worker_inflight", "gauge", "Worker in-flight deliveries by bounded queue.");
    this.collectMetric(lines, "nutsnews_worker_health_probe", "gauge", "Worker liveness, startup, and readiness state by probe and outcome.");
    this.collectMetric(lines, "nutsnews_worker_health_check", "gauge", "Worker dependency health state by bounded probe, check, and outcome.");
    this.collectMetric(lines, "nutsnews_worker_consumers", "gauge", "Active worker consumers for the service main queue.");
    this.collectMetric(lines, "nutsnews_worker_consumer_events_total", "counter", "Worker consumer lifecycle events by bounded queue and outcome.");
    this.collectMetric(lines, "nutsnews_worker_shutdown_draining", "gauge", "Worker shutdown drain state.");
    this.collectHistogram(lines, "nutsnews_worker_processing_duration_seconds", "Worker processing latency in seconds.");
    this.collectHistogram(lines, "nutsnews_worker_dependency_duration_seconds", "Worker dependency latency in seconds.");
    this.collectHistogram(lines, "nutsnews_worker_health_check_duration_seconds", "Worker health-check duration in seconds.");

    return `${lines.join("\n")}\n`;
  }

  setInFlight(queue: string, value: number): void {
    this.setGauge("nutsnews_worker_inflight", this.labels(queue, "in_flight"), Math.max(0, value));
  }

  setShutdownDraining(draining: boolean): void {
    this.setGauge("nutsnews_worker_shutdown_draining", this.identityLabels(), draining ? 1 : 0);
  }

  setExpectedActive(expected: boolean): void {
    this.setGauge("nutsnews_worker_expected_active", this.identityLabels(), expected ? 1 : 0);
  }

  setLastSuccessTimestamp(timestampSeconds: number): void {
    if (!Number.isFinite(timestampSeconds) || timestampSeconds < 0) {
      throw new RangeError("last-success timestamp must be a finite non-negative Unix timestamp");
    }
    this.setGauge(
      "nutsnews_worker_last_success_timestamp_seconds",
      this.identityLabels(),
      Math.floor(timestampSeconds)
    );
  }

  private completeMessage(
    queue: string,
    event: RuntimeTelemetryEvent,
    outcome: RuntimeTelemetryOutcome
  ): void {
    this.incrementGauge("nutsnews_worker_inflight", this.labels(queue, "in_flight"), -1);
    this.incrementCounter("nutsnews_worker_messages_total", this.messageLabels(event, queue, outcome), 1);

    if (event.durationMs !== undefined && Number.isFinite(event.durationMs)) {
      this.observeHistogram(
        "nutsnews_worker_processing_duration_seconds",
        this.messageLabels(event, queue, outcome),
        event.durationMs / 1_000
      );
    }
  }

  private setHealth(event: RuntimeTelemetryEvent): void {
    const probe = boundedProbe(stringAttribute(event, "probe"));
    const outcome = healthOutcome(event.outcome ?? outcomeAttribute(event));
    for (const status of RUNTIME_HEALTH_OUTCOMES) {
      this.setGauge("nutsnews_worker_health_probe", {
        ...this.healthIdentityLabels(),
        probe,
        outcome: status
      }, status === outcome ? 1 : 0);
    }

    const groupedChecks = new Map<string, (typeof RUNTIME_HEALTH_OUTCOMES)[number]>();

    for (const check of healthChecksAttribute(event)) {
      const checkName = this.boundedHealthCheck(check.name);
      const previous = groupedChecks.get(checkName);
      groupedChecks.set(checkName, worstHealthOutcome(previous, check.status));

      if (Number.isFinite(check.durationMs)) {
        this.observeHistogram("nutsnews_worker_health_check_duration_seconds", {
          ...this.healthIdentityLabels(),
          probe,
          check: checkName
        }, check.durationMs / 1_000);
      }
    }

    for (const [checkName, checkStatus] of groupedChecks) {
      for (const status of RUNTIME_HEALTH_OUTCOMES) {
        this.setGauge("nutsnews_worker_health_check", {
          ...this.healthIdentityLabels(),
          probe,
          check: checkName,
          outcome: status
        }, status === checkStatus ? 1 : 0);
      }
    }
  }

  private labels(queue: string, outcome: string): RuntimeMetricLabels {
    return {
      environment: metricLabelValue(this.identity.environment),
      host: metricLabelValue(this.identity.host ?? "unknown"),
      service: metricLabelValue(this.identity.service),
      version: metricLabelValue(this.identity.version),
      queue: boundedCardinalityValue(queue, this.allowedQueues),
      outcome: metricLabelValue(outcome)
    };
  }

  private messageLabels(
    event: RuntimeTelemetryEvent,
    queue: string,
    outcome: unknown,
    allowedOutcomes: ReadonlySet<string> = RUNTIME_MESSAGE_METRIC_OUTCOMES
  ): RuntimeMetricLabels {
    return {
      ...this.labels(queue, boundedCardinalityValue(outcome, allowedOutcomes)),
      stage: boundedCardinalityValue(event.stage, RUNTIME_METRIC_STAGES)
    };
  }

  private identityLabels(): RuntimeMetricLabels {
    return {
      environment: metricLabelValue(this.identity.environment),
      service: metricLabelValue(this.identity.service)
    };
  }

  private healthIdentityLabels(): RuntimeMetricLabels {
    return {
      environment: metricLabelValue(this.identity.environment),
      host: metricLabelValue(this.identity.host ?? "unknown"),
      service: metricLabelValue(this.identity.service),
      version: metricLabelValue(this.identity.version)
    };
  }

  private boundedDependency(value: string | undefined): string {
    return boundedCardinalityValue(value, this.allowedDependencies);
  }

  private boundedHealthCheck(value: string): string {
    return boundedCardinalityValue(value, this.allowedHealthChecks);
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

  private observeHistogram(metric: string, labels: RuntimeMetricLabels, valueSeconds: number): void {
    if (!Number.isFinite(valueSeconds)) {
      return;
    }
    const histograms = samplesFor(this.histograms, metric);
    const key = labelsKey(labels);
    const value = Math.max(0, valueSeconds);
    const existing = histograms.get(key) ?? {
      count: 0,
      sum: 0,
      buckets: RUNTIME_DURATION_HISTOGRAM_BUCKETS_SECONDS.map(() => 0)
    };
    const buckets = existing.buckets.map((count, index) => (
      value <= RUNTIME_DURATION_HISTOGRAM_BUCKETS_SECONDS[index]!
        ? count + 1
        : count
    ));
    histograms.set(key, {
      count: existing.count + 1,
      sum: existing.sum + value,
      buckets
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

  private collectHistogram(lines: string[], metric: string, help: string): void {
    const histograms = this.histograms.get(metric);

    if (histograms === undefined) {
      return;
    }

    lines.push(`# HELP ${metric} ${help}`);
    lines.push(`# TYPE ${metric} histogram`);

    for (const [key, value] of sortedEntries(histograms)) {
      RUNTIME_DURATION_HISTOGRAM_BUCKETS_SECONDS.forEach((bucket, index) => {
        lines.push(
          `${metric}_bucket${appendPrometheusLabel(key, "le", String(bucket))} `
          + formatMetricNumber(value.buckets[index] ?? 0)
        );
      });
      lines.push(`${metric}_bucket${appendPrometheusLabel(key, "le", "+Inf")} ${formatMetricNumber(value.count)}`);
      lines.push(`${metric}_sum${key} ${formatMetricNumber(value.sum)}`);
      lines.push(`${metric}_count${key} ${formatMetricNumber(value.count)}`);
    }
  }
}

function contractedMetricQueues(defaultQueue: string): ReadonlySet<string> {
  const queues = new Set<string>([
    "none",
    metricLabelValue(defaultQueue)
  ]);

  for (const stage of WORKER_STAGES) {
    const route = getWorkerRoute(stage);
    queues.add(metricLabelValue(route.mainQueue.name));
    queues.add(metricLabelValue(route.terminalDlq.name));

    for (const retryQueue of route.retryQueues) {
      queues.add(metricLabelValue(retryQueue.name));
    }
  }

  return queues;
}

interface RuntimeHealthMetricCheck {
  readonly name: string;
  readonly status: (typeof RUNTIME_HEALTH_OUTCOMES)[number];
  readonly durationMs: number;
}

function createCardinalityAllowlist(values: readonly string[], name: string): ReadonlySet<string> {
  if (values.length > MAX_CARDINALITY_ALLOWLIST_SIZE) {
    throw new RangeError(`${name} must contain at most ${String(MAX_CARDINALITY_ALLOWLIST_SIZE)} values`);
  }

  const normalized = new Set<string>();
  for (const value of values) {
    const candidate = metricLabelValue(value);
    if (candidate === "unknown" || candidate === "other") {
      throw new RangeError(`${name} cannot contain reserved value ${candidate}`);
    }
    if (normalized.has(candidate)) {
      throw new RangeError(`${name} contains a duplicate after label normalization: ${candidate}`);
    }
    normalized.add(candidate);
  }
  return normalized;
}

function boundedCardinalityValue(value: unknown, allowed: ReadonlySet<string>): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    return "unknown";
  }
  const normalized = metricLabelValue(value);
  if (normalized === "unknown") {
    return "unknown";
  }
  return allowed.has(normalized) ? normalized : "other";
}

function boundedProbe(value: string | undefined): string {
  return value === "liveness" || value === "startup" || value === "readiness"
    ? value
    : "unspecified";
}

function healthOutcome(value: unknown): (typeof RUNTIME_HEALTH_OUTCOMES)[number] {
  if (value === "ok" || value === "degraded") {
    return value;
  }

  return "unhealthy";
}

function worstHealthOutcome(
  left: (typeof RUNTIME_HEALTH_OUTCOMES)[number] | undefined,
  right: (typeof RUNTIME_HEALTH_OUTCOMES)[number]
): (typeof RUNTIME_HEALTH_OUTCOMES)[number] {
  const severity = {
    ok: 0,
    degraded: 1,
    unhealthy: 2
  } as const;

  return left === undefined || severity[right] > severity[left] ? right : left;
}

function healthChecksAttribute(event: RuntimeTelemetryEvent): readonly RuntimeHealthMetricCheck[] {
  const rawChecks = event.attributes?.["checks"];
  if (!Array.isArray(rawChecks)) {
    const check = stringAttribute(event, "check");
    if (check === undefined) {
      return [];
    }
    return [{
      name: check,
      status: healthOutcome(event.outcome ?? outcomeAttribute(event)),
      durationMs: numericAttribute(event, "durationMs") ?? event.durationMs ?? 0
    }];
  }

  return rawChecks.flatMap((raw): RuntimeHealthMetricCheck[] => {
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
      return [];
    }
    const check = raw as Readonly<Record<string, unknown>>;
    const name = check["name"];
    const status = check["status"];
    const durationMs = check["durationMs"];
    if (typeof name !== "string" || typeof status !== "string" || typeof durationMs !== "number") {
      return [];
    }
    return [{
      name,
      status: healthOutcome(isRuntimeTelemetryOutcome(status) ? status : undefined),
      durationMs
    }];
  });
}

function appendPrometheusLabel(key: string, name: string, value: string): string {
  if (key === "{}") {
    return `{${name}="${escapeLabelValue(value)}"}`;
  }
  return `${key.slice(0, -1)},${name}="${escapeLabelValue(value)}"}`;
}

function queueForEvent(event: RuntimeTelemetryEvent, fallback: string): string {
  if (event.queue !== undefined) {
    return event.queue;
  }

  if (event.stage !== undefined && isWorkerStage(event.stage)) {
    return getWorkerRoute(event.stage).mainQueue.name;
  }

  return fallback;
}

function numericAttribute(event: RuntimeTelemetryEvent, key: string): number | undefined {
  const value = event.attributes?.[key];

  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function stringAttribute(event: RuntimeTelemetryEvent, key: string): string | undefined {
  const value = event.attributes?.[key];

  return typeof value === "string" ? value : undefined;
}

function outcomeAttribute(event: RuntimeTelemetryEvent): string | undefined {
  const value = event.attributes?.["outcome"] ?? event.attributes?.["status"];

  return typeof value === "string" ? value : undefined;
}

function isRuntimeTelemetryOutcome(value: string): value is RuntimeTelemetryOutcome {
  return [
    "started",
    "success",
    "duplicate",
    "invalid",
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
  const parts = RUNTIME_ALLOWED_METRIC_LABELS.flatMap((name): string[] => {
    const value = labels[name];
    return value === undefined ? [] : [`${name}="${escapeLabelValue(value)}"`];
  });

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
    .replace(/"/gu, "\\\"")
    .replace(/\n/gu, "\\n");
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
