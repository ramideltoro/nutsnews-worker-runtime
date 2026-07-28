import {
  CURRENT_ENVELOPE_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  WORKER_ENVELOPE_SCHEMA_ID,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import { describe, expect, it } from "vitest";

import {
  RUNTIME_ALLOWED_METRIC_LABELS,
  RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS,
  createBrokerConsumerMonitor,
  createBufferedRuntimeTelemetrySink,
  createJsonRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink,
  readRuntimeTraceContextFromHeaders,
  runtimeTraceHeadersFromEnvelope
} from "../src/index.js";

describe("runtime JSON telemetry", () => {
  it("writes structured redacted JSON without dropping correlation fields", async () => {
    const lines: string[] = [];
    const sink = createJsonRuntimeTelemetrySink({
      identity: {
        service: "fetcher",
        version: "0.5.0",
        environment: "shadow",
        host: "backend.nutsnews.com"
      },
      writer: (line) => {
        lines.push(line);
      }
    });

    await sink.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-07-23T06:40:00.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      attempt: 1,
      durationMs: 42,
      outcome: "success",
      messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4001",
      idempotencyKey: "feed:world:story-006",
      correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b40ff",
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      attributes: {
        authorization: "Bearer secret",
        articleBody: "article contents",
        safeReason: "handled"
      }
    });

    const record = JSON.parse(lines[0] ?? "{}") as Record<string, unknown>;
    expect(record["service"]).toBe("fetcher");
    expect(record["messageId"]).toBe("018f1598-2dd5-7c4f-9f92-8f7a7f8b4001");
    expect(record["idempotencyKey"]).toBe("feed:world:story-006");
    expect(record["traceparent"]).toBe("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
    expect(record["attributes"]).toEqual({
      authorization: "[redacted]",
      articleBody: "[redacted]",
      safeReason: "handled"
    });
  });
});

describe("runtime Prometheus telemetry", () => {
  it("exports bounded-label metrics for worker outcomes, latency, health, and shutdown drain", async () => {
    const sink = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "fetcher",
        version: "0.5.0",
        environment: "shadow",
        host: "backend.nutsnews.com"
      }
    });

    await sink.emit({
      name: "runtime.message.started",
      level: "info",
      at: "2026-07-23T06:40:00.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      outcome: "started"
    });
    await sink.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-07-23T06:40:01.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      durationMs: 125,
      outcome: "success",
      messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4001",
      idempotencyKey: "feed:world:story-006",
      correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b40ff",
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    });
    sink.recordDependencyLatency("nutsnews.worker.fetch.v1", 37, "success");
    await sink.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-07-23T06:40:02.000Z",
      outcome: "ok",
      attributes: {
        status: "ok"
      }
    });
    await sink.emit({
      name: "runtime.shutdown.started",
      level: "info",
      at: "2026-07-23T06:40:03.000Z",
      outcome: "draining"
    });
    const consumer = createBrokerConsumerMonitor({
      stage: "fetch",
      telemetry: sink
    });
    consumer.markActive();
    consumer.markCancelled("broker-cancelled");

    const output = sink.collect();
    expect(sink.allowedLabels).toEqual(RUNTIME_ALLOWED_METRIC_LABELS);
    expect(output).toContain("nutsnews_worker_messages_total");
    expect(output).toContain("nutsnews_worker_processing_duration_ms_count");
    expect(output).toContain("nutsnews_worker_dependency_duration_ms_sum");
    expect(output).toContain("nutsnews_worker_inflight");
    expect(output).toContain("nutsnews_worker_health");
    expect(output).toContain("nutsnews_worker_consumers");
    expect(output).toContain("nutsnews_worker_consumer_events_total");
    expect(output).toContain("nutsnews_worker_shutdown_draining");
    expect(output).toContain('nutsnews_worker_consumers{environment="shadow",host="backend.nutsnews.com",service="fetcher",version="0.5.0",queue="nutsnews.worker.fetch.v1",outcome="active"} 0');
    expect(output).toContain('queue="nutsnews.worker.fetch.v1",outcome="cancelled"} 1');

    for (const forbidden of RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS) {
      expect(output).not.toContain(`${forbidden}=`);
    }

    expect(output).not.toContain("messageId=");
    expect(output).not.toContain("idempotencyKey=");
    expect(output).not.toContain("traceparent=");
    expect(output).not.toContain("correlationId=");
  });

  it("writes structured consumer cancellation and channel-drop events", () => {
    const telemetry = createBufferedRuntimeTelemetrySink();
    const consumer = createBrokerConsumerMonitor({
      stage: "approval",
      telemetry
    });

    consumer.markActive();
    consumer.markCancelled("broker-cancelled");
    consumer.markChannelDropped("channel-error");

    expect(consumer.status).toMatchObject({
      queue: "nutsnews.worker.approval.v1",
      state: "channel-dropped",
      activeConsumers: 0,
      reason: "channel-error"
    });
    expect(telemetry.events).toEqual([
      expect.objectContaining({
        name: "runtime.broker.consumer_state_changed",
        level: "info",
        outcome: "active"
      }),
      expect.objectContaining({
        name: "runtime.broker.consumer_state_changed",
        level: "error",
        outcome: "cancelled",
        attributes: expect.objectContaining({
          reason: "broker-cancelled",
          activeConsumers: 0
        }) as Record<string, unknown>
      }),
      expect.objectContaining({
        name: "runtime.broker.consumer_state_changed",
        level: "error",
        outcome: "channel-dropped",
        attributes: expect.objectContaining({
          reason: "channel-error",
          activeConsumers: 0
        }) as Record<string, unknown>
      })
    ]);
  });
});

describe("runtime trace context", () => {
  it("round-trips W3C trace headers while span export stays external", () => {
    const headers = runtimeTraceHeadersFromEnvelope(envelope({
      tracestate: "vendor=value"
    }));

    expect(headers).toEqual({
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      tracestate: "vendor=value"
    });
    expect(readRuntimeTraceContextFromHeaders(headers)).toEqual({
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      tracestate: "vendor=value"
    });
  });
});

function envelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  return {
    schemaId: WORKER_ENVELOPE_SCHEMA_ID,
    schemaVersion: CURRENT_ENVELOPE_SCHEMA_VERSION,
    route: "fetch",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4001",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4000",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b40ff",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "feed:world:story-006",
    aggregate: {
      type: "feed",
      id: "feed-world",
      version: 1
    },
    occurredAt: "2026-07-23T06:40:00.000Z",
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: "2026-07-23T06:40:00.000Z"
    },
    producer: {
      name: "scheduler",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "worker-payload://fixture/feed-fetch-request",
      mediaType: "application/json",
      sizeBytes: 512
    },
    ...overrides
  };
}
