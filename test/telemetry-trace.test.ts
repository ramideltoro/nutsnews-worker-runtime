import {
  CURRENT_ENVELOPE_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  WORKER_ENVELOPE_SCHEMA_ID,
  type WorkerStage,
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
  runtimeTraceHeadersFromEnvelope,
  type RuntimeTelemetryOutcome
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
      pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4002",
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
    expect(record["pipelineRunId"]).toBe("018f1598-2dd5-7c4f-9f92-8f7a7f8b4002");
    expect(record["idempotencyKey"]).toBe("feed:world:story-006");
    expect(record["traceparent"]).toBe("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
    expect(record["attributes"]).toEqual({
      authorization: "[redacted]",
      articleBody: "[redacted]",
      safeReason: "handled"
    });
  });

  it("canonicalizes safe identifiers and omits malformed or oversized correlation metadata", async () => {
    const lines: string[] = [];
    const sink = createJsonRuntimeTelemetrySink({
      identity: {
        service: "fetcher",
        version: "1.0.0",
        environment: "shadow"
      },
      writer: (line) => {
        lines.push(line);
      }
    });

    await sink.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-08-01T00:00:00.000Z",
      messageId: " 018F1598-2DD5-7C4F-9F92-8F7A7F8B4001 ",
      pipelineRunId: "x".repeat(2_048),
      idempotencyKey: "INVALID KEY",
      correlationId: " 018F1598-2DD5-7C4F-9F92-8F7A7F8B40FF ",
      causationId: "not-a-uuid",
      traceparent: " 00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01 ",
      tracestate: "x".repeat(513)
    });
    await sink.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-08-01T00:00:01.000Z",
      pipelineRunId: "pipeline-run\nforged-field"
    });

    const canonical = JSON.parse(lines[0] ?? "{}") as Record<string, unknown>;
    const malformed = JSON.parse(lines[1] ?? "{}") as Record<string, unknown>;
    expect(canonical["messageId"]).toBe("018f1598-2dd5-7c4f-9f92-8f7a7f8b4001");
    expect(canonical["correlationId"]).toBe("018f1598-2dd5-7c4f-9f92-8f7a7f8b40ff");
    expect(canonical["traceparent"]).toBe("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
    expect(canonical).not.toHaveProperty("pipelineRunId");
    expect(canonical).not.toHaveProperty("idempotencyKey");
    expect(canonical).not.toHaveProperty("causationId");
    expect(canonical).not.toHaveProperty("tracestate");
    expect(malformed).not.toHaveProperty("pipelineRunId");
    expect(lines.join("\n")).not.toContain("forged-field");
    expect(lines.join("\n")).not.toContain("x".repeat(1_024));
  });
});

describe("runtime Prometheus telemetry", () => {
  it("exports bounded-label metrics for worker outcomes, latency, health, and shutdown drain", async () => {
    const sink = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "fetcher",
        version: "0.5.0",
        environment: "shadow",
        host: "backend.nutsnews.com",
        revision: "abc123",
        deployment: "shadow",
        adapter: "production"
      },
      cardinality: {
        dependencies: ["http"],
        healthChecks: ["broker"]
      },
      expectedActive: false
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
      pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4002",
      idempotencyKey: "feed:world:story-006",
      correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b40ff",
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    });
    await sink.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-23T06:40:01.500Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      durationMs: 37,
      outcome: "success",
      attributes: {
        dependency: "http",
        durationMs: 999
      }
    });
    await sink.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-23T06:40:01.600Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      outcome: "success",
      attributes: {
        dependency: "attacker-controlled-dependency"
      }
    });
    await sink.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-07-23T06:40:02.000Z",
      outcome: "ok",
      attributes: {
        probe: "readiness",
        status: "ok",
        checks: [{
          name: "broker",
          status: "ok",
          critical: true,
          durationMs: 12
        }]
      }
    });
    await sink.emit({
      name: "runtime.shutdown.started",
      level: "info",
      at: "2026-07-23T06:40:03.000Z",
      outcome: "draining"
    });
    await sink.emit({
      name: "runtime.shutdown.completed",
      level: "info",
      at: "2026-07-23T06:40:04.000Z",
      outcome: "drained"
    });
    sink.setInFlight("nutsnews.worker.fetch.v1", 0);
    const consumer = createBrokerConsumerMonitor({
      stage: "fetch",
      telemetry: sink
    });
    consumer.markActive();
    consumer.markCancelled("broker-cancelled");
    sink.setLastSuccessTimestamp(1_753_251_600);

    const output = sink.collect();
    expect(sink.allowedLabels).toEqual(RUNTIME_ALLOWED_METRIC_LABELS);
    expect(RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS).toContain("pipeline");
    expect(sink.allowedLabels.some((label) => label.toLowerCase().includes("pipeline"))).toBe(false);
    expect(output).toContain("nutsnews_worker_messages_total");
    expect(output).toContain("nutsnews_worker_processing_duration_seconds_bucket");
    expect(output).toContain("nutsnews_worker_dependency_duration_seconds_bucket");
    expect(output).toContain('nutsnews_worker_processing_duration_seconds_bucket{environment="shadow",host="backend.nutsnews.com",service="fetcher",version="0.5.0",stage="fetch",queue="nutsnews.worker.fetch.v1",outcome="success",le="0.1"} 0');
    expect(output).toContain('nutsnews_worker_processing_duration_seconds_bucket{environment="shadow",host="backend.nutsnews.com",service="fetcher",version="0.5.0",stage="fetch",queue="nutsnews.worker.fetch.v1",outcome="success",le="0.25"} 1');
    expect(output).toContain('nutsnews_worker_processing_duration_seconds_sum{environment="shadow",host="backend.nutsnews.com",service="fetcher",version="0.5.0",stage="fetch",queue="nutsnews.worker.fetch.v1",outcome="success"} 0.125');
    expect(output).toContain('nutsnews_worker_dependency_duration_seconds_sum{environment="shadow",host="backend.nutsnews.com",service="fetcher",version="0.5.0",stage="fetch",queue="nutsnews.worker.fetch.v1",outcome="success",dependency="http"} 0.037');
    expect(output).toContain('nutsnews_worker_dependency_duration_seconds_count{environment="shadow",host="backend.nutsnews.com",service="fetcher",version="0.5.0",stage="fetch",queue="nutsnews.worker.fetch.v1",outcome="success",dependency="http"} 1');
    expect(output).toContain("nutsnews_worker_inflight");
    expect(output.split("\n").filter((line) => line.startsWith("nutsnews_worker_inflight{"))).toEqual([
      'nutsnews_worker_inflight{environment="shadow",host="backend.nutsnews.com",service="fetcher",version="0.5.0",queue="nutsnews.worker.fetch.v1",outcome="in_flight"} 0'
    ]);
    expect(output).toContain("nutsnews_worker_health_probe");
    expect(output).toContain("nutsnews_worker_health_check");
    expect(output).toContain('outcome="ok",probe="readiness",check="broker"} 1');
    expect(output).toContain('nutsnews_worker_build_info{environment="shadow",service="fetcher",version="0.5.0",revision="abc123"} 1');
    expect(output).toContain('nutsnews_worker_deployment_info{environment="shadow",service="fetcher",deployment="shadow",adapter="production"} 1');
    expect(output).toContain('nutsnews_worker_expected_active{environment="shadow",service="fetcher"} 0');
    expect(output).toContain('nutsnews_worker_last_success_timestamp_seconds{environment="shadow",service="fetcher"} 1753251600');
    expect(output).toContain("nutsnews_worker_consumers");
    expect(output).toContain("nutsnews_worker_consumer_events_total");
    expect(output).toContain("nutsnews_worker_shutdown_draining");
    expect(output.split("\n").filter((line) => line.startsWith("nutsnews_worker_shutdown_draining{"))).toEqual([
      'nutsnews_worker_shutdown_draining{environment="shadow",service="fetcher"} 0'
    ]);
    expect(output).toContain('nutsnews_worker_consumers{environment="shadow",host="backend.nutsnews.com",service="fetcher",version="0.5.0",stage="fetch",queue="nutsnews.worker.fetch.v1",outcome="active"} 0');
    expect(output).toContain('queue="nutsnews.worker.fetch.v1",outcome="cancelled"} 1');

    for (const forbidden of RUNTIME_FORBIDDEN_METRIC_LABEL_FRAGMENTS) {
      expect(output).not.toContain(`${forbidden}=`);
    }

    expect(output).not.toContain("messageId=");
    expect(output).not.toContain("pipelineRunId=");
    expect(output).not.toContain("pipeline_run_id=");
    expect(output).not.toContain("018f1598-2dd5-7c4f-9f92-8f7a7f8b4002");
    expect(output).not.toContain("idempotencyKey=");
    expect(output).not.toContain("traceparent=");
    expect(output).not.toContain("correlationId=");
  });

  it("keeps invalid validation outcomes distinct in terminal message and DLQ metrics", async () => {
    const sink = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "fetcher",
        version: "1.0.0",
        environment: "test"
      }
    });

    await sink.emit({
      name: "runtime.message.started",
      level: "info",
      at: "2026-07-31T00:00:00.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      outcome: "started"
    });
    await sink.emit({
      name: "runtime.message.invalid",
      level: "warn",
      at: "2026-07-31T00:00:00.001Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      outcome: "invalid"
    });

    const output = sink.collect();
    const labels = 'environment="test",host="unknown",service="fetcher",version="1.0.0",stage="fetch",queue="nutsnews.worker.fetch.v1",outcome="invalid"';

    expect(output).toContain(`nutsnews_worker_messages_total{${labels}} 1`);
    expect(output).toContain(`nutsnews_worker_dlq_total{${labels}} 1`);
    expect(output).not.toContain('nutsnews_worker_messages_total{environment="test",host="unknown",service="fetcher",version="1.0.0",stage="fetch",queue="nutsnews.worker.fetch.v1",outcome="failure"}');
  });

  it("collapses unapproved dependency values and identifiers to a fixed series set", async () => {
    const sink = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "fetcher",
        version: "1.0.0",
        environment: "shadow"
      },
      cardinality: {
        dependencies: ["http"]
      }
    });

    for (let index = 0; index < 1_000; index += 1) {
      await sink.emit({
        name: "runtime.dependency.observed",
        level: "info",
        at: "2026-07-31T00:00:00.000Z",
        stage: "fetch",
        queue: "nutsnews.worker.fetch.v1",
        durationMs: 10,
        outcome: "success",
        messageId: `message-${String(index)}`,
        correlationId: `correlation-${String(index)}`,
        traceparent: `trace-${String(index)}`,
        attributes: {
          dependency: `unbounded-${String(index)}`,
          feedId: `feed-${String(index)}`
        }
      });
    }

    const output = sink.collect();
    const dependencyCounts = output.split("\n").filter((line) => (
      line.startsWith("nutsnews_worker_dependency_duration_seconds_count")
    ));
    expect(dependencyCounts).toHaveLength(1);
    expect(dependencyCounts[0]).toContain('dependency="other"');
    expect(dependencyCounts[0]?.endsWith(" 1000")).toBe(true);
    expect(output).not.toContain("unbounded-999");
    expect(output).not.toContain("message-999");
    expect(output).not.toContain("feed-999");
  });

  it("collapses arbitrary queue names to one bounded fallback series", async () => {
    const sink = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "fetcher",
        version: "1.0.0",
        environment: "shadow"
      }
    });

    for (let index = 0; index < 1_000; index += 1) {
      await sink.emit({
        name: "runtime.message.accepted",
        level: "info",
        at: "2026-07-31T00:00:00.000Z",
        stage: "fetch",
        queue: `attacker-queue-${String(index)}`,
        outcome: "success",
        pipelineRunId: `pipeline-run-${String(index)}`
      });
      sink.setInFlight(`attacker-queue-${String(index)}`, index);
    }

    const output = sink.collect();
    const messageSamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_messages_total{"));
    const inFlightSamples = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_inflight{"));

    expect(messageSamples).toHaveLength(1);
    expect(messageSamples[0]).toContain('queue="other"');
    expect(messageSamples[0]?.endsWith(" 1000")).toBe(true);
    expect(inFlightSamples).toHaveLength(1);
    expect(inFlightSamples[0]).toContain('queue="other"');
    expect(output).not.toContain("attacker-queue-999");
    expect(output).not.toContain("pipeline-run-999");
  });

  it("collapses arbitrary stage and family-specific outcome values to fixed fallback series", async () => {
    const sink = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "fetcher",
        version: "1.0.0",
        environment: "shadow"
      },
      cardinality: {
        dependencies: ["http"]
      }
    });

    for (let index = 0; index < 1_000; index += 1) {
      const stage = `unbounded-stage-${String(index)}` as WorkerStage;
      const outcome = `unbounded-outcome-${String(index)}` as RuntimeTelemetryOutcome;

      await sink.emit({
        name: "runtime.message.accepted",
        level: "info",
        at: "2026-07-31T00:00:00.000Z",
        stage,
        queue: "nutsnews.worker.fetch.v1",
        outcome
      });
      await sink.emit({
        name: "runtime.dependency.observed",
        level: "info",
        at: "2026-07-31T00:00:00.000Z",
        stage,
        queue: "nutsnews.worker.fetch.v1",
        durationMs: 10,
        outcome,
        attributes: {
          dependency: "http"
        }
      });
      await sink.emit({
        name: "runtime.broker.consumer_state_changed",
        level: "warn",
        at: "2026-07-31T00:00:00.000Z",
        stage,
        queue: "nutsnews.worker.fetch.v1",
        outcome,
        attributes: {
          activeConsumers: 0
        }
      });
    }

    const output = sink.collect();
    const messages = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_messages_total{"));
    const dependencies = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_dependency_duration_seconds_count{"));
    const consumers = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_consumer_events_total{"));

    for (const samples of [
      messages,
      dependencies,
      consumers
    ]) {
      expect(samples).toHaveLength(1);
      expect(samples[0]).toContain('stage="other"');
      expect(samples[0]).toContain('outcome="other"');
      expect(samples[0]?.endsWith(" 1000")).toBe(true);
    }

    expect(output).not.toContain("unbounded-stage-999");
    expect(output).not.toContain("unbounded-outcome-999");
  });

  it("maps arbitrary attribute outcomes through each family fallback", async () => {
    const sink = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "fetcher",
        version: "1.0.0",
        environment: "shadow"
      },
      cardinality: {
        dependencies: ["http"]
      }
    });

    for (let index = 0; index < 1_000; index += 1) {
      await sink.emit({
        name: "runtime.dependency.observed",
        level: "info",
        at: "2026-07-31T00:00:00.000Z",
        stage: "fetch",
        queue: "nutsnews.worker.fetch.v1",
        durationMs: 10,
        attributes: {
          dependency: "http",
          outcome: `attribute-outcome-${String(index)}`
        }
      });
      await sink.emit({
        name: "runtime.broker.consumer_state_changed",
        level: "warn",
        at: "2026-07-31T00:00:00.000Z",
        stage: "fetch",
        queue: "nutsnews.worker.fetch.v1",
        attributes: {
          activeConsumers: 0,
          status: `attribute-status-${String(index)}`
        }
      });
    }

    const output = sink.collect();
    const dependencies = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_dependency_duration_seconds_count{"));
    const consumers = output.split("\n").filter((line) => line.startsWith("nutsnews_worker_consumer_events_total{"));

    for (const samples of [
      dependencies,
      consumers
    ]) {
      expect(samples).toHaveLength(1);
      expect(samples[0]).toContain('outcome="other"');
      expect(samples[0]?.endsWith(" 1000")).toBe(true);
    }

    expect(output).not.toContain('outcome="success"');
    expect(output).not.toContain('outcome="inactive"');
    expect(output).not.toContain("attribute-outcome-999");
    expect(output).not.toContain("attribute-status-999");
  });

  it("validates cardinality policies and replaces service-level signal samples", () => {
    expect(() => createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "approval",
        version: "1.0.0",
        environment: "test"
      },
      cardinality: {
        dependencies: ["backend api", "backend_api"]
      }
    })).toThrow(/duplicate after label normalization/u);

    const sink = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "approval",
        version: "1.0.0",
        environment: "test"
      },
      expectedActive: false
    });
    sink.setExpectedActive(true);
    sink.setLastSuccessTimestamp(10);
    sink.setLastSuccessTimestamp(20);
    const output = sink.collect();
    expect(output.match(/nutsnews_worker_expected_active\{/gu)).toHaveLength(1);
    expect(output).toContain('nutsnews_worker_expected_active{environment="test",service="approval"} 1');
    expect(output.match(/nutsnews_worker_last_success_timestamp_seconds\{/gu)).toHaveLength(1);
    expect(output).toContain('nutsnews_worker_last_success_timestamp_seconds{environment="test",service="approval"} 20');
  });

  it("maps ambiguous or failed health outcomes to unhealthy", async () => {
    const sink = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "approval",
        version: "1.0.0",
        environment: "test"
      },
      cardinality: {
        healthChecks: ["broker"]
      }
    });

    await sink.emit({
      name: "runtime.health.evaluated",
      level: "error",
      at: "2026-07-31T00:00:00.000Z",
      outcome: "failure",
      attributes: {
        probe: "readiness",
        checks: [{
          name: "broker",
          status: "not-a-health-status",
          critical: true,
          durationMs: 1
        }, {
          name: "unlisted-failure",
          status: "unhealthy",
          critical: true,
          durationMs: 1
        }, {
          name: "unlisted-success",
          status: "ok",
          critical: true,
          durationMs: 1
        }]
      }
    });

    const output = sink.collect();
    expect(output).toContain('nutsnews_worker_health_probe{environment="test",host="unknown",service="approval",version="1.0.0",outcome="unhealthy",probe="readiness"} 1');
    expect(output).toContain('nutsnews_worker_health_check{environment="test",host="unknown",service="approval",version="1.0.0",outcome="unhealthy",probe="readiness",check="broker"} 1');
    expect(output).toContain('nutsnews_worker_health_check{environment="test",host="unknown",service="approval",version="1.0.0",outcome="unhealthy",probe="readiness",check="other"} 1');
    expect(output.split("\n").filter((line) => (
      line.startsWith("nutsnews_worker_health_") && line.includes('outcome="ok"') && line.endsWith(" 1")
    ))).toHaveLength(0);
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
