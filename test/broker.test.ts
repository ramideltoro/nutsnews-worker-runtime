import {
  CURRENT_ENVELOPE_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  WORKER_ENVELOPE_SCHEMA_ID,
  WORKER_ROUTES,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import { describe, expect, it } from "vitest";

import {
  createBrokerLifecycle,
  createBufferedRuntimeTelemetrySink,
  type BrokerConsumerHandle,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport
} from "../src/index.js";

describe("broker lifecycle", () => {
  it("connects, asserts topology, publishes with confirms, and closes", async () => {
    const transport = new FakeBrokerTransport();
    const telemetry = createBufferedRuntimeTelemetrySink();
    const lifecycle = createBrokerLifecycle({
      transport,
      telemetry
    });

    await lifecycle.start();
    const receipt = await lifecycle.publish({
      envelope: envelope(),
      payload: {
        schemaId: "example"
      }
    });
    await lifecycle.stop("test-complete");

    expect(lifecycle.state).toBe("closed");
    expect(transport.connected).toBe(true);
    expect(transport.closed).toBe(true);
    expect(transport.assertedRoutes).toHaveLength(WORKER_ROUTES.length);
    expect(receipt).toMatchObject({
      confirmed: true,
      exchange: "nutsnews.worker.v1",
      routingKey: "nutsnews.worker.fetch.v1"
    });
    expect(telemetry.events.map((event) => event.name)).toContain("runtime.broker.topology_asserted");
  });

  it("cancels active consumers during stop", async () => {
    const transport = new FakeBrokerTransport();
    const lifecycle = createBrokerLifecycle({
      transport
    });

    await lifecycle.start();
    const handle = await lifecycle.consume("fetch", () => Promise.resolve({
      action: "ack",
      reason: "handled",
      envelope: envelope()
    }));
    await lifecycle.stop("test-stop");

    expect(handle.stage).toBe("fetch");
    expect(transport.cancelledStages).toEqual([
      "fetch"
    ]);
  });

  it("rejects publish before startup", async () => {
    const lifecycle = createBrokerLifecycle({
      transport: new FakeBrokerTransport()
    });

    await expect(lifecycle.publish({
      envelope: envelope(),
      payload: {}
    })).rejects.toThrow("must be ready");
  });
});

class FakeBrokerTransport implements RuntimeBrokerTransport {
  readonly name = "fake-broker";
  connected = false;
  closed = false;
  assertedRoutes: readonly WorkerRoute[] = [];
  cancelledStages: WorkerStage[] = [];

  connect(): Promise<void> {
    this.connected = true;

    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.assertedRoutes = routes;

    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    const route = getWorkerRoute(command.envelope.route);

    return Promise.resolve({
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: "2026-07-23T05:10:00.000Z"
    });
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    void handler;

    return Promise.resolve({
      stage,
      cancel: (): Promise<void> => {
        this.cancelledStages.push(stage);

        return Promise.resolve();
      }
    });
  }

  close(): Promise<void> {
    this.closed = true;

    return Promise.resolve();
  }
}

function envelope(): WorkerMessageEnvelope {
  return {
    schemaId: WORKER_ENVELOPE_SCHEMA_ID,
    schemaVersion: CURRENT_ENVELOPE_SCHEMA_VERSION,
    route: "fetch",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3801",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3800",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b37ff",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "feed:world:story-002",
    aggregate: {
      type: "feed",
      id: "feed-world",
      version: 1
    },
    occurredAt: "2026-07-23T05:10:00.000Z",
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: "2026-07-23T05:10:00.000Z"
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
    }
  };
}
