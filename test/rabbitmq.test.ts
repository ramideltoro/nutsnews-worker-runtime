import {
  CURRENT_ENVELOPE_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  WORKER_ENVELOPE_SCHEMA_ID,
  WORKER_ROUTES,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import { describe, expect, it } from "vitest";

import {
  RabbitMqPublishError,
  assertRabbitMqTopology,
  computeRabbitMqReconnectDelayMs,
  createRabbitMqTransport,
  publishEnvelopeWithConfirm,
  type BrokerDeliveryHandler,
  type RabbitMqConfirmChannel,
  type RabbitMqConnection,
  type RabbitMqConsumeMessage,
  type RabbitMqConsumeReply,
  type RabbitMqNetworkAdapter,
  type RabbitMqSocketOptions
} from "../src/index.js";

describe("RabbitMQ reconnect policy", () => {
  it("computes bounded jittered reconnect delays", () => {
    const delay = computeRabbitMqReconnectDelayMs({
      maxAttempts: 4,
      initialDelayMs: 100,
      maxDelayMs: 1_000,
      multiplier: 2,
      jitterRatio: 0.1
    }, 3, () => 1);

    expect(delay).toBe(440);
  });
});

describe("RabbitMQ topology assertion", () => {
  it("asserts exchanges, quorum queues, retry TTLs, DLQs, and bindings", async () => {
    const channel = new FakeRabbitMqChannel();

    await assertRabbitMqTopology(channel, [
      getWorkerRoute("fetch")
    ]);

    expect(channel.exchanges.map((exchange) => exchange.name)).toEqual([
      "nutsnews.worker.v1",
      "nutsnews.worker.retry.v1",
      "nutsnews.worker.dlq.v1"
    ]);
    expect(channel.queues.find((queue) => queue.name === "nutsnews.worker.fetch.v1")?.options.arguments).toEqual({
      "x-queue-type": "quorum"
    });
    expect(channel.queues.find((queue) => queue.name === "nutsnews.worker.fetch.v1.retry-5m")?.options.arguments).toMatchObject({
      "x-queue-type": "quorum",
      "x-message-ttl": 300_000,
      "x-dead-letter-exchange": "nutsnews.worker.v1",
      "x-dead-letter-routing-key": "nutsnews.worker.fetch.v1"
    });
    expect(channel.bindings).toContainEqual({
      queue: "nutsnews.worker.fetch.v1.dlq",
      source: "nutsnews.worker.dlq.v1",
      pattern: "nutsnews.worker.fetch.v1.dlq"
    });
  });
});

describe("RabbitMQ confirm publishing", () => {
  it("publishes persistent mandatory JSON envelopes and waits for confirms", async () => {
    const channel = new FakeRabbitMqChannel();
    const envelopeValue = envelope();

    await publishEnvelopeWithConfirm(channel, {
      envelope: envelopeValue,
      exchange: "nutsnews.worker.v1",
      routingKey: "nutsnews.worker.fetch.v1",
      confirmTimeoutMs: 1_000
    });

    expect(channel.published).toHaveLength(1);
    expect(channel.published[0]?.options).toMatchObject({
      persistent: true,
      mandatory: true,
      contentType: "application/json",
      contentEncoding: "utf-8",
      messageId: envelopeValue.messageId,
      correlationId: envelopeValue.correlationId
    });
    expect(JSON.parse(channel.published[0]!.content.toString("utf8"))).toMatchObject({
      messageId: envelopeValue.messageId,
      route: "fetch"
    });
  });

  it("fails visibly when the broker returns a mandatory publish", async () => {
    const channel = new FakeRabbitMqChannel();
    channel.returnNextPublish = true;

    await expect(publishEnvelopeWithConfirm(channel, {
      envelope: envelope(),
      exchange: "nutsnews.worker.v1",
      routingKey: "nutsnews.worker.missing.v1",
      confirmTimeoutMs: 1_000
    })).rejects.toMatchObject({
      name: "RabbitMqPublishError",
      code: "unroutable"
    } satisfies Partial<RabbitMqPublishError>);
  });

  it("fails visibly when confirms exceed the timeout", async () => {
    const channel = new FakeRabbitMqChannel();
    channel.holdNextConfirm = true;

    await expect(publishEnvelopeWithConfirm(channel, {
      envelope: envelope(),
      exchange: "nutsnews.worker.v1",
      routingKey: "nutsnews.worker.fetch.v1",
      confirmTimeoutMs: 1
    })).rejects.toMatchObject({
      name: "RabbitMqPublishError",
      code: "confirm-timeout"
    } satisfies Partial<RabbitMqPublishError>);
  });
});

describe("RabbitMQ transport", () => {
  it("connects with heartbeat/TLS socket options and applies prefetch", async () => {
    const channel = new FakeRabbitMqChannel();
    const network = new FakeRabbitMqNetwork(channel);
    const transport = createRabbitMqTransport({
      connection: {
        url: "amqps://worker.example.test/%2f",
        heartbeatSeconds: 30,
        tls: {
          servername: "worker.example.test",
          rejectUnauthorized: true
        }
      },
      prefetch: 7,
      seams: {
        network
      }
    });

    await transport.connect();
    await transport.assertTopology(WORKER_ROUTES);
    await transport.consume("fetch", noopHandler());

    expect(network.connectCalls[0]?.url).toBe("amqps://worker.example.test/%2f?heartbeat=30");
    expect(network.connectCalls[0]?.socketOptions).toMatchObject({
      servername: "worker.example.test",
      rejectUnauthorized: true
    });
    expect(channel.prefetches).toContain(7);
  });

  it("publishes retry transfers before acknowledging the original delivery", async () => {
    const channel = new FakeRabbitMqChannel();
    let loadedMessageId = "";
    const transport = createRabbitMqTransport({
      connection: {
        url: "amqp://worker.example.test"
      },
      messageIdFactory: () => "018f1598-2dd5-7c4f-9f92-8f7a7f8b3902",
      payloadLoader: (envelopeValue) => {
        loadedMessageId = envelopeValue.messageId;

        return {
          loaded: true
        };
      },
      seams: {
        network: new FakeRabbitMqNetwork(channel),
        random: () => 1
      }
    });
    const acked = channel.nextAck();

    await transport.connect();
    await transport.assertTopology(WORKER_ROUTES);
    await transport.consume("fetch", (delivery) => {
      expect(delivery.payload).toEqual({
        loaded: true
      });

      return Promise.resolve({
      action: "retry",
      reason: "retry-fixture",
      envelope: envelope(),
      destination: getWorkerRoute("fetch").retryQueues[0]!
      });
    });
    channel.deliver("fetch", message(envelope()));
    await acked;

    expect(loadedMessageId).toBe("018f1598-2dd5-7c4f-9f92-8f7a7f8b3901");
    expect(channel.events).toEqual([
      "publish:nutsnews.worker.retry.v1:nutsnews.worker.fetch.v1.retry-30s",
      "ack:1"
    ]);
    const retryEnvelope = JSON.parse(channel.published[0]!.content.toString("utf8")) as {
      readonly messageId: string;
      readonly causationId: string;
      readonly correlationId: string;
      readonly idempotencyKey: string;
      readonly attempt: {
        readonly count: number;
        readonly lastAttemptAt?: string;
      };
    };
    expect(retryEnvelope.messageId).toBe("018f1598-2dd5-7c4f-9f92-8f7a7f8b3902");
    expect(retryEnvelope.causationId).toBe("018f1598-2dd5-7c4f-9f92-8f7a7f8b3900");
    expect(retryEnvelope.correlationId).toBe("018f1598-2dd5-7c4f-9f92-8f7a7f8b38ff");
    expect(retryEnvelope.idempotencyKey).toBe("feed:world:story-003");
    expect(retryEnvelope.attempt).toMatchObject({
      count: 2,
      lastAttemptAt: expect.any(String) as string
    });
    expect(channel.published[0]?.options.headers).toMatchObject({
      retryJitterMs: 3_000
    });
  });
});

function noopHandler(): BrokerDeliveryHandler {
  return () => Promise.resolve({
    action: "ack",
    reason: "handled",
    envelope: envelope()
  });
}

class FakeRabbitMqNetwork implements RabbitMqNetworkAdapter {
  readonly connectCalls: {
    readonly url: string | object;
    readonly socketOptions: RabbitMqSocketOptions | undefined;
  }[] = [];

  constructor(private readonly channel: FakeRabbitMqChannel) {}

  connect(url: string | object, socketOptions?: RabbitMqSocketOptions): Promise<RabbitMqConnection> {
    this.connectCalls.push({
      url,
      socketOptions
    });

    return Promise.resolve(new FakeRabbitMqConnection(this.channel));
  }
}

class FakeRabbitMqConnection implements RabbitMqConnection {
  private readonly listeners = new Map<string, Set<(error?: unknown) => void>>();

  constructor(private readonly channel: FakeRabbitMqChannel) {}

  createConfirmChannel(): Promise<RabbitMqConfirmChannel> {
    return Promise.resolve(this.channel);
  }

  close(): Promise<void> {
    this.emit("close");

    return Promise.resolve();
  }

  on(event: "close" | "error", listener: (error?: unknown) => void): void {
    this.listenersFor(event).add(listener);
  }

  off(event: "close" | "error", listener: (error?: unknown) => void): void {
    this.listenersFor(event).delete(listener);
  }

  private emit(event: "close" | "error", error?: unknown): void {
    for (const listener of this.listenersFor(event)) {
      listener(error);
    }
  }

  private listenersFor(event: string): Set<(error?: unknown) => void> {
    const existing = this.listeners.get(event);

    if (existing !== undefined) {
      return existing;
    }

    const listeners = new Set<(error?: unknown) => void>();
    this.listeners.set(event, listeners);

    return listeners;
  }
}

class FakeRabbitMqChannel implements RabbitMqConfirmChannel {
  exchanges: {
    readonly name: string;
    readonly type: string;
    readonly options: object;
  }[] = [];
  queues: {
    readonly name: string;
    readonly options: {
      readonly arguments?: object;
    };
  }[] = [];
  bindings: {
    readonly queue: string;
    readonly source: string;
    readonly pattern: string;
  }[] = [];
  published: {
    readonly exchange: string;
    readonly routingKey: string;
    readonly content: Buffer;
    readonly options: Readonly<Record<string, unknown>>;
  }[] = [];
  prefetches: number[] = [];
  events: string[] = [];
  returnNextPublish = false;
  holdNextConfirm = false;
  private readonly listeners = new Map<string, Set<(value?: unknown) => void>>();
  private readonly consumers = new Map<WorkerStage, (message: RabbitMqConsumeMessage | null) => void>();
  private ackResolve: (() => void) | undefined;

  assertExchange(exchange: string, type: string, options: object): Promise<unknown> {
    this.exchanges.push({
      name: exchange,
      type,
      options
    });

    return Promise.resolve({});
  }

  assertQueue(queue: string, options: { readonly arguments?: object }): Promise<unknown> {
    this.queues.push({
      name: queue,
      options
    });

    return Promise.resolve({});
  }

  bindQueue(queue: string, source: string, pattern: string): Promise<unknown> {
    this.bindings.push({
      queue,
      source,
      pattern
    });

    return Promise.resolve({});
  }

  prefetch(count: number): Promise<unknown> {
    this.prefetches.push(count);

    return Promise.resolve({});
  }

  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: Readonly<Record<string, unknown>>,
    callback: (error: Error | null) => void
  ): boolean {
    this.published.push({
      exchange,
      routingKey,
      content,
      options
    });
    this.events.push(`publish:${exchange}:${routingKey}`);

    if (this.returnNextPublish) {
      this.returnNextPublish = false;
      this.emit("return", {
        properties: {
          messageId: options.messageId
        }
      });
    }

    if (!this.holdNextConfirm) {
      callback(null);
    }

    this.holdNextConfirm = false;

    return true;
  }

  consume(
    queue: string,
    onMessage: (message: RabbitMqConsumeMessage | null) => void,
    options: { readonly noAck?: boolean }
  ): Promise<RabbitMqConsumeReply> {
    expect(options.noAck).toBe(false);
    const stage = stageForQueue(queue);
    this.consumers.set(stage, onMessage);

    return Promise.resolve({
      consumerTag: `consumer-${queue}`
    });
  }

  cancel(consumerTag: string): Promise<unknown> {
    void consumerTag;

    return Promise.resolve({});
  }

  ack(messageValue: RabbitMqConsumeMessage): void {
    this.events.push(`ack:${String(messageValue.fields.deliveryTag)}`);
    this.ackResolve?.();
  }

  nack(messageValue: RabbitMqConsumeMessage, allUpTo: boolean, requeue: boolean): void {
    void allUpTo;
    this.events.push(`nack:${String(messageValue.fields.deliveryTag)}:${String(requeue)}`);
  }

  close(): Promise<void> {
    this.emit("close");

    return Promise.resolve();
  }

  on(event: "return" | "close" | "error" | "drain", listener: (value?: unknown) => void): void {
    this.listenersFor(event).add(listener);
  }

  off(event: "return" | "close" | "error" | "drain", listener: (value?: unknown) => void): void {
    this.listenersFor(event).delete(listener);
  }

  deliver(stage: WorkerStage, messageValue: RabbitMqConsumeMessage): void {
    this.consumers.get(stage)?.(messageValue);
  }

  nextAck(): Promise<void> {
    return new Promise((resolve) => {
      this.ackResolve = resolve;
    });
  }

  private emit(event: "return" | "close" | "error" | "drain", value?: unknown): void {
    for (const listener of this.listenersFor(event)) {
      listener(value);
    }
  }

  private listenersFor(event: string): Set<(value?: unknown) => void> {
    const existing = this.listeners.get(event);

    if (existing !== undefined) {
      return existing;
    }

    const listeners = new Set<(value?: unknown) => void>();
    this.listeners.set(event, listeners);

    return listeners;
  }
}

function stageForQueue(queue: string): WorkerStage {
  const route = WORKER_ROUTES.find((candidate) => candidate.mainQueue.name === queue);

  if (route === undefined) {
    throw new Error(`Unknown queue ${queue}`);
  }

  return route.stage;
}

function message(envelopeValue: WorkerMessageEnvelope): RabbitMqConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify(envelopeValue), "utf8"),
    fields: {
      consumerTag: "consumer-fetch",
      deliveryTag: 1,
      exchange: "nutsnews.worker.v1",
      routingKey: "nutsnews.worker.fetch.v1",
      redelivered: false
    },
    properties: {
      contentType: "application/json",
      contentEncoding: "utf-8",
      messageId: envelopeValue.messageId,
      correlationId: envelopeValue.correlationId
    }
  };
}

function envelope(): WorkerMessageEnvelope {
  return {
    schemaId: WORKER_ENVELOPE_SCHEMA_ID,
    schemaVersion: CURRENT_ENVELOPE_SCHEMA_VERSION,
    route: "fetch",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3901",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3900",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b38ff",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "feed:world:story-003",
    aggregate: {
      type: "feed",
      id: "feed-world",
      version: 1
    },
    occurredAt: "2026-07-23T05:20:00.000Z",
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: "2026-07-23T05:20:00.000Z"
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
