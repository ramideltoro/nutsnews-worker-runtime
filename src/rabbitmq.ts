import {
  WORKER_DELIVERY_BEHAVIOR,
  WORKER_EXCHANGES,
  WORKER_QUEUE_TYPE,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  connect as amqpConnect,
  type Options
} from "amqplib";

import type {
  BrokerConsumerHandle,
  BrokerDeliveryHandler,
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport
} from "./broker.js";
import type {
  RuntimeMessageProcessingResult
} from "./message.js";
import {
  computeRetryJitterMs,
  createRetryEnvelope,
  randomUuidMessageIdFactory,
  type RuntimeMessageIdFactory
} from "./retry.js";
import {
  SYSTEM_RUNTIME_CLOCK,
  emitRuntimeTelemetry,
  runtimeNow,
  type RuntimeClock,
  type RuntimeTelemetrySink
} from "./telemetry.js";

export type RabbitMqPublishFailureCode =
  | "confirm-nack"
  | "confirm-timeout"
  | "unroutable"
  | "channel-closed";

export class RabbitMqPublishError extends Error {
  readonly code: RabbitMqPublishFailureCode;
  readonly exchange: string;
  readonly routingKey: string;
  readonly messageId: string;

  constructor(
    code: RabbitMqPublishFailureCode,
    exchange: string,
    routingKey: string,
    messageId: string
  ) {
    super(`RabbitMQ publish failed (${code}) for ${exchange}:${routingKey} message ${messageId}.`);
    this.name = "RabbitMqPublishError";
    this.code = code;
    this.exchange = exchange;
    this.routingKey = routingKey;
    this.messageId = messageId;
  }
}

export interface RabbitMqReconnectPolicy {
  readonly maxAttempts: number;
  readonly initialDelayMs: number;
  readonly maxDelayMs: number;
  readonly multiplier: number;
  readonly jitterRatio: number;
}

export const DEFAULT_RABBITMQ_RECONNECT_POLICY = {
  maxAttempts: 5,
  initialDelayMs: 250,
  maxDelayMs: 5_000,
  multiplier: 2,
  jitterRatio: 0.2
} as const satisfies RabbitMqReconnectPolicy;

export interface RabbitMqTlsOptions {
  readonly ca?: string | Buffer | readonly (string | Buffer)[];
  readonly cert?: string | Buffer;
  readonly key?: string | Buffer;
  readonly servername?: string;
  readonly rejectUnauthorized?: boolean;
}

export type RabbitMqSocketOptions = Readonly<Record<string, unknown>>;

export interface RabbitMqConnectionOptions {
  readonly url: string | Options.Connect;
  readonly heartbeatSeconds?: number;
  readonly tls?: RabbitMqTlsOptions;
  readonly socketOptions?: RabbitMqSocketOptions;
}

export interface RabbitMqRuntimeSeams {
  readonly network?: RabbitMqNetworkAdapter;
  readonly clock?: RuntimeClock;
  readonly sleep?: (delayMs: number) => Promise<void>;
  readonly random?: () => number;
}

export interface RabbitMqTransportOptions {
  readonly connection: RabbitMqConnectionOptions;
  readonly prefetch?: number;
  readonly confirmTimeoutMs?: number;
  readonly reconnect?: Partial<RabbitMqReconnectPolicy>;
  readonly payloadLoader?: RabbitMqPayloadLoader;
  readonly retryJitterRatio?: number;
  readonly messageIdFactory?: RuntimeMessageIdFactory;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly seams?: RabbitMqRuntimeSeams;
  readonly onConsumerError?: (error: unknown, context: RabbitMqConsumerErrorContext) => void | Promise<void>;
}

export type RabbitMqPayloadLoader = (
  envelope: WorkerMessageEnvelope,
  message: RabbitMqConsumeMessage
) => Readonly<Record<string, unknown>> | Promise<Readonly<Record<string, unknown>>>;

export interface RabbitMqConsumerErrorContext {
  readonly stage: WorkerStage;
  readonly messageId?: string;
  readonly routingKey?: string;
}

export interface RabbitMqNetworkAdapter {
  connect(
    url: string | Options.Connect,
    socketOptions?: RabbitMqSocketOptions
  ): Promise<RabbitMqConnection>;
}

export interface RabbitMqConnection {
  createConfirmChannel(): Promise<RabbitMqConfirmChannel>;
  close(): Promise<void>;
  on(event: "close" | "error", listener: (error?: unknown) => void): void;
  off(event: "close" | "error", listener: (error?: unknown) => void): void;
}

export interface RabbitMqConfirmChannel {
  assertExchange(exchange: string, type: string, options: Options.AssertExchange): Promise<unknown>;
  assertQueue(queue: string, options: Options.AssertQueue): Promise<unknown>;
  bindQueue(queue: string, source: string, pattern: string): Promise<unknown>;
  prefetch(count: number): Promise<unknown>;
  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: Options.Publish,
    callback: (error: Error | null) => void
  ): boolean;
  consume(
    queue: string,
    onMessage: (message: RabbitMqConsumeMessage | null) => void,
    options: Options.Consume
  ): Promise<RabbitMqConsumeReply>;
  cancel(consumerTag: string): Promise<unknown>;
  ack(message: RabbitMqConsumeMessage): void;
  nack(message: RabbitMqConsumeMessage, allUpTo: boolean, requeue: boolean): void;
  close(): Promise<void>;
  on(event: "return" | "close" | "error" | "drain", listener: (value?: unknown) => void): void;
  off(event: "return" | "close" | "error" | "drain", listener: (value?: unknown) => void): void;
}

export interface RabbitMqConsumeReply {
  readonly consumerTag: string;
}

export interface RabbitMqConsumeMessage {
  readonly content: Buffer;
  readonly fields: {
    readonly consumerTag: string;
    readonly deliveryTag: number;
    readonly exchange: string;
    readonly routingKey: string;
    readonly redelivered: boolean;
  };
  readonly properties: {
    readonly contentType?: unknown;
    readonly contentEncoding?: unknown;
    readonly headers?: Readonly<Record<string, unknown>>;
    readonly messageId?: string;
    readonly correlationId?: string;
  };
}

interface ConsumerRegistration {
  readonly stage: WorkerStage;
  readonly handler: BrokerDeliveryHandler;
  readonly consumerTag: string;
}

export function createRabbitMqTransport(options: RabbitMqTransportOptions): RuntimeBrokerTransport {
  return new RabbitMqTransport(options);
}

export function computeRabbitMqReconnectDelayMs(
  policy: RabbitMqReconnectPolicy,
  attempt: number,
  random: () => number = Math.random
): number {
  if (!Number.isInteger(attempt) || attempt < 1) {
    throw new RangeError("Reconnect attempt must be a positive integer.");
  }

  const baseDelay = Math.min(
    policy.maxDelayMs,
    policy.initialDelayMs * (policy.multiplier ** (attempt - 1))
  );
  const jitter = baseDelay * policy.jitterRatio * ((random() * 2) - 1);

  return Math.max(0, Math.round(baseDelay + jitter));
}

class RabbitMqTransport implements RuntimeBrokerTransport {
  readonly name = "rabbitmq";

  private readonly options: RabbitMqTransportOptions;
  private readonly clock: RuntimeClock;
  private readonly sleep: (delayMs: number) => Promise<void>;
  private readonly random: () => number;
  private readonly network: RabbitMqNetworkAdapter;
  private readonly messageIdFactory: RuntimeMessageIdFactory;
  private readonly reconnectPolicy: RabbitMqReconnectPolicy;
  private readonly consumers = new Map<string, ConsumerRegistration>();
  private connection: RabbitMqConnection | undefined;
  private channel: RabbitMqConfirmChannel | undefined;
  private routes: readonly WorkerRoute[] = [];
  private closing = false;

  constructor(options: RabbitMqTransportOptions) {
    this.options = options;
    this.clock = options.seams?.clock ?? SYSTEM_RUNTIME_CLOCK;
    this.sleep = options.seams?.sleep ?? defaultSleep;
    this.random = options.seams?.random ?? Math.random;
    this.network = options.seams?.network ?? DEFAULT_RABBITMQ_NETWORK_ADAPTER;
    this.messageIdFactory = options.messageIdFactory ?? randomUuidMessageIdFactory;
    this.reconnectPolicy = {
      ...DEFAULT_RABBITMQ_RECONNECT_POLICY,
      ...options.reconnect
    };
  }

  async connect(): Promise<void> {
    this.closing = false;
    await this.ensureChannel();
  }

  async assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.routes = routes;
    const channel = await this.ensureChannel();

    await assertRabbitMqTopology(channel, routes);
  }

  async publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    const route = getWorkerRoute(command.envelope.route);
    const channel = await this.ensureChannel();

    await publishEnvelopeWithConfirm(channel, {
      envelope: command.envelope,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmTimeoutMs: this.confirmTimeoutMs
    });

    return {
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: runtimeNow(this.clock)
    };
  }

  async consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    const route = getWorkerRoute(stage);
    const channel = await this.ensureChannel();
    await channel.prefetch(this.prefetch);

    const reply = await channel.consume(route.mainQueue.name, (message) => {
      if (message === null) {
        this.consumers.delete(stage);
        return;
      }

      void this.handleDelivery(stage, handler, message);
    }, {
      noAck: false
    });

    const registration = {
      stage,
      handler,
      consumerTag: reply.consumerTag
    } as const satisfies ConsumerRegistration;
    this.consumers.set(stage, registration);

    return {
      stage,
      cancel: async (): Promise<void> => {
        const activeChannel = this.channel;
        this.consumers.delete(stage);

        if (activeChannel !== undefined) {
          await activeChannel.cancel(reply.consumerTag);
        }
      }
    };
  }

  async close(): Promise<void> {
    this.closing = true;

    for (const registration of this.consumers.values()) {
      const activeChannel = this.channel;

      if (activeChannel !== undefined) {
        await activeChannel.cancel(registration.consumerTag);
      }
    }

    this.consumers.clear();

    if (this.channel !== undefined) {
      await this.channel.close();
      this.channel = undefined;
    }

    if (this.connection !== undefined) {
      await this.connection.close();
      this.connection = undefined;
    }
  }

  private get prefetch(): number {
    return this.options.prefetch ?? WORKER_DELIVERY_BEHAVIOR.prefetch;
  }

  private get confirmTimeoutMs(): number {
    return this.options.confirmTimeoutMs ?? WORKER_DELIVERY_BEHAVIOR.confirmTimeoutMs;
  }

  private async ensureChannel(): Promise<RabbitMqConfirmChannel> {
    if (this.channel !== undefined) {
      return this.channel;
    }

    let lastError: unknown;

    for (let attempt = 1; attempt <= this.reconnectPolicy.maxAttempts; attempt += 1) {
      try {
        const connection = await this.network.connect(
          buildConnectionUrl(this.options.connection),
          buildSocketOptions(this.options.connection)
        );
        const channel = await connection.createConfirmChannel();
        connection.on("close", () => {
          this.connection = undefined;
          this.channel = undefined;
        });
        channel.on("close", () => {
          this.channel = undefined;
        });
        this.connection = connection;
        this.channel = channel;

        if (this.routes.length > 0) {
          await assertRabbitMqTopology(channel, this.routes);
          await this.recoverConsumers(channel);
        }

        return channel;
      } catch (error: unknown) {
        lastError = error;

        if (attempt >= this.reconnectPolicy.maxAttempts) {
          throw error;
        }

        const delayMs = computeRabbitMqReconnectDelayMs(this.reconnectPolicy, attempt, this.random);
        await this.sleep(delayMs);
      }
    }

    throw lastError;
  }

  private async recoverConsumers(channel: RabbitMqConfirmChannel): Promise<void> {
    const registrations = [
      ...this.consumers.values()
    ];
    this.consumers.clear();

    for (const registration of registrations) {
      const route = getWorkerRoute(registration.stage);
      await channel.prefetch(this.prefetch);
      const reply = await channel.consume(route.mainQueue.name, (message) => {
        if (message === null) {
          this.consumers.delete(registration.stage);
          return;
        }

        void this.handleDelivery(registration.stage, registration.handler, message);
      }, {
        noAck: false
      });
      this.consumers.set(registration.stage, {
        ...registration,
        consumerTag: reply.consumerTag
      });
    }
  }

  private async handleDelivery(
    stage: WorkerStage,
    handler: BrokerDeliveryHandler,
    message: RabbitMqConsumeMessage
  ): Promise<void> {
    const channel = this.channel;

    if (channel === undefined) {
      await this.handleConsumerError(
        new RabbitMqPublishError("channel-closed", message.fields.exchange, message.fields.routingKey, message.properties.messageId ?? "unknown"),
        consumerErrorContext(stage, message)
      );
      return;
    }

    try {
      const envelope = decodeEnvelope(message);
      const payload = await this.loadPayload(envelope, message);
      const result = await handler({
        envelope,
        payload,
        receivedAt: runtimeNow(this.clock)
      });
      await this.settleDelivery(channel, message, result);
    } catch (error: unknown) {
      channel.nack(message, false, false);
      await this.handleConsumerError(error, consumerErrorContext(stage, message));
    }
  }

  private async settleDelivery(
    channel: RabbitMqConfirmChannel,
    message: RabbitMqConsumeMessage,
    result: RuntimeMessageProcessingResult
  ): Promise<void> {
    if (result.action === "ack") {
      channel.ack(message);
      return;
    }

    if (result.action === "retry") {
      const retryEnvelope = createRetryEnvelope(result.envelope, {
        now: runtimeNow(this.clock),
        messageIdFactory: this.messageIdFactory
      });
      const retryJitterMs = computeRetryJitterMs(result.destination.ttlMs, this.options.retryJitterRatio ?? 0.1, this.random);
      await publishEnvelopeWithConfirm(channel, {
        envelope: retryEnvelope,
        exchange: getWorkerRoute(result.envelope.route).retryExchange,
        routingKey: result.destination.routingKey,
        confirmTimeoutMs: this.confirmTimeoutMs,
        retryJitterMs
      });
      channel.ack(message);
      return;
    }

    if (result.envelope !== undefined && result.destination !== undefined) {
      await publishEnvelopeWithConfirm(channel, {
        envelope: result.envelope,
        exchange: getWorkerRoute(result.envelope.route).dlqExchange,
        routingKey: result.destination.routingKey,
        confirmTimeoutMs: this.confirmTimeoutMs
      });
      channel.ack(message);
      return;
    }

    channel.nack(message, false, false);
  }

  private async handleConsumerError(error: unknown, context: RabbitMqConsumerErrorContext): Promise<void> {
    await emitRuntimeTelemetry(this.options.telemetry, {
      name: "runtime.message.dlq",
      level: "error",
      at: runtimeNow(this.clock),
      stage: context.stage,
      ...(context.messageId === undefined
        ? {}
        : {
            messageId: context.messageId
          }),
      attributes: {
        reason: error instanceof Error ? error.name : "unknown-consumer-error",
        destination: context.routingKey ?? "unknown"
      }
    });
    await this.options.onConsumerError?.(error, context);
  }

  private async loadPayload(
    envelope: WorkerMessageEnvelope,
    message: RabbitMqConsumeMessage
  ): Promise<Readonly<Record<string, unknown>>> {
    if (this.options.payloadLoader === undefined) {
      return {};
    }

    return await this.options.payloadLoader(envelope, message);
  }
}

export const DEFAULT_RABBITMQ_NETWORK_ADAPTER: RabbitMqNetworkAdapter = {
  async connect(url: string | Options.Connect, socketOptions?: RabbitMqSocketOptions): Promise<RabbitMqConnection> {
    return await amqpConnect(url, socketOptions) as unknown as RabbitMqConnection;
  }
};

export async function assertRabbitMqTopology(
  channel: RabbitMqConfirmChannel,
  routes: readonly WorkerRoute[]
): Promise<void> {
  await channel.assertExchange(WORKER_EXCHANGES.main.name, WORKER_EXCHANGES.main.type, {
    durable: WORKER_EXCHANGES.main.durable
  });
  await channel.assertExchange(WORKER_EXCHANGES.retry.name, WORKER_EXCHANGES.retry.type, {
    durable: WORKER_EXCHANGES.retry.durable
  });
  await channel.assertExchange(WORKER_EXCHANGES.dlq.name, WORKER_EXCHANGES.dlq.type, {
    durable: WORKER_EXCHANGES.dlq.durable
  });

  for (const route of routes) {
    await channel.assertQueue(route.mainQueue.name, quorumQueueOptions());
    await channel.bindQueue(route.mainQueue.name, route.exchange, route.routingKey);

    for (const retryQueue of route.retryQueues) {
      await channel.assertQueue(retryQueue.name, quorumQueueOptions({
        "x-message-ttl": retryQueue.ttlMs,
        "x-dead-letter-exchange": retryQueue.deadLetterExchange,
        "x-dead-letter-routing-key": retryQueue.deadLetterRoutingKey
      }));
      await channel.bindQueue(retryQueue.name, route.retryExchange, retryQueue.routingKey);
    }

    await channel.assertQueue(route.terminalDlq.name, quorumQueueOptions());
    await channel.bindQueue(route.terminalDlq.name, route.dlqExchange, route.terminalDlq.routingKey);
  }
}

export async function publishEnvelopeWithConfirm(
  channel: RabbitMqConfirmChannel,
  options: {
    readonly envelope: WorkerMessageEnvelope;
    readonly exchange: string;
    readonly routingKey: string;
    readonly confirmTimeoutMs: number;
    readonly retryJitterMs?: number;
  }
): Promise<void> {
  const content = Buffer.from(JSON.stringify(options.envelope), "utf8");
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let settled = false;

  await new Promise<void>((resolve, reject) => {
    const cleanup = (): void => {
      if (timeout !== undefined) {
        clearTimeout(timeout);
      }

      channel.off("return", onReturn);
      channel.off("close", onClose);
      channel.off("error", onChannelError);
      settled = true;
    };
    const fail = (error: Error): void => {
      if (settled) {
        return;
      }

      cleanup();
      reject(error);
    };
    const onReturn = (returned: unknown): void => {
      if (isReturnedMessageFor(returned, options.envelope.messageId)) {
        fail(new RabbitMqPublishError("unroutable", options.exchange, options.routingKey, options.envelope.messageId));
      }
    };
    const onClose = (): void => {
      fail(new RabbitMqPublishError("channel-closed", options.exchange, options.routingKey, options.envelope.messageId));
    };
    const onChannelError = (): void => {
      fail(new RabbitMqPublishError("channel-closed", options.exchange, options.routingKey, options.envelope.messageId));
    };

    timeout = setTimeout(() => {
      fail(new RabbitMqPublishError("confirm-timeout", options.exchange, options.routingKey, options.envelope.messageId));
    }, options.confirmTimeoutMs);

    channel.on("return", onReturn);
    channel.on("close", onClose);
    channel.on("error", onChannelError);

    channel.publish(options.exchange, options.routingKey, content, publishOptions(options.envelope, options.retryJitterMs), (error) => {
      if (error !== null) {
        fail(new RabbitMqPublishError("confirm-nack", options.exchange, options.routingKey, options.envelope.messageId));
        return;
      }

      if (settled) {
        return;
      }

      cleanup();
      resolve();
    });
  });
}

function publishOptions(envelope: WorkerMessageEnvelope, retryJitterMs: number | undefined): Options.Publish {
  return {
    persistent: true,
    mandatory: true,
    contentType: WORKER_DELIVERY_BEHAVIOR.contentType,
    contentEncoding: WORKER_DELIVERY_BEHAVIOR.contentEncoding,
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    timestamp: Math.floor(Date.parse(envelope.occurredAt) / 1_000),
    headers: {
      schemaId: envelope.schemaId,
      schemaVersion: envelope.schemaVersion,
      route: envelope.route,
      attemptCount: envelope.attempt.count,
      idempotencyKey: envelope.idempotencyKey,
      traceparent: envelope.traceparent,
      ...(retryJitterMs === undefined
        ? {}
        : {
            retryJitterMs
          })
    }
  };
}

function quorumQueueOptions(extraArguments: Readonly<Record<string, unknown>> = {}): Options.AssertQueue {
  return {
    durable: true,
    arguments: {
      "x-queue-type": WORKER_QUEUE_TYPE,
      ...extraArguments
    }
  };
}

function buildConnectionUrl(options: RabbitMqConnectionOptions): string | Options.Connect {
  if (typeof options.url !== "string") {
    if (options.heartbeatSeconds === undefined) {
      return options.url;
    }

    return {
      ...options.url,
      heartbeat: options.heartbeatSeconds
    };
  }

  if (options.heartbeatSeconds === undefined) {
    return options.url;
  }

  const url = new URL(options.url);
  url.searchParams.set("heartbeat", String(options.heartbeatSeconds));

  return url.toString();
}

function buildSocketOptions(options: RabbitMqConnectionOptions): RabbitMqSocketOptions | undefined {
  const socketOptions = {
    ...options.tls,
    ...options.socketOptions
  };

  if (Object.keys(socketOptions).length === 0) {
    return undefined;
  }

  return socketOptions;
}

function decodeEnvelope(message: RabbitMqConsumeMessage): WorkerMessageEnvelope {
  const parsed = JSON.parse(message.content.toString("utf8")) as unknown;

  if (!isRecord(parsed)) {
    throw new Error("RabbitMQ message body must be a JSON envelope object.");
  }

  return parsed as unknown as WorkerMessageEnvelope;
}

function consumerErrorContext(
  stage: WorkerStage,
  message: RabbitMqConsumeMessage
): RabbitMqConsumerErrorContext {
  if (message.properties.messageId === undefined) {
    return {
      stage,
      routingKey: message.fields.routingKey
    };
  }

  return {
    stage,
    messageId: message.properties.messageId,
    routingKey: message.fields.routingKey
  };
}

function isReturnedMessageFor(value: unknown, messageId: string): boolean {
  if (!isRecord(value)) {
    return false;
  }

  const properties = value["properties"];

  if (!isRecord(properties)) {
    return false;
  }

  return properties["messageId"] === messageId;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function defaultSleep(delayMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
}
