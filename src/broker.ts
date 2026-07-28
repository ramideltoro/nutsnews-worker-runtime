import {
  WORKER_DELIVERY_BEHAVIOR,
  WORKER_ROUTES,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";

import type {
  RuntimeMessageDelivery,
  RuntimeMessageProcessingResult
} from "./message.js";
import {
  SYSTEM_RUNTIME_CLOCK,
  emitRuntimeTelemetry,
  runtimeNow,
  type RuntimeClock,
  type RuntimeTelemetrySink
} from "./telemetry.js";

export type BrokerLifecycleState = "idle" | "connecting" | "ready" | "closing" | "closed" | "failed";
export type BrokerConsumerState =
  | "inactive"
  | "active"
  | "cancelled"
  | "channel-dropped"
  | "recovering"
  | "closed";

export interface BrokerPublishCommand {
  readonly envelope: WorkerMessageEnvelope;
  readonly payload: Readonly<Record<string, unknown>>;
}

export interface BrokerPublishReceipt {
  readonly messageId: string;
  readonly stage: WorkerStage;
  readonly exchange: string;
  readonly routingKey: string;
  readonly confirmed: true;
  readonly confirmedAt: string;
}

export type BrokerDeliveryHandler = (
  delivery: RuntimeMessageDelivery
) => Promise<RuntimeMessageProcessingResult>;

export interface BrokerConsumerHandle {
  readonly stage: WorkerStage;
  cancel(): Promise<void>;
}

export interface BrokerConsumerStatus {
  readonly stage: WorkerStage;
  readonly queue: string;
  readonly state: BrokerConsumerState;
  readonly activeConsumers: 0 | 1;
  readonly reason: string;
  readonly changedAt: string;
}

export interface BrokerConsumerMonitor {
  readonly status: BrokerConsumerStatus;
  markActive(reason?: string): void;
  markCancelled(reason?: string): void;
  markChannelDropped(reason?: string): void;
  markRecovering(reason?: string): void;
  markClosed(reason?: string): void;
}

export interface BrokerConsumerMonitorOptions {
  readonly stage: WorkerStage;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly clock?: RuntimeClock;
}

export interface RuntimeBrokerTransport {
  readonly name: string;
  readonly inFlightDeliveryCount?: number;
  consumerStatus?(stage: WorkerStage): BrokerConsumerStatus | undefined;
  connect(): Promise<void>;
  assertTopology(routes: readonly WorkerRoute[]): Promise<void>;
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle>;
  drain?(timeoutMs?: number): Promise<void>;
  close(): Promise<void>;
}

export interface BrokerLifecycleOptions {
  readonly transport: RuntimeBrokerTransport;
  readonly routes?: readonly WorkerRoute[];
  readonly telemetry?: RuntimeTelemetrySink;
  readonly clock?: RuntimeClock;
}

export interface BrokerLifecycle {
  readonly state: BrokerLifecycleState;
  readonly routes: readonly WorkerRoute[];
  consumerStatus(stage: WorkerStage): BrokerConsumerStatus;
  start(): Promise<void>;
  stop(reason?: string): Promise<void>;
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle>;
}

export function createBrokerLifecycle(options: BrokerLifecycleOptions): BrokerLifecycle {
  return new ManagedBrokerLifecycle(options);
}

export function createBrokerConsumerMonitor(
  options: BrokerConsumerMonitorOptions
): BrokerConsumerMonitor {
  return new RuntimeBrokerConsumerMonitor(options);
}

class ManagedBrokerLifecycle implements BrokerLifecycle {
  readonly routes: readonly WorkerRoute[];
  private readonly transport: RuntimeBrokerTransport;
  private readonly telemetry: RuntimeTelemetrySink | undefined;
  private readonly clock: RuntimeClock;
  private readonly consumerHandles = new Map<WorkerStage, BrokerConsumerHandle>();
  private stateValue: BrokerLifecycleState = "idle";

  constructor(options: BrokerLifecycleOptions) {
    this.transport = options.transport;
    this.routes = options.routes ?? WORKER_ROUTES;
    this.telemetry = options.telemetry;
    this.clock = options.clock ?? SYSTEM_RUNTIME_CLOCK;
  }

  get state(): BrokerLifecycleState {
    return this.stateValue;
  }

  consumerStatus(stage: WorkerStage): BrokerConsumerStatus {
    const transportStatus = this.transport.consumerStatus?.(stage);

    if (transportStatus !== undefined) {
      return transportStatus;
    }

    return {
      stage,
      queue: getWorkerRoute(stage).mainQueue.name,
      state: this.consumerHandles.has(stage) ? "active" : "inactive",
      activeConsumers: this.consumerHandles.has(stage) ? 1 : 0,
      reason: "transport-status-unavailable",
      changedAt: runtimeNow(this.clock)
    };
  }

  async start(): Promise<void> {
    if (this.stateValue === "ready") {
      return;
    }

    if (this.stateValue === "closing") {
      throw new Error("Broker lifecycle is closing.");
    }

    await this.transition("connecting", "start");

    try {
      await this.transport.connect();
      await this.transport.assertTopology(this.routes);
      await emitRuntimeTelemetry(this.telemetry, {
        name: "runtime.broker.topology_asserted",
        level: "info",
        at: runtimeNow(this.clock),
        attributes: {
          transport: this.transport.name,
          routeCount: this.routes.length,
          prefetch: WORKER_DELIVERY_BEHAVIOR.prefetch
        }
      });
      await this.transition("ready", "connected");
    } catch (error: unknown) {
      await this.transition("failed", classifyLifecycleError(error));
      throw error;
    }
  }

  async stop(reason = "shutdown"): Promise<void> {
    if (this.stateValue === "closed" || this.stateValue === "idle") {
      await this.transition("closed", reason);
      return;
    }

    if (this.stateValue === "closing") {
      return;
    }

    await this.transition("closing", reason);

    for (const handle of this.consumerHandles.values()) {
      await handle.cancel();
    }
    this.consumerHandles.clear();

    await this.transport.drain?.();
    await this.transport.close();
    await this.transition("closed", reason);
  }

  async publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    this.assertReady();

    const route = getWorkerRoute(command.envelope.route);
    const receipt = await this.transport.publish(command);

    if (receipt.exchange !== route.exchange || receipt.routingKey !== route.routingKey) {
      throw new Error("Broker transport returned a publish receipt for the wrong route.");
    }

    return receipt;
  }

  async consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    this.assertReady();

    const handle = await this.transport.consume(stage, handler);
    this.consumerHandles.set(stage, handle);

    return {
      stage: handle.stage,
      cancel: async (): Promise<void> => {
        await handle.cancel();

        if (this.consumerHandles.get(stage) === handle) {
          this.consumerHandles.delete(stage);
        }
      }
    };
  }

  private assertReady(): void {
    if (this.stateValue !== "ready") {
      throw new Error(`Broker lifecycle must be ready before use; current state is ${this.stateValue}.`);
    }
  }

  private async transition(next: BrokerLifecycleState, reason: string): Promise<void> {
    const previous = this.stateValue;
    this.stateValue = next;

    await emitRuntimeTelemetry(this.telemetry, {
      name: "runtime.broker.state_changed",
      level: next === "failed" ? "error" : "info",
      at: runtimeNow(this.clock),
      attributes: {
        transport: this.transport.name,
        previousState: previous,
        state: next,
        reason
      }
    });
  }
}

class RuntimeBrokerConsumerMonitor implements BrokerConsumerMonitor {
  private readonly stage: WorkerStage;
  private readonly queue: string;
  private readonly telemetry: RuntimeTelemetrySink | undefined;
  private readonly clock: RuntimeClock;
  private statusValue: BrokerConsumerStatus;

  constructor(options: BrokerConsumerMonitorOptions) {
    this.stage = options.stage;
    this.queue = getWorkerRoute(options.stage).mainQueue.name;
    this.telemetry = options.telemetry;
    this.clock = options.clock ?? SYSTEM_RUNTIME_CLOCK;
    this.statusValue = {
      stage: this.stage,
      queue: this.queue,
      state: "inactive",
      activeConsumers: 0,
      reason: "not-started",
      changedAt: runtimeNow(this.clock)
    };
  }

  get status(): BrokerConsumerStatus {
    return this.statusValue;
  }

  markActive(reason = "consume-confirmed"): void {
    this.transition("active", reason);
  }

  markCancelled(reason = "broker-cancelled"): void {
    this.transition("cancelled", reason);
  }

  markChannelDropped(reason = "channel-dropped"): void {
    this.transition("channel-dropped", reason);
  }

  markRecovering(reason = "consumer-reconnect"): void {
    this.transition("recovering", reason);
  }

  markClosed(reason = "consumer-closed"): void {
    this.transition("closed", reason);
  }

  private transition(state: BrokerConsumerState, reason: string): void {
    const previousState = this.statusValue.state;

    if (previousState === state && this.statusValue.reason === reason) {
      return;
    }

    const activeConsumers = state === "active" ? 1 : 0;
    const changedAt = runtimeNow(this.clock);
    this.statusValue = {
      stage: this.stage,
      queue: this.queue,
      state,
      activeConsumers,
      reason,
      changedAt
    };

    void emitRuntimeTelemetry(this.telemetry, {
      name: "runtime.broker.consumer_state_changed",
      level: state === "active" || state === "closed"
        ? "info"
        : state === "recovering"
          ? "warn"
          : "error",
      at: changedAt,
      stage: this.stage,
      queue: this.queue,
      outcome: state,
      attributes: {
        transport: "rabbitmq",
        previousState,
        state,
        reason,
        activeConsumers
      }
    }).catch(() => undefined);
  }
}

function classifyLifecycleError(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown-broker-error";
}
