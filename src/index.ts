export * from "./broker.js";
export * from "./failure.js";
export * from "./health.js";
export * from "./idempotency.js";
export * from "./message.js";
export * from "./processing.js";
export * from "./rabbitmq.js";
export * from "./retry.js";
export * from "./shutdown.js";
export * from "./telemetry.js";
export * from "./trace.js";

export const RUNTIME_PACKAGE_NAME = "@ramideltoro/nutsnews-worker-runtime" as const;
export const RUNTIME_PACKAGE_VERSION = "0.5.0" as const;
export const RUNTIME_PACKAGE_STATUS = "consumer-readiness-telemetry-primitives" as const;
export const RUNTIME_CONTRACTS_PACKAGE = "@ramideltoro/nutsnews-worker-contracts" as const;
export const RUNTIME_CONTRACTS_PACKAGE_VERSION = "0.3.1" as const;

export const RUNTIME_COMPATIBILITY_POLICY = {
  semver:
    "Major versions may change public runtime APIs; minor versions add backward-compatible helpers; patch versions fix implementation, generated types, or documentation without changing public API shapes.",
  consumerInstall:
    "Consumers install exact immutable GitHub Packages versions and upgrade only after service-local runtime tests pass.",
  dependencyBoundary:
    "The runtime package may depend only on the contracts package and narrow broker client libraries; deployable services provide business logic, idempotency storage, telemetry sinks, and shutdown adapters.",
  rollback:
    "Consumers roll back by restoring the previously pinned runtime package version; published versions are not republished in place."
} as const;

export const AVAILABLE_RUNTIME_MODULES = [
  "broker-lifecycle",
  "message-handling",
  "rabbitmq-client",
  "retry-dlq-idempotency-middleware",
  "idempotency-hooks",
  "telemetry",
  "prometheus-metrics",
  "health",
  "shutdown",
  "trace-context"
] as const;

export type RuntimePackageStatus = typeof RUNTIME_PACKAGE_STATUS;
export type AvailableRuntimeModule = (typeof AVAILABLE_RUNTIME_MODULES)[number];

export interface RuntimePackageMetadata {
  readonly packageName: typeof RUNTIME_PACKAGE_NAME;
  readonly packageVersion: typeof RUNTIME_PACKAGE_VERSION;
  readonly status: RuntimePackageStatus;
  readonly contractsPackageName: typeof RUNTIME_CONTRACTS_PACKAGE;
  readonly contractsPackageVersion: typeof RUNTIME_CONTRACTS_PACKAGE_VERSION;
  readonly availableRuntimeModules: readonly AvailableRuntimeModule[];
  readonly compatibilityPolicy: typeof RUNTIME_COMPATIBILITY_POLICY;
}

export function getRuntimePackageMetadata(): RuntimePackageMetadata {
  return {
    packageName: RUNTIME_PACKAGE_NAME,
    packageVersion: RUNTIME_PACKAGE_VERSION,
    status: RUNTIME_PACKAGE_STATUS,
    contractsPackageName: RUNTIME_CONTRACTS_PACKAGE,
    contractsPackageVersion: RUNTIME_CONTRACTS_PACKAGE_VERSION,
    availableRuntimeModules: AVAILABLE_RUNTIME_MODULES,
    compatibilityPolicy: RUNTIME_COMPATIBILITY_POLICY
  };
}
