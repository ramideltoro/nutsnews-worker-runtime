import { describe, expect, it } from "vitest";

import {
  AVAILABLE_RUNTIME_MODULES,
  RUNTIME_COMPATIBILITY_POLICY,
  RUNTIME_CONTRACTS_PACKAGE,
  RUNTIME_CONTRACTS_PACKAGE_VERSION,
  RUNTIME_PACKAGE_NAME,
  RUNTIME_PACKAGE_STATUS,
  RUNTIME_PACKAGE_VERSION,
  getRuntimePackageMetadata
} from "../src/index.js";

describe("runtime package metadata", () => {
  it("exports the package identity used by downstream installers", () => {
    expect(RUNTIME_PACKAGE_NAME).toBe("@ramideltoro/nutsnews-worker-runtime");
    expect(RUNTIME_PACKAGE_VERSION).toBe("1.0.0");
    expect(RUNTIME_PACKAGE_STATUS).toBe("grafana-cloud-telemetry-primitives");
  });

  it("pins the contracts package and broker-client dependency boundary", () => {
    expect(RUNTIME_CONTRACTS_PACKAGE).toBe("@ramideltoro/nutsnews-worker-contracts");
    expect(RUNTIME_CONTRACTS_PACKAGE_VERSION).toBe("1.0.0");
    expect(RUNTIME_COMPATIBILITY_POLICY.dependencyBoundary).toContain("contracts package and narrow broker client libraries");
  });

  it("identifies available runtime modules", () => {
    expect(AVAILABLE_RUNTIME_MODULES).toEqual([
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
    ]);
  });

  it("returns a stable metadata object", () => {
    expect(getRuntimePackageMetadata()).toEqual({
      packageName: RUNTIME_PACKAGE_NAME,
      packageVersion: RUNTIME_PACKAGE_VERSION,
      status: RUNTIME_PACKAGE_STATUS,
      contractsPackageName: RUNTIME_CONTRACTS_PACKAGE,
      contractsPackageVersion: RUNTIME_CONTRACTS_PACKAGE_VERSION,
      availableRuntimeModules: AVAILABLE_RUNTIME_MODULES,
      compatibilityPolicy: RUNTIME_COMPATIBILITY_POLICY
    });
  });
});
