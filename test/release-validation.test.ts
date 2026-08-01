import { describe, expect, it } from "vitest";

import {
  isExactGitHubPackagesPackageResolution,
  releaseRequiresMigrationNotes
} from "../src/release-validation.js";

const PACKAGE_NAME = "@ramideltoro/nutsnews-worker-contracts";
const VERSION = "1.0.0";
const ARTIFACT = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const EXPECTED_URL = `https://npm.pkg.github.com/download/${PACKAGE_NAME}/${VERSION}/${ARTIFACT}`;

describe("runtime release validation", () => {
  it("accepts only the exact GitHub Packages origin and package/version path", () => {
    expect(isExactGitHubPackagesPackageResolution(EXPECTED_URL, PACKAGE_NAME, VERSION)).toBe(true);

    for (const candidate of [
      `https://example.com/download/${PACKAGE_NAME}/${VERSION}/${ARTIFACT}`,
      `https://npm.pkg.github.com/download/@ramideltoro/other/${VERSION}/${ARTIFACT}`,
      `https://npm.pkg.github.com/download/${PACKAGE_NAME}/1.0.1/${ARTIFACT}`,
      `https://npm.pkg.github.com/download/${PACKAGE_NAME}/${VERSION}/${ARTIFACT}/extra`,
      `${EXPECTED_URL}?redirect=/1.0.0/`,
      `https://npm.pkg.github.com.evil.example/download/${PACKAGE_NAME}/${VERSION}/${ARTIFACT}`
    ]) {
      expect(isExactGitHubPackagesPackageResolution(candidate, PACKAGE_NAME, VERSION)).toBe(false);
    }
  });

  it("requires migration notes only for major transitions or explicit breaking releases", () => {
    expect(releaseRequiresMigrationNotes("1.0.0", "0.5.0")).toBe(true);
    expect(releaseRequiresMigrationNotes("1.0.1", "1.0.0")).toBe(false);
    expect(releaseRequiresMigrationNotes("1.1.0", "1.0.1")).toBe(false);
    expect(releaseRequiresMigrationNotes("2.0.0", "1.9.9")).toBe(true);
    expect(releaseRequiresMigrationNotes("1.1.0", "1.0.1", true)).toBe(true);
    expect(releaseRequiresMigrationNotes("0.1.0", undefined)).toBe(false);
    expect(() => releaseRequiresMigrationNotes("1.x.0", "1.0.0")).toThrow(/invalid stable semantic version/u);
  });
});
