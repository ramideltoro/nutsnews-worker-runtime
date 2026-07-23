import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { extname, join, relative, sep } from "node:path";

const root = new URL("..", import.meta.url).pathname;
const packageJson = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
const publicApi = readFileSync(join(root, "src", "index.ts"), "utf8");

const contractsPackageName = "@ramideltoro/nutsnews-worker-contracts";
const contractsPackageVersion = "0.3.1";
const contractsPackageSpec = "https://github.com/ramideltoro/nutsnews-worker-contracts/releases/download/v0.3.1/ramideltoro-nutsnews-worker-contracts-0.3.1.tgz";
const amqplibPackageName = "amqplib";
const amqplibPackageVersion = "2.0.1";
const allowedSourceImports = new Set([
  contractsPackageName,
  amqplibPackageName
]);
const requiredScripts = [
  "build",
  "typecheck",
  "lint",
  "test",
  "package:check",
  "ci"
];
const requiredFiles = [
  "dist",
  "README.md",
  "CHANGELOG.md",
  "LICENSE"
];
const requiredPackedRootFiles = [
  "CHANGELOG.md",
  "LICENSE",
  "README.md",
  "package.json"
];
const allowedPackedRootFiles = new Set(requiredPackedRootFiles);
const forbiddenSourcePatterns = [
  /SUPABASE_SERVICE_ROLE_KEY/u,
  /NUTSNEWS_BACKEND_API_TOKEN/u,
  /GRAFANA_[A-Z0-9_]+/u,
  /(AMQP|BROKER|RABBITMQ)_(URL|URI|USERNAME|PASSWORD|SECRET|TOKEN)/u,
  /RABBITMQ_[A-Z0-9_]*(PASSWORD|SECRET|TOKEN)/u,
  /CLOUDFLARE_API_TOKEN/u,
  /from\s+["']@ramideltoro\/nutsnews-(backend|worker-fetcher|worker-canonicalizer|worker-enrichment|worker-approval|worker-translation|worker-persistence|worker-publication)["']/u,
  /from\s+["']@ramideltoro\/nutsnews-worker["']/u,
  /from\s+["'][./]*worker\/src/u,
  /legacy-worker/u
];

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function walkFiles(directory) {
  const entries = readdirSync(directory, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const path = join(directory, entry.name);
    const localPath = relative(root, path);

    if (
      entry.isDirectory() &&
      [
        ".git",
        "coverage",
        "dist",
        "node_modules"
      ].includes(entry.name)
    ) {
      continue;
    }

    if (entry.isDirectory()) {
      files.push(...walkFiles(path));
      continue;
    }

    if (entry.isFile()) {
      files.push(localPath.split(sep).join("/"));
    }
  }

  return files;
}

function assertPackageMetadata() {
  assert(packageJson.name === "@ramideltoro/nutsnews-worker-runtime", "unexpected package name");
  assert(/^\d+\.\d+\.\d+$/u.test(packageJson.version), "package version must be semver without a prerelease");
  assert(packageJson.private !== true, "package must be publishable");
  assert(packageJson.type === "module", "package must use ESM");
  assert(packageJson.sideEffects === false, "package must declare sideEffects false");
  assert(packageJson.main === "./dist/index.js", "unexpected main entry");
  assert(packageJson.types === "./dist/index.d.ts", "unexpected types entry");
  assert(packageJson.publishConfig?.registry === "https://npm.pkg.github.com", "publish registry must be GitHub Packages");
  assert(packageJson.publishConfig?.access === "public", "publish access must be public");

  const dependencies = packageJson.dependencies ?? {};
  assert(Object.keys(dependencies).length === 2, "runtime package may depend only on the contracts package and approved broker client");
  assert(dependencies[contractsPackageName] === contractsPackageSpec, "contracts dependency must resolve to the approved exact release artifact");
  assert(dependencies[amqplibPackageName] === amqplibPackageVersion, "amqplib dependency must be pinned to the approved exact version");

  for (const script of requiredScripts) {
    assert(typeof packageJson.scripts?.[script] === "string", `missing npm script: ${script}`);
  }

  for (const file of requiredFiles) {
    assert(packageJson.files?.includes(file), `package files must include ${file}`);
  }
}

function assertInstalledContractsPackage() {
  const installedManifest = JSON.parse(readFileSync(join(root, "node_modules", "@ramideltoro", "nutsnews-worker-contracts", "package.json"), "utf8"));

  assert(installedManifest.name === contractsPackageName, "installed contracts package has the wrong name");
  assert(installedManifest.version === contractsPackageVersion, "installed contracts package has the wrong version");
}

function assertInstalledBrokerPackage() {
  const installedManifest = JSON.parse(readFileSync(join(root, "node_modules", "amqplib", "package.json"), "utf8"));

  assert(installedManifest.name === amqplibPackageName, "installed broker package has the wrong name");
  assert(installedManifest.version === amqplibPackageVersion, "installed broker package has the wrong version");
}

function assertVersionExportMatchesManifest() {
  assert(
    publicApi.includes(`RUNTIME_PACKAGE_VERSION = "${packageJson.version}"`),
    "RUNTIME_PACKAGE_VERSION must match package.json"
  );
}

function assertNoForbiddenSourceReferences() {
  const sourceFiles = walkFiles(root).filter((file) => [
    ".js",
    ".json",
    ".md",
    ".mjs",
    ".ts",
    ".yaml",
    ".yml"
  ].includes(extname(file)) && file !== "scripts/validate-package.mjs");

  for (const file of sourceFiles) {
    const contents = readFileSync(join(root, file), "utf8");

    for (const pattern of forbiddenSourcePatterns) {
      assert(!pattern.test(contents), `forbidden source reference matched ${pattern} in ${file}`);
    }
  }
}

function assertSourceImportsStayInBoundary() {
  const sourceFiles = walkFiles(join(root, "src")).filter((file) => file.endsWith(".ts"));
  const importPattern = /\bfrom\s+["']([^"']+)["']|import\s*\(\s*["']([^"']+)["']\s*\)/gu;

  for (const file of sourceFiles) {
    const contents = readFileSync(join(root, file), "utf8");
    const matches = contents.matchAll(importPattern);

    for (const match of matches) {
      const specifier = match[1] ?? match[2] ?? "";

      if (specifier.startsWith(".") || specifier.startsWith("node:") || allowedSourceImports.has(specifier)) {
        continue;
      }

      throw new Error(`unsupported runtime source import ${specifier} in ${file}`);
    }
  }
}

function assertGeneratedEntriesExist() {
  for (const file of expectedBuildOutputFiles()) {
    assert(statSync(join(root, file)).isFile(), `build output missing ${file}`);
  }
}

function assertPackedContents() {
  const packOutput = execFileSync("npm", [
    "pack",
    "--json",
    "--dry-run"
  ], {
    cwd: root,
    encoding: "utf8",
    stdio: [
      "ignore",
      "pipe",
      "inherit"
    ]
  });
  const [packResult] = JSON.parse(packOutput);
  const files = packResult.files.map((entry) => entry.path).sort();
  const requiredPackedFiles = [
    ...requiredPackedRootFiles,
    ...expectedBuildOutputFiles()
  ].sort();

  for (const file of requiredPackedFiles) {
    assert(files.includes(file), `packed tarball is missing ${file}`);
  }

  for (const file of files) {
    assert(file.startsWith("dist/") || allowedPackedRootFiles.has(file), `unexpected packed file: ${file}`);
    assert(!file.startsWith(".github/"), "packed tarball must not include GitHub workflow files");
    assert(!file.startsWith("src/"), "packed tarball must not include TypeScript source");
    assert(!file.startsWith("test/"), "packed tarball must not include tests");
    assert(!file.includes(".env"), "packed tarball must not include env files");
  }
}

function expectedBuildOutputFiles() {
  return walkFiles(join(root, "src"))
    .filter((file) => file.startsWith("src/") && file.endsWith(".ts"))
    .flatMap((file) => {
      const relativeOutput = file.slice("src/".length, -".ts".length);

      return [
        `dist/${relativeOutput}.d.ts`,
        `dist/${relativeOutput}.d.ts.map`,
        `dist/${relativeOutput}.js`,
        `dist/${relativeOutput}.js.map`
      ];
    });
}

assertPackageMetadata();
assertInstalledContractsPackage();
assertInstalledBrokerPackage();
assertVersionExportMatchesManifest();
assertNoForbiddenSourceReferences();
assertSourceImportsStayInBoundary();
assertGeneratedEntriesExist();
assertPackedContents();

console.log("Package metadata and packed contents are valid.");
