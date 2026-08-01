const STABLE_SEMVER_RE = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/u;
const GITHUB_PACKAGES_ORIGIN = "https://npm.pkg.github.com";
const GITHUB_PACKAGES_ARTIFACT_RE = /^[A-Fa-f0-9]{40,128}$/u;
const PACKAGE_NAME_RE = /^@[a-z0-9._-]+\/[a-z0-9._-]+$/u;

export function isExactGitHubPackagesPackageResolution(
  resolved: unknown,
  packageName: string,
  version: string
): boolean {
  if (
    typeof resolved !== "string"
    || !PACKAGE_NAME_RE.test(packageName)
    || !STABLE_SEMVER_RE.test(version)
  ) {
    return false;
  }

  let url: URL;
  try {
    url = new URL(resolved);
  } catch {
    return false;
  }

  if (
    url.origin !== GITHUB_PACKAGES_ORIGIN
    || url.username.length > 0
    || url.password.length > 0
    || url.search.length > 0
    || url.hash.length > 0
  ) {
    return false;
  }

  const expectedPrefix = `/download/${packageName}/${version}/`;
  if (!url.pathname.startsWith(expectedPrefix)) {
    return false;
  }

  const artifact = url.pathname.slice(expectedPrefix.length);
  return GITHUB_PACKAGES_ARTIFACT_RE.test(artifact);
}

export function releaseRequiresMigrationNotes(
  currentVersion: string,
  previousVersion: string | undefined,
  explicitlyBreaking = false
): boolean {
  const current = parseStableSemver(currentVersion);

  if (explicitlyBreaking) {
    return true;
  }

  if (previousVersion === undefined) {
    return false;
  }

  const previous = parseStableSemver(previousVersion);
  return current.major > previous.major;
}

function parseStableSemver(version: string): Readonly<{
  major: number;
  minor: number;
  patch: number;
}> {
  const match = STABLE_SEMVER_RE.exec(version);
  if (match === null) {
    throw new TypeError(`invalid stable semantic version: ${version}`);
  }

  return {
    major: Number.parseInt(match[1]!, 10),
    minor: Number.parseInt(match[2]!, 10),
    patch: Number.parseInt(match[3]!, 10)
  };
}
