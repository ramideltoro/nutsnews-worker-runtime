# Package Integrity

Consumers install exact immutable versions from GitHub Packages:

```sh
npm install @ramideltoro/nutsnews-worker-runtime@0.3.0
```

CI consumers use the owner-scoped npm registry and repository `GITHUB_TOKEN` package-read permissions:

```ini
@ramideltoro:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

## Verification

Before rollout, consumers verify the exact version and integrity:

```sh
npm view @ramideltoro/nutsnews-worker-runtime@0.3.0 version --registry=https://npm.pkg.github.com
npm view @ramideltoro/nutsnews-worker-runtime@0.3.0 dist.integrity --registry=https://npm.pkg.github.com
```

Release workflow evidence must include:

- the merged commit SHA;
- the GitHub release tag matching `package.json`;
- the publish workflow run;
- the artifact attestation for the packed tarball;
- the package install smoke workflow run for the exact version.

## Rollback

Consumers roll back by restoring the previous exact version in `package.json` and the service lockfile. Runtime package deletion or republishing is not part of normal rollback.
