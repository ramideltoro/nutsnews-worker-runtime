# Package Access

Package: `@ramideltoro/nutsnews-worker-runtime`

Consumers must install exact immutable versions from GitHub Packages:

```sh
npm install @ramideltoro/nutsnews-worker-runtime@0.2.0
```

CI consumers use the owner-scoped npm registry and a short-lived `GITHUB_TOKEN`:

```ini
@ramideltoro:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

Source checkout and long-lived package tokens are not part of the supported access path.
