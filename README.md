# nutsnews-worker-runtime

Worker-uplift repository for NutsNews.

## Responsibility

Own shared worker runtime concerns such as RabbitMQ connection helpers, retry policy helpers, structured logging, tracing, metrics adapters, and process lifecycle behavior.

## Owner

@ramideltoro

## Deployable / Package Type

GitHub Packages npm package: `@ramideltoro/nutsnews-worker-runtime`. This repository is not deployable.

## Support Boundary

This repository owns its package or service implementation, CI, package or image publishing workflow, and service-local operational notes. It does not own the backend host, production deployment secrets, Grafana Cloud resources, or cross-system explanatory documentation.

## Production Boundary

`ramideltoro/nutsnews-backend` owns backend-host runtime and deployments. `production-backend` in that repository remains the runtime secret and deployment boundary. No production secret belongs in this repository.

`ramideltoro/nutsnews-infra` owns Grafana Cloud resources. `ramideltoro/nutsnews-docs` owns explanatory architecture and operations documentation.

## Package / Image Access

Downstream builds consume this package through GitHub Packages with an exact immutable version, for example `@ramideltoro/nutsnews-worker-runtime@0.0.0`, using a short-lived `GITHUB_TOKEN` and the owner-scoped npm registry.

No long-lived GitHub Packages token is required for CI. Workflows use least-privilege permissions and request `packages: write` only for publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.
