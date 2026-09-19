# ADR-002: Development toolchains

## Status

Accepted

## Context

Both module boundaries need deterministic, familiar build, test, formatting, and static-analysis workflows before runtime behavior is introduced.

## Decision

Use the following development toolchains.

Streaming engine:

- Scala 2.13.18
- JDK 21 LTS
- sbt 1.13.0
- ScalaTest
- Scalafmt

Model control plane:

- Python 3.13
- uv
- pytest
- Ruff
- mypy

Versions are pinned in module configuration or the Python lockfile where appropriate. Root `make` targets and CI expose the same checks.

JDK 21 is the selected modern LTS JVM baseline for the planned Spark 4.x streaming system. Python 3.13 is the current model control plane runtime baseline. Critical Python dependencies must be checked for Python 3.13 compatibility before adoption; a genuine future incompatibility requires a documented decision rather than a silent runtime downgrade.

Runtime and data technologies are intentionally deferred until a concrete requirement needs them and their operational implications can be evaluated.

## Alternatives considered

- A repository-wide polyglot build framework.
- Unpinned, globally managed language dependencies.
- Introducing prospective runtime frameworks during foundation setup.

## Trade-offs

Native build tools keep each module understandable and independently buildable, at the cost of developers installing both language toolchains. Pinning improves reproducibility but requires deliberate upgrades.

## Consequences

Contributors use sbt for Scala and uv for Python, with `make verify` as the common entry point. Kafka, Spark, Delta Lake, MLflow, and other runtime or data technologies are not selected or introduced by this decision.
