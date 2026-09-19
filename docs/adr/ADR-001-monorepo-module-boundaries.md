# ADR-001: Monorepo module boundaries

## Status

Accepted

## Context

SentinelAegisForge has two planned runtime concerns with different languages and lifecycles: stream processing on Scala/JVM and model lifecycle control in Python. The project is at its engineering-foundation phase, and coordinated changes, shared architectural decisions, and simple local validation are more important than independent repository administration.

## Decision

Maintain one monorepo with two explicit, independently buildable modules:

- `streaming-engine`
- `model-control-plane`

Shared documentation and repository-level verification remain at the root. Each module owns its language-specific build and dependencies, and neither module imports implementation code from the other.

## Alternatives considered

- Separate repositories for the Scala and Python modules.
- A single mixed-language build system controlling both modules internally.

## Trade-offs

A monorepo makes cross-module changes, architecture review, and one-command verification straightforward. It also couples repository access and may eventually create broader CI scope. Independent module builds retain useful boundaries without adding early multi-repository coordination overhead.

## Consequences

Changes affecting both modules can be reviewed atomically, while each module remains testable on its own. If release cadence, ownership, access control, or repository scale later justify a split, that change will require a new ADR.
