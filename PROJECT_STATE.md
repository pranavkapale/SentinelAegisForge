# Project state

## Current Phase

Phase 0 — Engineering Foundation

The Phase 0 foundation is functionally complete. This state includes the narrow runtime-baseline correction to JDK 21 and Python 3.13; no Phase 1 functionality has begun.

## Implemented Capabilities

- Independently buildable Scala module foundation with pinned Scala and sbt versions.
- Independently buildable Python package foundation with a pinned Python range and locked development dependencies.
- Formatting, unit-test, lint, and static-analysis configuration.
- Root verification targets and baseline GitHub Actions CI.
- Architecture overview and lightweight ADR process.

No streaming, fraud decisioning, persistence, model lifecycle, or other runtime capability is implemented.

## Current Architecture

- `streaming-engine`: Scala/JVM build and temporary foundation smoke test only.
- `model-control-plane`: Python package and temporary foundation import test only.
- `docs`: shared architecture overview and accepted ADRs.
- Repository root: shared verification, hygiene, CI, and project-state metadata.

The modules have no runtime integration.

## Runtime Baseline

- `streaming-engine`: JDK 21 LTS with Scala 2.13.18 and sbt 1.13.0.
- `model-control-plane`: Python 3.13 with uv.

## Accepted ADRs

- [ADR-001: Monorepo module boundaries](docs/adr/ADR-001-monorepo-module-boundaries.md)
- [ADR-002: Development toolchains](docs/adr/ADR-002-development-toolchains.md)

## Verification Status

### Previous foundation verification

Before the runtime-baseline correction, the Python foundation was verified using CPython 3.12.14. `uv lock --check`, `uv sync --locked --group dev`, pytest, `ruff check`, `ruff format --check`, mypy, and `git diff --check` passed; the single Python foundation smoke test passed.

Previous Scala verification did not execute because local `sbt` was unavailable. This was a missing prerequisite, not a Scala build or test failure.

### Current post-migration verification

- `uv python find 3.13`: passed and resolved `/Library/Frameworks/Python.framework/Versions/3.13/bin/python3.13`.
- `uv lock --check`: passed using CPython 3.13.9; 12 packages resolved.
- `uv sync --locked --group dev`: passed using CPython 3.13.9.
- `uv run --locked python --version`: passed and reported Python 3.13.9.
- `uv run --locked pytest`: passed; 1 foundation smoke test passed.
- `uv run --locked ruff check .`: passed.
- `uv run --locked ruff format --check .`: passed; 2 files were already formatted.
- `uv run --locked mypy src tests`: passed with no issues in 2 source files.
- `make verify-python`: passed under Python 3.13.9.
- `sbt compile`, `sbt test`, and `sbt scalafmtCheckAll`: attempted but blocked before execution because local `sbt` is unavailable.
- `make verify-scala`: blocked because local `sbt` is unavailable.
- `make verify`: stopped at `verify-scala` because local `sbt` is unavailable; `verify-python` passed separately.
- JDK 21 verification is blocked because JDK 21 is not installed or selectable locally. The installed JDKs are versions 25 and 26, and neither is accepted as proof of the JDK 21 baseline.
- `git diff --check`: passed after the runtime-baseline correction.

## Known Technical Debt

- The temporary Scala and Python foundation smoke tests should be removed once substantive tests provide equivalent build-wiring coverage.

## Known Failures

- Local Scala verification is blocked because `sbt` is not installed (`command not found`, exit 127).
- JDK 21 is not installed or selectable locally. The active Java runtime is version 25, and additional installed JDKs are version 26; CI is configured to use JDK 21.

## Deferred Decisions

- Transaction event contract.
- Serialization and schema strategy.
- Kafka topology.
- Kafka partition key.
- Spark version and runtime dependencies.
- Delta write and idempotency strategy.
- Streaming state design.
- ML runtime contract.

## Next Planned Capability

Define and evaluate the transaction event contract in a later phase. No contract work is included in Phase 0.

The next functional milestone remains deferred; this runtime-baseline correction does not begin Phase 1.
