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

- `streaming-engine`: JDK 21 LTS with Scala 2.13.18 and sbt 2.0.9.
- `model-control-plane`: Python 3.13 with uv.

## Accepted ADRs

- [ADR-001: Monorepo module boundaries](docs/adr/ADR-001-monorepo-module-boundaries.md)
- [ADR-002: Development toolchains](docs/adr/ADR-002-development-toolchains.md)

## Verification Status

### Previous foundation verification

Before the runtime-baseline correction, the Python foundation was verified using CPython 3.12.14. `uv lock --check`, `uv sync --locked --group dev`, pytest, `ruff check`, `ruff format --check`, mypy, and `git diff --check` passed; the single Python foundation smoke test passed.

Previous Scala verification did not execute because local `sbt` was unavailable. This was a missing prerequisite, not a Scala build or test failure.

### Runtime-baseline migration verification

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

The Scala and JDK blockers above describe the earlier runtime-baseline migration environment and were resolved before the final Phase 0 verification.

### Final Phase 0 runtime/build verification

- The project shell used Temurin JDK 21.0.12.1 from the repository's SDKMAN selection.
- The installed sbt runner reported version 2.0.9; the project now independently pins `sbt.version=2.0.9`.
- The original sbt-scalafmt 2.5.5 plugin had no published sbt 2 artifact. It was narrowly upgraded to sbt-scalafmt 2.6.2, which successfully resolved and loaded under sbt 2.0.9.
- The existing build syntax, Scala 2.13.18, ScalaTest 3.2.19, and Java 21 compiler options loaded successfully under sbt 2.0.9.
- `sbt clean` and `sbt compile`: passed under JDK 21 and sbt 2.0.9. The production source directory is intentionally empty in Phase 0.
- `sbt test`: passed under JDK 21 and sbt 2.0.9; 1 Scala foundation smoke test passed.
- `sbt scalafmtCheckAll`: passed for 1 Scala source.
- The compiled Scala smoke-test class has class-file major version 65, confirming the Java 21 bytecode target.
- `uv python find 3.13`, `uv lock --check`, and `uv sync --locked --group dev`: passed.
- `uv run --locked python --version`: reported Python 3.13.9.
- `uv run --locked pytest`: passed; 1 Python foundation smoke test passed.
- `uv run --locked ruff check .`, `uv run --locked ruff format --check .`, and `uv run --locked mypy src tests`: passed.
- `make verify-scala`, `make verify-python`, and `make verify`: passed. The Makefile and CI use sbt 2's required quoted, semicolon-separated multi-command syntax.
- `git diff --check`: passed after the sbt 2 migration and final verification updates.

## Known Technical Debt

- The temporary Scala and Python foundation smoke tests should be removed once substantive tests provide equivalent build-wiring coverage.

## Known Failures

No current Phase 0 build or verification failures are known. During the sbt 2 migration, the first compatibility probe correctly failed because sbt-scalafmt 2.5.5 had no sbt 2 artifact; upgrading the plugin to 2.6.2 resolved that incompatibility.

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
