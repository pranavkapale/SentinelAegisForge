.PHONY: verify verify-scala verify-python infra-up infra-down infra-status infra-reset verify-infra

verify: verify-scala verify-python

verify-scala:
	cd streaming-engine && sbt "scalafmtCheckAll ; compile ; test"

verify-python:
	cd model-control-plane && uv sync --locked --group dev
	cd model-control-plane && uv run --locked pytest
	cd model-control-plane && uv run --locked ruff check .
	cd model-control-plane && uv run --locked ruff format --check .
	cd model-control-plane && uv run --locked mypy src tests

infra-up:
	docker compose up -d --wait kafka
	docker compose run --rm kafka-init
	$(MAKE) verify-infra

infra-down:
	docker compose down

infra-status:
	docker compose ps

infra-reset:
	@echo "Removing local Kafka containers and persistent data volume."
	docker compose down --volumes --remove-orphans

verify-infra:
	bash scripts/verify-local-kafka.sh
