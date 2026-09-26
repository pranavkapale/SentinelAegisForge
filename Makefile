.PHONY: verify verify-scala verify-python infra-up infra-down infra-status infra-reset verify-infra produce-sample consume-sample ingest-delta inspect-delta reset-delta deduplicate-transactions inspect-deduplicated reset-deduplication

COUNT ?= 10
SEED ?= 42
BASE_TIME ?= 2026-09-21T00:00:00Z
BOOTSTRAP_SERVERS ?= localhost:9092
SCHEMA_REGISTRY_URL ?= http://localhost:8081
CHECKPOINT_DIR ?= $(CURDIR)/.local/checkpoints/transaction-ingestion
DELTA_PATH ?= $(CURDIR)/.local/delta/transactions_validated
DELTA_CHECKPOINT_DIR ?= $(CURDIR)/.local/checkpoints/delta-transaction-ingestion
DELTA_TXN_APP_ID ?= sentinel-transactions-validated-v1
DEDUPLICATED_DELTA_PATH ?= $(CURDIR)/.local/delta/transactions_deduplicated
DEDUP_CHECKPOINT_DIR ?= $(CURDIR)/.local/checkpoints/transaction-deduplication
DEDUP_DELTA_TXN_APP_ID ?= sentinel-transactions-deduplicated-v1
WATERMARK_DELAY ?= 10 minutes
FAIL_AFTER_DELTA_BATCH_ID ?=
STARTING_OFFSETS ?= earliest
SPARK_MASTER ?= local[*]

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
	docker compose up -d --wait kafka schema-registry
	docker compose run --rm kafka-init
	bash scripts/provision-schema-registry.sh
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

produce-sample:
	$(MAKE) verify-infra
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.kafka.producer.TransactionProducerApp --count=$(COUNT) --seed=$(SEED) --base-time=$(BASE_TIME) --bootstrap-servers=$(BOOTSTRAP_SERVERS) --schema-registry-url=$(SCHEMA_REGISTRY_URL) --topic=transactions.raw"

consume-sample:
	$(MAKE) verify-infra
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.kafka.ingestion.TransactionIngestionApp --bootstrap-servers=$(BOOTSTRAP_SERVERS) --schema-registry-url=$(SCHEMA_REGISTRY_URL) --topic=transactions.raw --checkpoint-location=$(CHECKPOINT_DIR) --starting-offsets=$(STARTING_OFFSETS) --spark-master=$(SPARK_MASTER)"

ingest-delta:
	$(MAKE) verify-infra
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.persistence.delta.DeltaTransactionIngestionApp --bootstrap-servers=$(BOOTSTRAP_SERVERS) --schema-registry-url=$(SCHEMA_REGISTRY_URL) --topic=transactions.raw --checkpoint-location=$(DELTA_CHECKPOINT_DIR) --starting-offsets=$(STARTING_OFFSETS) --spark-master=$(SPARK_MASTER) --delta-path=$(DELTA_PATH) --delta-txn-app-id=$(DELTA_TXN_APP_ID) $(if $(FAIL_AFTER_DELTA_BATCH_ID),--fail-after-delta-batch-id=$(FAIL_AFTER_DELTA_BATCH_ID),)"

inspect-delta:
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.persistence.delta.DeltaInspectionApp --delta-path=$(DELTA_PATH) --spark-master=$(SPARK_MASTER)"

reset-delta:
	@echo "Removing the default local Delta table and its coupled checkpoint."
	bash scripts/reset-local-delta.sh

deduplicate-transactions:
	cd streaming-engine && WATERMARK_DELAY="$(WATERMARK_DELAY)" sbt "runMain io.sentinelaegisforge.streaming.processing.deduplication.TransactionDeduplicationApp --validated-delta-path=$(DELTA_PATH) --deduplicated-delta-path=$(DEDUPLICATED_DELTA_PATH) --checkpoint-location=$(DEDUP_CHECKPOINT_DIR) --delta-txn-app-id=$(DEDUP_DELTA_TXN_APP_ID) --spark-master=$(SPARK_MASTER)"

inspect-deduplicated:
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.persistence.delta.DeltaInspectionApp --delta-path=$(DEDUPLICATED_DELTA_PATH) --spark-master=$(SPARK_MASTER)"

reset-deduplication:
	@echo "Removing the default local deduplicated Delta table and its coupled checkpoint."
	bash scripts/reset-local-deduplication.sh
