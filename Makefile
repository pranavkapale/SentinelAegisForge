.PHONY: verify verify-scala verify-python infra-up infra-down infra-status infra-reset verify-infra produce-sample consume-sample ingest-delta inspect-delta reset-delta deduplicate-transactions inspect-deduplicated reset-deduplication process-customer-state inspect-customer-state reset-customer-state process-rolling-features inspect-rolling-features reset-rolling-features process-statistical-features inspect-statistical-features reset-statistical-features

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
CUSTOMER_STATE_INACTIVITY ?= 24 hours
CUSTOMER_STATE_DELTA_PATH ?= $(CURDIR)/.local/delta/customer_activity_snapshots
CUSTOMER_STATE_CHECKPOINT_DIR ?= $(CURDIR)/.local/checkpoints/customer-activity-state
CUSTOMER_STATE_DELTA_TXN_APP_ID ?= sentinel-customer-activity-snapshots-v1
FEATURE_DELTA_PATH ?= $(CURDIR)/.local/delta/transaction_customer_features_v2
FEATURE_CHECKPOINT_DIR ?= $(CURDIR)/.local/checkpoints/customer-rolling-features-v2
FEATURE_DELTA_TXN_APP_ID ?= sentinel-transaction-customer-features-v2
STATISTICAL_FEATURE_DELTA_PATH ?= $(CURDIR)/.local/delta/transaction_statistical_features_v2
STATISTICAL_FEATURE_CHECKPOINT_DIR ?= $(CURDIR)/.local/checkpoints/customer-statistical-features-v2
STATISTICAL_FEATURE_DELTA_TXN_APP_ID ?= sentinel-transaction-statistical-features-v2
STATISTICAL_FEATURE_INACTIVITY ?= 24 hours
RISK_DELTA_PATH ?= $(CURDIR)/.local/delta/transaction_risk_decisions
RISK_CHECKPOINT_DIR ?= $(CURDIR)/.local/checkpoints/risk-decisions-v1
RISK_DELTA_TXN_APP_ID ?= sentinel-transaction-risk-decisions-v1
# No hidden policy defaults: supply all five parameters explicitly.
POLICY_VERSION ?=
HIGH_VELOCITY_THRESHOLD_5M ?=
HIGH_AMOUNT_ZSCORE_THRESHOLD ?=
COMBINED_VELOCITY_THRESHOLD_5M ?=
COMBINED_AMOUNT_ZSCORE_THRESHOLD ?=
ML_FEATURES_DELTA_PATH ?= $(STATISTICAL_FEATURE_DELTA_PATH)
ML_LABELS_PATH ?=
ML_AS_OF_TIME ?=
ML_SOURCE_DELTA_VERSION ?=
ML_DATASET_OUTPUT_ROOT ?= $(CURDIR)/.local/ml/datasets/transaction-fraud-v1
ML_DATASET_SNAPSHOT_PATH ?=
BASELINE_DATASET_SNAPSHOT_PATH ?=
BASELINE_TRAIN_END ?=
BASELINE_VALIDATION_END ?=
BASELINE_TEST_END ?=
BASELINE_MIN_TRAIN_ROWS ?= 20
BASELINE_MIN_VALIDATION_ROWS ?= 10
BASELINE_MIN_TEST_ROWS ?= 10
BASELINE_OUTPUT_ROOT ?= $(CURDIR)/.local/ml/runs/fraud-logistic-baseline-v1
BASELINE_RUN_PATH ?=
SCENARIO_SEED ?=
SCENARIO_BASE_TIME ?=
SCENARIO_COUNT ?= 1500
SCENARIO_CUSTOMERS ?= 90
SCENARIO_HORIZON_DAYS ?= 9
SCENARIO_USD_WEIGHT_PERCENT ?= 65
SCENARIO_OUTPUT_ROOT ?= $(CURDIR)/.local/ml/scenarios
SCENARIO_CORPUS_PATH ?=
SCENARIO_WAVE ?=
SCENARIO_TRAIN_END ?=
SCENARIO_VALIDATION_END ?=
SCENARIO_TEST_END ?=
SCENARIO_AS_OF ?=
SCENARIO_VALIDATED_PATH ?=
SCENARIO_DEDUPLICATED_PATH ?=
SCENARIO_ROLLING_PATH ?=
SCENARIO_STATISTICAL_PATH ?=
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

process-customer-state:
	cd streaming-engine && CUSTOMER_STATE_INACTIVITY="$(CUSTOMER_STATE_INACTIVITY)" WATERMARK_DELAY="$(WATERMARK_DELAY)" sbt "runMain io.sentinelaegisforge.streaming.processing.customerstate.CustomerActivityStateApp --deduplicated-delta-path=$(DEDUPLICATED_DELTA_PATH) --snapshot-delta-path=$(CUSTOMER_STATE_DELTA_PATH) --checkpoint-location=$(CUSTOMER_STATE_CHECKPOINT_DIR) --delta-txn-app-id=$(CUSTOMER_STATE_DELTA_TXN_APP_ID) --spark-master=$(SPARK_MASTER)"

inspect-customer-state:
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.processing.customerstate.CustomerActivityInspectionApp --delta-path=$(CUSTOMER_STATE_DELTA_PATH) --spark-master=$(SPARK_MASTER)"

reset-customer-state:
	@echo "Removing the default local customer-state snapshot table and its coupled checkpoint."
	bash scripts/reset-local-customer-state.sh

process-rolling-features:
	cd streaming-engine && WATERMARK_DELAY="$(WATERMARK_DELAY)" sbt "runMain io.sentinelaegisforge.streaming.processing.rollingfeatures.TransactionRollingFeatureApp --deduplicated-delta-path=$(DEDUPLICATED_DELTA_PATH) --feature-delta-path=$(FEATURE_DELTA_PATH) --checkpoint-location=$(FEATURE_CHECKPOINT_DIR) --delta-txn-app-id=$(FEATURE_DELTA_TXN_APP_ID) --spark-master=$(SPARK_MASTER)"

inspect-rolling-features:
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.processing.rollingfeatures.TransactionRollingFeatureInspectionApp --delta-path=$(FEATURE_DELTA_PATH) --spark-master=$(SPARK_MASTER)"

reset-rolling-features:
	@echo "Removing the default local rolling-feature table and its coupled checkpoint."
	bash scripts/reset-local-rolling-features.sh

process-statistical-features:
	cd streaming-engine && WATERMARK_DELAY="$(WATERMARK_DELAY)" STATISTICAL_FEATURE_INACTIVITY="$(STATISTICAL_FEATURE_INACTIVITY)" sbt "runMain io.sentinelaegisforge.streaming.processing.statisticalfeatures.TransactionStatisticalFeatureApp --source-delta-path=$(FEATURE_DELTA_PATH) --target-delta-path=$(STATISTICAL_FEATURE_DELTA_PATH) --checkpoint-location=$(STATISTICAL_FEATURE_CHECKPOINT_DIR) --delta-txn-app-id=$(STATISTICAL_FEATURE_DELTA_TXN_APP_ID) --spark-master=$(SPARK_MASTER)"

inspect-statistical-features:
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.processing.statisticalfeatures.TransactionStatisticalFeatureInspectionApp --delta-path=$(STATISTICAL_FEATURE_DELTA_PATH) --spark-master=$(SPARK_MASTER)"

reset-statistical-features:
	@echo "Removing the default local statistical-feature table and its coupled checkpoint."
	bash scripts/reset-local-statistical-features.sh

.PHONY: process-risk-decisions inspect-risk-decisions
process-risk-decisions:
	@test -n "$(POLICY_VERSION)" && test -n "$(HIGH_VELOCITY_THRESHOLD_5M)" && test -n "$(HIGH_AMOUNT_ZSCORE_THRESHOLD)" && test -n "$(COMBINED_VELOCITY_THRESHOLD_5M)" && test -n "$(COMBINED_AMOUNT_ZSCORE_THRESHOLD)" || { echo "Supply POLICY_VERSION and all four explicit risk thresholds (see deterministic-risk-engine.md)."; exit 1; }
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.processing.risk.TransactionRiskDecisionApp --source-delta-path=$(STATISTICAL_FEATURE_DELTA_PATH) --target-delta-path=$(RISK_DELTA_PATH) --checkpoint-location=$(RISK_CHECKPOINT_DIR) --delta-txn-app-id=$(RISK_DELTA_TXN_APP_ID) --spark-master=$(SPARK_MASTER) --policy-version=$(POLICY_VERSION) --high-velocity-threshold-5m=$(HIGH_VELOCITY_THRESHOLD_5M) --high-amount-zscore-threshold=$(HIGH_AMOUNT_ZSCORE_THRESHOLD) --combined-velocity-threshold-5m=$(COMBINED_VELOCITY_THRESHOLD_5M) --combined-amount-zscore-threshold=$(COMBINED_AMOUNT_ZSCORE_THRESHOLD)"

inspect-risk-decisions:
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.processing.risk.TransactionRiskDecisionInspectionApp --delta-path=$(RISK_DELTA_PATH) --spark-master=$(SPARK_MASTER)"

.PHONY: build-ml-dataset inspect-ml-dataset
build-ml-dataset:
	@test -n "$(ML_LABELS_PATH)" && test -n "$(ML_AS_OF_TIME)" || { echo "Supply ML_LABELS_PATH and ML_AS_OF_TIME explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.datasets.cli build --features-delta "$(ML_FEATURES_DELTA_PATH)" --labels "$(ML_LABELS_PATH)" --as-of "$(ML_AS_OF_TIME)" --output-root "$(ML_DATASET_OUTPUT_ROOT)" $(if $(ML_SOURCE_DELTA_VERSION),--source-version=$(ML_SOURCE_DELTA_VERSION),)

inspect-ml-dataset:
	@test -n "$(ML_DATASET_SNAPSHOT_PATH)" || { echo "Supply ML_DATASET_SNAPSHOT_PATH explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.datasets.cli inspect --snapshot "$(ML_DATASET_SNAPSHOT_PATH)"

.PHONY: train-baseline-model inspect-baseline-run
train-baseline-model:
	@test -n "$(BASELINE_DATASET_SNAPSHOT_PATH)" && test -n "$(BASELINE_TRAIN_END)" && test -n "$(BASELINE_VALIDATION_END)" && test -n "$(BASELINE_TEST_END)" || { echo "Supply BASELINE_DATASET_SNAPSHOT_PATH and all three UTC split boundaries."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.training.cli train --dataset-snapshot "$(BASELINE_DATASET_SNAPSHOT_PATH)" --train-end "$(BASELINE_TRAIN_END)" --validation-end "$(BASELINE_VALIDATION_END)" --test-end "$(BASELINE_TEST_END)" --min-train-rows "$(BASELINE_MIN_TRAIN_ROWS)" --min-validation-rows "$(BASELINE_MIN_VALIDATION_ROWS)" --min-test-rows "$(BASELINE_MIN_TEST_ROWS)" --output-root "$(BASELINE_OUTPUT_ROOT)"

inspect-baseline-run:
	@test -n "$(BASELINE_RUN_PATH)" || { echo "Supply BASELINE_RUN_PATH explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.training.cli inspect --run "$(BASELINE_RUN_PATH)"

.PHONY: generate-synthetic-corpus inspect-synthetic-corpus scenario-quality publish-synthetic-wave reconcile-synthetic-corpus
generate-synthetic-corpus:
	@test -n "$(SCENARIO_SEED)" && test -n "$(SCENARIO_BASE_TIME)" || { echo "Supply SCENARIO_SEED and SCENARIO_BASE_TIME."; exit 1; }
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.simulation.SyntheticScenarioApp generate --seed=$(SCENARIO_SEED) --base-time=$(SCENARIO_BASE_TIME) --count=$(SCENARIO_COUNT) --customers=$(SCENARIO_CUSTOMERS) --horizon-days=$(SCENARIO_HORIZON_DAYS) --usd-weight-percent=$(SCENARIO_USD_WEIGHT_PERCENT) --output-root=$(SCENARIO_OUTPUT_ROOT)"

inspect-synthetic-corpus:
	@test -n "$(SCENARIO_CORPUS_PATH)" || { echo "Supply SCENARIO_CORPUS_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.synthetic.cli inspect --corpus "$(SCENARIO_CORPUS_PATH)"

scenario-quality:
	@test -n "$(SCENARIO_CORPUS_PATH)" && test -n "$(SCENARIO_TRAIN_END)" && test -n "$(SCENARIO_VALIDATION_END)" && test -n "$(SCENARIO_TEST_END)" && test -n "$(SCENARIO_AS_OF)" || { echo "Supply the corpus path and all four UTC quality boundaries."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.synthetic.cli quality --corpus "$(SCENARIO_CORPUS_PATH)" --train-end "$(SCENARIO_TRAIN_END)" --validation-end "$(SCENARIO_VALIDATION_END)" --test-end "$(SCENARIO_TEST_END)" --as-of "$(SCENARIO_AS_OF)"

publish-synthetic-wave:
	@test -n "$(SCENARIO_CORPUS_PATH)" && test -n "$(SCENARIO_WAVE)" || { echo "Supply SCENARIO_CORPUS_PATH and SCENARIO_WAVE (0, 1 or 2)."; exit 1; }
	$(MAKE) verify-infra
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.simulation.SyntheticScenarioApp publish --corpus=$(SCENARIO_CORPUS_PATH) --wave=$(SCENARIO_WAVE) --bootstrap-servers=$(BOOTSTRAP_SERVERS) --schema-registry-url=$(SCHEMA_REGISTRY_URL)"

reconcile-synthetic-corpus:
	@test -n "$(SCENARIO_CORPUS_PATH)" && test -n "$(SCENARIO_VALIDATED_PATH)" && test -n "$(SCENARIO_DEDUPLICATED_PATH)" && test -n "$(SCENARIO_ROLLING_PATH)" && test -n "$(SCENARIO_STATISTICAL_PATH)" || { echo "Supply the corpus and all four isolated Delta paths."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.synthetic.cli reconcile --corpus "$(SCENARIO_CORPUS_PATH)" --validated "$(SCENARIO_VALIDATED_PATH)" --deduplicated "$(SCENARIO_DEDUPLICATED_PATH)" --rolling "$(SCENARIO_ROLLING_PATH)" --statistical "$(SCENARIO_STATISTICAL_PATH)"
