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
STATISTICAL_FAIL_AFTER_DELTA_BATCH_ID ?=
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
MLFLOW_DB ?= $(CURDIR)/.local/mlflow/tracking.db
MLFLOW_ARTIFACTS ?= $(CURDIR)/.local/mlflow/artifacts
MLFLOW_EXPERIMENT ?= sentinelaegisforge-synthetic-fraud-baseline
MLFLOW_RUN_ID ?=
MLFLOW_UI_PORT ?= 5000
MLFLOW_CORPUS_PATH ?=
MLFLOW_VALIDATED_PATH ?=
MLFLOW_DEDUPLICATED_PATH ?=
MLFLOW_ROLLING_PATH ?=
MLFLOW_STATISTICAL_PATH ?=
MODEL_CANDIDATE_OUTPUT_ROOT ?= $(CURDIR)/.local/ml/model-candidates
MODEL_CANDIDATE_PATH ?=
MODEL_INPUT_JSON ?=
REGISTERED_MODEL_NAME ?= sentinelaegisforge-offline-fraud-logistic
REGISTERED_MODEL_VERSION ?=
REGISTERED_MODEL_VERSIONS ?=
REGISTRATION_RECEIPTS ?= $(CURDIR)/.local/mlflow/registration-receipts
MODEL_VALIDATION_OUTPUT_ROOT ?= $(CURDIR)/.local/ml/model-validations
MODEL_ASSESSMENT_PATH ?=
MODEL_ASSESSMENT_PATHS ?=
MONITORING_FEATURE_DELTA_PATH ?=
MONITORING_SOURCE_VERSION ?=
MONITORING_SCORE_START ?=
MONITORING_SCORE_END ?=
MONITORING_LEDGER_ROOT ?= $(CURDIR)/.local/ml/prediction-ledgers
MONITORING_LEDGER_PATH ?=
MONITORING_CORPUS_PATH ?=
MONITORING_EVALUATION_AS_OF ?=
MONITORING_SNAPSHOT_ROOT ?= $(CURDIR)/.local/ml/monitoring-snapshots
MONITORING_SNAPSHOT_PATH ?=
MONITORING_OTHER_SNAPSHOT_PATH ?=
DRIFT_LEDGER_PATH ?=
DRIFT_REFERENCE_START ?=
DRIFT_REFERENCE_END ?=
DRIFT_COMPARISON_START ?=
DRIFT_COMPARISON_END ?=
DRIFT_MINIMUM_ROWS ?= 30
DRIFT_MINIMUM_NON_NULL ?= 5
DRIFT_PSI_BINS ?= 10
DRIFT_PSI_SMOOTHING ?= 0.5
DRIFT_SNAPSHOT_ROOT ?= $(CURDIR)/.local/ml/drift-snapshots
DRIFT_SNAPSHOT_PATH ?=
DRIFT_OTHER_SNAPSHOT_PATH ?=
INVESTIGATION_DRIFT_PATH ?=
INVESTIGATION_LEDGER_PATH ?=
INVESTIGATION_EARLIER_PATH ?=
INVESTIGATION_LATER_PATH ?=
INVESTIGATION_DOSSIER_PATH ?=
INVESTIGATION_OUTPUT_ROOT ?= $(CURDIR)/.local/ml/investigations
INVESTIGATION_PACKET_PATH ?=
FEEDBACK_BATCH_PATH ?=
FEEDBACK_SOURCE_DELTA_PATH ?=
FEEDBACK_SOURCE_DELTA_VERSION ?=
FEEDBACK_BASE_LEDGER_PATH ?=
FEEDBACK_LEDGER_ROOT ?= $(CURDIR)/.local/ml/outcome-feedback
FEEDBACK_LEDGER_PATH ?=
FEEDBACK_AS_OF ?=
FEEDBACK_PREDICTION_LEDGER_PATH ?=
FEEDBACK_MONITORING_ROOT ?= $(CURDIR)/.local/ml/feedback-monitoring-v2
FEEDBACK_MONITORING_PATH ?=
FEEDBACK_FIXTURE_ROOT ?= $(CURDIR)/.local/ml/feedback-fixtures
FEEDBACK_FIXTURE_PATH ?=
SHADOW_SOURCE_DELTA_PATH ?=
SHADOW_SOURCE_VERSION ?=
SHADOW_START ?=
SHADOW_END ?=
SHADOW_MAX_REQUESTS ?= 5000
SHADOW_PLAN_ROOT ?= $(CURDIR)/.local/ml/shadow-replay-plans
SHADOW_PLAN_PATH ?=
SHADOW_EXECUTION_ROOT ?= $(CURDIR)/.local/ml/shadow-executions
SHADOW_EXECUTION_PATH ?=
SHADOW_REFERENCE_LEDGER_PATH ?=
SHADOW_PARITY_ROOT ?= $(CURDIR)/.local/ml/shadow-parity
FEATURE_AVAILABILITY_SOURCE_DELTA_PATH ?=
FEATURE_AVAILABILITY_START_VERSION ?=
FEATURE_AVAILABILITY_END_VERSION ?=
FEATURE_COMMIT_SEGMENT_ROOT ?= $(CURDIR)/.local/ml/feature-commit-segments
FEATURE_COMMIT_SEGMENT_PATH ?=
FEATURE_READY_OUTBOX_ROOT ?= $(CURDIR)/.local/ml/feature-ready-outboxes
FEATURE_READY_OUTBOX_PATH ?=
FEATURE_READY_RECEIVER_ROOT ?=
FEATURE_READY_MAXIMUM ?=
FEATURE_KAFKA_BATCH_ROOT ?= $(CURDIR)/.local/kafka/feature-ready-batches
FEATURE_KAFKA_BATCH_PATH ?=
FEATURE_KAFKA_JOURNAL_ROOT ?= $(CURDIR)/.local/kafka/feature-ready-publications
FEATURE_KAFKA_RECEIPT_ROOT ?=
FEATURE_KAFKA_GROUP ?=
FEATURE_KAFKA_MAXIMUM ?= 5000
FEATURE_KAFKA_MAX_DELIVERIES ?= 5000
FEATURE_KAFKA_FAIL_AFTER_ACK_AT ?=
FEATURE_KAFKA_FAIL_BEFORE_RECEIPT_AT ?=
FEATURE_KAFKA_FAIL_AFTER_RECEIPT_AT ?=
SHADOW_KAFKA_GROUP = sentinel-feature-ready-shadow-v1
SHADOW_KAFKA_RECEIPT_ROOT ?= $(CURDIR)/.local/kafka/feature-ready-receivers/shadow-v1
SHADOW_INBOX_ROOT ?= $(CURDIR)/.local/ml/kafka-shadow-inboxes
SHADOW_INBOX_PATH ?=
SHADOW_RESULT_ROOT ?= $(CURDIR)/.local/ml/kafka-shadow-results
SHADOW_RUN_ROOT ?= $(CURDIR)/.local/ml/kafka-shadow-runs
SHADOW_RUN_PATH ?=
SHADOW_REGISTRY_VERSION ?= 1
SHADOW_SCORE_START ?=
SHADOW_SCORE_END ?=
SHADOW_MAX_EVENTS ?= 5000
SHADOW_MAX_CACHED_ROWS ?= 10000
SHADOW_MAX_ATTEMPTS ?= 5000
SHADOW_INFERENCE_BATCH_SIZE ?= 100
SHADOW_FAIL_AFTER_RESULT ?=
INCREMENTAL_SOURCE_DELTA_PATH ?= $(CURDIR)/.local/phase27/statistical-fixture
INCREMENTAL_ROOT ?= $(CURDIR)/.local/phase27/streams
INCREMENTAL_FENCE_PATH ?= $(CURDIR)/.local/phase27/operational-fence.json
INCREMENTAL_GROUP ?= sentinel-feature-ready-incremental-shadow-v1
INCREMENTAL_STREAM_PATH ?=
INCREMENTAL_BATCH_ID ?=
INCREMENTAL_BATCH_PATH ?=
INCREMENTAL_PRECEDING_BOUNDARY ?= $(INCREMENTAL_FENCE_PATH)
INCREMENTAL_FIXTURE_SEED ?= 2701
INCREMENTAL_FIXTURE_FIRST_INDEX ?= 0
INCREMENTAL_FIXTURE_COUNT ?= 10
INCREMENTAL_FIXTURE_BASE_TIME ?= 2030-01-05T00:00:00Z
INCREMENTAL_FIRST_VERSION ?= 0
INCREMENTAL_MODEL_VERSION ?= 1
INCREMENTAL_UNDISCOVERED_LIMIT ?= 8
INCREMENTAL_UNPUBLISHED_LIMIT ?= 500
INCREMENTAL_ACCEPTED_LIMIT ?= 20
INCREMENTAL_TICK_LIMIT ?= 10
INCREMENTAL_CACHED_ROWS_LIMIT ?= 5000
INCREMENTAL_SCORE_START ?= 2030-01-04T00:00:00Z
INCREMENTAL_SCORE_END ?= 2030-01-10T00:00:00Z
INCREMENTAL_CANDIDATE_PATH ?=
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
PHASE28_ROOT ?= $(CURDIR)/.local/phase28/integrations
PHASE28_CORPUS_PATH ?=
PHASE28_TRANSACTION_FENCE ?=
PHASE28_INTEGRATION_PATH ?=
PHASE28_STREAM_PATH ?=
PHASE28_WAVE ?=
PHASE28_EVENT_ID ?=
PHASE28_RECOVERY_EVIDENCE ?=
PHASE29_INTEGRATION_PATH ?=
PHASE29_STREAM_PATH ?=
PHASE29_WAVE ?=
PHASE29_FAULT_BATCH_ID ?= 0
PHASE29_DOSSIER_PATH ?=
PHASE29_CURRENT_FENCE ?=
PHASE29_READY_OFFSETS_LOG ?=
PHASE29_A_INTEGRATION ?=
PHASE29_B_INTEGRATION ?=
PHASE29_C_INTEGRATION ?=
PHASE29_D_INTEGRATION ?=
PHASE29_A_STREAM ?=
PHASE29_B_STREAM ?=
PHASE29_C_STREAM ?=
PHASE29_D_STREAM ?=
PHASE29_OUTAGE_EVIDENCE ?=
PHASE29_BROKER_BATCH_ID ?=
PHASE29_BROKER_FAILURE_LOG ?=
PHASE29_BROKER_EXIT_CODE ?=
PHASE29_CANDIDATE ?= $(CURDIR)/.local/ml/model-candidates/b4fff4f1f74818165d04bae094b89f7c7b8dc7367d6e3dad10465b1a6160fadb
PHASE29_DOSSIER_ROOT ?= $(CURDIR)/.local/phase29/reliability
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
	cd streaming-engine && WATERMARK_DELAY="$(WATERMARK_DELAY)" STATISTICAL_FEATURE_INACTIVITY="$(STATISTICAL_FEATURE_INACTIVITY)" sbt "runMain io.sentinelaegisforge.streaming.processing.statisticalfeatures.TransactionStatisticalFeatureApp --source-delta-path=$(FEATURE_DELTA_PATH) --target-delta-path=$(STATISTICAL_FEATURE_DELTA_PATH) --checkpoint-location=$(STATISTICAL_FEATURE_CHECKPOINT_DIR) --delta-txn-app-id=$(STATISTICAL_FEATURE_DELTA_TXN_APP_ID) --spark-master=$(SPARK_MASTER) $(if $(STATISTICAL_FAIL_AFTER_DELTA_BATCH_ID),--fail-after-delta-batch-id=$(STATISTICAL_FAIL_AFTER_DELTA_BATCH_ID),)"

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

.PHONY: track-baseline-run inspect-tracked-run compare-tracked-runs mlflow-ui
track-baseline-run:
	@test -n "$(BASELINE_RUN_PATH)" || { echo "Supply BASELINE_RUN_PATH explicitly; tracking never retrains."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.tracking.cli --database "$(MLFLOW_DB)" --artifacts "$(MLFLOW_ARTIFACTS)" --experiment "$(MLFLOW_EXPERIMENT)" track --run "$(BASELINE_RUN_PATH)" $(if $(MLFLOW_CORPUS_PATH),--corpus "$(MLFLOW_CORPUS_PATH)",) $(if $(MLFLOW_VALIDATED_PATH),--validated "$(MLFLOW_VALIDATED_PATH)",) $(if $(MLFLOW_DEDUPLICATED_PATH),--deduplicated "$(MLFLOW_DEDUPLICATED_PATH)",) $(if $(MLFLOW_ROLLING_PATH),--rolling "$(MLFLOW_ROLLING_PATH)",) $(if $(MLFLOW_STATISTICAL_PATH),--statistical "$(MLFLOW_STATISTICAL_PATH)",)

inspect-tracked-run:
	@test -n "$(MLFLOW_RUN_ID)" || { echo "Supply MLFLOW_RUN_ID explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.tracking.cli --database "$(MLFLOW_DB)" --artifacts "$(MLFLOW_ARTIFACTS)" --experiment "$(MLFLOW_EXPERIMENT)" inspect --run-id "$(MLFLOW_RUN_ID)"

compare-tracked-runs:
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.tracking.cli --database "$(MLFLOW_DB)" --artifacts "$(MLFLOW_ARTIFACTS)" --experiment "$(MLFLOW_EXPERIMENT)" compare

mlflow-ui:
	cd model-control-plane && uv run --locked mlflow ui --backend-store-uri "sqlite:///$(MLFLOW_DB)" --default-artifact-root "file://$(MLFLOW_ARTIFACTS)" --host 127.0.0.1 --port "$(MLFLOW_UI_PORT)"

.PHONY: build-model-candidate inspect-model-candidate score-model-candidate
build-model-candidate:
	@test -n "$(BASELINE_RUN_PATH)" || { echo "Supply BASELINE_RUN_PATH; candidate build verifies and reconstructs the existing run."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.candidates.cli build --run "$(BASELINE_RUN_PATH)" --output-root "$(MODEL_CANDIDATE_OUTPUT_ROOT)" $(if $(MLFLOW_RUN_ID),--mlflow-run-id "$(MLFLOW_RUN_ID)",) --database "$(MLFLOW_DB)" --artifacts "$(MLFLOW_ARTIFACTS)" --experiment "$(MLFLOW_EXPERIMENT)"

inspect-model-candidate:
	@test -n "$(MODEL_CANDIDATE_PATH)" || { echo "Supply MODEL_CANDIDATE_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.candidates.cli inspect --candidate "$(MODEL_CANDIDATE_PATH)" --database "$(MLFLOW_DB)" --artifacts "$(MLFLOW_ARTIFACTS)" --experiment "$(MLFLOW_EXPERIMENT)"

score-model-candidate:
	@test -n "$(MODEL_CANDIDATE_PATH)" && test -n "$(MODEL_INPUT_JSON)" || { echo "Supply MODEL_CANDIDATE_PATH and MODEL_INPUT_JSON."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.candidates.cli score --candidate "$(MODEL_CANDIDATE_PATH)" --input-json "$(MODEL_INPUT_JSON)" --database "$(MLFLOW_DB)" --artifacts "$(MLFLOW_ARTIFACTS)" --experiment "$(MLFLOW_EXPERIMENT)"

.PHONY: register-model-candidate inspect-registered-candidate compare-registered-candidates verify-registered-model-artifact
REGISTRY_ARGS = --database "$(MLFLOW_DB)" --artifacts "$(MLFLOW_ARTIFACTS)" --experiment "$(MLFLOW_EXPERIMENT)" --model-name "$(REGISTERED_MODEL_NAME)" --receipts "$(REGISTRATION_RECEIPTS)"

register-model-candidate:
	@test -n "$(MODEL_CANDIDATE_PATH)" || { echo "Supply MODEL_CANDIDATE_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.registry.cli register --candidate "$(MODEL_CANDIDATE_PATH)" $(REGISTRY_ARGS)

inspect-registered-candidate:
	@test -n "$(REGISTERED_MODEL_VERSION)" || { echo "Supply REGISTERED_MODEL_VERSION explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.registry.cli inspect --version "$(REGISTERED_MODEL_VERSION)" $(REGISTRY_ARGS)

compare-registered-candidates:
	@test -n "$(REGISTERED_MODEL_VERSIONS)" || { echo "Supply REGISTERED_MODEL_VERSIONS explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.registry.cli compare --versions $(REGISTERED_MODEL_VERSIONS) $(REGISTRY_ARGS)

verify-registered-model-artifact:
	@test -n "$(REGISTERED_MODEL_VERSION)" || { echo "Supply REGISTERED_MODEL_VERSION explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.registry.cli verify-artifact --version "$(REGISTERED_MODEL_VERSION)" $(REGISTRY_ARGS)

.PHONY: assess-registered-model inspect-model-assessment compare-model-assessments
assess-registered-model:
	@test -n "$(REGISTERED_MODEL_VERSION)" || { echo "Supply REGISTERED_MODEL_VERSION explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.validation.cli assess --version "$(REGISTERED_MODEL_VERSION)" --output-root "$(MODEL_VALIDATION_OUTPUT_ROOT)" $(REGISTRY_ARGS)

inspect-model-assessment:
	@test -n "$(MODEL_ASSESSMENT_PATH)" || { echo "Supply MODEL_ASSESSMENT_PATH explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.validation.cli inspect --assessment "$(MODEL_ASSESSMENT_PATH)" $(REGISTRY_ARGS)

compare-model-assessments:
	@test -n "$(MODEL_ASSESSMENT_PATHS)" || { echo "Supply MODEL_ASSESSMENT_PATHS explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.validation.cli compare --assessments $(MODEL_ASSESSMENT_PATHS) $(REGISTRY_ARGS)

.PHONY: build-prediction-ledger inspect-prediction-ledger evaluate-delayed-outcomes inspect-monitoring-snapshot compare-monitoring-snapshots
build-prediction-ledger:
	@test -n "$(REGISTERED_MODEL_VERSION)" && test -n "$(MONITORING_FEATURE_DELTA_PATH)" && test -n "$(MONITORING_SOURCE_VERSION)" && test -n "$(MONITORING_SCORE_START)" && test -n "$(MONITORING_SCORE_END)" || { echo "Supply exact Registry version, Delta path/version and UTC scoring window."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.monitoring.cli build-prediction-ledger --database "$(MLFLOW_DB)" --artifacts "$(MLFLOW_ARTIFACTS)" --experiment "$(MLFLOW_EXPERIMENT)" --model-name "$(REGISTERED_MODEL_NAME)" --receipts "$(REGISTRATION_RECEIPTS)" --model-version "$(REGISTERED_MODEL_VERSION)" --features-delta "$(MONITORING_FEATURE_DELTA_PATH)" --source-version "$(MONITORING_SOURCE_VERSION)" --score-start "$(MONITORING_SCORE_START)" --score-end "$(MONITORING_SCORE_END)" --output-root "$(MONITORING_LEDGER_ROOT)"

inspect-prediction-ledger:
	@test -n "$(MONITORING_LEDGER_PATH)" || { echo "Supply MONITORING_LEDGER_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.monitoring.cli inspect-prediction-ledger --ledger "$(MONITORING_LEDGER_PATH)"

evaluate-delayed-outcomes:
	@test -n "$(MONITORING_LEDGER_PATH)" && test -n "$(MONITORING_CORPUS_PATH)" && test -n "$(MONITORING_EVALUATION_AS_OF)" || { echo "Supply ledger, verified corpus and explicit UTC evaluation cutoff."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.monitoring.cli evaluate-delayed-outcomes --ledger "$(MONITORING_LEDGER_PATH)" --corpus "$(MONITORING_CORPUS_PATH)" --evaluation-as-of "$(MONITORING_EVALUATION_AS_OF)" --output-root "$(MONITORING_SNAPSHOT_ROOT)"

inspect-monitoring-snapshot:
	@test -n "$(MONITORING_SNAPSHOT_PATH)" || { echo "Supply MONITORING_SNAPSHOT_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.monitoring.cli inspect-monitoring-snapshot --snapshot "$(MONITORING_SNAPSHOT_PATH)"

compare-monitoring-snapshots:
	@test -n "$(MONITORING_SNAPSHOT_PATH)" && test -n "$(MONITORING_OTHER_SNAPSHOT_PATH)" || { echo "Supply both monitoring snapshot paths."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.monitoring.cli compare-monitoring-snapshots --first "$(MONITORING_SNAPSHOT_PATH)" --second "$(MONITORING_OTHER_SNAPSHOT_PATH)"

.PHONY: build-drift-snapshot inspect-drift-snapshot compare-drift-snapshots
build-drift-snapshot:
	@test -n "$(DRIFT_LEDGER_PATH)" && test -n "$(DRIFT_REFERENCE_START)" && test -n "$(DRIFT_REFERENCE_END)" && test -n "$(DRIFT_COMPARISON_START)" && test -n "$(DRIFT_COMPARISON_END)" || { echo "Supply a ledger and all four explicit UTC drift boundaries."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.monitoring.cli build-drift-snapshot --ledger "$(DRIFT_LEDGER_PATH)" --reference-start "$(DRIFT_REFERENCE_START)" --reference-end "$(DRIFT_REFERENCE_END)" --comparison-start "$(DRIFT_COMPARISON_START)" --comparison-end "$(DRIFT_COMPARISON_END)" --minimum-rows "$(DRIFT_MINIMUM_ROWS)" --minimum-non-null "$(DRIFT_MINIMUM_NON_NULL)" --psi-bins "$(DRIFT_PSI_BINS)" --psi-smoothing "$(DRIFT_PSI_SMOOTHING)" --output-root "$(DRIFT_SNAPSHOT_ROOT)"

inspect-drift-snapshot:
	@test -n "$(DRIFT_SNAPSHOT_PATH)" || { echo "Supply DRIFT_SNAPSHOT_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.monitoring.cli inspect-drift-snapshot --snapshot "$(DRIFT_SNAPSHOT_PATH)"

compare-drift-snapshots:
	@test -n "$(DRIFT_SNAPSHOT_PATH)" && test -n "$(DRIFT_OTHER_SNAPSHOT_PATH)" || { echo "Supply both drift snapshot paths."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.monitoring.cli compare-drift-snapshots --first "$(DRIFT_SNAPSHOT_PATH)" --second "$(DRIFT_OTHER_SNAPSHOT_PATH)"

.PHONY: build-monitoring-investigation inspect-monitoring-investigation
INVESTIGATION_REGISTRY_ARGS = --database "$(MLFLOW_DB)" --artifacts "$(MLFLOW_ARTIFACTS)" --experiment "$(MLFLOW_EXPERIMENT)" --model-name "$(REGISTERED_MODEL_NAME)" --receipts "$(REGISTRATION_RECEIPTS)"
build-monitoring-investigation:
	@test -n "$(INVESTIGATION_DRIFT_PATH)" || { echo "Supply INVESTIGATION_DRIFT_PATH explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.monitoring.cli build-monitoring-investigation --drift "$(INVESTIGATION_DRIFT_PATH)" $(if $(INVESTIGATION_LEDGER_PATH),--ledger "$(INVESTIGATION_LEDGER_PATH)",) $(if $(INVESTIGATION_EARLIER_PATH),--earlier "$(INVESTIGATION_EARLIER_PATH)",) $(if $(INVESTIGATION_LATER_PATH),--later "$(INVESTIGATION_LATER_PATH)",) $(if $(INVESTIGATION_DOSSIER_PATH),--dossier "$(INVESTIGATION_DOSSIER_PATH)",) --output-root "$(INVESTIGATION_OUTPUT_ROOT)" $(INVESTIGATION_REGISTRY_ARGS)

inspect-monitoring-investigation:
	@test -n "$(INVESTIGATION_PACKET_PATH)" || { echo "Supply INVESTIGATION_PACKET_PATH explicitly."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.monitoring.cli inspect-monitoring-investigation --packet "$(INVESTIGATION_PACKET_PATH)" $(INVESTIGATION_REGISTRY_ARGS)

.PHONY: generate-local-feedback-fixture inspect-local-feedback-fixture build-feedback-ledger inspect-feedback-ledger resolve-feedback-as-of evaluate-feedback-outcomes inspect-feedback-monitoring
generate-local-feedback-fixture:
	@test -n "$(FEEDBACK_PREDICTION_LEDGER_PATH)" || { echo "Supply FEEDBACK_PREDICTION_LEDGER_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.feedback.cli generate-local-feedback-fixture --prediction-ledger "$(FEEDBACK_PREDICTION_LEDGER_PATH)" --output-root "$(FEEDBACK_FIXTURE_ROOT)"

inspect-local-feedback-fixture:
	@test -n "$(FEEDBACK_FIXTURE_PATH)" || { echo "Supply FEEDBACK_FIXTURE_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.feedback.cli inspect-local-feedback-fixture --fixture "$(FEEDBACK_FIXTURE_PATH)"

build-feedback-ledger:
	@test -n "$(FEEDBACK_BATCH_PATH)" && test -n "$(FEEDBACK_SOURCE_DELTA_PATH)" && test -n "$(FEEDBACK_SOURCE_DELTA_VERSION)" || { echo "Supply feedback batch and fixed Delta path/version."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.feedback.cli build-feedback-ledger --batch "$(FEEDBACK_BATCH_PATH)" --source-delta "$(FEEDBACK_SOURCE_DELTA_PATH)" --source-version "$(FEEDBACK_SOURCE_DELTA_VERSION)" --output-root "$(FEEDBACK_LEDGER_ROOT)" $(if $(FEEDBACK_BASE_LEDGER_PATH),--base-ledger "$(FEEDBACK_BASE_LEDGER_PATH)",)

inspect-feedback-ledger:
	@test -n "$(FEEDBACK_LEDGER_PATH)" || { echo "Supply FEEDBACK_LEDGER_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.feedback.cli inspect-feedback-ledger --ledger "$(FEEDBACK_LEDGER_PATH)"

resolve-feedback-as-of:
	@test -n "$(FEEDBACK_LEDGER_PATH)" && test -n "$(FEEDBACK_AS_OF)" || { echo "Supply FEEDBACK_LEDGER_PATH and FEEDBACK_AS_OF."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.feedback.cli resolve-feedback-as-of --ledger "$(FEEDBACK_LEDGER_PATH)" --as-of "$(FEEDBACK_AS_OF)"

evaluate-feedback-outcomes:
	@test -n "$(FEEDBACK_PREDICTION_LEDGER_PATH)" && test -n "$(FEEDBACK_LEDGER_PATH)" && test -n "$(FEEDBACK_AS_OF)" || { echo "Supply prediction ledger, feedback ledger and UTC AS_OF."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.feedback.cli evaluate-feedback-outcomes --prediction-ledger "$(FEEDBACK_PREDICTION_LEDGER_PATH)" --feedback-ledger "$(FEEDBACK_LEDGER_PATH)" --evaluation-as-of "$(FEEDBACK_AS_OF)" --output-root "$(FEEDBACK_MONITORING_ROOT)"

inspect-feedback-monitoring:
	@test -n "$(FEEDBACK_MONITORING_PATH)" || { echo "Supply FEEDBACK_MONITORING_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.feedback.cli inspect-feedback-monitoring --snapshot "$(FEEDBACK_MONITORING_PATH)"

.PHONY: build-shadow-replay-plan inspect-shadow-replay-plan execute-shadow-replay inspect-shadow-execution verify-shadow-parity
SHADOW_REGISTRY_ARGS = --database "$(MLFLOW_DB)" --artifacts "$(MLFLOW_ARTIFACTS)" --experiment "$(MLFLOW_EXPERIMENT)" --model-name "$(REGISTERED_MODEL_NAME)" --receipts "$(REGISTRATION_RECEIPTS)"
build-shadow-replay-plan:
	@test -n "$(REGISTERED_MODEL_VERSION)" && test -n "$(SHADOW_SOURCE_DELTA_PATH)" && test -n "$(SHADOW_SOURCE_VERSION)" && test -n "$(SHADOW_START)" && test -n "$(SHADOW_END)" || { echo "Supply exact Registry version, fixed Delta path/version and UTC replay window."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.shadow.cli $(SHADOW_REGISTRY_ARGS) build-shadow-replay-plan --model-version "$(REGISTERED_MODEL_VERSION)" --source-delta "$(SHADOW_SOURCE_DELTA_PATH)" --source-version "$(SHADOW_SOURCE_VERSION)" --start "$(SHADOW_START)" --end "$(SHADOW_END)" --maximum "$(SHADOW_MAX_REQUESTS)" --output-root "$(SHADOW_PLAN_ROOT)"

inspect-shadow-replay-plan:
	@test -n "$(SHADOW_PLAN_PATH)" || { echo "Supply SHADOW_PLAN_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.shadow.cli $(SHADOW_REGISTRY_ARGS) inspect-shadow-replay-plan --plan "$(SHADOW_PLAN_PATH)"

execute-shadow-replay:
	@test -n "$(SHADOW_PLAN_PATH)" || { echo "Supply SHADOW_PLAN_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.shadow.cli $(SHADOW_REGISTRY_ARGS) execute-shadow-replay --plan "$(SHADOW_PLAN_PATH)" --maximum "$(SHADOW_MAX_REQUESTS)" --output-root "$(SHADOW_EXECUTION_ROOT)"

inspect-shadow-execution:
	@test -n "$(SHADOW_PLAN_PATH)" && test -n "$(SHADOW_EXECUTION_PATH)" || { echo "Supply SHADOW_PLAN_PATH and SHADOW_EXECUTION_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.shadow.cli $(SHADOW_REGISTRY_ARGS) inspect-shadow-execution --plan "$(SHADOW_PLAN_PATH)" --execution "$(SHADOW_EXECUTION_PATH)"

verify-shadow-parity:
	@test -n "$(SHADOW_PLAN_PATH)" && test -n "$(SHADOW_EXECUTION_PATH)" && test -n "$(SHADOW_REFERENCE_LEDGER_PATH)" || { echo "Supply SHADOW_PLAN_PATH, SHADOW_EXECUTION_PATH and SHADOW_REFERENCE_LEDGER_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.shadow.cli $(SHADOW_REGISTRY_ARGS) verify-shadow-parity --plan "$(SHADOW_PLAN_PATH)" --execution "$(SHADOW_EXECUTION_PATH)" --reference-ledger "$(SHADOW_REFERENCE_LEDGER_PATH)" --output-root "$(SHADOW_PARITY_ROOT)"

.PHONY: discover-feature-commits inspect-feature-commit-segment build-feature-ready-outbox inspect-feature-ready-outbox handoff-ready-events inspect-feature-handoff verify-feature-handoff
discover-feature-commits:
	@test -n "$(FEATURE_AVAILABILITY_SOURCE_DELTA_PATH)" && test -n "$(FEATURE_AVAILABILITY_START_VERSION)" && test -n "$(FEATURE_AVAILABILITY_END_VERSION)" || { echo "Supply Delta path and inclusive start/end versions."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.availability.cli discover-feature-commits --source-delta "$(FEATURE_AVAILABILITY_SOURCE_DELTA_PATH)" --start-version "$(FEATURE_AVAILABILITY_START_VERSION)" --end-version "$(FEATURE_AVAILABILITY_END_VERSION)" --segment-root "$(FEATURE_COMMIT_SEGMENT_ROOT)"

inspect-feature-commit-segment:
	@test -n "$(FEATURE_COMMIT_SEGMENT_PATH)" || { echo "Supply FEATURE_COMMIT_SEGMENT_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.availability.cli inspect-feature-commit-segment --segment "$(FEATURE_COMMIT_SEGMENT_PATH)"

build-feature-ready-outbox:
	@test -n "$(FEATURE_AVAILABILITY_SOURCE_DELTA_PATH)" && test -n "$(FEATURE_AVAILABILITY_START_VERSION)" && test -n "$(FEATURE_AVAILABILITY_END_VERSION)" || { echo "Supply Delta path and inclusive start/end versions."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.availability.cli build-feature-ready-outbox --source-delta "$(FEATURE_AVAILABILITY_SOURCE_DELTA_PATH)" --start-version "$(FEATURE_AVAILABILITY_START_VERSION)" --end-version "$(FEATURE_AVAILABILITY_END_VERSION)" --segment-root "$(FEATURE_COMMIT_SEGMENT_ROOT)" --outbox-root "$(FEATURE_READY_OUTBOX_ROOT)"

inspect-feature-ready-outbox:
	@test -n "$(FEATURE_READY_OUTBOX_PATH)" || { echo "Supply FEATURE_READY_OUTBOX_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.availability.cli inspect-feature-ready-outbox --outbox "$(FEATURE_READY_OUTBOX_PATH)"

handoff-ready-events:
	@test -n "$(FEATURE_READY_OUTBOX_PATH)" && test -n "$(FEATURE_READY_RECEIVER_ROOT)" && test -n "$(FEATURE_READY_MAXIMUM)" || { echo "Supply outbox path, receiver root and explicit maximum."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.availability.cli handoff-ready-events --outbox "$(FEATURE_READY_OUTBOX_PATH)" --receiver-root "$(FEATURE_READY_RECEIVER_ROOT)" --maximum "$(FEATURE_READY_MAXIMUM)"

inspect-feature-handoff:
	@test -n "$(FEATURE_READY_OUTBOX_PATH)" && test -n "$(FEATURE_READY_RECEIVER_ROOT)" || { echo "Supply outbox path and receiver root."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.availability.cli inspect-feature-handoff --outbox "$(FEATURE_READY_OUTBOX_PATH)" --receiver-root "$(FEATURE_READY_RECEIVER_ROOT)"

verify-feature-handoff:
	@test -n "$(FEATURE_READY_OUTBOX_PATH)" && test -n "$(FEATURE_READY_RECEIVER_ROOT)" || { echo "Supply outbox path and receiver root."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.availability.cli verify-feature-handoff --outbox "$(FEATURE_READY_OUTBOX_PATH)" --receiver-root "$(FEATURE_READY_RECEIVER_ROOT)"

.PHONY: provision-feature-ready-topic verify-feature-ready-topic prepare-feature-kafka-batch inspect-feature-kafka-batch feature-kafka-offsets publish-feature-ready-kafka inspect-feature-kafka-publication consume-feature-ready-kafka inspect-feature-kafka-receipts verify-feature-kafka-reconciliation verify-feature-ready-kafka-integration
FEATURE_KAFKA_APP = io.sentinelaegisforge.streaming.kafka.featureavailability.FeatureReadyKafkaApp
FEATURE_KAFKA_ARGS = --bootstrap-servers=$(BOOTSTRAP_SERVERS) --batch=$(FEATURE_KAFKA_BATCH_PATH)

provision-feature-ready-topic:
	bash scripts/provision-feature-ready-topic.sh
	$(MAKE) verify-feature-ready-topic

verify-feature-ready-topic:
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=topic --bootstrap-servers=$(BOOTSTRAP_SERVERS)"

prepare-feature-kafka-batch:
	@test -n "$(FEATURE_READY_OUTBOX_PATH)" || { echo "Supply FEATURE_READY_OUTBOX_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.availability.cli prepare-feature-kafka-batch --outbox "$(FEATURE_READY_OUTBOX_PATH)" --output-root "$(FEATURE_KAFKA_BATCH_ROOT)" --maximum "$(FEATURE_KAFKA_MAXIMUM)"

inspect-feature-kafka-batch:
	@test -n "$(FEATURE_KAFKA_BATCH_PATH)" || { echo "Supply FEATURE_KAFKA_BATCH_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.availability.cli inspect-feature-kafka-batch --batch "$(FEATURE_KAFKA_BATCH_PATH)"

feature-kafka-offsets:
	@test -n "$(FEATURE_KAFKA_BATCH_PATH)" && test -n "$(FEATURE_KAFKA_GROUP)" || { echo "Supply batch path and stable group ID."; exit 1; }
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=offsets $(FEATURE_KAFKA_ARGS) --group=$(FEATURE_KAFKA_GROUP)"

publish-feature-ready-kafka:
	@test -n "$(FEATURE_KAFKA_BATCH_PATH)" || { echo "Supply FEATURE_KAFKA_BATCH_PATH."; exit 1; }
	$(MAKE) inspect-feature-kafka-batch FEATURE_KAFKA_BATCH_PATH="$(FEATURE_KAFKA_BATCH_PATH)"
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=publish $(FEATURE_KAFKA_ARGS) --journal-root=$(FEATURE_KAFKA_JOURNAL_ROOT) --maximum-deliveries=$(FEATURE_KAFKA_MAX_DELIVERIES) $(if $(FEATURE_KAFKA_REPLAY),--replay-acknowledged,) $(if $(FEATURE_KAFKA_FAIL_AFTER_ACK_AT),--fail-after-ack-before-journal-at=$(FEATURE_KAFKA_FAIL_AFTER_ACK_AT),)"

inspect-feature-kafka-publication:
	@test -n "$(FEATURE_KAFKA_BATCH_PATH)" || { echo "Supply FEATURE_KAFKA_BATCH_PATH."; exit 1; }
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=inspect-publication $(FEATURE_KAFKA_ARGS) --journal-root=$(FEATURE_KAFKA_JOURNAL_ROOT)"

consume-feature-ready-kafka:
	@test -n "$(FEATURE_KAFKA_BATCH_PATH)" && test -n "$(FEATURE_KAFKA_RECEIPT_ROOT)" && test -n "$(FEATURE_KAFKA_GROUP)" || { echo "Supply batch path, receipt root and stable group ID."; exit 1; }
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=consume $(FEATURE_KAFKA_ARGS) --receipt-root=$(FEATURE_KAFKA_RECEIPT_ROOT) --group=$(FEATURE_KAFKA_GROUP) --maximum=$(FEATURE_KAFKA_MAXIMUM) $(if $(FEATURE_KAFKA_FAIL_BEFORE_RECEIPT_AT),--fail-before-receipt-at=$(FEATURE_KAFKA_FAIL_BEFORE_RECEIPT_AT),) $(if $(FEATURE_KAFKA_FAIL_AFTER_RECEIPT_AT),--fail-after-receipt-before-commit-at=$(FEATURE_KAFKA_FAIL_AFTER_RECEIPT_AT),)"

inspect-feature-kafka-receipts:
	@test -n "$(FEATURE_KAFKA_BATCH_PATH)" && test -n "$(FEATURE_KAFKA_RECEIPT_ROOT)" && test -n "$(FEATURE_KAFKA_GROUP)" || { echo "Supply batch path, receipt root and stable group ID."; exit 1; }
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=inspect-receipts $(FEATURE_KAFKA_ARGS) --receipt-root=$(FEATURE_KAFKA_RECEIPT_ROOT) --group=$(FEATURE_KAFKA_GROUP)"

verify-feature-kafka-reconciliation:
	@test -n "$(FEATURE_KAFKA_BATCH_PATH)" && test -n "$(FEATURE_KAFKA_RECEIPT_ROOT)" && test -n "$(FEATURE_KAFKA_GROUP)" || { echo "Supply batch path, receipt root and stable group ID."; exit 1; }
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=reconcile $(FEATURE_KAFKA_ARGS) --journal-root=$(FEATURE_KAFKA_JOURNAL_ROOT) --receipt-root=$(FEATURE_KAFKA_RECEIPT_ROOT) --group=$(FEATURE_KAFKA_GROUP)"

verify-feature-ready-kafka-integration:
	@test -n "$(FEATURE_KAFKA_BATCH_PATH)" && test -n "$(FEATURE_KAFKA_RECEIPT_ROOT)" && test -n "$(FEATURE_KAFKA_GROUP)" || { echo "Supply batch path, receipt root and stable group ID. This target publishes and explicitly replays notifications."; exit 1; }
	$(MAKE) verify-feature-ready-topic
	$(MAKE) publish-feature-ready-kafka
	$(MAKE) consume-feature-ready-kafka
	$(MAKE) verify-feature-kafka-reconciliation
	$(MAKE) publish-feature-ready-kafka FEATURE_KAFKA_REPLAY=1
	$(MAKE) consume-feature-ready-kafka
	$(MAKE) verify-feature-kafka-reconciliation

.PHONY: consume-feature-ready-shadow seal-feature-ready-shadow-inbox inspect-feature-ready-shadow-inbox execute-feature-ready-shadow inspect-feature-ready-shadow verify-feature-ready-shadow-parity verify-feature-ready-shadow-integration

consume-feature-ready-shadow:
	@test -n "$(FEATURE_KAFKA_BATCH_PATH)" || { echo "Supply FEATURE_KAFKA_BATCH_PATH."; exit 1; }
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=consume $(FEATURE_KAFKA_ARGS) --receipt-root=$(SHADOW_KAFKA_RECEIPT_ROOT) --group=$(SHADOW_KAFKA_GROUP) --maximum=$(FEATURE_KAFKA_MAXIMUM) $(if $(FEATURE_KAFKA_FAIL_BEFORE_RECEIPT_AT),--fail-before-receipt-at=$(FEATURE_KAFKA_FAIL_BEFORE_RECEIPT_AT),) $(if $(FEATURE_KAFKA_FAIL_AFTER_RECEIPT_AT),--fail-after-receipt-before-commit-at=$(FEATURE_KAFKA_FAIL_AFTER_RECEIPT_AT),)"

seal-feature-ready-shadow-inbox:
	@test -n "$(FEATURE_KAFKA_BATCH_PATH)" || { echo "Supply FEATURE_KAFKA_BATCH_PATH."; exit 1; }
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=seal-shadow-inbox $(FEATURE_KAFKA_ARGS) --journal-root=$(FEATURE_KAFKA_JOURNAL_ROOT) --receipt-root=$(SHADOW_KAFKA_RECEIPT_ROOT) --group=$(SHADOW_KAFKA_GROUP) --inbox-root=$(SHADOW_INBOX_ROOT)"

inspect-feature-ready-shadow-inbox:
	@test -n "$(SHADOW_INBOX_PATH)" || { echo "Supply SHADOW_INBOX_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.kafka_shadow.cli inspect-inbox --inbox "$(SHADOW_INBOX_PATH)"

execute-feature-ready-shadow:
	@test -n "$(SHADOW_INBOX_PATH)" && test -n "$(SHADOW_SOURCE_DELTA_PATH)" && test -n "$(SHADOW_SCORE_START)" && test -n "$(SHADOW_SCORE_END)" && test -n "$(SHADOW_REFERENCE_LEDGER_PATH)" || { echo "Supply inbox, source Delta, explicit UTC window and Phase 19 ledger."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.kafka_shadow.cli execute --inbox "$(SHADOW_INBOX_PATH)" --source-delta "$(SHADOW_SOURCE_DELTA_PATH)" --model-version "$(SHADOW_REGISTRY_VERSION)" --score-start "$(SHADOW_SCORE_START)" --score-end "$(SHADOW_SCORE_END)" --reference-ledger "$(SHADOW_REFERENCE_LEDGER_PATH)" --result-root "$(SHADOW_RESULT_ROOT)" --run-root "$(SHADOW_RUN_ROOT)" --maximum-events "$(SHADOW_MAX_EVENTS)" --maximum-cached-rows "$(SHADOW_MAX_CACHED_ROWS)" --maximum-attempts "$(SHADOW_MAX_ATTEMPTS)" --inference-batch-size "$(SHADOW_INFERENCE_BATCH_SIZE)" $(if $(SHADOW_FAIL_AFTER_RESULT),--fail-after-result "$(SHADOW_FAIL_AFTER_RESULT)",)

inspect-feature-ready-shadow:
	@test -n "$(SHADOW_INBOX_PATH)" && test -n "$(SHADOW_CANDIDATE_ID)" && test -n "$(SHADOW_MODEL_TREE_SHA256)" || { echo "Supply inbox, candidate ID and model-tree hash."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.kafka_shadow.cli inspect-results --inbox "$(SHADOW_INBOX_PATH)" --candidate-id "$(SHADOW_CANDIDATE_ID)" --model-tree-sha256 "$(SHADOW_MODEL_TREE_SHA256)" --result-root "$(SHADOW_RESULT_ROOT)"

verify-feature-ready-shadow-parity:
	@test -n "$(SHADOW_RUN_PATH)" || { echo "Supply SHADOW_RUN_PATH."; exit 1; }
	cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.kafka_shadow.cli inspect-run --run "$(SHADOW_RUN_PATH)"

verify-feature-ready-shadow-integration:
	@test -n "$(FEATURE_KAFKA_BATCH_PATH)" && test -n "$(SHADOW_INBOX_PATH)" || { echo "Supply Phase 25 batch and expected sealed inbox path."; exit 1; }
	$(MAKE) consume-feature-ready-shadow
	$(MAKE) seal-feature-ready-shadow-inbox
	$(MAKE) inspect-feature-ready-shadow-inbox
	$(MAKE) execute-feature-ready-shadow
	$(MAKE) publish-feature-ready-kafka FEATURE_KAFKA_REPLAY=1
	$(MAKE) consume-feature-ready-shadow
	$(MAKE) seal-feature-ready-shadow-inbox
	$(MAKE) inspect-feature-ready-shadow-inbox
	$(MAKE) execute-feature-ready-shadow

# Phase 27 is an operator-triggered, research-only lineage. No target below is part of `verify`.
.PHONY: initialize-incremental-fixture append-incremental-fixture initialize-incremental-shadow discover-incremental-features publish-incremental-shadow-batch accept-incremental-shadow-batch process-incremental-shadow-work inspect-incremental-shadow verify-incremental-shadow report-incremental-shadow verify-incremental-shadow-integration
INCREMENTAL_CLI = cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.incremental_shadow.cli

initialize-incremental-fixture:
	$(INCREMENTAL_CLI) fixture-init --source "$(INCREMENTAL_SOURCE_DELTA_PATH)"

append-incremental-fixture:
	$(INCREMENTAL_CLI) fixture-append --source "$(INCREMENTAL_SOURCE_DELTA_PATH)" --seed "$(INCREMENTAL_FIXTURE_SEED)" --first-index "$(INCREMENTAL_FIXTURE_FIRST_INDEX)" --count "$(INCREMENTAL_FIXTURE_COUNT)" --base-time "$(INCREMENTAL_FIXTURE_BASE_TIME)" $(if $(INCREMENTAL_STREAM_PATH),--observation-root "$(INCREMENTAL_STREAM_PATH)/observations",)

initialize-incremental-shadow:
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=initialize-operational-fence --bootstrap-servers=$(BOOTSTRAP_SERVERS) --fence-path=$(INCREMENTAL_FENCE_PATH) --group=$(INCREMENTAL_GROUP)"
	$(INCREMENTAL_CLI) initialize --source "$(INCREMENTAL_SOURCE_DELTA_PATH)" --root "$(INCREMENTAL_ROOT)" --fence "$(INCREMENTAL_FENCE_PATH)" --first-version "$(INCREMENTAL_FIRST_VERSION)" --model-version "$(INCREMENTAL_MODEL_VERSION)" --group "$(INCREMENTAL_GROUP)" --maximum-undiscovered-versions "$(INCREMENTAL_UNDISCOVERED_LIMIT)" --maximum-unpublished-ready-events "$(INCREMENTAL_UNPUBLISHED_LIMIT)" --maximum-accepted-unprocessed-events "$(INCREMENTAL_ACCEPTED_LIMIT)" --maximum-inference-events-per-tick "$(INCREMENTAL_TICK_LIMIT)" --maximum-cached-source-rows "$(INCREMENTAL_CACHED_ROWS_LIMIT)"

discover-incremental-features:
	@test -n "$(INCREMENTAL_STREAM_PATH)" || { echo "Supply INCREMENTAL_STREAM_PATH."; exit 1; }
	$(INCREMENTAL_CLI) discover --stream "$(INCREMENTAL_STREAM_PATH)"

publish-incremental-shadow-batch:
	@test -n "$(INCREMENTAL_STREAM_PATH)" && test -n "$(INCREMENTAL_BATCH_ID)" && test -n "$(INCREMENTAL_BATCH_PATH)" || { echo "Supply stream, batch ID and batch path."; exit 1; }
	$(INCREMENTAL_CLI) authorize-publish --stream "$(INCREMENTAL_STREAM_PATH)" --batch-id "$(INCREMENTAL_BATCH_ID)"
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=publish --bootstrap-servers=$(BOOTSTRAP_SERVERS) --batch=$(INCREMENTAL_BATCH_PATH) --journal-root=$(INCREMENTAL_STREAM_PATH)/journals --observation-root=$(INCREMENTAL_STREAM_PATH)/observations --maximum-deliveries=$(FEATURE_KAFKA_MAX_DELIVERIES) $(if $(FEATURE_KAFKA_FAIL_AFTER_ACK_AT),--fail-after-ack-before-journal-at=$(FEATURE_KAFKA_FAIL_AFTER_ACK_AT),)"

accept-incremental-shadow-batch:
	@test -n "$(INCREMENTAL_STREAM_PATH)" && test -n "$(INCREMENTAL_BATCH_ID)" && test -n "$(INCREMENTAL_BATCH_PATH)" || { echo "Supply stream, batch ID and batch path."; exit 1; }
	cd streaming-engine && sbt "runMain $(FEATURE_KAFKA_APP) --mode=accept-operational-batch --bootstrap-servers=$(BOOTSTRAP_SERVERS) --batch=$(INCREMENTAL_BATCH_PATH) --group=$(INCREMENTAL_GROUP) --journal-root=$(INCREMENTAL_STREAM_PATH)/journals --receipt-root=$(INCREMENTAL_STREAM_PATH)/receipts/$(INCREMENTAL_BATCH_ID) --preceding-boundary=$(INCREMENTAL_PRECEDING_BOUNDARY) --acceptance-path=$(INCREMENTAL_STREAM_PATH)/acceptance/$(INCREMENTAL_BATCH_ID).json --observation-root=$(INCREMENTAL_STREAM_PATH)/observations --maximum=$(FEATURE_KAFKA_MAXIMUM) $(if $(FEATURE_KAFKA_FAIL_AFTER_RECEIPT_AT),--fail-after-receipt-before-commit-at=$(FEATURE_KAFKA_FAIL_AFTER_RECEIPT_AT),)"

process-incremental-shadow-work:
	@test -n "$(INCREMENTAL_STREAM_PATH)" || { echo "Supply INCREMENTAL_STREAM_PATH."; exit 1; }
	$(INCREMENTAL_CLI) process --stream "$(INCREMENTAL_STREAM_PATH)" --model-version "$(INCREMENTAL_MODEL_VERSION)" --score-start "$(INCREMENTAL_SCORE_START)" --score-end "$(INCREMENTAL_SCORE_END)" --maximum "$(INCREMENTAL_TICK_LIMIT)" $(if $(SHADOW_FAIL_AFTER_RESULT),--fail-after-result "$(SHADOW_FAIL_AFTER_RESULT)",)

inspect-incremental-shadow:
	@test -n "$(INCREMENTAL_STREAM_PATH)" || { echo "Supply INCREMENTAL_STREAM_PATH."; exit 1; }
	$(INCREMENTAL_CLI) inspect --stream "$(INCREMENTAL_STREAM_PATH)"

verify-incremental-shadow:
	@test -n "$(INCREMENTAL_STREAM_PATH)" || { echo "Supply INCREMENTAL_STREAM_PATH."; exit 1; }
	$(INCREMENTAL_CLI) reconcile --stream "$(INCREMENTAL_STREAM_PATH)"
	@test -n "$(INCREMENTAL_CANDIDATE_PATH)" || { echo "Supply INCREMENTAL_CANDIDATE_PATH for independent offline scoring."; exit 1; }
	$(INCREMENTAL_CLI) verify-reference --stream "$(INCREMENTAL_STREAM_PATH)" --candidate "$(INCREMENTAL_CANDIDATE_PATH)"

report-incremental-shadow:
	@test -n "$(INCREMENTAL_STREAM_PATH)" || { echo "Supply INCREMENTAL_STREAM_PATH."; exit 1; }
	$(INCREMENTAL_CLI) report --stream "$(INCREMENTAL_STREAM_PATH)"

verify-incremental-shadow-integration:
	@echo "Run fixture waves and the explicit publish/accept/process targets between each wave; then make verify-incremental-shadow. This target never fabricates a continuous experiment."
	$(MAKE) verify-incremental-shadow

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

# Phase 28 is explicit and Docker-dependent; ordinary `make verify` remains local-only.
.PHONY: capture-phase28-transaction-fence initialize-stateful-shadow-integration prime-stateful-shadow-ingestion run-stateful-shadow-wave reconcile-stateful-shadow-wave handoff-stateful-shadow-wave inspect-stateful-shadow-transport verify-stateful-shadow-integration trace-stateful-shadow-event build-stateful-shadow-evidence inspect-stateful-shadow-evidence
PHASE28_CLI = cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.stateful_shadow_integration.cli

capture-phase28-transaction-fence:
	@test -n "$(PHASE28_TRANSACTION_FENCE)" || { echo "Supply a fresh PHASE28_TRANSACTION_FENCE path."; exit 1; }
	$(MAKE) verify-infra
	cd streaming-engine && sbt "runMain io.sentinelaegisforge.streaming.kafka.ingestion.TransactionTopicFenceApp --bootstrap-servers=$(BOOTSTRAP_SERVERS) --output-path=$(PHASE28_TRANSACTION_FENCE)"

initialize-stateful-shadow-integration:
	@test -n "$(PHASE28_CORPUS_PATH)" && test -n "$(PHASE28_TRANSACTION_FENCE)" || { echo "Supply PHASE28_CORPUS_PATH and PHASE28_TRANSACTION_FENCE."; exit 1; }
	$(PHASE28_CLI) initialize --corpus "$(PHASE28_CORPUS_PATH)" --root "$(PHASE28_ROOT)" --transaction-fence "$(PHASE28_TRANSACTION_FENCE)"

prime-stateful-shadow-ingestion:
	@test -n "$(PHASE28_INTEGRATION_PATH)" || { echo "Supply PHASE28_INTEGRATION_PATH."; exit 1; }
	$(PHASE28_CLI) prime --integration "$(PHASE28_INTEGRATION_PATH)"

run-stateful-shadow-wave:
	@test -n "$(PHASE28_INTEGRATION_PATH)" && test -n "$(PHASE28_WAVE)" || { echo "Supply PHASE28_INTEGRATION_PATH and PHASE28_WAVE."; exit 1; }
	$(PHASE28_CLI) run-wave --integration "$(PHASE28_INTEGRATION_PATH)" --wave "$(PHASE28_WAVE)"

reconcile-stateful-shadow-wave:
	@test -n "$(PHASE28_INTEGRATION_PATH)" && test -n "$(PHASE28_WAVE)" || { echo "Supply PHASE28_INTEGRATION_PATH and PHASE28_WAVE."; exit 1; }
	$(PHASE28_CLI) inspect-wave --integration "$(PHASE28_INTEGRATION_PATH)" --wave "$(PHASE28_WAVE)"

handoff-stateful-shadow-wave:
	@test -n "$(PHASE28_INTEGRATION_PATH)" && test -n "$(PHASE28_STREAM_PATH)" && test -n "$(PHASE28_WAVE)" || { echo "Supply PHASE28_INTEGRATION_PATH, PHASE28_STREAM_PATH and PHASE28_WAVE."; exit 1; }
	$(PHASE28_CLI) handoff --integration "$(PHASE28_INTEGRATION_PATH)" --stream "$(PHASE28_STREAM_PATH)" --wave "$(PHASE28_WAVE)" $(if $(PHASE28_PAUSE_INFERENCE),--pause-inference,) $(if $(PHASE28_DEMONSTRATE_BACKPRESSURE),--demonstrate-backpressure,)

inspect-stateful-shadow-transport:
	@test -n "$(PHASE28_INTEGRATION_PATH)" && test -n "$(PHASE28_STREAM_PATH)" || { echo "Supply PHASE28_INTEGRATION_PATH and PHASE28_STREAM_PATH."; exit 1; }
	$(PHASE28_CLI) inspect-transport --integration "$(PHASE28_INTEGRATION_PATH)" --stream "$(PHASE28_STREAM_PATH)"

verify-stateful-shadow-integration:
	@test -n "$(PHASE28_INTEGRATION_PATH)" && test -n "$(PHASE28_STREAM_PATH)" || { echo "Supply PHASE28_INTEGRATION_PATH and PHASE28_STREAM_PATH."; exit 1; }
	$(PHASE28_CLI) verify-final --integration "$(PHASE28_INTEGRATION_PATH)" --stream "$(PHASE28_STREAM_PATH)"

trace-stateful-shadow-event:
	@test -n "$(PHASE28_INTEGRATION_PATH)" && test -n "$(PHASE28_STREAM_PATH)" && test -n "$(PHASE28_EVENT_ID)" || { echo "Supply integration, stream and event ID."; exit 1; }
	$(PHASE28_CLI) trace --integration "$(PHASE28_INTEGRATION_PATH)" --stream "$(PHASE28_STREAM_PATH)" --event-id "$(PHASE28_EVENT_ID)"

build-stateful-shadow-evidence:
	@test -n "$(PHASE28_INTEGRATION_PATH)" && test -n "$(PHASE28_STREAM_PATH)" && test -n "$(INCREMENTAL_CANDIDATE_PATH)" && test -n "$(PHASE28_RECOVERY_EVIDENCE)" || { echo "Supply integration, stream, candidate and measured recovery evidence."; exit 1; }
	$(PHASE28_CLI) build-evidence --integration "$(PHASE28_INTEGRATION_PATH)" --stream "$(PHASE28_STREAM_PATH)" --candidate "$(INCREMENTAL_CANDIDATE_PATH)" --recovery "$(PHASE28_RECOVERY_EVIDENCE)"

inspect-stateful-shadow-evidence:
	@test -n "$(PHASE28_INTEGRATION_PATH)" || { echo "Supply PHASE28_INTEGRATION_PATH."; exit 1; }
	$(PHASE28_CLI) inspect-evidence --integration "$(PHASE28_INTEGRATION_PATH)"

# Phase 29 operations are explicit and reuse Phase 28's isolated lineages.
.PHONY: verify-reliability-unit run-reliability-wave inject-statistical-postcommit-failure verify-statistical-postcommit-recovery verify-reliability-integration record-reliability-broker-failure verify-reliability-outage inspect-streaming-state inspect-source-retention inspect-reliability-experiment build-reliability-dossier inspect-reliability-dossier
PHASE29_CLI = cd model-control-plane && uv run --locked python -m sentinelaegisforge_control_plane.reliability.cli

verify-reliability-unit:
	cd model-control-plane && uv run --locked pytest tests/test_reliability.py

run-reliability-wave:
	@test -n "$(PHASE29_INTEGRATION_PATH)" && test -n "$(PHASE29_WAVE)" || { echo "Supply PHASE29_INTEGRATION_PATH and PHASE29_WAVE."; exit 1; }
	$(PHASE29_CLI) run-wave --integration "$(PHASE29_INTEGRATION_PATH)" --wave "$(PHASE29_WAVE)"

inject-statistical-postcommit-failure:
	@test -n "$(PHASE29_INTEGRATION_PATH)" && test -n "$(PHASE29_WAVE)" || { echo "Supply PHASE29_INTEGRATION_PATH and PHASE29_WAVE."; exit 1; }
	$(PHASE29_CLI) fault-wave --integration "$(PHASE29_INTEGRATION_PATH)" --wave "$(PHASE29_WAVE)" --batch-id "$(PHASE29_FAULT_BATCH_ID)"

verify-statistical-postcommit-recovery:
	@test -n "$(PHASE29_INTEGRATION_PATH)" || { echo "Supply PHASE29_INTEGRATION_PATH."; exit 1; }
	$(PHASE29_CLI) inspect-recovery --integration "$(PHASE29_INTEGRATION_PATH)"

verify-reliability-integration: inspect-reliability-experiment

record-reliability-broker-failure:
	@test -n "$(PHASE29_INTEGRATION_PATH)" && test -n "$(PHASE29_STREAM_PATH)" && test -n "$(PHASE29_BROKER_BATCH_ID)" && test -n "$(PHASE29_BROKER_FAILURE_LOG)" && test -n "$(PHASE29_BROKER_EXIT_CODE)" || { echo "Supply the D integration/stream, failed batch, log and nonzero exit code."; exit 1; }
	$(PHASE29_CLI) record-broker-failure --integration "$(PHASE29_INTEGRATION_PATH)" --stream "$(PHASE29_STREAM_PATH)" --batch-id "$(PHASE29_BROKER_BATCH_ID)" --log "$(PHASE29_BROKER_FAILURE_LOG)" --exit-code "$(PHASE29_BROKER_EXIT_CODE)"

verify-reliability-outage:
	@test -n "$(PHASE29_INTEGRATION_PATH)" && test -n "$(PHASE29_STREAM_PATH)" && test -n "$(PHASE29_OUTAGE_EVIDENCE)" || { echo "Supply D integration, stream and new output path."; exit 1; }
	$(PHASE29_CLI) verify-outage --integration "$(PHASE29_INTEGRATION_PATH)" --stream "$(PHASE29_STREAM_PATH)" --output "$(PHASE29_OUTAGE_EVIDENCE)"

inspect-streaming-state:
	@test -n "$(PHASE29_INTEGRATION_PATH)" || { echo "Supply PHASE29_INTEGRATION_PATH."; exit 1; }
	$(PHASE29_CLI) inspect-state --integration "$(PHASE29_INTEGRATION_PATH)"

inspect-source-retention:
	@test -n "$(PHASE29_INTEGRATION_PATH)" && test -n "$(PHASE29_CURRENT_FENCE)" || { echo "Supply PHASE29_INTEGRATION_PATH and PHASE29_CURRENT_FENCE."; exit 1; }
	$(PHASE29_CLI) inspect-retention --integration "$(PHASE29_INTEGRATION_PATH)" --current-fence "$(PHASE29_CURRENT_FENCE)"

inspect-reliability-experiment:
	@test -n "$(PHASE29_INTEGRATION_PATH)" && test -n "$(PHASE29_STREAM_PATH)" || { echo "Supply PHASE29_INTEGRATION_PATH and PHASE29_STREAM_PATH."; exit 1; }
	$(PHASE29_CLI) observe --integration "$(PHASE29_INTEGRATION_PATH)" --stream "$(PHASE29_STREAM_PATH)" $(if $(PHASE29_OBSERVATION_OUTPUT),--output "$(PHASE29_OBSERVATION_OUTPUT)",)

build-reliability-dossier:
	@test -n "$(PHASE29_READY_OFFSETS_LOG)" || { echo "Supply PHASE29_READY_OFFSETS_LOG from feature-kafka-offsets."; exit 1; }
	@test -n "$(PHASE29_A_INTEGRATION)" && test -n "$(PHASE29_B_INTEGRATION)" && test -n "$(PHASE29_C_INTEGRATION)" && test -n "$(PHASE29_D_INTEGRATION)" && test -n "$(PHASE29_A_STREAM)" && test -n "$(PHASE29_B_STREAM)" && test -n "$(PHASE29_C_STREAM)" && test -n "$(PHASE29_D_STREAM)" && test -n "$(PHASE29_OUTAGE_EVIDENCE)" && test -n "$(PHASE29_CURRENT_FENCE)" || { echo "Supply all Phase 29 integration/stream paths, outage evidence and current Kafka fence."; exit 1; }
	$(PHASE29_CLI) build-dossier --a-integration "$(PHASE29_A_INTEGRATION)" --b-integration "$(PHASE29_B_INTEGRATION)" --c-integration "$(PHASE29_C_INTEGRATION)" --d-integration "$(PHASE29_D_INTEGRATION)" --a-stream "$(PHASE29_A_STREAM)" --b-stream "$(PHASE29_B_STREAM)" --c-stream "$(PHASE29_C_STREAM)" --d-stream "$(PHASE29_D_STREAM)" --outage-evidence "$(PHASE29_OUTAGE_EVIDENCE)" --candidate "$(PHASE29_CANDIDATE)" --current-transaction-fence "$(PHASE29_CURRENT_FENCE)" --current-ready-offset-log "$(PHASE29_READY_OFFSETS_LOG)" --root "$(PHASE29_DOSSIER_ROOT)"

inspect-reliability-dossier:
	@test -n "$(PHASE29_DOSSIER_PATH)" || { echo "Supply PHASE29_DOSSIER_PATH."; exit 1; }
	$(PHASE29_CLI) inspect-dossier --path "$(PHASE29_DOSSIER_PATH)"

.PHONY: compare-reliability-experiments
compare-reliability-experiments:
	@test -n "$(PHASE29_BASELINE_OBSERVATION)" && test -n "$(PHASE29_CHANGED_OBSERVATION)" || { echo "Supply saved baseline and changed-workload observation JSON paths."; exit 1; }
	$(PHASE29_CLI) compare --baseline "$(PHASE29_BASELINE_OBSERVATION)" --changed "$(PHASE29_CHANGED_OBSERVATION)"

.PHONY: record-reliability-backlog record-reliability-backpressure
record-reliability-backlog:
	@test -n "$(PHASE29_INTEGRATION_PATH)" && test -n "$(PHASE29_STREAM_PATH)" && test -n "$(PHASE29_BACKLOG_LABEL)" || { echo "Supply integration, stream and controlled backlog label."; exit 1; }
	$(PHASE29_CLI) record-backlog --integration "$(PHASE29_INTEGRATION_PATH)" --stream "$(PHASE29_STREAM_PATH)" --label "$(PHASE29_BACKLOG_LABEL)"

record-reliability-backpressure:
	@test -n "$(PHASE29_INTEGRATION_PATH)" && test -n "$(PHASE29_STREAM_PATH)" && test -n "$(PHASE29_BROKER_BATCH_ID)" || { echo "Supply integration, stream and pending batch ID."; exit 1; }
	$(PHASE29_CLI) record-backpressure --integration "$(PHASE29_INTEGRATION_PATH)" --stream "$(PHASE29_STREAM_PATH)" --batch-id "$(PHASE29_BROKER_BATCH_ID)"

# Phase 31: explicit operator actions; none are dependencies of ordinary verification.
RECOVERY_CLI = cd model-control-plane && uv run --frozen python -m sentinelaegisforge_control_plane.recovery.cli
RECOVERY_ARGS ?=
.PHONY: recovery inspect-recovery-bundle inspect-restored-research-lineage inspect-recovery-rehearsal
recovery:
	@test -n "$(RECOVERY_ACTION)" || { echo "Supply RECOVERY_ACTION and explicit absolute paths in RECOVERY_ARGS; see recovery documentation."; exit 1; }
	$(RECOVERY_CLI) $(RECOVERY_ACTION) $(RECOVERY_ARGS)

inspect-recovery-bundle:
	$(RECOVERY_CLI) verify --bundle "$(RECOVERY_BUNDLE)"

inspect-restored-research-lineage:
	$(RECOVERY_CLI) preflight-docker --output "$(RECOVERY_OUTPUT)"

inspect-recovery-rehearsal:
	$(RECOVERY_CLI) inspect-rehearsal --output "$(RECOVERY_OUTPUT)"
