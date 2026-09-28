package io.sentinelaegisforge.streaming.processing.rollingfeatures

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionIngestionConfig
import io.sentinelaegisforge.streaming.processing.customerstate.CustomerActivityConfig
import io.sentinelaegisforge.streaming.processing.deduplication.TransactionDeduplicationConfig

final case class RollingFeatureConfig(
    deduplicatedDeltaPath: String,
    featureDeltaPath: String,
    checkpointLocation: String,
    deltaTxnAppId: String,
    watermarkDelay: String,
    sparkMaster: String
) {
  require(deduplicatedDeltaPath.trim.nonEmpty)
  require(featureDeltaPath.trim.nonEmpty)
  require(checkpointLocation.trim.nonEmpty)
  require(deltaTxnAppId.trim.nonEmpty)
  require(watermarkDelay.trim.nonEmpty)
  require(sparkMaster.trim.nonEmpty)
}

object RollingFeatureConfig {
  val DefaultDeduplicatedDeltaPath: String =
    TransactionDeduplicationConfig.DefaultDeduplicatedDeltaPath
  val DefaultFeatureDeltaPath: String = ".local/delta/transaction_customer_features"
  val DefaultCheckpointLocation: String = ".local/checkpoints/customer-rolling-features"
  val DefaultTxnAppId: String = "sentinel-transaction-customer-features-v1"
  val DefaultWatermarkDelay: String = "10 minutes"
  val DefaultSparkMaster: String = TransactionIngestionConfig.DefaultSparkMaster

  val StateStoreProviderConfig: String = CustomerActivityConfig.StateStoreProviderConfig
  val RequiredStateStoreProvider: String = CustomerActivityConfig.RequiredStateStoreProvider
}
