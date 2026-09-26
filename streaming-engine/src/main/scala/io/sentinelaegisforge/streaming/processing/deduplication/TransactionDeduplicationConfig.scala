package io.sentinelaegisforge.streaming.processing.deduplication

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionIngestionConfig
import io.sentinelaegisforge.streaming.persistence.delta.DeltaIngestionConfig

final case class TransactionDeduplicationConfig(
    validatedDeltaPath: String,
    deduplicatedDeltaPath: String,
    checkpointLocation: String,
    deltaTxnAppId: String,
    watermarkDelay: String,
    sparkMaster: String
) {
  require(validatedDeltaPath.trim.nonEmpty, "validatedDeltaPath must not be blank")
  require(deduplicatedDeltaPath.trim.nonEmpty, "deduplicatedDeltaPath must not be blank")
  require(checkpointLocation.trim.nonEmpty, "checkpointLocation must not be blank")
  require(deltaTxnAppId.trim.nonEmpty, "deltaTxnAppId must not be blank")
  require(watermarkDelay.trim.nonEmpty, "watermarkDelay must not be blank")
  require(sparkMaster.trim.nonEmpty, "sparkMaster must not be blank")
}

object TransactionDeduplicationConfig {
  val DefaultValidatedDeltaPath: String = DeltaIngestionConfig.DefaultDeltaPath
  val DefaultDeduplicatedDeltaPath: String =
    ".local/delta/transactions_deduplicated"
  val DefaultCheckpointLocation: String =
    ".local/checkpoints/transaction-deduplication"
  val DefaultTxnAppId: String = "sentinel-transactions-deduplicated-v1"
  val DefaultWatermarkDelay: String = "10 minutes"
  val DefaultSparkMaster: String = TransactionIngestionConfig.DefaultSparkMaster
}
