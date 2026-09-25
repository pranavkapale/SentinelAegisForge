package io.sentinelaegisforge.streaming.persistence.delta

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionIngestionConfig

final case class DeltaIngestionConfig(
    source: TransactionIngestionConfig,
    deltaPath: String,
    deltaTxnAppId: String,
    failAfterDeltaBatchId: Option[Long]
) {
  require(deltaPath.trim.nonEmpty, "deltaPath must not be blank")
  require(deltaTxnAppId.trim.nonEmpty, "deltaTxnAppId must not be blank")
  require(
    failAfterDeltaBatchId.forall(_ >= 0L),
    "failAfterDeltaBatchId must be non-negative when supplied"
  )
}

object DeltaIngestionConfig {
  val DefaultDeltaPath: String = ".local/delta/transactions_validated"
  val DefaultCheckpointLocation: String =
    ".local/checkpoints/delta-transaction-ingestion"
  val DefaultTxnAppId: String = "sentinel-transactions-validated-v1"
}
