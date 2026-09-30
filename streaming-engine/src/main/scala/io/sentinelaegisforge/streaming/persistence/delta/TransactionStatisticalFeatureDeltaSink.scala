package io.sentinelaegisforge.streaming.persistence.delta

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset}

import io.sentinelaegisforge.streaming.processing.statisticalfeatures.TransactionStatisticalFeatures

object TransactionStatisticalFeatureDeltaSchema {
  val columns: Vector[(String, DataType)] =
    TransactionCustomerFeatureDeltaSchema.columns ++ Vector(
      "prior_amount_observation_count" -> LongType,
      "prior_amount_mean" -> DoubleType,
      "prior_amount_stddev" -> DoubleType,
      "amount_zscore" -> DoubleType,
      "statistical_feature_status" -> StringType
    )

  def project(features: Dataset[TransactionStatisticalFeatures]): DataFrame = {
    val storage = features.select(
      col("base.eventId").as("event_id"),
      col("base.transactionId").as("transaction_id"),
      col("base.customerId").as("customer_id"),
      col("base.merchantId").as("merchant_id"),
      col("base.eventTime").as("event_time"),
      col("base.ingestionTime").as("ingestion_time"),
      col("base.amount").cast("decimal(18,4)").as("amount"),
      col("base.currency").as("currency"),
      col("base.country").as("country"),
      col("base.deviceId").as("device_id"),
      col("base.ipAddress").as("ip_address"),
      col("base.transactionType").as("transaction_type"),
      col("base.schemaVersion").as("schema_version"),
      col("base.kafkaKey").as("kafka_key"),
      col("base.kafkaTopic").as("kafka_topic"),
      col("base.kafkaPartition").as("kafka_partition"),
      col("base.kafkaOffset").as("kafka_offset"),
      col("base.kafkaTimestamp").as("kafka_timestamp"),
      col("base.priorTransactionCount5m").as("prior_transaction_count_5m"),
      col("base.priorAmountSum10m").cast("decimal(38,4)").as("prior_amount_sum_10m"),
      col("priorAmountObservationCount").as("prior_amount_observation_count"),
      col("priorAmountMean").as("prior_amount_mean"),
      col("priorAmountStddev").as("prior_amount_stddev"),
      col("amountZscore").as("amount_zscore"),
      col("statisticalFeatureStatus").as("statistical_feature_status")
    )
    val actual = storage.schema.fields.toVector.map(field => field.name -> field.dataType)
    require(actual == columns, s"Unexpected statistical-feature storage schema: $actual")
    storage
  }
}

final class TransactionStatisticalFeatureDeltaSink(
    deltaPath: String,
    txnAppId: String,
    postCommitHook: DeltaPostCommitHook = DeltaPostCommitHook.NoOp
) extends Serializable {
  private val delegate = new DeltaTransactionSink(deltaPath, txnAppId, postCommitHook)

  def plan(batchId: Long): DeltaBatchWritePlan = delegate.plan(batchId)

  def writeBatch(batch: Dataset[TransactionStatisticalFeatures], batchId: Long): Unit = {
    val storage = TransactionStatisticalFeatureDeltaSchema.project(batch).persist()
    try {
      if (storage.isEmpty) {
        println(s"Statistical-feature Delta batch skipped because it was empty: batchId=$batchId")
      } else {
        delegate.writeNonEmptyStorageBatch(storage, batchId)
      }
    } finally {
      storage.unpersist()
    }
  }
}
