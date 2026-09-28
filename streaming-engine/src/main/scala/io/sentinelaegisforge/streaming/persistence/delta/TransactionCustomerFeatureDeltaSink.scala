package io.sentinelaegisforge.streaming.persistence.delta

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, DecimalType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset}

import io.sentinelaegisforge.streaming.processing.rollingfeatures.{
  RollingFeatureProcessor,
  TransactionCustomerFeatures
}

object TransactionCustomerFeatureDeltaSchema {
  val columns: Vector[(String, DataType)] =
    ValidatedTransactionDeltaSchema.columns ++ Vector(
      "prior_transaction_count_5m" -> LongType,
      "prior_amount_sum_10m" -> DecimalType(
        RollingFeatureProcessor.AmountPrecision,
        RollingFeatureProcessor.AmountScale
      )
    )

  def project(features: Dataset[TransactionCustomerFeatures]): DataFrame = {
    val storage = features.select(
      col("eventId").as("event_id"),
      col("transactionId").as("transaction_id"),
      col("customerId").as("customer_id"),
      col("merchantId").as("merchant_id"),
      col("eventTime").as("event_time"),
      col("ingestionTime").as("ingestion_time"),
      col("amount").cast(DecimalType(18, 4)).as("amount"),
      col("currency"),
      col("country"),
      col("deviceId").as("device_id"),
      col("ipAddress").as("ip_address"),
      col("transactionType").as("transaction_type"),
      col("schemaVersion").as("schema_version"),
      col("kafkaKey").as("kafka_key"),
      col("kafkaTopic").as("kafka_topic"),
      col("kafkaPartition").as("kafka_partition"),
      col("kafkaOffset").as("kafka_offset"),
      col("kafkaTimestamp").as("kafka_timestamp"),
      col("priorTransactionCount5m").as("prior_transaction_count_5m"),
      col("priorAmountSum10m")
        .cast(
          DecimalType(RollingFeatureProcessor.AmountPrecision, RollingFeatureProcessor.AmountScale)
        )
        .as("prior_amount_sum_10m")
    )
    val actual = storage.schema.fields.toVector.map(field => field.name -> field.dataType)
    require(actual == columns, s"Unexpected customer-feature storage schema: $actual")
    storage
  }
}

final class TransactionCustomerFeatureDeltaSink(
    deltaPath: String,
    txnAppId: String,
    postCommitHook: DeltaPostCommitHook = DeltaPostCommitHook.NoOp
) extends Serializable {
  private val delegate = new DeltaTransactionSink(deltaPath, txnAppId, postCommitHook)

  def plan(batchId: Long): DeltaBatchWritePlan = delegate.plan(batchId)

  def writeBatch(batch: Dataset[TransactionCustomerFeatures], batchId: Long): Unit = {
    val storageFrame = TransactionCustomerFeatureDeltaSchema.project(batch).persist()
    try {
      if (storageFrame.isEmpty) {
        println(s"Customer-feature Delta batch skipped because it was empty: batchId=$batchId")
      } else {
        delegate.writeNonEmptyStorageBatch(storageFrame, batchId)
      }
    } finally {
      storageFrame.unpersist()
    }
  }
}
