package io.sentinelaegisforge.streaming.processing.rollingfeatures

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, TimeMode}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

import io.sentinelaegisforge.streaming.persistence.delta.ValidatedTransactionDeltaSchema

object RollingFeatureTransformer {
  def transform(
      source: DataFrame,
      watermarkDelay: String
  ): Dataset[TransactionCustomerFeatures] = {
    require(watermarkDelay.trim.nonEmpty, "watermarkDelay must not be blank")
    val actual = source.schema.fields.toVector.map(field => field.name -> field.dataType)
    require(
      actual == ValidatedTransactionDeltaSchema.columns,
      s"Unexpected deduplicated-transaction source schema: $actual"
    )

    val input = source
      .withWatermark("event_time", watermarkDelay)
      .select(
        col("event_id").as("eventId"),
        col("transaction_id").as("transactionId"),
        col("customer_id").as("customerId"),
        col("merchant_id").as("merchantId"),
        col("event_time").as("eventTime"),
        col("ingestion_time").as("ingestionTime"),
        col("amount"),
        col("currency"),
        col("country"),
        col("device_id").as("deviceId"),
        col("ip_address").as("ipAddress"),
        col("transaction_type").as("transactionType"),
        col("schema_version").as("schemaVersion"),
        col("kafka_key").as("kafkaKey"),
        col("kafka_topic").as("kafkaTopic"),
        col("kafka_partition").as("kafkaPartition"),
        col("kafka_offset").as("kafkaOffset"),
        col("kafka_timestamp").as("kafkaTimestamp")
      )
      .as[RollingFeatureInput](Encoders.product[RollingFeatureInput])

    implicit val outputEncoder = Encoders.product[TransactionCustomerFeatures]
    input
      .groupByKey(_.customerId)(Encoders.STRING)
      .transformWithState(
        new RollingFeatureProcessor,
        TimeMode.EventTime(),
        OutputMode.Update()
      )
  }
}
