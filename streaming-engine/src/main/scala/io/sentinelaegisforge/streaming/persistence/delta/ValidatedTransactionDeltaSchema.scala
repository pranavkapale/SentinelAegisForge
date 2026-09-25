package io.sentinelaegisforge.streaming.persistence.delta

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{
  DataType,
  DecimalType,
  IntegerType,
  LongType,
  StringType,
  TimestampType
}
import org.apache.spark.sql.{DataFrame, Dataset}

import io.sentinelaegisforge.streaming.kafka.ingestion.ValidatedTransactionRecord

object ValidatedTransactionDeltaSchema {
  val columns: Vector[(String, DataType)] = Vector(
    "event_id" -> StringType,
    "transaction_id" -> StringType,
    "customer_id" -> StringType,
    "merchant_id" -> StringType,
    "event_time" -> TimestampType,
    "ingestion_time" -> TimestampType,
    "amount" -> DecimalType(18, 4),
    "currency" -> StringType,
    "country" -> StringType,
    "device_id" -> StringType,
    "ip_address" -> StringType,
    "transaction_type" -> StringType,
    "schema_version" -> IntegerType,
    "kafka_key" -> StringType,
    "kafka_topic" -> StringType,
    "kafka_partition" -> IntegerType,
    "kafka_offset" -> LongType,
    "kafka_timestamp" -> TimestampType
  )

  def project(records: Dataset[ValidatedTransactionRecord]): DataFrame = {
    val projected = records.select(
      col("eventId").cast(StringType).as("event_id"),
      col("transactionId").cast(StringType).as("transaction_id"),
      col("customerId").cast(StringType).as("customer_id"),
      col("merchantId").cast(StringType).as("merchant_id"),
      col("eventTime").cast(TimestampType).as("event_time"),
      col("ingestionTime").cast(TimestampType).as("ingestion_time"),
      col("amount").cast(DecimalType(18, 4)).as("amount"),
      col("currency").cast(StringType).as("currency"),
      col("country").cast(StringType).as("country"),
      col("deviceId").cast(StringType).as("device_id"),
      col("ipAddress").cast(StringType).as("ip_address"),
      col("transactionType").cast(StringType).as("transaction_type"),
      col("schemaVersion").cast(IntegerType).as("schema_version"),
      col("kafkaKey").cast(StringType).as("kafka_key"),
      col("kafkaTopic").cast(StringType).as("kafka_topic"),
      col("kafkaPartition").cast(IntegerType).as("kafka_partition"),
      col("kafkaOffset").cast(LongType).as("kafka_offset"),
      col("kafkaTimestamp").cast(TimestampType).as("kafka_timestamp")
    )

    requireExpectedSchema(projected)
    projected
  }

  private[delta] def requireExpectedSchema(frame: DataFrame): Unit = {
    val actual = frame.schema.fields.toVector.map(field => field.name -> field.dataType)
    require(actual == columns, s"Unexpected validated-transaction storage schema: $actual")
  }
}
