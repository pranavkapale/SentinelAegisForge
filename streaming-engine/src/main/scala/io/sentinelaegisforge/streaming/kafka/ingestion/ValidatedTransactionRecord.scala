package io.sentinelaegisforge.streaming.kafka.ingestion

import java.sql.Timestamp

import io.sentinelaegisforge.streaming.domain.transaction.TransactionEvent

/** Spark-friendly record created only after wire decoding and domain validation succeed. */
final case class ValidatedTransactionRecord(
    eventId: String,
    transactionId: String,
    customerId: String,
    merchantId: String,
    eventTime: Timestamp,
    ingestionTime: Timestamp,
    amount: java.math.BigDecimal,
    currency: String,
    country: String,
    deviceId: String,
    ipAddress: String,
    transactionType: String,
    schemaVersion: Int,
    kafkaKey: String,
    kafkaTopic: String,
    kafkaPartition: Int,
    kafkaOffset: Long,
    kafkaTimestamp: Timestamp
)

object ValidatedTransactionRecord {
  def from(
      event: TransactionEvent,
      kafkaKey: String,
      source: KafkaSourceRecord
  ): ValidatedTransactionRecord =
    ValidatedTransactionRecord(
      eventId = event.eventId.toString,
      transactionId = event.transactionId,
      customerId = event.customerId,
      merchantId = event.merchantId,
      eventTime = Timestamp.from(event.eventTime),
      ingestionTime = Timestamp.from(event.ingestionTime),
      amount = event.amount.bigDecimal,
      currency = event.currency,
      country = event.country,
      deviceId = event.deviceId,
      ipAddress = event.ipAddress,
      transactionType = event.transactionType.externalName,
      schemaVersion = event.schemaVersion,
      kafkaKey = kafkaKey,
      kafkaTopic = source.topic,
      kafkaPartition = source.partition,
      kafkaOffset = source.offset,
      kafkaTimestamp = source.timestamp
    )
}
