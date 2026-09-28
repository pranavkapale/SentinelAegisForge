package io.sentinelaegisforge.streaming.processing.rollingfeatures

import java.sql.Timestamp

/** The deduplicated transaction and its transport lineage, before feature calculation. */
final case class RollingFeatureInput(
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

/** Only the event-time and amount needed by the two rolling windows. */
final case class RollingCustomerEvent(eventTimeMicros: Long, amount: java.math.BigDecimal)

/** One emitted row per accepted deduplicated event.
  *
  * The aggregate travels through Spark's product encoder as exact decimal text. The Delta
  * projection validates and casts it to decimal(38,4), avoiding the product encoder's default
  * decimal(38,18) limit on BigDecimal fields.
  */
final case class TransactionCustomerFeatures(
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
    kafkaTimestamp: Timestamp,
    priorTransactionCount5m: Long,
    priorAmountSum10m: String
)

object TransactionCustomerFeatures {
  def from(
      input: RollingFeatureInput,
      priorCount: Long,
      priorAmount: java.math.BigDecimal
  ): TransactionCustomerFeatures =
    TransactionCustomerFeatures(
      input.eventId,
      input.transactionId,
      input.customerId,
      input.merchantId,
      input.eventTime,
      input.ingestionTime,
      input.amount,
      input.currency,
      input.country,
      input.deviceId,
      input.ipAddress,
      input.transactionType,
      input.schemaVersion,
      input.kafkaKey,
      input.kafkaTopic,
      input.kafkaPartition,
      input.kafkaOffset,
      input.kafkaTimestamp,
      priorCount,
      priorAmount.toPlainString
    )
}
