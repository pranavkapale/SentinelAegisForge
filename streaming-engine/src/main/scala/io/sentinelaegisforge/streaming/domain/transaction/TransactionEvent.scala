package io.sentinelaegisforge.streaming.domain.transaction

import java.time.Instant
import java.util.UUID

/** A validated version 1 transaction event.
  *
  * Construction is restricted to the transaction domain package so untrusted boundary values must
  * pass through [[TransactionEventValidator]].
  */
final class TransactionEvent private[transaction] (
    val eventId: UUID,
    val transactionId: String,
    val customerId: String,
    val merchantId: String,
    val eventTime: Instant,
    val ingestionTime: Instant,
    val amount: BigDecimal,
    val currency: String,
    val country: String,
    val deviceId: String,
    val ipAddress: String,
    val transactionType: TransactionType,
    val schemaVersion: Int
) {
  override def equals(other: Any): Boolean = other match {
    case that: TransactionEvent =>
      eventId == that.eventId &&
      transactionId == that.transactionId &&
      customerId == that.customerId &&
      merchantId == that.merchantId &&
      eventTime == that.eventTime &&
      ingestionTime == that.ingestionTime &&
      amount == that.amount &&
      currency == that.currency &&
      country == that.country &&
      deviceId == that.deviceId &&
      ipAddress == that.ipAddress &&
      transactionType == that.transactionType &&
      schemaVersion == that.schemaVersion
    case _ => false
  }

  override def hashCode(): Int = {
    var result = eventId.hashCode()
    result = 31 * result + transactionId.hashCode
    result = 31 * result + customerId.hashCode
    result = 31 * result + merchantId.hashCode
    result = 31 * result + eventTime.hashCode
    result = 31 * result + ingestionTime.hashCode
    result = 31 * result + amount.hashCode
    result = 31 * result + currency.hashCode
    result = 31 * result + country.hashCode
    result = 31 * result + deviceId.hashCode
    result = 31 * result + ipAddress.hashCode
    result = 31 * result + transactionType.hashCode
    result = 31 * result + schemaVersion
    result
  }

  override def toString: String =
    s"TransactionEvent(eventId=$eventId, transactionId=$transactionId, customerId=$customerId, " +
      s"merchantId=$merchantId, eventTime=$eventTime, ingestionTime=$ingestionTime, " +
      s"amount=$amount, currency=$currency, country=$country, deviceId=$deviceId, " +
      s"ipAddress=$ipAddress, transactionType=${transactionType.externalName}, " +
      s"schemaVersion=$schemaVersion)"
}
