package io.sentinelaegisforge.streaming.domain.transaction

/** Untrusted boundary values that have not yet passed domain validation.
  *
  * Strings and optionality are intentional: this type must represent missing and malformed input
  * without depending on a transport or serialization format.
  */
final case class TransactionEventCandidate(
    eventId: Option[String],
    transactionId: Option[String],
    customerId: Option[String],
    merchantId: Option[String],
    eventTime: Option[String],
    ingestionTime: Option[String],
    amount: Option[String],
    currency: Option[String],
    country: Option[String],
    deviceId: Option[String],
    ipAddress: Option[String],
    transactionType: Option[String],
    schemaVersion: Option[String]
)
