package io.sentinelaegisforge.streaming.domain.transaction

sealed abstract class TransactionField(val externalName: String) extends Product with Serializable

object TransactionField {
  case object EventId extends TransactionField("event_id")
  case object TransactionId extends TransactionField("transaction_id")
  case object CustomerId extends TransactionField("customer_id")
  case object MerchantId extends TransactionField("merchant_id")
  case object EventTime extends TransactionField("event_time")
  case object IngestionTime extends TransactionField("ingestion_time")
  case object Amount extends TransactionField("amount")
  case object Currency extends TransactionField("currency")
  case object Country extends TransactionField("country")
  case object DeviceId extends TransactionField("device_id")
  case object IpAddress extends TransactionField("ip_address")
  case object TransactionType extends TransactionField("transaction_type")
  case object SchemaVersion extends TransactionField("schema_version")

  val all: Vector[TransactionField] = Vector(
    EventId,
    TransactionId,
    CustomerId,
    MerchantId,
    EventTime,
    IngestionTime,
    Amount,
    Currency,
    Country,
    DeviceId,
    IpAddress,
    TransactionType,
    SchemaVersion
  )
}
