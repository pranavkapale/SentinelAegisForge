package io.sentinelaegisforge.streaming.serialization.avro

import java.nio.ByteBuffer
import java.time.Instant

import scala.util.control.NonFatal

import org.apache.avro.{Conversions, LogicalTypes, Schema}
import org.apache.avro.generic.{GenericData, GenericRecord}

import io.sentinelaegisforge.streaming.domain.transaction.{
  TransactionEvent,
  TransactionEventCandidate,
  TransactionEventValidator,
  TransactionEventV1Contract
}
import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroError._

object TransactionEventAvroMapper {
  private val DecimalConversion = new Conversions.DecimalConversion()
  private val MicrosPerSecond = 1000000L
  private val NanosPerMicro = 1000

  def toRecord(
      event: TransactionEvent,
      schema: Schema
  ): Either[TransactionEventAvroError, GenericRecord] =
    for {
      eventTime <- instantToMicros("event_time", event.eventTime)
      ingestionTime <- instantToMicros("ingestion_time", event.ingestionTime)
      amount <- decimalToBytes(event.amount, schema.getField("amount").schema())
    } yield {
      val record = new GenericData.Record(schema)
      record.put("event_id", event.eventId.toString)
      record.put("transaction_id", event.transactionId)
      record.put("customer_id", event.customerId)
      record.put("merchant_id", event.merchantId)
      record.put("event_time", eventTime)
      record.put("ingestion_time", ingestionTime)
      record.put("amount", amount)
      record.put("currency", event.currency)
      record.put("country", event.country)
      record.put("device_id", event.deviceId)
      record.put("ip_address", event.ipAddress)
      record.put("transaction_type", event.transactionType.externalName)
      record.put("schema_version", event.schemaVersion)
      record
    }

  def fromRecord(
      record: GenericRecord
  ): Either[TransactionEventAvroError, TransactionEvent] =
    for {
      eventId <- stringField(record, "event_id")
      transactionId <- stringField(record, "transaction_id")
      customerId <- stringField(record, "customer_id")
      merchantId <- stringField(record, "merchant_id")
      eventTime <- longField(record, "event_time")
      ingestionTime <- longField(record, "ingestion_time")
      amount <- decimalField(record, "amount")
      currency <- stringField(record, "currency")
      country <- stringField(record, "country")
      deviceId <- stringField(record, "device_id")
      ipAddress <- stringField(record, "ip_address")
      transactionType <- stringField(record, "transaction_type")
      schemaVersion <- intField(record, "schema_version")
      event <- validateCandidate(
        TransactionEventCandidate(
          eventId = Some(eventId),
          transactionId = Some(transactionId),
          customerId = Some(customerId),
          merchantId = Some(merchantId),
          eventTime = Some(microsToInstant(eventTime).toString),
          ingestionTime = Some(microsToInstant(ingestionTime).toString),
          amount = Some(amount.bigDecimal.toPlainString),
          currency = Some(currency),
          country = Some(country),
          deviceId = Some(deviceId),
          ipAddress = Some(ipAddress),
          transactionType = Some(transactionType),
          schemaVersion = Some(schemaVersion.toString)
        )
      )
    } yield event

  private def instantToMicros(
      field: String,
      instant: Instant
  ): Either[TransactionEventAvroError, Long] =
    if (instant.getNano % NanosPerMicro != 0) {
      Left(UnsupportedTimestampPrecision(field, instant))
    } else {
      try {
        Right(
          Math.addExact(
            Math.multiplyExact(instant.getEpochSecond, MicrosPerSecond),
            instant.getNano / NanosPerMicro
          )
        )
      } catch {
        case _: ArithmeticException => Left(TimestampOutOfRange(field, instant))
      }
    }

  private def microsToInstant(value: Long): Instant = {
    val seconds = Math.floorDiv(value, MicrosPerSecond)
    val micros = Math.floorMod(value, MicrosPerSecond)
    Instant.ofEpochSecond(seconds, micros * NanosPerMicro)
  }

  private def decimalToBytes(
      amount: BigDecimal,
      schema: Schema
  ): Either[TransactionEventAvroError, ByteBuffer] =
    try {
      val decimal = schema.getLogicalType.asInstanceOf[LogicalTypes.Decimal]
      val scaled = amount.bigDecimal.setScale(TransactionEventV1Contract.AmountScale)
      Right(DecimalConversion.toBytes(scaled, schema, decimal))
    } catch {
      case NonFatal(error) => Left(InvalidDecimal(error.getMessage))
    }

  private def stringField(
      record: GenericRecord,
      field: String
  ): Either[TransactionEventAvroError, String] =
    requiredField(record, field).flatMap {
      case value: CharSequence => Right(value.toString)
      case value               => Left(InvalidFieldType(field, "string", value.getClass.getName))
    }

  private def longField(
      record: GenericRecord,
      field: String
  ): Either[TransactionEventAvroError, Long] =
    requiredField(record, field).flatMap {
      case value: java.lang.Long => Right(value.longValue())
      case value                 => Left(InvalidFieldType(field, "long", value.getClass.getName))
    }

  private def intField(
      record: GenericRecord,
      field: String
  ): Either[TransactionEventAvroError, Int] =
    requiredField(record, field).flatMap {
      case value: java.lang.Integer => Right(value.intValue())
      case value                    => Left(InvalidFieldType(field, "int", value.getClass.getName))
    }

  private def decimalField(
      record: GenericRecord,
      field: String
  ): Either[TransactionEventAvroError, BigDecimal] =
    requiredField(record, field).flatMap {
      case value: ByteBuffer =>
        try {
          val schema = record.getSchema.getField(field).schema()
          val decimal = schema.getLogicalType.asInstanceOf[LogicalTypes.Decimal]
          Right(BigDecimal(DecimalConversion.fromBytes(value.duplicate(), schema, decimal)))
        } catch {
          case NonFatal(error) => Left(InvalidDecimal(error.getMessage))
        }
      case value => Left(InvalidFieldType(field, "decimal bytes", value.getClass.getName))
    }

  private def requiredField(
      record: GenericRecord,
      field: String
  ): Either[TransactionEventAvroError, AnyRef] =
    try {
      Option(record.get(field)).toRight(MissingField(field))
    } catch {
      case NonFatal(_) => Left(MissingField(field))
    }

  private def validateCandidate(
      candidate: TransactionEventCandidate
  ): Either[TransactionEventAvroError, TransactionEvent] =
    TransactionEventValidator.validate(candidate).left.map(DomainValidationFailed)
}
