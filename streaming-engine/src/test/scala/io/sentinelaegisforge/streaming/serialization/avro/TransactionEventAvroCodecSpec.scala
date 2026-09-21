package io.sentinelaegisforge.streaming.serialization.avro

import java.time.{Duration, Instant}

import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.domain.transaction.TransactionField.CustomerId
import io.sentinelaegisforge.streaming.domain.transaction.TransactionValidationError.MissingRequiredField
import io.sentinelaegisforge.streaming.domain.transaction.{
  TransactionEvent,
  TransactionEventCandidate,
  TransactionEventValidator
}
import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroError._
import io.sentinelaegisforge.streaming.simulation.{
  DeterministicTransactionGenerator,
  TransactionGeneratorConfig
}

final class TransactionEventAvroCodecSpec extends AnyFunSuite {
  test("domain fields map explicitly to their Avro representations") {
    val schema = loadedSchema
    val record = mapped(validEvent, schema)

    assert(record.get("event_id").toString == validEvent.eventId.toString)
    assert(record.get("transaction_id").toString == validEvent.transactionId)
    assert(record.get("customer_id").toString == validEvent.customerId)
    assert(record.get("merchant_id").toString == validEvent.merchantId)
    assert(record.get("event_time") == 1767225600123456L)
    assert(record.get("ingestion_time") == 1767225602123456L)
    assert(record.get("currency").toString == validEvent.currency)
    assert(record.get("country").toString == validEvent.country)
    assert(record.get("device_id").toString == validEvent.deviceId)
    assert(record.get("ip_address").toString == validEvent.ipAddress)
    assert(record.get("transaction_type").toString == "CARD_PAYMENT")
    assert(record.get("schema_version") == 1)
    assert(TransactionEventAvroMapper.fromRecord(record) == Right(validEvent))
  }

  test("binary encoding round trips a validated event") {
    val encoded = encode(validEvent)
    assert(TransactionEventAvroCodec.decode(encoded) == Right(validEvent))
  }

  test("binary encoding is deterministic for the same event and schema") {
    assert(encode(validEvent).sameElements(encode(validEvent)))
  }

  test("sub-microsecond instants are rejected rather than truncated") {
    val event = validated(
      validCandidate.copy(eventTime = Some("2026-01-01T00:00:00.123456789Z"))
    )

    TransactionEventAvroCodec.encode(event) match {
      case Left(UnsupportedTimestampPrecision("event_time", eventTime)) =>
        assert(eventTime == event.eventTime)
      case other => fail(s"Expected an explicit precision error, received: $other")
    }
  }

  test("malformed Avro binary data returns a controlled decoding error") {
    assert(TransactionEventAvroCodec.decode(Array.emptyByteArray).isLeft)
    assert(
      TransactionEventAvroCodec.decode(Array.emptyByteArray) match {
        case Left(_: BinaryDecodingFailed) => true
        case _                             => false
      }
    )
  }

  test("semantically invalid Avro records cannot bypass domain validation") {
    val record = mapped(validEvent, loadedSchema)
    record.put("customer_id", " ")

    TransactionEventAvroMapper.fromRecord(record) match {
      case Left(DomainValidationFailed(errors)) =>
        assert(errors.contains(MissingRequiredField(CustomerId)))
      case other => fail(s"Expected domain validation failure, received: $other")
    }
  }

  test("a deterministic generated candidate validates, encodes, and decodes") {
    val config = TransactionGeneratorConfig(
      seed = 99L,
      baseEventTime = Instant.parse("2030-05-01T00:00:00Z"),
      eventSpacing = Duration.ofSeconds(10),
      ingestionDelay = Duration.ofSeconds(2)
    )
    val candidate = new DeterministicTransactionGenerator(config).generateValidCandidates(1).head
    val event = validated(candidate)

    assert(TransactionEventAvroCodec.decode(encode(event)) == Right(event))
  }

  private def mapped(
      event: TransactionEvent,
      schema: org.apache.avro.Schema
  ): org.apache.avro.generic.GenericRecord =
    TransactionEventAvroMapper.toRecord(event, schema) match {
      case Right(record) => record
      case Left(error)   => fail(error.message)
    }

  private def encode(event: TransactionEvent): Array[Byte] =
    TransactionEventAvroCodec.encode(event) match {
      case Right(bytes) => bytes
      case Left(error)  => fail(error.message)
    }

  private def validated(candidate: TransactionEventCandidate): TransactionEvent =
    TransactionEventValidator.validate(candidate) match {
      case Right(event) => event
      case Left(errors) => fail(s"Candidate did not validate: $errors")
    }

  private def loadedSchema: org.apache.avro.Schema =
    TransactionEventAvroSchema.load match {
      case Right(schema) => schema
      case Left(error)   => fail(error.message)
    }

  private lazy val validEvent: TransactionEvent = validated(validCandidate)

  private val validCandidate = TransactionEventCandidate(
    eventId = Some("6ba7b810-9dad-41d1-80b4-00c04fd430c8"),
    transactionId = Some("txn-1001"),
    customerId = Some("customer-42"),
    merchantId = Some("merchant-7"),
    eventTime = Some("2026-01-01T00:00:00.123456Z"),
    ingestionTime = Some("2026-01-01T00:00:02.123456Z"),
    amount = Some("42.1250"),
    currency = Some("USD"),
    country = Some("US"),
    deviceId = Some("device-123"),
    ipAddress = Some("192.0.2.10"),
    transactionType = Some("CARD_PAYMENT"),
    schemaVersion = Some("1")
  )
}
