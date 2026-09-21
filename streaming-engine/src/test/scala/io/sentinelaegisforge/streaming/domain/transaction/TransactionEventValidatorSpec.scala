package io.sentinelaegisforge.streaming.domain.transaction

import java.time.Instant
import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.domain.transaction.TransactionField.CustomerId
import io.sentinelaegisforge.streaming.domain.transaction.TransactionValidationError._

final class TransactionEventValidatorSpec extends AnyFunSuite {
  test("a valid version 1 candidate becomes a typed transaction event") {
    TransactionEventValidator.validate(validCandidate) match {
      case Right(event) =>
        assert(event.eventId == UUID.fromString(validCandidate.eventId.get))
        assert(event.transactionId == "txn-1001")
        assert(event.eventTime == Instant.parse("2026-01-01T00:00:00Z"))
        assert(event.ingestionTime == Instant.parse("2026-01-01T00:00:02Z"))
        assert(event.amount == BigDecimal("42.50"))
        assert(event.transactionType == TransactionType.CardPayment)
        assert(event.schemaVersion == 1)
      case Left(errors) => fail(s"Expected a valid event, received: $errors")
    }
  }

  test("validation collects multiple independent errors") {
    val invalid = validCandidate.copy(
      eventId = Some("not-a-uuid"),
      customerId = Some(" "),
      amount = Some("0"),
      currency = Some("usd"),
      country = Some("USA"),
      ipAddress = Some("999.1.1.1")
    )

    val errors = validationErrors(invalid)
    assert(errors.contains(MalformedEventId("not-a-uuid")))
    assert(errors.contains(MissingRequiredField(CustomerId)))
    assert(errors.contains(NonPositiveAmount(BigDecimal("0"))))
    assert(errors.contains(InvalidCurrencyFormat("usd")))
    assert(errors.contains(InvalidCountryFormat("USA")))
    assert(errors.contains(InvalidIpAddressFormat("999.1.1.1")))
  }

  test("all version 1 fields are required") {
    val empty = TransactionEventCandidate(
      eventId = None,
      transactionId = None,
      customerId = None,
      merchantId = None,
      eventTime = None,
      ingestionTime = None,
      amount = None,
      currency = None,
      country = None,
      deviceId = None,
      ipAddress = None,
      transactionType = None,
      schemaVersion = None
    )

    val missingFields = validationErrors(empty).collect { case MissingRequiredField(field) =>
      field
    }
    assert(missingFields == TransactionField.all)
  }

  test("unsupported schema versions and transaction types are rejected") {
    val invalid = validCandidate.copy(
      transactionType = Some("WIRE_TRANSFER"),
      schemaVersion = Some("2")
    )

    val errors = validationErrors(invalid)
    assert(errors.contains(UnsupportedTransactionType("WIRE_TRANSFER")))
    assert(errors.contains(UnsupportedSchemaVersion(2)))
  }

  test("malformed UUID, timestamp, amount, and schema values return errors without throwing") {
    val invalid = validCandidate.copy(
      eventId = Some("bad-uuid"),
      eventTime = Some("not-an-instant"),
      ingestionTime = Some("also-not-an-instant"),
      amount = Some("many-dollars"),
      schemaVersion = Some("one")
    )

    val errors = validationErrors(invalid)
    assert(errors.contains(MalformedEventId("bad-uuid")))
    assert(errors.contains(MalformedEventTime("not-an-instant")))
    assert(errors.contains(MalformedIngestionTime("also-not-an-instant")))
    assert(errors.contains(MalformedAmount("many-dollars")))
    assert(errors.contains(MalformedSchemaVersion("one")))
  }

  test("validation is deterministic and does not impose a late-data policy") {
    val eventAfterIngestion = validCandidate.copy(
      eventTime = Some("2026-01-02T00:00:00Z"),
      ingestionTime = Some("2026-01-01T00:00:00Z")
    )

    val first = TransactionEventValidator.validate(eventAfterIngestion)
    val second = TransactionEventValidator.validate(eventAfterIngestion)
    assert(first == second)
    assert(first.isRight)
  }

  test("amounts exceeding the v1 wire scale are rejected without rounding") {
    val errors = validationErrors(validCandidate.copy(amount = Some("1.23456")))

    assert(errors.contains(AmountScaleExceeded(BigDecimal("1.23456"), 4)))
  }

  test("amounts exceeding the v1 wire precision are rejected") {
    val errors = validationErrors(validCandidate.copy(amount = Some("100000000000000")))

    assert(
      errors.contains(AmountPrecisionExceeded(BigDecimal("100000000000000"), 18, 4))
    )
  }

  private def validationErrors(
      candidate: TransactionEventCandidate
  ): Vector[TransactionValidationError] =
    TransactionEventValidator.validate(candidate) match {
      case Left(errors) => errors
      case Right(event) => fail(s"Expected validation errors, received: $event")
    }

  private val validCandidate = TransactionEventCandidate(
    eventId = Some("6ba7b810-9dad-41d1-80b4-00c04fd430c8"),
    transactionId = Some("txn-1001"),
    customerId = Some("customer-42"),
    merchantId = Some("merchant-7"),
    eventTime = Some("2026-01-01T00:00:00Z"),
    ingestionTime = Some("2026-01-01T00:00:02Z"),
    amount = Some("42.50"),
    currency = Some("USD"),
    country = Some("US"),
    deviceId = Some("device-123"),
    ipAddress = Some("192.0.2.10"),
    transactionType = Some("CARD_PAYMENT"),
    schemaVersion = Some("1")
  )
}
