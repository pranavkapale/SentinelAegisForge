package io.sentinelaegisforge.streaming.domain.transaction

import java.time.Instant
import java.util.UUID

import scala.util.Try

import io.sentinelaegisforge.streaming.domain.transaction.TransactionField._
import io.sentinelaegisforge.streaming.domain.transaction.TransactionValidationError._

object TransactionEventValidator {
  type ValidationResult = Either[Vector[TransactionValidationError], TransactionEvent]

  private val CurrencyFormat = "[A-Z]{3}".r
  private val CountryFormat = "[A-Z]{2}".r

  def validate(candidate: TransactionEventCandidate): ValidationResult = {
    val errors = Vector.newBuilder[TransactionValidationError]

    val eventIdValue = required(EventId, candidate.eventId, errors)
    val transactionId = required(TransactionId, candidate.transactionId, errors)
    val customerId = required(CustomerId, candidate.customerId, errors)
    val merchantId = required(MerchantId, candidate.merchantId, errors)
    val eventTimeValue = required(EventTime, candidate.eventTime, errors)
    val ingestionTimeValue = required(IngestionTime, candidate.ingestionTime, errors)
    val amountValue = required(Amount, candidate.amount, errors)
    val currency = required(Currency, candidate.currency, errors)
    val country = required(Country, candidate.country, errors)
    val deviceId = required(DeviceId, candidate.deviceId, errors)
    val ipAddress = required(IpAddress, candidate.ipAddress, errors)
    val transactionTypeValue = required(TransactionType, candidate.transactionType, errors)
    val schemaVersionValue = required(SchemaVersion, candidate.schemaVersion, errors)

    val eventId = eventIdValue.flatMap(parseEventId(_, errors))
    val eventTime = eventTimeValue.flatMap(parseEventTime(_, errors))
    val ingestionTime = ingestionTimeValue.flatMap(parseIngestionTime(_, errors))
    val amount = amountValue.flatMap(parseAmount(_, errors))
    val transactionType = transactionTypeValue.flatMap(parseTransactionType(_, errors))
    val schemaVersion = schemaVersionValue.flatMap(parseSchemaVersion(_, errors))

    currency.foreach { value =>
      if (!CurrencyFormat.pattern.matcher(value).matches()) {
        errors += InvalidCurrencyFormat(value)
      }
    }

    country.foreach { value =>
      if (!CountryFormat.pattern.matcher(value).matches()) {
        errors += InvalidCountryFormat(value)
      }
    }

    ipAddress.foreach { value =>
      if (!isIpv4Address(value)) {
        errors += InvalidIpAddressFormat(value)
      }
    }

    val validationErrors = errors.result()
    if (validationErrors.nonEmpty) {
      Left(validationErrors)
    } else {
      Right(
        new TransactionEvent(
          eventId = eventId.get,
          transactionId = transactionId.get,
          customerId = customerId.get,
          merchantId = merchantId.get,
          eventTime = eventTime.get,
          ingestionTime = ingestionTime.get,
          amount = amount.get,
          currency = currency.get,
          country = country.get,
          deviceId = deviceId.get,
          ipAddress = ipAddress.get,
          transactionType = transactionType.get,
          schemaVersion = schemaVersion.get
        )
      )
    }
  }

  private def required(
      field: TransactionField,
      rawValue: Option[String],
      errors: scala.collection.mutable.Builder[TransactionValidationError, Vector[
        TransactionValidationError
      ]]
  ): Option[String] = {
    val normalized = Option(rawValue).flatten.flatMap(value => Option(value).map(_.trim))
    normalized.filter(_.nonEmpty) match {
      case some @ Some(_) => some
      case None           =>
        errors += MissingRequiredField(field)
        None
    }
  }

  private def parseEventId(
      value: String,
      errors: scala.collection.mutable.Builder[TransactionValidationError, Vector[
        TransactionValidationError
      ]]
  ): Option[UUID] =
    Try(UUID.fromString(value)).toOption.orElse {
      errors += MalformedEventId(value)
      None
    }

  private def parseEventTime(
      value: String,
      errors: scala.collection.mutable.Builder[TransactionValidationError, Vector[
        TransactionValidationError
      ]]
  ): Option[Instant] =
    Try(Instant.parse(value)).toOption.orElse {
      errors += MalformedEventTime(value)
      None
    }

  private def parseIngestionTime(
      value: String,
      errors: scala.collection.mutable.Builder[TransactionValidationError, Vector[
        TransactionValidationError
      ]]
  ): Option[Instant] =
    Try(Instant.parse(value)).toOption.orElse {
      errors += MalformedIngestionTime(value)
      None
    }

  private def parseAmount(
      value: String,
      errors: scala.collection.mutable.Builder[TransactionValidationError, Vector[
        TransactionValidationError
      ]]
  ): Option[BigDecimal] =
    Try(BigDecimal(value)).toOption match {
      case Some(parsed) if parsed > 0 => Some(parsed)
      case Some(parsed)               =>
        errors += NonPositiveAmount(parsed)
        None
      case None =>
        errors += MalformedAmount(value)
        None
    }

  private def parseTransactionType(
      value: String,
      errors: scala.collection.mutable.Builder[TransactionValidationError, Vector[
        TransactionValidationError
      ]]
  ): Option[TransactionType] =
    io.sentinelaegisforge.streaming.domain.transaction.TransactionType
      .fromExternalName(value)
      .orElse {
        errors += UnsupportedTransactionType(value)
        None
      }

  private def parseSchemaVersion(
      value: String,
      errors: scala.collection.mutable.Builder[TransactionValidationError, Vector[
        TransactionValidationError
      ]]
  ): Option[Int] =
    Try(value.toInt).toOption match {
      case Some(1)     => Some(1)
      case Some(other) =>
        errors += UnsupportedSchemaVersion(other)
        None
      case None =>
        errors += MalformedSchemaVersion(value)
        None
    }

  private def isIpv4Address(value: String): Boolean = {
    val parts = value.split("\\.", -1)
    parts.length == 4 && parts.forall { part =>
      part.nonEmpty &&
      part.forall(character => character >= '0' && character <= '9') &&
      Try(part.toInt).toOption.exists(number => number >= 0 && number <= 255)
    }
  }
}
