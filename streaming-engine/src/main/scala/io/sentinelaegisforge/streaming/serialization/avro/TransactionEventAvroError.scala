package io.sentinelaegisforge.streaming.serialization.avro

import java.time.Instant

import io.sentinelaegisforge.streaming.domain.transaction.TransactionValidationError

sealed trait TransactionEventAvroError extends Product with Serializable {
  def message: String
}

object TransactionEventAvroError {
  final case class SchemaResourceMissing(path: String) extends TransactionEventAvroError {
    override val message: String = s"Avro schema resource was not found: $path"
  }

  final case class SchemaParsingFailed(details: String) extends TransactionEventAvroError {
    override val message: String = s"Avro schema could not be parsed: $details"
  }

  final case class UnsupportedTimestampPrecision(field: String, value: Instant)
      extends TransactionEventAvroError {
    override val message: String =
      s"$field has sub-microsecond precision that the v1 wire contract cannot represent: $value"
  }

  final case class TimestampOutOfRange(field: String, value: Instant)
      extends TransactionEventAvroError {
    override val message: String = s"$field is outside the timestamp-micros range: $value"
  }

  final case class MissingField(field: String) extends TransactionEventAvroError {
    override val message: String = s"Avro field is missing or null: $field"
  }

  final case class InvalidFieldType(field: String, expected: String, actual: String)
      extends TransactionEventAvroError {
    override val message: String =
      s"Avro field $field expected $expected but received $actual"
  }

  final case class InvalidDecimal(details: String) extends TransactionEventAvroError {
    override val message: String = s"Avro amount is not a valid decimal(18,4): $details"
  }

  final case class DomainValidationFailed(errors: Vector[TransactionValidationError])
      extends TransactionEventAvroError {
    override val message: String =
      s"Decoded event failed domain validation: ${errors.mkString(", ")}"
  }

  final case class BinaryEncodingFailed(details: String) extends TransactionEventAvroError {
    override val message: String = s"Avro binary encoding failed: $details"
  }

  final case class BinaryDecodingFailed(details: String) extends TransactionEventAvroError {
    override val message: String = s"Avro binary decoding failed: $details"
  }
}
