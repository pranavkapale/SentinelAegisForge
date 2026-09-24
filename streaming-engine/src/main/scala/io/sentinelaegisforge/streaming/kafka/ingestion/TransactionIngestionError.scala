package io.sentinelaegisforge.streaming.kafka.ingestion

import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroError

sealed trait TransactionIngestionError extends Product with Serializable {
  def message: String
}

object TransactionIngestionError {
  final case class DeserializerConfigurationFailed(details: String)
      extends TransactionIngestionError {
    override val message: String =
      s"Schema Registry deserializer configuration failed: $details"
  }

  final case class NullKafkaKey(coordinates: KafkaRecordCoordinates)
      extends TransactionIngestionError {
    override val message: String = s"Kafka key was null at $coordinates"
  }

  final case class InvalidUtf8KafkaKey(coordinates: KafkaRecordCoordinates)
      extends TransactionIngestionError {
    override val message: String = s"Kafka key was not valid UTF-8 at $coordinates"
  }

  final case class NullKafkaValue(coordinates: KafkaRecordCoordinates)
      extends TransactionIngestionError {
    override val message: String = s"Kafka value was null at $coordinates"
  }

  final case class RegistryAvroDecodeFailed(
      coordinates: KafkaRecordCoordinates,
      details: String
  ) extends TransactionIngestionError {
    override val message: String =
      s"Registry-framed Avro decoding failed at $coordinates: $details"
  }

  final case class DecodedValueWasNotGenericRecord(
      coordinates: KafkaRecordCoordinates,
      actualType: String
  ) extends TransactionIngestionError {
    override val message: String =
      s"Decoded value at $coordinates was $actualType rather than an Avro GenericRecord"
  }

  final case class DomainDecodingFailed(
      coordinates: KafkaRecordCoordinates,
      cause: TransactionEventAvroError
  ) extends TransactionIngestionError {
    override val message: String =
      s"Decoded value at $coordinates did not cross the transaction domain boundary: ${cause.message}"
  }

  final case class CustomerKeyMismatch(
      coordinates: KafkaRecordCoordinates,
      kafkaKey: String,
      customerId: String
  ) extends TransactionIngestionError {
    override val message: String =
      s"Kafka key '$kafkaKey' did not match customer_id '$customerId' at $coordinates"
  }
}

final class TransactionIngestionException(val ingestionError: TransactionIngestionError)
    extends RuntimeException(ingestionError.message)
