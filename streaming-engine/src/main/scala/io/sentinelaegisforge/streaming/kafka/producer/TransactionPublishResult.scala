package io.sentinelaegisforge.streaming.kafka.producer

import java.util.UUID

import io.sentinelaegisforge.streaming.serialization.avro.TransactionEventAvroError

final case class PublishedTransactionMetadata(topic: String, partition: Int, offset: Long)

final case class TransactionPublishFailure(index: Int, eventId: UUID, details: String)

final case class TransactionPublishReport(
    requested: Int,
    acknowledged: Vector[PublishedTransactionMetadata],
    failures: Vector[TransactionPublishFailure]
) {
  val acknowledgedCount: Int = acknowledged.size
  val failedCount: Int = failures.size
}

sealed trait TransactionPublishError extends Product with Serializable {
  def message: String
}

object TransactionPublishError {
  final case class RecordConstructionFailed(
      index: Int,
      eventId: UUID,
      cause: TransactionEventAvroError
  ) extends TransactionPublishError {
    override val message: String =
      s"Record $index for event $eventId could not be mapped: ${cause.message}"
  }

  final case class DeliveryFailed(report: TransactionPublishReport)
      extends TransactionPublishError {
    override val message: String =
      s"${report.failedCount} of ${report.requested} transaction records failed to publish"
  }

  final case class ProducerOperationFailed(details: String) extends TransactionPublishError {
    override val message: String = s"Kafka producer operation failed: $details"
  }

  final case class ProducerCloseFailed(details: String) extends TransactionPublishError {
    override val message: String = s"Kafka producer close failed: $details"
  }
}
