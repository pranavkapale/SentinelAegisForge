package io.sentinelaegisforge.streaming.kafka.ingestion

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.TaskContext
import org.apache.spark.sql.{Dataset, Encoders}

object TransactionIngestionTransformer {
  def decode(
      source: Dataset[KafkaSourceRecord],
      schemaRegistryUrl: String
  ): Dataset[ValidatedTransactionRecord] = {
    implicit val outputEncoder = Encoders.product[ValidatedTransactionRecord]
    source.mapPartitions(records => decodePartition(records, schemaRegistryUrl))
  }

  private[ingestion] def decodePartition(
      records: Iterator[KafkaSourceRecord],
      schemaRegistryUrl: String
  ): Iterator[ValidatedTransactionRecord] = {
    val decoder = RegistryBackedTransactionDecoder
      .configured(schemaRegistryUrl)
      .fold(error => throw new TransactionIngestionException(error), identity)
    val closed = new AtomicBoolean(false)

    def closeDecoder(): Unit =
      if (closed.compareAndSet(false, true)) {
        decoder.close()
      }

    Option(TaskContext.get()).foreach(
      _.addTaskCompletionListener[Unit](_ => closeDecoder())
    )

    new Iterator[ValidatedTransactionRecord] {
      override def hasNext: Boolean =
        try {
          val available = records.hasNext
          if (!available) closeDecoder()
          available
        } catch {
          case error: Throwable =>
            closeDecoder()
            throw error
        }

      override def next(): ValidatedTransactionRecord =
        try {
          decoder
            .decode(records.next())
            .fold(error => throw new TransactionIngestionException(error), identity)
        } catch {
          case error: Throwable =>
            closeDecoder()
            throw error
        }
    }
  }
}
