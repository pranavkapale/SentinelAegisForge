package io.sentinelaegisforge.streaming.kafka.ingestion

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{count, max, min}

final class DiagnosticTransactionSink {
  private val processedRows = new AtomicLong(0L)

  def process(batch: Dataset[ValidatedTransactionRecord], batchId: Long): Unit = {
    val cached = batch.persist()
    try {
      val rowCount = cached.count()
      processedRows.addAndGet(rowCount)
      println(s"Ingestion batch: batchId=$batchId rowCount=$rowCount")

      cached
        .select(
          "customerId",
          "eventId",
          "eventTime",
          "amount",
          "currency",
          "kafkaTopic",
          "kafkaPartition",
          "kafkaOffset",
          "kafkaTimestamp"
        )
        .orderBy("kafkaPartition", "kafkaOffset")
        .show(20, truncate = false)

      cached
        .groupBy("kafkaTopic", "kafkaPartition")
        .agg(
          count("*").as("records"),
          min("kafkaOffset").as("minimumOffset"),
          max("kafkaOffset").as("maximumOffset")
        )
        .orderBy("kafkaPartition")
        .show(truncate = false)
    } finally {
      cached.unpersist()
    }
  }

  def totalProcessedRows: Long = processedRows.get()
}
