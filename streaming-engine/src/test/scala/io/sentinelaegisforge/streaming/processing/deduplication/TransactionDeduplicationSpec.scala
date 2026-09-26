package io.sentinelaegisforge.streaming.processing.deduplication

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.kafka.ingestion.{
  TransactionSparkSession,
  ValidatedTransactionRecord
}
import io.sentinelaegisforge.streaming.persistence.delta.{
  DeduplicatedTransactionDeltaSink,
  ValidatedTransactionDeltaSchema
}

final class TransactionDeduplicationSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val WatermarkDelay = "10 minutes"
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val warehouse = Files.createTempDirectory("sentinel-deduplication-warehouse")
    spark = TransactionSparkSession
      .builder("local[2]")
      .appName("transaction-deduplication-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.toUri.toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally {
      super.afterAll()
    }
  }

  test("configuration defines a separate checkpoint, output path, app ID, and watermark") {
    val config = TransactionDeduplicationApp.parseArguments(Array.empty).fold(fail(_), identity)

    assert(
      config.validatedDeltaPath == TransactionDeduplicationConfig.DefaultValidatedDeltaPath
    )
    assert(
      config.deduplicatedDeltaPath ==
        TransactionDeduplicationConfig.DefaultDeduplicatedDeltaPath
    )
    assert(config.checkpointLocation == TransactionDeduplicationConfig.DefaultCheckpointLocation)
    assert(config.deltaTxnAppId == "sentinel-transactions-deduplicated-v1")
    assert(config.watermarkDelay == WatermarkDelay)
  }

  test(
    "event-time state suppresses event IDs, drops too-late data, and accepts eligible disorder"
  ) {
    val root = Files.createTempDirectory("sentinel-event-time-deduplication")
    val sourcePath = root.resolve("transactions_validated").toString
    val targetPath = root.resolve("transactions_deduplicated").toString
    val checkpointPath = root.resolve("checkpoint").toString
    val appId = "sentinel-test-deduplicated-v1"
    val initialTime = Instant.parse("2030-01-01T12:00:00Z")
    val initial = Vector.tabulate(3) { index =>
      record(index, initialTime.plusSeconds(index.toLong * 30L), index.toLong)
    }

    appendToValidatedSource(sourcePath, initial)
    val initialProgress = runDeduplication(
      sourcePath,
      targetPath,
      checkpointPath,
      appId
    )
    assert(rowCount(targetPath) == 3L)
    assert(initialProgress.exists(_.stateOperators.exists(_.numRowsTotal > 0L)))

    val transportDuplicates = initial.map(record =>
      record.copy(
        kafkaOffset = record.kafkaOffset + 100L,
        kafkaTimestamp = Timestamp.from(record.kafkaTimestamp.toInstant.plusSeconds(60L))
      )
    )
    appendToValidatedSource(sourcePath, transportDuplicates)
    runDeduplication(sourcePath, targetPath, checkpointPath, appId)
    assert(rowCount(targetPath) == 3L)
    assert(
      spark.read
        .format("delta")
        .load(targetPath)
        .select("kafka_offset")
        .collect()
        .map(_.getLong(0))
        .toSet == Set(0L, 1L, 2L)
    )

    val futureTime = Instant.parse("2030-01-01T12:30:00Z")
    appendToValidatedSource(sourcePath, Vector(record(10, futureTime, 200L)))
    val futureProgress = runDeduplication(
      sourcePath,
      targetPath,
      checkpointPath,
      appId
    )
    assert(rowCount(targetPath) == 4L)
    val activeWatermark = latestWatermark(futureProgress)
    assert(activeWatermark.isAfter(initialTime))
    assert(activeWatermark.isBefore(futureTime))
    assert(futureProgress.exists(_.stateOperators.exists(_.memoryUsedBytes > 0L)))
    assert(futureProgress.map(_.stateOperators.map(_.numRowsRemoved).sum).sum > 0L)

    val tooLateTime = activeWatermark.minusSeconds(1L)
    appendToValidatedSource(sourcePath, Vector(record(11, tooLateTime, 201L)))
    val tooLateProgress = runDeduplication(
      sourcePath,
      targetPath,
      checkpointPath,
      appId
    )
    assert(rowCount(targetPath) == 4L)
    assert(tooLateProgress.map(_.rowsDroppedByWatermark).sum > 0L)

    val lateButEligibleTime = activeWatermark.plusSeconds(60L)
    assert(lateButEligibleTime.isBefore(futureTime))
    appendToValidatedSource(sourcePath, Vector(record(12, lateButEligibleTime, 202L)))
    runDeduplication(sourcePath, targetPath, checkpointPath, appId)
    assert(rowCount(targetPath) == 5L)
  }

  test("deduplicated Delta output independently suppresses a retried sink transaction") {
    val session = spark
    import session.implicits._

    val root = Files.createTempDirectory("sentinel-deduplicated-delta-retry")
    val targetPath = root.resolve("transactions_deduplicated").toString
    val appId = "sentinel-test-deduplicated-retry-v1"
    val sink = new DeduplicatedTransactionDeltaSink(targetPath, appId)
    val firstBatch = ValidatedTransactionDeltaSchema.project(
      Seq(record(20, Instant.parse("2030-01-01T12:00:00Z"), 0L)).toDS()
    )

    assert(sink.plan(0L).options == Map("txnAppId" -> appId, "txnVersion" -> "0"))
    sink.writeBatch(firstBatch, 0L)
    sink.writeBatch(firstBatch, 0L)
    assert(rowCount(targetPath) == 1L)
    assert(DeltaTable.forPath(spark, targetPath).history().count() == 1L)

    val secondBatch = ValidatedTransactionDeltaSchema.project(
      Seq(record(21, Instant.parse("2030-01-01T12:01:00Z"), 1L)).toDS()
    )
    sink.writeBatch(secondBatch, 1L)
    assert(rowCount(targetPath) == 2L)
    assert(DeltaTable.forPath(spark, targetPath).history().count() == 2L)
  }

  private def runDeduplication(
      sourcePath: String,
      targetPath: String,
      checkpointPath: String,
      appId: String
  ): Vector[DeduplicationProgress] = {
    val source = spark.readStream.format("delta").load(sourcePath)
    val transformed = TransactionDeduplicationTransformer.deduplicate(
      source,
      WatermarkDelay
    )
    val sink = new DeduplicatedTransactionDeltaSink(targetPath, appId)
    val query = transformed.writeStream
      .queryName("transaction-event-time-deduplication-test")
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.AvailableNow())
      .foreachBatch((batch: DataFrame, batchId: Long) => sink.writeBatch(batch, batchId))
      .start()

    query.awaitTermination()
    query.recentProgress.toVector.map(DeduplicationProgress.from)
  }

  private def appendToValidatedSource(
      sourcePath: String,
      records: Seq[ValidatedTransactionRecord]
  ): Unit = {
    val session = spark
    import session.implicits._

    ValidatedTransactionDeltaSchema
      .project(records.toDS())
      .write
      .format("delta")
      .mode("append")
      .save(sourcePath)
  }

  private def rowCount(path: String): Long =
    spark.read.format("delta").load(path).count()

  private def latestWatermark(progress: Seq[DeduplicationProgress]): Instant =
    progress
      .flatMap(_.watermark)
      .map(Instant.parse)
      .maxBy(_.toEpochMilli)

  private def record(
      identity: Int,
      eventTime: Instant,
      kafkaOffset: Long
  ): ValidatedTransactionRecord = {
    val eventId = UUID
      .nameUUIDFromBytes(
        s"event-$identity".getBytes(StandardCharsets.UTF_8)
      )
      .toString
    val timestamp = Timestamp.from(eventTime)

    ValidatedTransactionRecord(
      eventId = eventId,
      transactionId = s"transaction-$identity",
      customerId = s"customer-$identity",
      merchantId = s"merchant-$identity",
      eventTime = timestamp,
      ingestionTime = Timestamp.from(eventTime.plusSeconds(2L)),
      amount = new java.math.BigDecimal("10.0000"),
      currency = "USD",
      country = "US",
      deviceId = s"device-$identity",
      ipAddress = "192.0.2.1",
      transactionType = "CARD_PAYMENT",
      schemaVersion = 1,
      kafkaKey = s"customer-$identity",
      kafkaTopic = "transactions.raw",
      kafkaPartition = (kafkaOffset % 3L).toInt,
      kafkaOffset = kafkaOffset,
      kafkaTimestamp = Timestamp.from(eventTime.plusSeconds(3L))
    )
  }
}
