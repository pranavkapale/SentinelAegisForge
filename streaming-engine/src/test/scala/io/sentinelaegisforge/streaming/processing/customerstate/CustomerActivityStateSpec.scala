package io.sentinelaegisforge.streaming.processing.customerstate

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.Timestamp
import java.time.{Duration, Instant}
import java.util.UUID

import io.delta.tables.DeltaTable
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.kafka.ingestion.{
  TransactionSparkSession,
  ValidatedTransactionRecord
}
import io.sentinelaegisforge.streaming.persistence.delta.{
  CustomerActivityDeltaSchema,
  CustomerActivityDeltaSink,
  ValidatedTransactionDeltaSchema
}

final class CustomerActivityStateSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val WatermarkDelay = "10 minutes"
  private val Inactivity = Duration.ofHours(1L)
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val warehouse = Files.createTempDirectory("sentinel-customer-state-warehouse")
    spark = TransactionSparkSession
      .builder("local[2]")
      .appName("customer-activity-state-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", warehouse.toUri.toString)
      .config(
        CustomerActivityConfig.StateStoreProviderConfig,
        CustomerActivityConfig.RequiredStateStoreProvider
      )
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

  test("configuration defines an isolated state lineage and the required Spark 4.2 store") {
    val config = CustomerActivityStateApp.parseArguments(Array.empty).fold(fail(_), identity)

    assert(
      config.deduplicatedDeltaPath == CustomerActivityConfig.DefaultDeduplicatedDeltaPath
    )
    assert(config.snapshotDeltaPath == ".local/delta/customer_activity_snapshots")
    assert(config.checkpointLocation == ".local/checkpoints/customer-activity-state")
    assert(config.deltaTxnAppId == "sentinel-customer-activity-snapshots-v1")
    assert(config.watermarkDelay == WatermarkDelay)
    assert(config.inactivityTimeout == Duration.ofHours(24L))
    assert(DurationArgument.parse("1 hour") == Right(Duration.ofHours(1L)))
    assert(
      CustomerActivityConfig.RequiredStateStoreProvider.endsWith("RocksDBStateStoreProvider")
    )
  }

  test(
    "Delta streaming state survives restarts, expires from watermark, and starts fresh afterward"
  ) {
    val root = Files.createTempDirectory("sentinel-customer-activity-state")
    val sourcePath = root.resolve("transactions_deduplicated").toString
    val targetPath = root.resolve("customer_activity_snapshots").toString
    val checkpointPath = root.resolve("checkpoint").toString
    val appId = "sentinel-test-customer-activity-v1"

    appendSource(
      sourcePath,
      Seq(
        record(1, "customer-a", "2030-01-01T12:00:00Z", "10.0000", 1L),
        record(2, "customer-a", "2030-01-01T12:05:00Z", "20.0000", 2L),
        record(3, "customer-b", "2030-01-01T12:02:00Z", "7.5000", 3L)
      )
    )
    val initialProgress = runState(sourcePath, targetPath, checkpointPath, appId)
    assert(rowCount(targetPath) == 2L)
    assert(initialProgress.exists(_.stateOperators.exists(_.numRowsTotal == 2L)))

    val initialRows = readSnapshots(targetPath)
    assert(initialRows(("customer-a", "UPDATED")).getAs[Long]("state_event_count") == 2L)
    assert(initialRows(("customer-b", "UPDATED")).getAs[Long]("state_event_count") == 1L)

    appendSource(
      sourcePath,
      Seq(record(4, "customer-a", "2030-01-01T12:20:00Z", "5.0000", 4L))
    )
    runState(sourcePath, targetPath, checkpointPath, appId)
    assert(rowCount(targetPath) == 3L)
    val latestA = spark.read
      .format("delta")
      .load(targetPath)
      .where("customer_id = 'customer-a' AND lifecycle_type = 'UPDATED'")
      .orderBy(org.apache.spark.sql.functions.col("state_event_count").desc)
      .head()
    assert(latestA.getAs[Long]("state_event_count") == 3L)
    assert(latestA.getAs[java.math.BigDecimal]("state_amount_total").compareTo(decimal("35")) == 0)

    appendSource(
      sourcePath,
      Seq(record(5, "customer-c", "2030-01-01T14:30:00Z", "9.0000", 5L))
    )
    val expiryProgress = runState(sourcePath, targetPath, checkpointPath, appId)
    val observedWatermark = latestWatermark(expiryProgress)
    assert(!observedWatermark.isBefore(Instant.parse("2030-01-01T14:20:00Z")))
    assert(expiryProgress.exists(_.stateOperators.exists(_.numRowsRemoved >= 2L)))
    assert(expiryProgress.exists(_.stateOperators.exists(_.memoryUsedBytes > 0L)))
    assert(
      expiryProgress.exists(
        _.stateOperators.exists(state =>
          state.numShufflePartitions > 0L && state.numStateStoreInstances > 0L
        )
      )
    )

    val afterExpiry = spark.read.format("delta").load(targetPath)
    assert(
      afterExpiry
        .where("lifecycle_type = 'EXPIRED' AND customer_id IN ('customer-a', 'customer-b')")
        .count() == 2L
    )
    assert(rowCount(targetPath) == 6L)

    appendSource(
      sourcePath,
      Seq(record(6, "customer-a", "2030-01-01T14:40:00Z", "3.0000", 6L))
    )
    runState(sourcePath, targetPath, checkpointPath, appId)
    val reactivated = spark.read
      .format("delta")
      .load(targetPath)
      .where("customer_id = 'customer-a' AND lifecycle_type = 'UPDATED'")
      .orderBy(org.apache.spark.sql.functions.col("state_latest_event_time").desc)
      .head()
    assert(reactivated.getAs[Long]("state_event_count") == 1L)
    assert(
      reactivated.getAs[java.math.BigDecimal]("state_amount_total").compareTo(decimal("3")) == 0
    )
    assert(rowCount(targetPath) == 7L)
  }

  test("snapshot Delta sink uses its own retry-safe transaction lineage") {
    val session = spark
    import session.implicits._

    val path =
      Files.createTempDirectory("sentinel-customer-snapshot-retry").resolve("table").toString
    val appId = "sentinel-test-customer-snapshot-retry-v1"
    val sink = new CustomerActivityDeltaSink(path, appId)
    val snapshot = CustomerActivitySnapshot(
      customerId = "customer-a",
      lifecycleType = CustomerActivityLifecycle.Updated.externalName,
      batchEventCount = 1L,
      batchAmountTotal = decimal("10"),
      stateEventCount = 1L,
      stateAmountTotal = decimal("10"),
      stateFirstEventTime = timestamp("2030-01-01T12:00:00Z"),
      stateLatestEventTime = timestamp("2030-01-01T12:00:00Z"),
      stateExpiryTime = timestamp("2030-01-01T13:00:00Z")
    )

    assert(sink.plan(0L).options == Map("txnAppId" -> appId, "txnVersion" -> "0"))
    sink.writeBatch(Seq(snapshot).toDS(), 0L)
    sink.writeBatch(Seq(snapshot).toDS(), 0L)

    assert(rowCount(path) == 1L)
    assert(DeltaTable.forPath(spark, path).history().count() == 1L)
    assert(
      DeltaTable
        .forPath(spark, path)
        .detail()
        .select("partitionColumns")
        .head()
        .getSeq[String](0)
        .isEmpty
    )
    assert(
      spark.read
        .format("delta")
        .load(path)
        .schema
        .fields
        .toVector
        .map(field => field.name -> field.dataType) == CustomerActivityDeltaSchema.columns
    )
  }

  private def runState(
      sourcePath: String,
      targetPath: String,
      checkpointPath: String,
      appId: String
  ): Vector[CustomerActivityProgress] = {
    val source = spark.readStream.format("delta").load(sourcePath)
    val snapshots = CustomerActivityTransformer.transform(source, WatermarkDelay, Inactivity)
    val sink = new CustomerActivityDeltaSink(targetPath, appId)
    val query = snapshots.writeStream
      .queryName("customer-activity-state-test")
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.AvailableNow())
      .foreachBatch((batch: Dataset[CustomerActivitySnapshot], batchId: Long) =>
        sink.writeBatch(batch, batchId)
      )
      .start()

    query.awaitTermination()
    query.recentProgress.toVector.map(CustomerActivityProgress.from)
  }

  private def appendSource(path: String, records: Seq[ValidatedTransactionRecord]): Unit = {
    val session = spark
    import session.implicits._
    ValidatedTransactionDeltaSchema
      .project(records.toDS())
      .write
      .format("delta")
      .mode("append")
      .save(path)
  }

  private def readSnapshots(path: String) =
    spark.read
      .format("delta")
      .load(path)
      .collect()
      .map(row => (row.getAs[String]("customer_id"), row.getAs[String]("lifecycle_type")) -> row)
      .toMap

  private def latestWatermark(progress: Seq[CustomerActivityProgress]): Instant =
    progress.flatMap(_.watermark).map(Instant.parse).maxBy(_.toEpochMilli)

  private def rowCount(path: String): Long = spark.read.format("delta").load(path).count()

  private def record(
      identity: Int,
      customerId: String,
      eventTimeText: String,
      amount: String,
      kafkaOffset: Long
  ): ValidatedTransactionRecord = {
    val eventTime = Instant.parse(eventTimeText)
    val eventId = UUID
      .nameUUIDFromBytes(s"state-event-$identity".getBytes(StandardCharsets.UTF_8))
      .toString
    ValidatedTransactionRecord(
      eventId = eventId,
      transactionId = s"transaction-$identity",
      customerId = customerId,
      merchantId = s"merchant-$identity",
      eventTime = Timestamp.from(eventTime),
      ingestionTime = Timestamp.from(eventTime.plusSeconds(2L)),
      amount = new java.math.BigDecimal(amount),
      currency = "USD",
      country = "US",
      deviceId = s"device-$identity",
      ipAddress = "192.0.2.1",
      transactionType = "CARD_PAYMENT",
      schemaVersion = 1,
      kafkaKey = customerId,
      kafkaTopic = "transactions.raw",
      kafkaPartition = (kafkaOffset % 3L).toInt,
      kafkaOffset = kafkaOffset,
      kafkaTimestamp = Timestamp.from(eventTime.plusSeconds(3L))
    )
  }

  private def decimal(value: String): java.math.BigDecimal =
    new java.math.BigDecimal(value).setScale(CustomerActivityProcessor.AmountScale)

  private def timestamp(value: String): Timestamp = Timestamp.from(Instant.parse(value))
}
