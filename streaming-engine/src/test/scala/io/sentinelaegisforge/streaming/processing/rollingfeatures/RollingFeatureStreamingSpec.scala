package io.sentinelaegisforge.streaming.processing.rollingfeatures

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import io.delta.tables.DeltaTable
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.kafka.ingestion.{
  TransactionSparkSession,
  ValidatedTransactionRecord
}
import io.sentinelaegisforge.streaming.persistence.delta.{
  TransactionCustomerFeatureDeltaSchema,
  TransactionCustomerFeatureDeltaSink,
  ValidatedTransactionDeltaSchema
}

final class RollingFeatureStreamingSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = TransactionSparkSession
      .builder("local[2]")
      .appName("rolling-customer-feature-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config(
        RollingFeatureConfig.StateStoreProviderConfig,
        RollingFeatureConfig.RequiredStateStoreProvider
      )
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally super.afterAll()
  }

  test("configuration keeps a separate state and Delta lineage") {
    val config = TransactionRollingFeatureApp.parseArguments(Array.empty).fold(fail(_), identity)
    assert(config.deduplicatedDeltaPath == RollingFeatureConfig.DefaultDeduplicatedDeltaPath)
    assert(config.featureDeltaPath == ".local/delta/transaction_customer_features")
    assert(config.checkpointLocation == ".local/checkpoints/customer-rolling-features")
    assert(config.deltaTxnAppId == "sentinel-transaction-customer-features-v1")
    assert(config.watermarkDelay == "10 minutes")
    assert(RollingFeatureConfig.RequiredStateStoreProvider.endsWith("RocksDBStateStoreProvider"))
  }

  test("Delta stream restores rolling history, isolates keys, and leaves prior outputs immutable") {
    val root = Files.createTempDirectory("sentinel-rolling-features")
    val source = root.resolve("transactions_deduplicated").toString
    val target = root.resolve("transaction_customer_features").toString
    val checkpoint = root.resolve("checkpoint").toString
    val appId = "sentinel-test-rolling-features-v1"

    appendSource(
      source,
      Seq(
        record("a0", "a", "2030-01-01T12:00:00Z", "100.0000"),
        record("a1", "a", "2030-01-01T12:01:00Z", "200.0000"),
        record("b0", "b", "2030-01-01T12:01:00Z", "7.0000")
      )
    )
    val firstProgress = runQuery(source, target, checkpoint, appId)
    assert(firstProgress.exists(_.stateOperators.exists(_.numRowsTotal > 0L)))
    assertFeature(row(target, "a0"), 0L, "0.0000")
    assertFeature(row(target, "a1"), 1L, "100.0000")
    assertFeature(row(target, "b0"), 0L, "0.0000")

    appendSource(source, Seq(record("a4", "a", "2030-01-01T12:04:00Z", "300.0000")))
    runQuery(source, target, checkpoint, appId)
    assertFeature(row(target, "a4"), 2L, "300.0000")

    appendSource(source, Seq(record("a8", "a", "2030-01-01T12:08:00Z", "40.0000")))
    runQuery(source, target, checkpoint, appId)
    assertFeature(row(target, "a8"), 1L, "600.0000")

    appendSource(source, Seq(record("a5", "a", "2030-01-01T12:05:00Z", "5.0000")))
    runQuery(source, target, checkpoint, appId)
    assertFeature(row(target, "a5"), 3L, "600.0000")
    assertFeature(row(target, "a8"), 1L, "600.0000")
    assert(spark.read.format("delta").load(target).count() == 6L)

    appendSource(source, Seq(record("c30", "c", "2030-01-01T12:30:00Z", "1.0000")))
    val cleanupProgress = runQuery(source, target, checkpoint, appId)
    assert(
      cleanupProgress
        .flatMap(_.watermark)
        .exists(value => !Instant.parse(value).isBefore(Instant.parse("2030-01-01T12:20:00Z")))
    )
    assert(cleanupProgress.exists(_.stateOperators.exists(_.numRowsRemoved > 0L)))
    assert(cleanupProgress.exists(_.stateOperators.exists(_.memoryUsedBytes > 0L)))
    assertFeature(row(target, "c30"), 0L, "0.0000")
  }

  test("feature Delta sink uses its own transaction ID and decimal(38,4) output") {
    val root = Files.createTempDirectory("sentinel-feature-retry")
    val source = root.resolve("source").toString
    val target = root.resolve("target").toString
    val checkpoint = root.resolve("checkpoint").toString
    val appId = "sentinel-test-feature-retry-v1"
    appendSource(source, Seq(record("retry", "a", "2030-01-01T12:00:00Z", "10.0000")))
    runQuery(source, target, checkpoint, appId)
    assert(spark.read.format("delta").load(target).count() == 1L)

    val session = spark
    import session.implicits._
    val output = TransactionCustomerFeatures.from(
      RollingFeatureInput(
        "retry",
        "transaction-retry",
        "a",
        "merchant",
        timestamp("2030-01-01T12:00:00Z"),
        timestamp("2030-01-01T12:00:00Z"),
        new java.math.BigDecimal("10.0000"),
        "USD",
        "US",
        "device",
        "192.0.2.1",
        "CARD_PAYMENT",
        1,
        "a",
        "transactions.raw",
        0,
        1L,
        timestamp("2030-01-01T12:00:00Z")
      ),
      0L,
      new java.math.BigDecimal("0.0000")
    )
    val sink = new TransactionCustomerFeatureDeltaSink(target, appId)
    assert(sink.plan(0L).options == Map("txnAppId" -> appId, "txnVersion" -> "0"))
    sink.writeBatch(Seq(output).toDS(), 0L)
    assert(spark.read.format("delta").load(target).count() == 1L)
    assert(DeltaTable.forPath(spark, target).history().count() == 1L)
    val table = spark.read.format("delta").load(target)
    assert(
      table.schema.fields.toVector.map(f => f.name -> f.dataType) ==
        TransactionCustomerFeatureDeltaSchema.columns
    )
    assert(
      DeltaTable
        .forPath(spark, target)
        .detail()
        .select("partitionColumns")
        .head()
        .getSeq[String](0)
        .isEmpty
    )
  }

  private def runQuery(
      source: String,
      target: String,
      checkpoint: String,
      appId: String
  ): Vector[RollingFeatureProgress] = {
    val input = spark.readStream.format("delta").load(source)
    val features = RollingFeatureTransformer.transform(input, "10 minutes")
    val sink = new TransactionCustomerFeatureDeltaSink(target, appId)
    val query = features.writeStream
      .queryName("rolling-feature-test")
      .option("checkpointLocation", checkpoint)
      .trigger(Trigger.AvailableNow())
      .foreachBatch((batch: Dataset[TransactionCustomerFeatures], batchId: Long) =>
        sink.writeBatch(batch, batchId)
      )
      .start()
    query.awaitTermination()
    query.recentProgress.toVector.map(RollingFeatureProgress.from)
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

  private def row(path: String, eventId: String): Row =
    spark.read.format("delta").load(path).where(s"event_id = '${uuid(eventId)}'").head()

  private def assertFeature(row: Row, count: Long, sum: String): Unit = {
    assert(row.getAs[Long]("prior_transaction_count_5m") == count)
    assert(
      row
        .getAs[java.math.BigDecimal]("prior_amount_sum_10m")
        .compareTo(
          new java.math.BigDecimal(sum)
        ) == 0
    )
  }

  private def record(
      id: String,
      customer: String,
      time: String,
      amount: String
  ): ValidatedTransactionRecord = {
    val eventTime = timestamp(time)
    ValidatedTransactionRecord(
      uuid(id),
      s"transaction-$id",
      customer,
      "merchant",
      eventTime,
      Timestamp.from(eventTime.toInstant.plusSeconds(1L)),
      new java.math.BigDecimal(amount),
      "USD",
      "US",
      "device",
      "192.0.2.1",
      "CARD_PAYMENT",
      1,
      customer,
      "transactions.raw",
      0,
      id.hashCode.toLong.abs,
      Timestamp.from(eventTime.toInstant.plusSeconds(2L))
    )
  }

  private def timestamp(value: String): Timestamp = Timestamp.from(Instant.parse(value))

  private def uuid(value: String): String =
    UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8)).toString
}
