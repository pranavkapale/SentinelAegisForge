package io.sentinelaegisforge.streaming.processing.statisticalfeatures

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant

import io.delta.tables.DeltaTable
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionSparkSession
import io.sentinelaegisforge.streaming.persistence.delta.{
  TransactionCustomerFeatureDeltaSchema,
  TransactionStatisticalFeatureDeltaSchema,
  TransactionStatisticalFeatureDeltaSink
}
import io.sentinelaegisforge.streaming.processing.rollingfeatures.TransactionCustomerFeatures

final class StatisticalFeatureStreamingSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = TransactionSparkSession
      .builder("local[2]")
      .appName("customer-statistical-feature-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config(
        StatisticalFeatureConfig.StateStoreProviderConfig,
        StatisticalFeatureConfig.RequiredStateStoreProvider
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

  test("configuration has a separate source, target, checkpoint and transaction identity") {
    val config =
      TransactionStatisticalFeatureApp.parseArguments(Array.empty).fold(fail(_), identity)
    assert(config.sourceDeltaPath == ".local/delta/transaction_customer_features_v2")
    assert(config.targetDeltaPath == ".local/delta/transaction_statistical_features_v2")
    assert(config.checkpointLocation == ".local/checkpoints/customer-statistical-features-v2")
    assert(config.deltaTxnAppId == "sentinel-transaction-statistical-features-v2")
    assert(config.inactivityTimeout.toHours == 24L)
    assert(
      StatisticalFeatureConfig.RequiredStateStoreProvider.endsWith("RocksDBStateStoreProvider")
    )
  }

  test("Delta stream restores prior Welford state across runs and keeps customers independent") {
    val root = Files.createTempDirectory("sentinel-statistical-features")
    val source = root.resolve("transaction_customer_features").toString
    val target = root.resolve("transaction_statistical_features").toString
    val checkpoint = root.resolve("checkpoint").toString
    val appId = "sentinel-test-statistical-features-v2"

    appendSource(
      source,
      Seq(
        input("a", 0L, "12:00", "100.0000"),
        input("a", 1L, "12:01", "200.0000"),
        input("b", 0L, "12:01", "7.0000")
      )
    )
    val firstProgress = runQuery(source, target, checkpoint, appId)
    assert(firstProgress.exists(_.stateOperators.exists(_.numRowsTotal > 0L)))
    assert(row(target, "a", 0L).getAs[Long]("prior_amount_observation_count") == 0L)
    assert(row(target, "a", 1L).getAs[Long]("prior_amount_observation_count") == 1L)
    assert(row(target, "b", 0L).getAs[Long]("prior_amount_observation_count") == 0L)

    appendSource(source, Seq(input("a", 2L, "12:02", "300.0000")))
    val secondProgress = runQuery(source, target, checkpoint, appId)
    assert(secondProgress.exists(_.stateOperators.exists(_.numRowsTotal > 0L)))
    val restored = row(target, "a", 2L)
    assert(restored.getAs[Long]("prior_amount_observation_count") == 2L)
    assertClose(restored.getAs[Double]("prior_amount_mean"), 150.0)
    assertClose(restored.getAs[Double]("prior_amount_stddev"), math.sqrt(5000.0))
    assertClose(restored.getAs[Double]("amount_zscore"), 150.0 / math.sqrt(5000.0))
    assert(restored.getAs[String]("statistical_feature_status") == "READY")
    assert(restored.getAs[java.math.BigDecimal]("amount").toPlainString == "300.0000")
    assert(restored.getAs[Long]("prior_transaction_count_5m") == 0L)
    assert(restored.getAs[java.math.BigDecimal]("prior_amount_sum_10m").toPlainString == "0.0000")
    assert(spark.read.format("delta").load(target).count() == 4L)
    assert(spark.read.format("delta").load(source).count() == 4L)
    assert(
      spark.read
        .format("delta")
        .load(target)
        .schema
        .fields
        .toVector
        .map(f => f.name -> f.dataType) ==
        TransactionStatisticalFeatureDeltaSchema.columns
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

  test("v2 checkpoint restores independent customer-currency baselines across runs") {
    val root = Files.createTempDirectory("sentinel-statistical-currency-v2")
    val source = root.resolve("rolling-v2").toString
    val target = root.resolve("statistics-v2").toString
    val checkpoint = root.resolve("statistics-v2-checkpoint").toString
    val appId = "sentinel-test-statistical-currency-v2"
    appendSource(
      source,
      Seq(
        input("a", 0L, "12:00", "100"),
        input("a", 1L, "12:01", "1000", "EUR"),
        input("a", 2L, "12:02", "200")
      )
    )
    runQuery(source, target, checkpoint, appId)
    val firstEur = row(target, "a", 1L)
    assert(firstEur.getAs[Long]("prior_amount_observation_count") == 0L)
    assert(firstEur.getAs[String]("statistical_feature_status") == "NO_HISTORY")
    val secondUsd = row(target, "a", 2L)
    assert(secondUsd.getAs[Long]("prior_amount_observation_count") == 1L)
    assertClose(secondUsd.getAs[Double]("prior_amount_mean"), 100.0)

    appendSource(
      source,
      Seq(
        input("a", 3L, "12:03", "300"),
        input("a", 4L, "12:04", "1200", "EUR")
      )
    )
    val recoveredProgress = runQuery(source, target, checkpoint, appId)
    assert(recoveredProgress.exists(_.stateOperators.exists(_.numRowsTotal == 2L)))
    val usd = row(target, "a", 3L)
    assert(usd.getAs[String]("currency") == "USD")
    assert(usd.getAs[Long]("prior_amount_observation_count") == 2L)
    assertClose(usd.getAs[Double]("prior_amount_mean"), 150.0)
    assertClose(usd.getAs[Double]("prior_amount_stddev"), math.sqrt(5000.0))
    assertClose(usd.getAs[Double]("amount_zscore"), 150.0 / math.sqrt(5000.0))
    assert(usd.getAs[String]("statistical_feature_status") == "READY")
    val eur = row(target, "a", 4L)
    assert(eur.getAs[String]("currency") == "EUR")
    assert(eur.getAs[Long]("prior_amount_observation_count") == 1L)
    assertClose(eur.getAs[Double]("prior_amount_mean"), 1000.0)
    assert(eur.isNullAt(eur.fieldIndex("prior_amount_stddev")))
    assert(eur.isNullAt(eur.fieldIndex("amount_zscore")))
    assert(eur.getAs[String]("statistical_feature_status") == "INSUFFICIENT_VARIANCE_HISTORY")
    assert(spark.read.format("delta").load(target).count() == 5L)
    assert(spark.read.format("delta").load(source).count() == 5L)
    assert(row(target, "a", 1L) == firstEur)
    assert(
      spark.read
        .format("delta")
        .load(target)
        .schema
        .fields
        .toVector
        .map(f => f.name -> f.dataType) ==
        TransactionStatisticalFeatureDeltaSchema.columns
    )
  }

  test("dedicated Delta transaction identity suppresses a repeated output batch") {
    val root = Files.createTempDirectory("sentinel-statistical-retry")
    val source = root.resolve("source").toString
    val target = root.resolve("target").toString
    val checkpoint = root.resolve("checkpoint").toString
    val appId = "sentinel-test-statistical-retry-v1"
    val original = input("a", 0L, "12:00", "100.0000")
    appendSource(source, Seq(original))
    runQuery(source, target, checkpoint, appId)
    assert(spark.read.format("delta").load(target).count() == 1L)

    val session = spark
    import session.implicits._
    val sink = new TransactionStatisticalFeatureDeltaSink(target, appId)
    assert(sink.plan(0L).options == Map("txnAppId" -> appId, "txnVersion" -> "0"))
    sink.writeBatch(
      Seq(TransactionStatisticalFeatures(original, 0L, None, None, None, "NO_HISTORY")).toDS(),
      0L
    )
    assert(spark.read.format("delta").load(target).count() == 1L)
    assert(DeltaTable.forPath(spark, target).history().count() == 1L)
  }

  private def runQuery(
      source: String,
      target: String,
      checkpoint: String,
      appId: String
  ) = {
    val input = spark.readStream.format("delta").load(source)
    val features = StatisticalFeatureTransformer.transform(
      input,
      "10 minutes",
      java.time.Duration.ofHours(24L)
    )
    val sink = new TransactionStatisticalFeatureDeltaSink(target, appId)
    val query = features.writeStream
      .queryName("statistical-feature-test")
      .option("checkpointLocation", checkpoint)
      .trigger(Trigger.AvailableNow())
      .foreachBatch((batch: Dataset[TransactionStatisticalFeatures], batchId: Long) =>
        sink.writeBatch(batch, batchId)
      )
      .start()
    query.awaitTermination()
    query.recentProgress.toVector
  }

  private def appendSource(path: String, records: Seq[TransactionCustomerFeatures]): Unit = {
    val session = spark
    import session.implicits._
    TransactionCustomerFeatureDeltaSchema
      .project(records.toDS())
      .write
      .format("delta")
      .mode("append")
      .save(path)
  }

  private def row(path: String, customer: String, offset: Long): Row =
    spark.read
      .format("delta")
      .load(path)
      .where(s"customer_id = '$customer' AND kafka_offset = $offset")
      .head()

  private def input(
      customer: String,
      offset: Long,
      hhmm: String,
      amount: String,
      currency: String = "USD"
  ): TransactionCustomerFeatures = {
    val time = Timestamp.from(Instant.parse(s"2030-01-01T${hhmm}:00Z"))
    TransactionCustomerFeatures(
      s"event-$customer-$offset",
      s"transaction-$customer-$offset",
      customer,
      "merchant",
      time,
      time,
      new java.math.BigDecimal(amount),
      currency,
      "US",
      "device",
      "192.0.2.1",
      "CARD_PAYMENT",
      1,
      customer,
      "transactions.raw",
      0,
      offset,
      time,
      0L,
      "0.0000"
    )
  }

  private def assertClose(actual: Double, expected: Double): Unit =
    assert(math.abs(actual - expected) < 1e-8)
}
