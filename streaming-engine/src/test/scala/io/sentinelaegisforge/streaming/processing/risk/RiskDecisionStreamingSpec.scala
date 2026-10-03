package io.sentinelaegisforge.streaming.processing.risk

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.kafka.ingestion.TransactionSparkSession
import io.sentinelaegisforge.streaming.persistence.delta.{
  TransactionRiskDecisionDeltaSchema,
  TransactionRiskDecisionDeltaSink,
  TransactionStatisticalFeatureDeltaSchema
}
import io.sentinelaegisforge.streaming.processing.rollingfeatures.TransactionCustomerFeatures
import io.sentinelaegisforge.streaming.processing.statisticalfeatures.TransactionStatisticalFeatures

final class RiskDecisionStreamingSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private val policy = RiskPolicy("phase11-test-v1", 3L, 2.0, 3L, 1.0)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = TransactionSparkSession
      .builder("local[2]")
      .appName("risk-decision-test")
      .config("spark.ui.enabled", "false")
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

  test("stateless Delta query retains all source evidence and resumes without duplicate output") {
    val root = Files.createTempDirectory("sentinel-risk-decisions")
    val source = root.resolve("transaction_statistical_features_v2").toString
    val target = root.resolve("transaction_risk_decisions").toString
    val checkpoint = root.resolve("risk-checkpoint").toString
    val appId = "sentinel-test-risk-decisions-v1"
    append(source, Seq(input(0L, 0L, None, "USD"), input(1L, 3L, Some(2.0), "EUR")))
    val progress = runQuery(source, target, checkpoint, appId)
    assert(progress.nonEmpty && progress.forall(_.stateOperators.isEmpty))
    assert(!spark.conf.get("spark.sql.streaming.stateStore.providerClass").contains("RocksDB"))
    val output = spark.read.format("delta").load(target)
    TransactionRiskDecisionDeltaSchema.check(output)
    assert(output.count() == 2L)
    assert(output.schema("matched_rule_ids").dataType.isInstanceOf[ArrayType])
    assert(output.schema("reason_codes").dataType.asInstanceOf[ArrayType].elementType == StringType)
    val clear = output.filter(col("kafka_offset") === 0L).head()
    assert(clear.getAs[String]("risk_disposition") == "CLEAR")
    assert(clear.getSeq[String](clear.fieldIndex("matched_rule_ids")).isEmpty)
    assert(clear.getSeq[String](clear.fieldIndex("reason_codes")).isEmpty)
    assert(clear.getAs[Int]("matched_rule_count") == 0)
    val review = output.filter(col("kafka_offset") === 1L).head()
    assert(review.getAs[String]("risk_disposition") == "REVIEW")
    assert(
      review.getSeq[String](review.fieldIndex("matched_rule_ids")) == RiskRules.ordered.map(_.id)
    )
    assert(
      review.getSeq[String](review.fieldIndex("reason_codes")) == RiskRules.ordered.map(
        _.reasonCode
      )
    )
    output.collect().foreach { row =>
      assert(row.getAs[String]("policy_version") == policy.policyVersion)
      assert(row.getAs[String]("policy_fingerprint") == policy.policyFingerprint)
    }
    val names = TransactionStatisticalFeatureDeltaSchema.columns.map(_._1)
    assert(
      output.select(names.map(col): _*).orderBy("kafka_offset").collect().toVector ==
        spark.read.format("delta").load(source).orderBy("kafka_offset").collect().toVector
    )
    assert(
      review
        .getAs[java.math.BigDecimal]("prior_amount_sum_10m")
        .toPlainString == "100000000000000000000.1234"
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
    val commits = DeltaTable.forPath(spark, target).history().count()
    runQuery(source, target, checkpoint, appId)
    assert(spark.read.format("delta").load(target).count() == 2L)
    assert(DeltaTable.forPath(spark, target).history().count() == commits)
    append(source, Seq(input(2L, 1L, Some(-10.0), "USD")))
    runQuery(source, target, checkpoint, appId)
    assert(spark.read.format("delta").load(target).count() == 3L)
  }

  test("dedicated risk Delta sink protects retry of the identical transactional batch") {
    val target = Files.createTempDirectory("sentinel-risk-retry").resolve("decisions").toString
    val output =
      RiskDecisionTransformer.transform(storage(Seq(input(0L, 3L, Some(2.0), "USD"))), policy)
    val sink = new TransactionRiskDecisionDeltaSink(target, "sentinel-test-risk-retry-v1")
    assert(
      sink.plan(0L).options == Map("txnAppId" -> "sentinel-test-risk-retry-v1", "txnVersion" -> "0")
    )
    sink.writeBatch(output, 0L)
    sink.writeBatch(output, 0L)
    assert(spark.read.format("delta").load(target).count() == 1L)
    assert(DeltaTable.forPath(spark, target).history().count() == 1L)
  }

  test("inconsistent statistical context fails instead of silently producing CLEAR or REVIEW") {
    val bad = storage(Seq(input(0L, 3L, Some(2.0), "USD")))
      .withColumn("statistical_feature_status", lit("ZERO_VARIANCE"))
    val error = intercept[Exception](RiskDecisionTransformer.transform(bad, policy).collect())
    assert(causeMessages(error).contains("Invalid risk features"))
    assert(causeMessages(error).contains("InconsistentStatisticalContext"))
  }

  test("missing required decision evidence and incorrect input schema fail explicitly") {
    val original = storage(Seq(input(0L, 0L, None, "USD")))
    val missing = original.withColumn("prior_transaction_count_5m", lit(null).cast("long"))
    val error = intercept[Exception](RiskDecisionTransformer.transform(missing, policy).collect())
    assert(causeMessages(error).contains("MissingRequiredFeature(prior_transaction_count_5m)"))
    intercept[IllegalArgumentException](
      RiskDecisionTransformer.transform(original.drop("currency"), policy)
    )
  }

  private def causeMessages(error: Throwable): String =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString("\n")

  private def storage(inputs: Seq[TransactionStatisticalFeatures]): DataFrame = {
    val session = spark
    import session.implicits._
    TransactionStatisticalFeatureDeltaSchema.project(inputs.toDS())
  }
  private def append(path: String, rows: Seq[TransactionStatisticalFeatures]): Unit =
    storage(rows).write.format("delta").mode("append").save(path)

  private def runQuery(source: String, target: String, checkpoint: String, appId: String) = {
    val decisions =
      RiskDecisionTransformer.transform(spark.readStream.format("delta").load(source), policy)
    val sink = new TransactionRiskDecisionDeltaSink(target, appId)
    val query = decisions.writeStream
      .option("checkpointLocation", checkpoint)
      .trigger(Trigger.AvailableNow())
      .foreachBatch((batch: DataFrame, batchId: Long) => sink.writeBatch(batch, batchId))
      .start()
    query.awaitTermination()
    query.recentProgress.toVector
  }

  private def input(
      offset: Long,
      velocity: Long,
      z: Option[Double],
      currency: String
  ): TransactionStatisticalFeatures = {
    val time = Timestamp.from(Instant.parse("2030-01-01T12:00:00Z").plusSeconds(offset * 60L))
    val base = TransactionCustomerFeatures(
      s"event-$offset",
      s"transaction-$offset",
      "customer-a",
      "merchant-a",
      time,
      time,
      new java.math.BigDecimal("100.0000"),
      currency,
      "US",
      "device-a",
      "192.0.2.1",
      "CARD_PAYMENT",
      1,
      "customer-a",
      "transactions.raw",
      0,
      offset,
      time,
      velocity,
      "100000000000000000000.1234"
    )
    if (z.isDefined) TransactionStatisticalFeatures(base, 2L, Some(90.0), Some(10.0), z, "READY")
    else TransactionStatisticalFeatures(base, 0L, None, None, None, "NO_HISTORY")
  }
}
