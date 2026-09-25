package io.sentinelaegisforge.streaming.persistence.delta

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType, TimestampType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.domain.transaction.TransactionEventValidator
import io.sentinelaegisforge.streaming.kafka.ingestion.{
  KafkaSourceRecord,
  TransactionSparkSession,
  ValidatedTransactionRecord
}
import io.sentinelaegisforge.streaming.simulation.{
  DeterministicTransactionGenerator,
  TransactionGeneratorConfig
}

final class DeltaTransactionSinkSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val warehouse = Files.createTempDirectory("sentinel-delta-test-warehouse")
    spark = TransactionSparkSession
      .builder("local[2]")
      .appName("delta-transaction-sink-test")
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

  test("validated records project to the explicit durable storage schema") {
    val session = spark
    import session.implicits._

    val projected = ValidatedTransactionDeltaSchema.project(Seq(record(0L)).toDS())

    assert(
      projected.schema.fields.toVector.map(field => field.name -> field.dataType) == Vector(
        "event_id" -> StringType,
        "transaction_id" -> StringType,
        "customer_id" -> StringType,
        "merchant_id" -> StringType,
        "event_time" -> TimestampType,
        "ingestion_time" -> TimestampType,
        "amount" -> DecimalType(18, 4),
        "currency" -> StringType,
        "country" -> StringType,
        "device_id" -> StringType,
        "ip_address" -> StringType,
        "transaction_type" -> StringType,
        "schema_version" -> IntegerType,
        "kafka_key" -> StringType,
        "kafka_topic" -> StringType,
        "kafka_partition" -> IntegerType,
        "kafka_offset" -> LongType,
        "kafka_timestamp" -> TimestampType
      )
    )
  }

  test("a batch write plan uses the stable application ID and batch ID as Delta transaction IDs") {
    val path = Files.createTempDirectory("sentinel-delta-plan").resolve("table").toString
    val sink = new DeltaTransactionSink(path, "sentinel-test-ingestion-v1")

    val plan = sink.plan(17L)

    assert(plan.deltaPath == path)
    assert(plan.txnAppId == "sentinel-test-ingestion-v1")
    assert(plan.txnVersion == 17L)
    assert(plan.options == Map("txnAppId" -> "sentinel-test-ingestion-v1", "txnVersion" -> "17"))
  }

  test("retrying after a post-commit failure does not append the same micro-batch twice") {
    val session = spark
    import session.implicits._

    val path = Files.createTempDirectory("sentinel-delta-idempotency").resolve("table").toString
    val applicationId = "sentinel-test-recovery-v1"
    val firstBatch = Seq(record(0L)).toDS()
    val failingSink = new DeltaTransactionSink(
      path,
      applicationId,
      DeltaPostCommitHook.FailAfterBatch(0L)
    )

    intercept[IntentionalDeltaPostCommitFailure] {
      failingSink.writeBatch(firstBatch, 0L)
    }
    assert(spark.read.format("delta").load(path).count() == 1L)
    val deltaTable = DeltaTable.forPath(spark, path)
    assert(deltaTable.history().count() == 1L)
    assert(deltaTable.detail().select("partitionColumns").head().getSeq[String](0).isEmpty)

    val retryingSink = new DeltaTransactionSink(path, applicationId)
    retryingSink.writeBatch(firstBatch, 0L)
    assert(spark.read.format("delta").load(path).count() == 1L)
    assert(DeltaTable.forPath(spark, path).history().count() == 1L)

    retryingSink.writeBatch(Seq(record(1L)).toDS(), 1L)
    assert(spark.read.format("delta").load(path).count() == 2L)
    assert(DeltaTable.forPath(spark, path).history().count() == 2L)
  }

  private def record(offset: Long): ValidatedTransactionRecord = {
    val event = new DeterministicTransactionGenerator(
      TransactionGeneratorConfig(
        seed = 8000L + offset,
        baseEventTime = Instant.parse("2030-01-01T00:00:00Z")
      )
    ).generateValidCandidates(1)
      .headOption
      .toRight("Expected one generated candidate")
      .flatMap(TransactionEventValidator.validate(_).left.map(_.mkString(", ")))
      .fold(message => fail(message), identity)
    val source = KafkaSourceRecord(
      key = event.customerId.getBytes(StandardCharsets.UTF_8),
      value = Array.emptyByteArray,
      topic = "transactions.raw",
      partition = (offset % 3L).toInt,
      offset = offset,
      timestamp = Timestamp.from(Instant.parse("2030-01-01T00:00:10Z").plusSeconds(offset))
    )

    ValidatedTransactionRecord.from(event, event.customerId, source)
  }
}
