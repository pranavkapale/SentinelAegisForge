package io.sentinelaegisforge.streaming.kafka.ingestion

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import io.sentinelaegisforge.streaming.domain.transaction.{
  TransactionEvent,
  TransactionEventValidator
}
import io.sentinelaegisforge.streaming.simulation.{
  DeterministicTransactionGenerator,
  TransactionGeneratorConfig
}

final class TransactionIngestionTransformerSpec extends AnyFunSuite {
  test("a local Spark transformation validates records and preserves Kafka metadata") {
    val fixture = new RegistryFramedAvroFixture()
    val warehouse = Files.createTempDirectory("sentinel-spark-warehouse")
    val spark = SparkSession
      .builder()
      .appName("transaction-ingestion-transformer-test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouse.toUri.toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      import spark.implicits._

      val events = generatedEvents(2)
      val input = events.zipWithIndex.map { case (event, index) =>
        KafkaSourceRecord(
          key = event.customerId.getBytes(StandardCharsets.UTF_8),
          value = fixture.serialize(event),
          topic = TransactionIngestionConfig.DefaultTopic,
          partition = index,
          offset = 100L + index,
          timestamp = Timestamp.from(Instant.parse("2030-01-01T00:00:10Z").plusSeconds(index))
        )
      }

      val output = TransactionIngestionTransformer
        .decode(input.toDS().repartition(1), fixture.registryUrl)
        .collect()
        .sortBy(_.kafkaOffset)

      assert(output.map(_.customerId).toVector == events.map(_.customerId))
      assert(output.map(_.kafkaKey).toVector == events.map(_.customerId))
      assert(output.map(_.kafkaTopic).toSet == Set(TransactionIngestionConfig.DefaultTopic))
      assert(output.map(_.kafkaPartition).toVector == Vector(0, 1))
      assert(output.map(_.kafkaOffset).toVector == Vector(100L, 101L))
      assert(output.map(_.kafkaTimestamp).toVector == input.map(_.timestamp))
      assert(output.forall(record => record.eventId != record.kafkaOffset.toString))
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      fixture.close()
    }
  }

  private def generatedEvents(count: Int): Vector[TransactionEvent] =
    new DeterministicTransactionGenerator(
      TransactionGeneratorConfig(91L, Instant.parse("2030-01-01T00:00:00Z"))
    ).generateValidCandidates(count).map { candidate =>
      TransactionEventValidator
        .validate(candidate)
        .fold(errors => fail(s"Generated candidate failed validation: $errors"), identity)
    }
}
